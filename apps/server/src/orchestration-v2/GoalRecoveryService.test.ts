import { describe, expect, it } from "vite-plus/test";
import {
  type GoalAttempt,
  GoalAttemptId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProviderInstanceId,
} from "@t3tools/contracts";

import { planGoalRecovery, shouldRefreshExpiredGoalLease } from "./GoalRecoveryService.ts";

const attempt = (overrides: Partial<GoalAttempt> = {}): GoalAttempt => ({
  id: GoalAttemptId.make("attempt:1"),
  goalId: GoalId.make("goal:1"),
  graphVersionId: GoalGraphVersionId.make("graph:1"),
  nodeId: GoalNodeId.make("node:1"),
  ordinal: 1,
  status: "running" as const,
  requestedRoute: {
    type: "exact" as const,
    providerInstanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  },
  resolvedRoute: {
    requested: {
      type: "exact" as const,
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    providerInstanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
    capabilitySnapshot: [],
    rationale: "chosen once",
  },
  providerSessionId: null,
  executionThreadId: null,
  runId: null,
  rootExecutionNodeId: null,
  baseIntegrationSha: null,
  workspacePath: null,
  leaseOwner: "worker",
  leaseExpiresAt: "2026-07-12T00:02:00.000Z",
  usage: {
    inputTokens: null,
    outputTokens: null,
    cachedTokens: null,
    costMicros: null,
    nativeDescendantCount: 0,
  },
  failureReason: null,
  createdAt: "2026-07-12T00:00:00.000Z",
  updatedAt: "2026-07-12T00:01:00.000Z",
  ...overrides,
});

describe("planGoalRecovery", () => {
  it("refreshes an expired lease only while its durable launch effect is unsettled", () => {
    expect(shouldRefreshExpiredGoalLease("pending")).toBe(true);
    expect(shouldRefreshExpiredGoalLease("running")).toBe(true);
    expect(shouldRefreshExpiredGoalLease("succeeded")).toBe(false);
    expect(shouldRefreshExpiredGoalLease(null)).toBe(false);
  });
  it("marks ten minutes of silence stalled and enforces the sixty-minute ceiling", () => {
    expect(
      planGoalRecovery({
        attempts: [attempt()],
        now: "2026-07-12T00:11:00.000Z",
      }).actions,
    ).toContainEqual(expect.objectContaining({ type: "mark_stalled", attemptId: "attempt:1" }));
    expect(
      planGoalRecovery({
        attempts: [attempt()],
        now: "2026-07-12T01:00:00.000Z",
      }).actions,
    ).toContainEqual(
      expect.objectContaining({ type: "terminate_ceiling", attemptId: "attempt:1" }),
    );
  });

  it("requires reconciliation before an expired lease can be reclaimed", () => {
    const result = planGoalRecovery({
      attempts: [attempt({ status: "leased" })],
      now: "2026-07-12T00:03:00.000Z",
    });
    expect(result.actions).toEqual([
      expect.objectContaining({
        type: "reconcile_required",
        attemptId: "attempt:1",
        resolvedProviderInstanceId: "codex",
        resolvedModel: "gpt-5.4",
      }),
    ]);
  });

  it("allows exactly one retry for retryable infrastructure failure", () => {
    const first = planGoalRecovery({
      attempts: [attempt({ status: "failed", failureReason: "retryable:provider_lost" })],
      now: "2026-07-12T00:02:00.000Z",
    });
    expect(first.actions).toContainEqual(
      expect.objectContaining({ type: "retry", attemptId: "attempt:1" }),
    );
    const second = planGoalRecovery({
      attempts: [
        attempt({ status: "failed", failureReason: "retryable:provider_lost" }),
        attempt({
          id: GoalAttemptId.make("attempt:2"),
          ordinal: 2,
          status: "failed",
          failureReason: "retryable:provider_lost",
        }),
      ],
      now: "2026-07-12T00:02:00.000Z",
    });
    expect(second.actions).toContainEqual(
      expect.objectContaining({ type: "retry_exhausted", attemptId: "attempt:2" }),
    );
  });

  it("pauses launches when native descendants exceed the lifetime backstop", () => {
    const result = planGoalRecovery({
      attempts: [attempt({ usage: { ...attempt().usage, nativeDescendantCount: 1_001 } })],
      now: "2026-07-12T00:02:00.000Z",
    });
    expect(result.pauseNewLaunches).toBe(true);
    expect(result.actions).toContainEqual(
      expect.objectContaining({ type: "native_descendant_overage", observed: 1_002 }),
    );
  });
});
