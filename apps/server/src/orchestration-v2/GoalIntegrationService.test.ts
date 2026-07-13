import { describe, expect, it } from "@effect/vitest";
import {
  GoalArtifactId,
  GoalAttemptId,
  GoalEvidenceId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ThreadId,
  type GoalDetail,
} from "@t3tools/contracts";

import { hasIndependentAcceptedEvidence } from "./GoalIntegrationService.ts";

describe("goal completion evidence gate", () => {
  const goalId = GoalId.make("goal:evidence");
  const producerId = GoalAttemptId.make("attempt:producer");
  const verifierId = GoalAttemptId.make("attempt:verifier");
  const graphVersionId = GoalGraphVersionId.make("graph:evidence");
  const attempt = (input: { id: typeof producerId; nodeId: GoalNodeId; threadId: ThreadId }) =>
    ({
      id: input.id,
      goalId,
      graphVersionId,
      nodeId: input.nodeId,
      executionThreadId: input.threadId,
    }) as GoalDetail["attempts"][number];
  const detail = {
    goal: { integrationSha: "sha:current", currentGraphVersionId: graphVersionId },
    attempts: [
      attempt({
        id: producerId,
        nodeId: GoalNodeId.make("node:producer"),
        threadId: ThreadId.make("thread:producer"),
      }),
      attempt({
        id: verifierId,
        nodeId: GoalNodeId.make("node:verifier"),
        threadId: ThreadId.make("thread:verifier"),
      }),
    ],
    evidence: [
      {
        id: GoalEvidenceId.make("evidence:accepted"),
        attemptId: verifierId,
        producerAttemptId: producerId,
        integrationSha: "sha:current",
        commands: [
          {
            command: "vp test",
            exitCode: 0,
            logArtifactId: GoalArtifactId.make("artifact:verification-log"),
          },
        ],
        artifacts: [GoalArtifactId.make("artifact:verification-log")],
        verdict: "accepted",
      },
    ],
  } as unknown as GoalDetail;

  it("accepts only evidence from a distinct attempt, node, and thread for the current SHA", () => {
    expect(hasIndependentAcceptedEvidence(detail)).toBe(true);
    expect(
      hasIndependentAcceptedEvidence({
        ...detail,
        evidence: [{ ...detail.evidence[0]!, commands: [] }],
      }),
    ).toBe(false);
    expect(
      hasIndependentAcceptedEvidence({
        ...detail,
        evidence: [
          {
            ...detail.evidence[0]!,
            commands: [{ command: "vp test", exitCode: 0, logArtifactId: null }],
          },
        ],
      }),
    ).toBe(false);
    expect(
      hasIndependentAcceptedEvidence({
        ...detail,
        evidence: [{ ...detail.evidence[0]!, integrationSha: "sha:stale" }],
      }),
    ).toBe(false);
    expect(
      hasIndependentAcceptedEvidence({
        ...detail,
        evidence: [{ ...detail.evidence[0]!, producerAttemptId: verifierId }],
      }),
    ).toBe(false);
    expect(
      hasIndependentAcceptedEvidence({
        ...detail,
        goal: {
          ...detail.goal,
          currentGraphVersionId: GoalGraphVersionId.make("graph:evidence:reopened"),
        },
      }),
    ).toBe(false);
  });
});
