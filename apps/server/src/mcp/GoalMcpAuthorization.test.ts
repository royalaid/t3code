import {
  GoalAttemptId,
  type GoalDetail,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { selectGoalNodeForAuthority, validateGoalWorkerBinding } from "./GoalMcpAuthorization.ts";

const authority = {
  kind: "goal_worker" as const,
  goalId: "goal:1",
  rootThreadId: "thread:root",
  executionThreadId: "thread:worker",
  nodeId: "node:1",
  attemptId: "attempt:1",
  nativeOwnerAttemptId: "attempt:1",
};
const scope = {
  threadId: "thread:worker",
  providerSessionId: "session:1",
  providerInstanceId: "codex",
};
const attempt = {
  id: "attempt:1",
  goalId: "goal:1",
  nodeId: "node:1",
  status: "running",
  executionThreadId: "thread:worker",
  providerSessionId: "session:1",
  resolvedRoute: { providerInstanceId: "codex" },
};

describe("validateGoalWorkerBinding", () => {
  it("accepts only the active exact provider session, route, and execution thread", () => {
    expect(validateGoalWorkerBinding({ authority, scope, attempt })).toBeNull();
    expect(
      validateGoalWorkerBinding({
        authority,
        scope: { ...scope, providerSessionId: "session:stale" },
        attempt,
      }),
    ).toMatch(/session/iu);
    expect(
      validateGoalWorkerBinding({
        authority,
        scope,
        attempt: { ...attempt, resolvedRoute: { providerInstanceId: "claude" } },
      }),
    ).toMatch(/provider/iu);
    expect(
      validateGoalWorkerBinding({ authority, scope, attempt: { ...attempt, status: "succeeded" } }),
    ).toMatch(/active/iu);
  });

  it("rejects artifacts and evidence with embedded authority mismatches", () => {
    expect(
      validateGoalWorkerBinding({
        authority,
        scope,
        attempt,
        artifacts: [{ goalId: "goal:other", nodeId: "node:1", attemptId: "attempt:1" }],
      }),
    ).toMatch(/artifact/iu);
    expect(
      validateGoalWorkerBinding({
        authority,
        scope,
        attempt,
        evidence: {
          goalId: "goal:1",
          nodeId: "node:other",
          attemptId: "attempt:1",
          producerAttemptId: "attempt:1",
          artifacts: [],
          commands: [],
        },
        resolvedArtifacts: [],
      }),
    ).toMatch(/evidence/iu);
  });

  it("allows a distinct producer while rejecting referenced artifact spoofing", () => {
    const evidence = {
      goalId: "goal:1",
      nodeId: "node:1",
      attemptId: "attempt:1",
      producerAttemptId: "attempt:other",
      artifacts: ["artifact:owned"],
      commands: [],
    };
    expect(
      validateGoalWorkerBinding({
        authority,
        scope,
        attempt,
        evidence,
        resolvedArtifacts: [
          {
            id: "artifact:owned",
            goalId: "goal:1",
            nodeId: "node:1",
            attemptId: "attempt:1",
          },
        ],
      }),
    ).toBeNull();
    expect(
      validateGoalWorkerBinding({
        authority,
        scope,
        attempt,
        evidence: {
          ...evidence,
          commands: [{ logArtifactId: "artifact:cross-worker" }],
        },
        resolvedArtifacts: [
          {
            id: "artifact:owned",
            goalId: "goal:1",
            nodeId: "node:1",
            attemptId: "attempt:1",
          },
          {
            id: "artifact:cross-worker",
            goalId: "goal:1",
            nodeId: "node:other",
            attemptId: "attempt:other",
          },
        ],
      }),
    ).toMatch(/artifact/iu);
  });
});

describe("selectGoalNodeForAuthority", () => {
  const graphVersionId = GoalGraphVersionId.make("graph:v3");
  const attemptId = GoalAttemptId.make("attempt:3");
  const nodeId = GoalNodeId.make("node:1");
  const detail = {
    goal: { currentGraphVersionId: graphVersionId },
    attempts: [{ id: attemptId, graphVersionId }],
    nodes: [
      { graphVersionId: GoalGraphVersionId.make("graph:v1"), node: { id: nodeId } },
      { graphVersionId, node: { id: nodeId } },
    ],
  } as unknown as GoalDetail;

  it("selects the worker attempt revision instead of an older node with the same ID", () => {
    expect(
      selectGoalNodeForAuthority(
        detail,
        {
          kind: "goal_worker",
          goalId: GoalId.make("goal:3"),
          rootThreadId: ThreadId.make("thread:root:3"),
          executionThreadId: ThreadId.make("thread:worker:3"),
          nodeId,
          attemptId,
          nativeOwnerAttemptId: attemptId,
        },
        nodeId,
      )?.graphVersionId,
    ).toBe(graphVersionId);
  });

  it("selects the active graph revision for the lead", () => {
    expect(
      selectGoalNodeForAuthority(
        detail,
        {
          kind: "goal_lead",
          goalId: GoalId.make("goal:3"),
          rootThreadId: ThreadId.make("thread:root:3"),
        },
        nodeId,
      )?.graphVersionId,
    ).toBe(graphVersionId);
  });
});
