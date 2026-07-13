import { describe, expect, it } from "vite-plus/test";
import {
  GoalGraphVersionId,
  GoalEdgeId,
  GoalAttemptId,
  GoalId,
  GoalNodeId,
  ProviderInstanceId,
  ThreadId,
  type GoalAttempt,
  type GoalDetail,
  type GoalGraphNode,
} from "@t3tools/contracts";

import { goalWorkerCapacity, planGoalScheduling } from "./GoalScheduler.ts";

const policy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};

const node = (id: string, workspaceMode: "read_only" | "writer" = "read_only") =>
  ({
    id: GoalNodeId.make(id),
    role: id,
    persona: "worker",
    objective: id,
    successCriteria: ["done"],
    contextPacket: {
      schemaVersion: 1,
      digest: null,
      objective: id,
      artifacts: [],
      dependencyOutputs: [],
      notes: [],
    },
    outputContract: {
      kind: workspaceMode === "writer" ? "commit" : "structured_result",
      description: "result",
      requiredFields: [],
    },
    requiredCapabilities: [],
    workspaceMode,
    routingRequest: {
      type: "exact",
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    evidenceRequirements: [],
    policy,
  }) satisfies GoalGraphNode;

function detail(input: {
  goal: string;
  nodes: ReadonlyArray<GoalGraphNode>;
  status?: GoalDetail["goal"]["status"];
  edges?: ReadonlyArray<readonly [string, string]>;
  statuses?: Readonly<Record<string, GoalDetail["nodes"][number]["status"]>>;
  attempts?: GoalDetail["attempts"];
}): GoalDetail {
  const goalId = GoalId.make(input.goal);
  const graphVersionId = GoalGraphVersionId.make(`graph:${input.goal}`);
  const createdAt = "2026-07-12T00:00:00.000Z";
  return {
    goal: {
      id: goalId,
      objective: input.goal,
      status: input.status ?? "running",
      sourceThreadId: ThreadId.make(`source:${input.goal}`),
      rootThreadId: ThreadId.make(`root:${input.goal}`),
      policy,
      currentGraphVersionId: graphVersionId,
      currentRevision: 1,
      integrationBranch: null,
      integrationWorktreePath: null,
      integrationSha: null,
      verifiedSha: null,
      createdAt,
      updatedAt: createdAt,
    },
    graphVersions: [
      {
        id: graphVersionId,
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("lead"),
        nodes: input.nodes,
        edges: (input.edges ?? []).map(([from, to], index) => ({
          id: GoalEdgeId.make(`edge:${input.goal}:${index}`),
          fromNodeId: GoalNodeId.make(from),
          toNodeId: GoalNodeId.make(to),
        })),
        createdAt,
      },
    ],
    nodes: input.nodes.map((entry) => ({
      goalId,
      graphVersionId,
      node: entry,
      status: input.statuses?.[entry.id] ?? "pending",
      activeAttemptId: null,
      blocker: null,
      updatedAt: createdAt,
    })),
    attempts: input.attempts ?? [],
    artifacts: [],
    evidence: [],
    writerCommits: [],
    failures: [],
  };
}

function attempt(input: {
  readonly detail: GoalDetail;
  readonly id: string;
  readonly nodeId?: GoalNodeId;
  readonly graphVersionId?: GoalAttempt["graphVersionId"];
  readonly ordinal?: number;
  readonly status?: GoalAttempt["status"];
  readonly nativeDescendantCount?: number;
}): GoalAttempt {
  return {
    id: GoalAttemptId.make(input.id),
    goalId: input.detail.goal.id,
    graphVersionId: input.graphVersionId ?? input.detail.goal.currentGraphVersionId!,
    nodeId: input.nodeId ?? GoalNodeId.make("historical"),
    ordinal: input.ordinal ?? 1,
    status: input.status ?? "succeeded",
    requestedRoute: {
      type: "exact",
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    resolvedRoute: null,
    providerSessionId: null,
    executionThreadId: null,
    runId: null,
    rootExecutionNodeId: null,
    baseIntegrationSha: null,
    workspacePath: null,
    leaseOwner: null,
    leaseExpiresAt: null,
    usage: {
      inputTokens: null,
      outputTokens: null,
      cachedTokens: null,
      costMicros: null,
      nativeDescendantCount: input.nativeDescendantCount ?? 0,
    },
    failureReason: null,
    createdAt: "2026-07-12T00:00:00.000Z",
    updatedAt: "2026-07-12T00:00:00.000Z",
  };
}

describe("goal scheduler planning", () => {
  it("derives host capacity and launches independent dependency-ready nodes", () => {
    expect(goalWorkerCapacity(16)).toBe(14);
    expect(goalWorkerCapacity(1)).toBe(2);
    expect(goalWorkerCapacity(64)).toBe(16);
    const result = planGoalScheduling({
      goals: [detail({ goal: "goal:a", nodes: [node("a"), node("b")] })],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(result.launches.map((launch) => launch.node.node.id)).toEqual(["a", "b"]);
  });

  it("waits for dependencies and blocks dependents after dependency failure", () => {
    const waiting = planGoalScheduling({
      goals: [
        detail({
          goal: "goal:deps",
          nodes: [node("build"), node("verify")],
          edges: [["build", "verify"]],
          statuses: { build: "running" },
        }),
      ],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(waiting.launches).toHaveLength(0);
    const blocked = planGoalScheduling({
      goals: [
        detail({
          goal: "goal:deps",
          nodes: [node("build"), node("verify")],
          edges: [["build", "verify"]],
          statuses: { build: "failed" },
        }),
      ],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(blocked.transitions).toContainEqual(
      expect.objectContaining({ nodeId: "verify", status: "blocked" }),
    );
  });

  it("shares slots round-robin across goals and caps writers at four", () => {
    const result = planGoalScheduling({
      goals: [
        detail({ goal: "goal:a", nodes: [node("a1", "writer"), node("a2", "writer")] }),
        detail({ goal: "goal:b", nodes: [node("b1", "writer"), node("b2", "writer")] }),
      ],
      workerCapacity: 3,
      writerCapacity: 2,
    });
    expect(result.launches.map((launch) => launch.node.node.id)).toEqual(["a1", "b1"]);
    expect(result.transitions.filter((transition) => transition.status === "queued")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ nodeId: "a2", queuePosition: 1 }),
        expect.objectContaining({ nodeId: "b2", queuePosition: 2 }),
      ]),
    );
  });

  it("does not schedule new work from a goal paused for a budget violation", () => {
    const result = planGoalScheduling({
      goals: [
        detail({
          goal: "goal:paused-overage",
          status: "paused",
          nodes: [node("should-not-launch")],
        }),
      ],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(result.launches).toHaveLength(0);
    expect(result.transitions).toHaveLength(0);
  });

  it("counts active paused and blocked attempts before admitting new workers or writers", () => {
    const pausedBase = detail({
      goal: "goal:paused-reader",
      status: "paused",
      nodes: [node("paused-reader")],
      statuses: { "paused-reader": "running" },
    });
    const paused = {
      ...pausedBase,
      attempts: [
        attempt({
          detail: pausedBase,
          id: "attempt:paused-reader",
          nodeId: GoalNodeId.make("paused-reader"),
          status: "running",
        }),
      ],
    } satisfies GoalDetail;
    const readerResult = planGoalScheduling({
      goals: [paused, detail({ goal: "goal:new-reader", nodes: [node("new-reader")] })],
      workerCapacity: 1,
      writerCapacity: 4,
    });
    expect(readerResult.launches).toHaveLength(0);
    expect(readerResult.transitions).toContainEqual(
      expect.objectContaining({ nodeId: "new-reader", reason: "capacity" }),
    );

    const blockedBase = detail({
      goal: "goal:blocked-historical-writer",
      status: "blocked",
      nodes: [node("current-reader")],
    });
    const historicalVersionId = GoalGraphVersionId.make("graph:blocked-historical-writer:old");
    const historicalWriter = node("historical-writer", "writer");
    const historicalAttempt = attempt({
      detail: blockedBase,
      id: "attempt:blocked-historical-writer",
      graphVersionId: historicalVersionId,
      nodeId: historicalWriter.id,
      status: "launching",
    });
    const blocked = {
      ...blockedBase,
      goal: { ...blockedBase.goal, currentRevision: 2 },
      graphVersions: [
        {
          id: historicalVersionId,
          goalId: blockedBase.goal.id,
          revision: 1,
          publishedByNodeId: GoalNodeId.make("lead:historical"),
          nodes: [historicalWriter],
          edges: [],
          createdAt: "2026-07-11T23:59:00.000Z",
        },
        { ...blockedBase.graphVersions[0]!, revision: 2 },
      ],
      nodes: [
        ...blockedBase.nodes,
        {
          goalId: blockedBase.goal.id,
          graphVersionId: historicalVersionId,
          node: historicalWriter,
          status: "running" as const,
          activeAttemptId: historicalAttempt.id,
          blocker: null,
          updatedAt: "2026-07-11T23:59:00.000Z",
        },
      ],
      attempts: [historicalAttempt],
    } satisfies GoalDetail;
    const writerResult = planGoalScheduling({
      goals: [
        blocked,
        detail({
          goal: "goal:new-writer",
          nodes: [node("new-reader"), node("new-writer", "writer")],
        }),
      ],
      workerCapacity: 3,
      writerCapacity: 1,
    });
    expect(writerResult.launches.map((launch) => launch.node.node.id)).toEqual(["new-reader"]);
    expect(writerResult.transitions).toContainEqual(
      expect.objectContaining({ nodeId: "new-writer", reason: "writer_capacity" }),
    );
  });

  it("allows exactly the 1,000th agent before queuing later ready work", () => {
    const base = detail({
      goal: "goal:near-agent-limit",
      nodes: [node("agent-1000"), node("agent-1001")],
    });
    const nearLimit = {
      ...base,
      attempts: [
        attempt({
          detail: base,
          id: "attempt:native-descendant-owner",
          nativeDescendantCount: 998,
        }),
      ],
    } satisfies GoalDetail;
    const result = planGoalScheduling({
      goals: [nearLimit],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(result.launches.map((launch) => launch.node.node.id)).toEqual(["agent-1000"]);
    expect(result.transitions).toContainEqual(
      expect.objectContaining({
        nodeId: "agent-1001",
        status: "queued",
        reason: "resource_backstop",
        queuePosition: null,
      }),
    );
    expect(nearLimit.attempts.length + 998 + result.launches.length).toBe(1_000);
  });

  it("enforces the lifetime backstop and warns once creation reaches 25", () => {
    const base = detail({ goal: "goal:limits", nodes: [node("next")] });
    const attempts = Array.from({ length: 25 }, (_, index) => ({
      id: GoalAttemptId.make(`attempt:${index}`),
      goalId: base.goal.id,
      graphVersionId: base.goal.currentGraphVersionId!,
      nodeId: GoalNodeId.make("historical"),
      ordinal: index + 1,
      status: "succeeded" as const,
      requestedRoute: {
        type: "exact" as const,
        providerInstanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      },
      resolvedRoute: null,
      providerSessionId: null,
      executionThreadId: null,
      runId: null,
      rootExecutionNodeId: null,
      baseIntegrationSha: null,
      workspacePath: null,
      leaseOwner: null,
      leaseExpiresAt: null,
      usage: {
        inputTokens: null,
        outputTokens: null,
        cachedTokens: null,
        costMicros: null,
        nativeDescendantCount: index === 0 ? 975 : 0,
      },
      failureReason: null,
      createdAt: "2026-07-12T00:00:00.000Z",
      updatedAt: "2026-07-12T00:00:00.000Z",
    }));
    const result = planGoalScheduling({
      goals: [{ ...base, attempts }],
      workerCapacity: 14,
      writerCapacity: 4,
    });
    expect(result.launches).toHaveLength(0);
    expect(result.warnings).toContainEqual(expect.objectContaining({ createdAgents: 1000 }));
    expect(result.transitions[0]).toEqual(
      expect.objectContaining({ status: "queued", reason: "resource_backstop" }),
    );
  });
});
