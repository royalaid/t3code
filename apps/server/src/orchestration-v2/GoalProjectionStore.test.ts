import { assert, expect, it } from "@effect/vitest";
import {
  GoalArtifactId,
  GoalAttemptId,
  GoalEvidenceId,
  GoalGraphVersionId,
  GoalEdgeId,
  GoalId,
  GoalNodeId,
  ProviderSessionId,
  RunId,
  ThreadId,
  type GoalGraphNode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { canonicalGoalLeadPublisherId, goalGraphPublisherIssue } from "./GoalGraphSemantics.ts";
import {
  GoalProjectionStore,
  GoalProjectionValidationError,
  validateGoalGraph,
  layer as goalProjectionStoreLayer,
} from "./GoalProjectionStore.ts";

const TestLayer = goalProjectionStoreLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory));
const policy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["shell"],
};
const readOnlyPolicy = { ...policy, sandboxMode: "read-only" as const, writableRoots: [] };
const makeNode = (id: string) => ({
  id: GoalNodeId.make(id),
  role: id,
  persona: id,
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
  outputContract: { kind: "structured_result" as const, description: "report", requiredFields: [] },
  requiredCapabilities: ["tools"],
  workspaceMode: "read_only" as const,
  routingRequest: {
    type: "requirements" as const,
    capabilities: ["tools"],
    latencyClass: "standard" as const,
    costClass: "standard" as const,
  },
  evidenceRequirements: [],
  policy: readOnlyPolicy,
});
const makeWriter = (id: string) => ({
  ...makeNode(id),
  workspaceMode: "writer" as const,
  outputContract: { kind: "commit" as const, description: "commit", requiredFields: [] },
  policy,
});
const makeVerifier = (id: string) => ({
  ...makeNode(id),
  outputContract: {
    kind: "verification" as const,
    description: "verification",
    requiredFields: ["verdict"],
  },
  evidenceRequirements: [{ kind: "command" as const, description: "test log", required: true }],
});
const makeCompletionGraph = (input: {
  readonly goalId: GoalId;
  readonly rootThreadId: ThreadId;
  readonly revision?: number;
  readonly id?: string;
}) => {
  const writer = makeWriter(`writer:${input.revision ?? 1}`);
  const verifier = makeVerifier(`verifier:${input.revision ?? 1}`);
  return {
    id: GoalGraphVersionId.make(input.id ?? `graph:${input.revision ?? 1}`),
    goalId: input.goalId,
    revision: input.revision ?? 1,
    publishedByNodeId: canonicalGoalLeadPublisherId(input.rootThreadId),
    nodes: [writer, verifier],
    edges: [
      {
        id: GoalEdgeId.make(`edge:${input.revision ?? 1}`),
        fromNodeId: writer.id,
        toNodeId: verifier.id,
      },
    ],
    createdAt: "2026-07-11T00:00:01.000Z",
  } as const;
};

it("allows approval narrowing and rejects approval authority expansion", () => {
  const graphWithApproval = (approvalPolicy: "untrusted" | "on-request" | "never") => ({
    id: GoalGraphVersionId.make(`graph:approval:${approvalPolicy}`),
    goalId: GoalId.make("goal:approval"),
    revision: 1,
    publishedByNodeId: GoalNodeId.make("lead"),
    nodes: [
      {
        ...makeNode("worker"),
        policy: { ...readOnlyPolicy, approvalPolicy },
      },
    ],
    edges: [],
    createdAt: "2026-07-11T00:00:00.000Z",
  });

  expect(() =>
    validateGoalGraph(graphWithApproval("untrusted"), {
      ...policy,
      approvalPolicy: "on-request",
    }),
  ).not.toThrow();
  expect(() =>
    validateGoalGraph(graphWithApproval("never"), {
      ...policy,
      approvalPolicy: "on-request",
    }),
  ).toThrow(GoalProjectionValidationError);
  expect(() =>
    validateGoalGraph(graphWithApproval("never"), {
      ...policy,
      approvalPolicy: "untrusted",
    }),
  ).toThrow(GoalProjectionValidationError);
});

it("rejects graph workspace modes that could bypass isolated writer integration", () => {
  const graph = (id: string, node: GoalGraphNode) => ({
    id: GoalGraphVersionId.make(`graph:workspace:${id}`),
    goalId: GoalId.make("goal:workspace"),
    revision: 1,
    publishedByNodeId: GoalNodeId.make("lead"),
    nodes: [node],
    edges: [],
    createdAt: "2026-07-11T00:00:00.000Z",
  });
  expect(() =>
    validateGoalGraph(graph("read-only-write", { ...makeNode("read-only-write"), policy }), policy),
  ).toThrow(GoalProjectionValidationError);
  expect(() =>
    validateGoalGraph(
      graph("writer-read-only", {
        ...makeNode("writer-read-only"),
        workspaceMode: "writer",
        outputContract: { kind: "commit", description: "commit", requiredFields: [] },
        policy: readOnlyPolicy,
      }),
      policy,
    ),
  ).toThrow(GoalProjectionValidationError);
  expect(() =>
    validateGoalGraph(
      graph("integration", {
        ...makeNode("integration"),
        workspaceMode: "integration",
        policy,
      }),
      policy,
    ),
  ).toThrow(GoalProjectionValidationError);
  expect(() =>
    validateGoalGraph(
      graph("read-only", { ...makeNode("read-only"), policy: readOnlyPolicy }),
      policy,
    ),
  ).not.toThrow();
});

it.layer(TestLayer)("GoalProjectionStore", (it) => {
  it.effect("enforces canonical publisher provenance and completion-valid graphs", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:strict-graph");
      const rootThreadId = ThreadId.make("thread:strict-root");
      yield* store.create({
        id: goalId,
        objective: "strict graph",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:strict-source"),
        rootThreadId,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/strict",
        integrationWorktreePath: "/strict",
        integrationSha: "sha:strict",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const valid = makeCompletionGraph({ goalId, rootThreadId, id: "graph:strict" });
      const invalidGraphs = [
        { ...valid, nodes: [], edges: [] },
        { ...valid, nodes: [makeWriter("writer:only")], edges: [] },
        {
          ...valid,
          nodes: [makeWriter("writer:pseudo"), makeNode("pseudo-verifier")],
          edges: [
            {
              id: GoalEdgeId.make("edge:pseudo"),
              fromNodeId: GoalNodeId.make("writer:pseudo"),
              toNodeId: GoalNodeId.make("pseudo-verifier"),
            },
          ],
        },
      ];
      for (const graph of invalidGraphs) {
        const error = yield* Effect.flip(
          store.activateGraph({ goalId, expectedRevision: 0, graph }),
        );
        assert.equal(error.reason, "non_terminal_graph");
        assert.match(error.detail, /verifier|completion-valid/iu);
      }

      const spoofed = { ...valid, publishedByNodeId: GoalNodeId.make("spoofed-lead") };
      assert.match(goalGraphPublisherIssue(spoofed, rootThreadId) ?? "", /authenticated/iu);
      const spoofedError = yield* Effect.flip(
        store.activateGraph({ goalId, expectedRevision: 0, graph: spoofed }),
      );
      assert.equal(spoofedError.reason, "referential_integrity");

      const leadOwned = {
        ...valid,
        nodes: [...valid.nodes, makeNode(rootThreadId)],
      };
      const leadOwnedError = yield* Effect.flip(
        store.activateGraph({ goalId, expectedRevision: 0, graph: leadOwned }),
      );
      assert.equal(leadOwnedError.reason, "referential_integrity");

      yield* store.activateGraph({ goalId, expectedRevision: 0, graph: valid });
      assert.equal((yield* store.getDetail(goalId)).goal.status, "running");
    }),
  );

  it.effect("fences graph activation by lifecycle while preserving running replacement", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const rootThreadId = ThreadId.make("thread:lifecycle-root");
      const goalId = GoalId.make("goal:lifecycle-running");
      yield* store.create({
        id: goalId,
        objective: "running replacement",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:lifecycle-source"),
        rootThreadId,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/lifecycle",
        integrationWorktreePath: "/lifecycle",
        integrationSha: "sha:lifecycle",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      yield* store.activateGraph({
        goalId,
        expectedRevision: 0,
        graph: makeCompletionGraph({ goalId, rootThreadId, revision: 1 }),
      });
      yield* store.activateGraph({
        goalId,
        expectedRevision: 1,
        graph: makeCompletionGraph({ goalId, rootThreadId, revision: 2 }),
      });
      assert.equal((yield* store.getDetail(goalId)).goal.currentRevision, 2);

      const runningGoal = (yield* store.getDetail(goalId)).goal;
      yield* store.apply({
        type: "goal.updated",
        payload: {
          ...runningGoal,
          status: "paused",
          updatedAt: "2026-07-11T00:00:02.000Z",
        },
      });
      yield* store.activateGraph({
        goalId,
        expectedRevision: 2,
        graph: makeCompletionGraph({ goalId, rootThreadId, revision: 3 }),
      });
      const resumedGoal = (yield* store.getDetail(goalId)).goal;
      assert.equal(resumedGoal.status, "running");
      assert.equal(resumedGoal.currentRevision, 3);

      for (const lifecycleStatus of [
        "waiting_for_source",
        "provisioning",
        "verifying",
        "completed",
        "failed",
        "cancelled",
      ] as const) {
        const terminalGoalId = GoalId.make(`goal:lifecycle:${lifecycleStatus}`);
        const terminalRoot = ThreadId.make(`thread:lifecycle:${lifecycleStatus}`);
        yield* store.create({
          id: terminalGoalId,
          objective: lifecycleStatus,
          status: lifecycleStatus,
          sourceThreadId: ThreadId.make(`source:lifecycle:${lifecycleStatus}`),
          rootThreadId: terminalRoot,
          policy,
          currentGraphVersionId: null,
          currentRevision: 0,
          integrationBranch: null,
          integrationWorktreePath: null,
          integrationSha: "sha:lifecycle",
          verifiedSha: null,
          createdAt: "2026-07-11T00:00:00.000Z",
          updatedAt: "2026-07-11T00:00:00.000Z",
        });
        const error = yield* Effect.flip(
          store.activateGraph({
            goalId: terminalGoalId,
            expectedRevision: 0,
            graph: makeCompletionGraph({ goalId: terminalGoalId, rootThreadId: terminalRoot }),
          }),
        );
        assert.equal(error.reason, "invalid_revision", lifecycleStatus);
        assert.equal((yield* store.getDetail(terminalGoalId)).goal.status, lifecycleStatus);
      }

      const blockedGoalId = GoalId.make("goal:lifecycle:blocked");
      const blockedRoot = ThreadId.make("thread:lifecycle:blocked");
      yield* store.create({
        id: blockedGoalId,
        objective: "blocked recovery",
        status: "blocked",
        sourceThreadId: ThreadId.make("source:lifecycle:blocked"),
        rootThreadId: blockedRoot,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: "sha:lifecycle",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      yield* store.activateGraph({
        goalId: blockedGoalId,
        expectedRevision: 0,
        graph: makeCompletionGraph({
          goalId: blockedGoalId,
          rootThreadId: blockedRoot,
          id: "graph:lifecycle:blocked",
        }),
      });
      assert.equal((yield* store.getDetail(blockedGoalId)).goal.status, "running");
    }),
  );

  it.effect("CASes the lifecycle status captured with graph activation", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:lifecycle-cas");
      const rootThreadId = ThreadId.make("thread:lifecycle-cas-root");
      const goal = {
        id: goalId,
        objective: "lifecycle CAS",
        status: "planning" as const,
        sourceThreadId: ThreadId.make("thread:lifecycle-cas-source"),
        rootThreadId,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/lifecycle-cas",
        integrationWorktreePath: "/lifecycle-cas",
        integrationSha: "sha:lifecycle-cas",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      };
      yield* store.create(goal);
      const graph = makeCompletionGraph({
        goalId,
        rootThreadId,
        id: "graph:lifecycle-cas",
      });
      yield* store.apply({
        type: "goal.updated",
        payload: {
          ...goal,
          status: "blocked",
          updatedAt: "2026-07-11T00:00:01.000Z",
        },
      });
      const stale = yield* Effect.flip(
        store.apply({
          type: "goal.graph-version-activated",
          payload: {
            goalId,
            expectedRevision: 0,
            expectedStatus: "planning",
            graph,
            activatedAt: "2026-07-11T00:00:02.000Z",
          },
        }),
      );
      assert.equal(stale.reason, "stale_revision");
      assert.match(stale.detail, /current lifecycle is blocked/iu);
      assert.equal((yield* store.getDetail(goalId)).goal.status, "blocked");

      yield* store.apply({
        type: "goal.graph-version-activated",
        payload: {
          goalId,
          expectedRevision: 0,
          expectedStatus: "blocked",
          graph,
          activatedAt: "2026-07-11T00:00:03.000Z",
        },
      });
      assert.equal((yield* store.getDetail(goalId)).goal.status, "running");
    }),
  );

  it.effect("accepts evidence only from a succeeded writer ancestor in the active graph", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:evidence-ancestry");
      const rootThreadId = ThreadId.make("thread:evidence-ancestry-root");
      yield* store.create({
        id: goalId,
        objective: "evidence ancestry",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:evidence-ancestry-source"),
        rootThreadId,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/evidence-ancestry",
        integrationWorktreePath: "/evidence-ancestry",
        integrationSha: "sha:evidence-current",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const baseGraph = makeCompletionGraph({
        goalId,
        rootThreadId,
        id: "graph:evidence-ancestry",
      });
      const observer = makeNode("observer:evidence-ancestry");
      const graph = { ...baseGraph, nodes: [...baseGraph.nodes, observer] };
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      const writer = baseGraph.nodes[0];
      const verifier = baseGraph.nodes[1];
      const attempt = (input: {
        readonly id: string;
        readonly node: GoalGraphNode;
        readonly ordinal: number;
        readonly status: "running" | "succeeded";
      }) => ({
        id: GoalAttemptId.make(input.id),
        goalId,
        graphVersionId: graph.id,
        nodeId: input.node.id,
        ordinal: input.ordinal,
        status: input.status,
        requestedRoute: input.node.routingRequest,
        resolvedRoute: null,
        providerSessionId: null,
        executionThreadId: ThreadId.make(`thread:${input.id}`),
        runId: null,
        rootExecutionNodeId: null,
        baseIntegrationSha: "sha:evidence-current",
        workspacePath: `/workspace/${input.id}`,
        leaseOwner: "test",
        leaseExpiresAt: null,
        usage: {
          inputTokens: null,
          outputTokens: null,
          cachedTokens: null,
          costMicros: null,
          nativeDescendantCount: 0,
        },
        failureReason: null,
        createdAt: "2026-07-11T00:00:02.000Z",
        updatedAt: "2026-07-11T00:00:02.000Z",
      });
      const succeededProducer = attempt({
        id: "attempt:producer:succeeded",
        node: writer,
        ordinal: 1,
        status: "succeeded",
      });
      const runningProducer = attempt({
        id: "attempt:producer:running",
        node: writer,
        ordinal: 2,
        status: "running",
      });
      const unrelatedProducer = attempt({
        id: "attempt:producer:unrelated",
        node: observer,
        ordinal: 1,
        status: "succeeded",
      });
      const verifierAttempt = attempt({
        id: "attempt:verifier",
        node: verifier,
        ordinal: 1,
        status: "running",
      });
      for (const candidate of [
        succeededProducer,
        runningProducer,
        unrelatedProducer,
        verifierAttempt,
      ]) {
        yield* store.apply({ type: "goal.attempt-created", payload: candidate });
      }
      const logArtifact = {
        id: GoalArtifactId.make("artifact:evidence-log"),
        goalId,
        nodeId: verifier.id,
        attemptId: verifierAttempt.id,
        kind: "log" as const,
        uri: "artifact://evidence-log",
        digest: "digest:evidence-log",
        metadata: {},
        createdAt: "2026-07-11T00:00:03.000Z",
      };
      yield* store.apply({ type: "goal.artifact-published", payload: logArtifact });
      const evidence = {
        id: GoalEvidenceId.make("evidence:ancestry"),
        goalId,
        nodeId: verifier.id,
        attemptId: verifierAttempt.id,
        integrationSha: "sha:evidence-current",
        producerAttemptId: succeededProducer.id,
        commands: [{ command: "vp test", exitCode: 0, logArtifactId: logArtifact.id }],
        artifacts: [logArtifact.id],
        verdict: "accepted" as const,
        summary: "verified",
        createdAt: "2026-07-11T00:00:04.000Z",
      };
      for (const [id, producerAttemptId] of [
        ["evidence:running-producer", runningProducer.id],
        ["evidence:unrelated-producer", unrelatedProducer.id],
      ] as const) {
        const error = yield* Effect.flip(
          store.apply({
            type: "goal.evidence-submitted",
            payload: { ...evidence, id: GoalEvidenceId.make(id), producerAttemptId },
          }),
        );
        assert.equal(error.reason, "independent_verification_required");
        assert.match(error.detail, /succeeded writer producer.*ancestor/iu);
      }
      yield* store.apply({ type: "goal.evidence-submitted", payload: evidence });
      assert.equal((yield* store.getDetail(goalId)).evidence[0]?.id, evidence.id);
    }),
  );

  it.effect("activates immutable graph versions with revision CAS and exact replay", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:projection");
      const rootThreadId = ThreadId.make("thread:goal-root");
      yield* store.create({
        id: goalId,
        objective: "ship it",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:source"),
        policy,
        rootThreadId,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/projection",
        integrationWorktreePath: "/repo-goal",
        integrationSha: "abc",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const graph = makeCompletionGraph({
        goalId,
        rootThreadId,
        id: "goal-graph:1",
      });
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      const replayed = yield* store.getDetail(goalId);
      assert.equal(replayed.goal.currentRevision, 1);
      assert.deepEqual(replayed.graphVersions, [graph]);
      const stale = yield* Effect.flip(
        store.activateGraph({
          goalId,
          expectedRevision: 0,
          graph: { ...graph, id: GoalGraphVersionId.make("goal-graph:2"), revision: 2 },
        }),
      );
      assert.instanceOf(stale, GoalProjectionValidationError);
      assert.equal(stale.reason, "stale_revision");
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 1);
    }),
  );

  it.effect("rejects cycles, missing dependencies, duplicates, and policy expansion", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:validation");
      yield* store.create({
        id: goalId,
        objective: "validate",
        status: "planning",
        sourceThreadId: ThreadId.make("source"),
        rootThreadId: ThreadId.make("root"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/validation",
        integrationWorktreePath: "/goal",
        integrationSha: "abc",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const base = {
        id: GoalGraphVersionId.make("graph:bad"),
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("a"),
        createdAt: "2026-07-11T00:00:00.000Z",
      };
      const cases = [
        { nodes: [makeNode("a"), makeNode("a")], edges: [] },
        {
          nodes: [makeNode("a")],
          edges: [
            {
              id: GoalEdgeId.make("e"),
              fromNodeId: GoalNodeId.make("missing"),
              toNodeId: GoalNodeId.make("a"),
            },
          ],
        },
        {
          nodes: [makeNode("a"), makeNode("b")],
          edges: [
            {
              id: GoalEdgeId.make("e1"),
              fromNodeId: GoalNodeId.make("a"),
              toNodeId: GoalNodeId.make("b"),
            },
            {
              id: GoalEdgeId.make("e2"),
              fromNodeId: GoalNodeId.make("b"),
              toNodeId: GoalNodeId.make("a"),
            },
          ],
        },
        {
          nodes: [
            { ...makeNode("a"), policy: { ...policy, writableRoots: ["/repo", "/outside"] } },
          ],
          edges: [],
        },
      ];
      for (const graph of cases) {
        assert.instanceOf(
          yield* Effect.flip(
            store.activateGraph({ goalId, expectedRevision: 0, graph: { ...base, ...graph } }),
          ),
          GoalProjectionValidationError,
        );
      }
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 0);
    }),
  );

  it.effect("treats Windows roots case-insensitively with either separator", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:windows-policy");
      const windowsPolicy = { ...policy, writableRoots: ["C:\\Repo"] };
      yield* store.create({
        id: goalId,
        objective: "windows",
        status: "planning",
        sourceThreadId: ThreadId.make("source:windows"),
        rootThreadId: ThreadId.make("root:windows"),
        policy: windowsPolicy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const writer = {
        ...makeNode("worker:windows"),
        workspaceMode: "writer" as const,
        outputContract: { kind: "commit" as const, description: "commit", requiredFields: [] },
        policy: { ...windowsPolicy, writableRoots: ["c:/repo/packages"] },
      };
      const verifier = makeVerifier("verifier:windows");
      const graph = {
        id: GoalGraphVersionId.make("graph:windows"),
        goalId,
        revision: 1,
        publishedByNodeId: canonicalGoalLeadPublisherId(ThreadId.make("root:windows")),
        nodes: [writer, verifier],
        edges: [
          {
            id: GoalEdgeId.make("edge:windows"),
            fromNodeId: writer.id,
            toNodeId: verifier.id,
          },
        ],
        createdAt: "2026-07-11T00:00:01.000Z",
      } as const;
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      assert.equal((yield* store.getDetail(goalId)).goal.currentRevision, 1);
    }),
  );

  it.effect("does not confuse Windows path prefixes with containment", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:windows-prefix");
      const windowsPolicy = { ...policy, writableRoots: ["C:\\Repo"] };
      yield* store.create({
        id: goalId,
        objective: "windows",
        status: "planning",
        sourceThreadId: ThreadId.make("source:prefix"),
        rootThreadId: ThreadId.make("root:prefix"),
        policy: windowsPolicy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      for (const [index, writableRoot] of ["C:\\Repository", "C:\\Repo\\..\\outside"].entries()) {
        const error = yield* Effect.flip(
          store.activateGraph({
            goalId,
            expectedRevision: 0,
            graph: {
              id: GoalGraphVersionId.make(`graph:prefix:${index}`),
              goalId,
              revision: 1,
              publishedByNodeId: GoalNodeId.make("lead"),
              nodes: [
                {
                  ...makeNode("worker:prefix"),
                  workspaceMode: "writer",
                  outputContract: { kind: "commit", description: "commit", requiredFields: [] },
                  policy: { ...windowsPolicy, writableRoots: [writableRoot] },
                },
              ],
              edges: [],
              createdAt: "2026-07-11T00:00:01.000Z",
            },
          }),
        );
        assert.equal(error.reason, "policy_expansion");
      }
    }),
  );

  it.effect("rejects POSIX traversal outside an allowed root", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:posix-traversal");
      yield* store.create({
        id: goalId,
        objective: "posix",
        status: "planning",
        sourceThreadId: ThreadId.make("source:posix"),
        rootThreadId: ThreadId.make("root:posix"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const error = yield* Effect.flip(
        store.activateGraph({
          goalId,
          expectedRevision: 0,
          graph: {
            id: GoalGraphVersionId.make("graph:posix-traversal"),
            goalId,
            revision: 1,
            publishedByNodeId: GoalNodeId.make("lead"),
            nodes: [
              {
                ...makeNode("worker:posix"),
                workspaceMode: "writer",
                outputContract: { kind: "commit", description: "commit", requiredFields: [] },
                policy: { ...policy, writableRoots: ["/repo/../outside"] },
              },
            ],
            edges: [],
            createdAt: "2026-07-11T00:00:01.000Z",
          },
        }),
      );
      assert.equal(error.reason, "policy_expansion");
    }),
  );

  it.effect("claims graph revisions once under concurrent CAS", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:concurrent-cas");
      yield* store.create({
        id: goalId,
        objective: "cas",
        status: "planning",
        sourceThreadId: ThreadId.make("source:cas"),
        rootThreadId: ThreadId.make("root:cas"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const makeGraph = (suffix: string) =>
        makeCompletionGraph({
          goalId,
          rootThreadId: ThreadId.make("root:cas"),
          id: `graph:cas:${suffix}`,
        });
      const exits = yield* Effect.all(
        [makeGraph("a"), makeGraph("b")].map((graph) =>
          Effect.exit(store.activateGraph({ goalId, expectedRevision: 0, graph })),
        ),
        { concurrency: "unbounded" },
      );
      assert.equal(exits.filter(Exit.isSuccess).length, 1);
      assert.equal(exits.filter(Exit.isFailure).length, 1);
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 1);
    }),
  );

  it.effect(
    "preserves aggregate identity and invalidates verification on integration changes",
    () =>
      Effect.gen(function* () {
        const store = yield* GoalProjectionStore;
        const goalId = GoalId.make("goal:lifecycle-invariants");
        const original = {
          id: goalId,
          objective: "original",
          status: "completed" as const,
          sourceThreadId: ThreadId.make("source:original"),
          rootThreadId: ThreadId.make("root:original"),
          policy,
          currentGraphVersionId: null,
          currentRevision: 0,
          integrationBranch: "goal/original",
          integrationWorktreePath: "/goal",
          integrationSha: "sha:old",
          verifiedSha: "sha:old",
          createdAt: "2026-07-11T00:00:00.000Z",
          updatedAt: "2026-07-11T00:00:00.000Z",
        };
        yield* store.create(original);
        assert.equal(
          (yield* Effect.flip(
            store.create({ ...original, rootThreadId: ThreadId.make("root:conflict") }),
          )).reason,
          "referential_integrity",
        );
        yield* store.apply({
          type: "goal.integration-updated",
          payload: {
            ...original,
            sourceThreadId: ThreadId.make("source:malicious"),
            rootThreadId: ThreadId.make("root:malicious"),
            policy: { ...policy, writableRoots: ["/outside"] },
            currentRevision: 99,
            integrationSha: "sha:new",
            verifiedSha: "sha:new",
            updatedAt: "2026-07-11T00:01:00.000Z",
          },
        });
        const persisted = (yield* store.getDetail(goalId)).goal;
        assert.equal(persisted.sourceThreadId, original.sourceThreadId);
        assert.equal(persisted.rootThreadId, original.rootThreadId);
        assert.deepEqual(persisted.policy, policy);
        assert.equal(persisted.currentRevision, 0);
        assert.isNull(persisted.verifiedSha);
      }),
  );

  it.effect("reopens a completed goal without reusing its active graph or verification", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:reopen");
      const graphVersionId = GoalGraphVersionId.make("graph:reopen:1");
      const completed = {
        id: goalId,
        objective: "reopen",
        status: "completed" as const,
        sourceThreadId: ThreadId.make("source:reopen"),
        rootThreadId: ThreadId.make("root:reopen"),
        policy,
        currentGraphVersionId: graphVersionId,
        currentRevision: 1,
        integrationBranch: "goal/reopen",
        integrationWorktreePath: "/goal/reopen",
        integrationSha: "sha:reopen",
        verifiedSha: "sha:reopen",
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      };
      yield* store.create(completed);
      yield* store.apply({
        type: "goal.reopened",
        payload: {
          ...completed,
          status: "planning",
          currentGraphVersionId: null,
          verifiedSha: null,
          updatedAt: "2026-07-11T00:01:00.000Z",
        },
      });
      const reopened = (yield* store.getDetail(goalId)).goal;
      assert.equal(reopened.status, "planning");
      assert.isNull(reopened.currentGraphVersionId);
      assert.equal(reopened.currentRevision, 1);
      assert.equal(reopened.integrationSha, "sha:reopen");
      assert.isNull(reopened.verifiedSha);
    }),
  );

  it.effect("persists graph-owned attempts, writer commits, and structured failures", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:records");
      yield* store.create({
        id: goalId,
        objective: "records",
        status: "planning",
        sourceThreadId: ThreadId.make("source:records"),
        rootThreadId: ThreadId.make("root:records"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/records",
        integrationWorktreePath: "/records",
        integrationSha: "sha:base",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const node = {
        ...makeNode("worker:records"),
        workspaceMode: "writer" as const,
        outputContract: { kind: "commit" as const, description: "commit", requiredFields: [] },
        policy,
      };
      const verifier = makeVerifier("verifier:records");
      const graph = {
        id: GoalGraphVersionId.make("graph:records"),
        goalId,
        revision: 1,
        publishedByNodeId: canonicalGoalLeadPublisherId(ThreadId.make("root:records")),
        nodes: [node, verifier],
        edges: [
          {
            id: GoalEdgeId.make("edge:records"),
            fromNodeId: node.id,
            toNodeId: verifier.id,
          },
        ],
        createdAt: "2026-07-11T00:00:01.000Z",
      } as const;
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      const attemptId = GoalAttemptId.make("attempt:records");
      const attempt = {
        id: attemptId,
        goalId,
        graphVersionId: graph.id,
        nodeId: node.id,
        ordinal: 1,
        status: "running" as const,
        requestedRoute: node.routingRequest,
        resolvedRoute: null,
        providerSessionId: null,
        executionThreadId: ThreadId.make("thread:attempt:records"),
        runId: null,
        rootExecutionNodeId: null,
        baseIntegrationSha: "sha:base",
        workspacePath: "/records-worker",
        leaseOwner: "worker",
        leaseExpiresAt: "2026-07-11T00:10:00.000Z",
        usage: {
          inputTokens: null,
          outputTokens: null,
          cachedTokens: null,
          costMicros: null,
          nativeDescendantCount: 0,
        },
        failureReason: null,
        createdAt: "2026-07-11T00:00:02.000Z",
        updatedAt: "2026-07-11T00:00:02.000Z",
      };
      const dangling = yield* Effect.flip(
        store.apply({
          type: "goal.attempt-created",
          payload: { ...attempt, graphVersionId: GoalGraphVersionId.make("graph:missing") },
        }),
      );
      assert.equal(dangling.reason, "referential_integrity");
      yield* store.apply({ type: "goal.attempt-created", payload: attempt });
      assert.equal(
        (yield* Effect.flip(
          store.apply({ type: "goal.attempt-created", payload: { ...attempt, ordinal: 2 } }),
        )).reason,
        "referential_integrity",
      );
      const providerSessionId = ProviderSessionId.make("provider-session:records");
      const runId = RunId.make("run:records");
      yield* store.apply({
        type: "goal.attempt-transitioned",
        payload: { ...attempt, providerSessionId, updatedAt: "2026-07-11T00:00:03.000Z" },
      });
      yield* store.apply({
        type: "goal.attempt-transitioned",
        payload: { ...attempt, runId, updatedAt: "2026-07-11T00:00:04.000Z" },
      });
      const mergedAttempt = (yield* store.getDetail(goalId)).attempts.find(
        (candidate) => candidate.id === attemptId,
      );
      assert.equal(mergedAttempt?.providerSessionId, providerSessionId);
      assert.equal(mergedAttempt?.runId, runId);
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.attempt-transitioned",
            payload: {
              ...attempt,
              providerSessionId: ProviderSessionId.make("provider-session:records:replacement"),
            },
          }),
        )).reason,
        "referential_integrity",
      );
      const writerCommit = {
        id: GoalArtifactId.make("commit-record:1"),
        goalId,
        graphVersionId: graph.id,
        nodeId: node.id,
        attemptId,
        baseSha: "sha:base",
        commitSha: "sha:commit",
        cleanSingleCommit: true,
        integrationBeforeSha: "sha:base",
        integrationAfterSha: "sha:after",
        state: "integrated" as const,
        createdAt: "2026-07-11T00:00:03.000Z",
        updatedAt: "2026-07-11T00:00:04.000Z",
      };
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.writer-commit-recorded",
            payload: {
              ...writerCommit,
              id: GoalArtifactId.make("commit-record:unclean"),
              cleanSingleCommit: false,
            },
          }),
        )).reason,
        "referential_integrity",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.writer-commit-recorded",
            payload: {
              ...writerCommit,
              id: GoalArtifactId.make("commit-record:wrong-base"),
              baseSha: "sha:wrong-base",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({
        type: "goal.writer-commit-recorded",
        payload: {
          ...writerCommit,
          integrationAfterSha: null,
          state: "integrating",
        },
      });
      const replacementDuringIntegration = yield* Effect.flip(
        store.activateGraph({
          goalId,
          expectedRevision: 1,
          expectedStatus: "running",
          graph: makeCompletionGraph({
            goalId,
            rootThreadId: ThreadId.make("root:records"),
            revision: 2,
            id: "graph:records:replacement",
          }),
        }),
      );
      assert.equal(replacementDuringIntegration.reason, "stale_revision");
      yield* store.apply({ type: "goal.writer-commit-recorded", payload: writerCommit });
      yield* store.apply({
        type: "goal.failure-recorded",
        payload: {
          id: GoalEvidenceId.make("failure:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          reason: {
            type: "integration_conflict",
            artifactId: GoalArtifactId.make("commit-record:1"),
            detail: "conflict",
          },
          recoveryState: "unresolved",
          blocker: "resolve conflict",
          occurredAt: "2026-07-11T00:00:05.000Z",
        },
      });
      const artifact = {
        id: GoalArtifactId.make("artifact:records"),
        goalId,
        nodeId: node.id,
        attemptId,
        kind: "result" as const,
        uri: "artifact://result",
        digest: "digest:1",
        metadata: {},
        createdAt: "2026-07-11T00:00:05.000Z",
      };
      yield* store.apply({ type: "goal.artifact-published", payload: artifact });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.artifact-published",
            payload: { ...artifact, uri: "artifact://different" },
          }),
        )).reason,
        "referential_integrity",
      );
      const evidence = {
        id: GoalEvidenceId.make("evidence:records"),
        goalId,
        nodeId: node.id,
        attemptId,
        integrationSha: "sha:after",
        producerAttemptId: attemptId,
        commands: [],
        artifacts: [artifact.id],
        verdict: "inconclusive" as const,
        summary: "pending",
        createdAt: "2026-07-11T00:00:06.000Z",
      };
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.evidence-submitted",
            payload: {
              ...evidence,
              id: GoalEvidenceId.make("evidence:missing-artifact"),
              artifacts: [GoalArtifactId.make("artifact:missing")],
            },
          }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({ type: "goal.evidence-submitted", payload: evidence });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.verdict-recorded",
            payload: { ...evidence, verdict: "accepted" },
          }),
        )).reason,
        "machine_evidence_required",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.verdict-recorded",
            payload: {
              ...evidence,
              integrationSha: "sha:different",
              commands: [{ command: "vp test", exitCode: 0, logArtifactId: artifact.id }],
              verdict: "accepted",
            },
          }),
        )).reason,
        "stale_evidence",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.writer-commit-recorded",
            payload: {
              id: GoalArtifactId.make("commit-record:1"),
              goalId,
              graphVersionId: graph.id,
              nodeId: node.id,
              attemptId,
              baseSha: "sha:base",
              commitSha: "sha:different",
              cleanSingleCommit: true,
              integrationBeforeSha: "sha:base",
              integrationAfterSha: "sha:after",
              state: "integrated",
              createdAt: "2026-07-11T00:00:03.000Z",
              updatedAt: "2026-07-11T00:00:07.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.failure-recorded",
            payload: {
              id: GoalEvidenceId.make("failure:1"),
              goalId,
              graphVersionId: graph.id,
              nodeId: node.id,
              attemptId,
              reason: { type: "dependency_failure", dependencyNodeId: node.id },
              recoveryState: "resolved",
              blocker: null,
              occurredAt: "2026-07-11T00:00:05.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({
        type: "goal.verdict-recorded",
        payload: { ...evidence, verdict: "rejected" },
      });
      yield* store.apply({
        type: "goal.writer-commit-recorded",
        payload: {
          id: GoalArtifactId.make("commit-record:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          baseSha: "sha:base",
          commitSha: "sha:commit",
          cleanSingleCommit: true,
          integrationBeforeSha: "sha:base",
          integrationAfterSha: null,
          state: "conflicted",
          createdAt: "2026-07-11T00:00:03.000Z",
          updatedAt: "2026-07-11T00:00:09.000Z",
        },
      });
      yield* store.apply({
        type: "goal.failure-recorded",
        payload: {
          id: GoalEvidenceId.make("failure:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          reason: {
            type: "integration_conflict",
            artifactId: GoalArtifactId.make("commit-record:1"),
            detail: "conflict",
          },
          recoveryState: "resolved",
          blocker: null,
          occurredAt: "2026-07-11T00:00:05.000Z",
        },
      });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.failure-recorded",
            payload: {
              id: GoalEvidenceId.make("failure:bad-owner"),
              goalId,
              graphVersionId: graph.id,
              nodeId: GoalNodeId.make("node:other"),
              attemptId: null,
              reason: { type: "dependency_failure", dependencyNodeId: node.id },
              recoveryState: "unresolved",
              blocker: null,
              occurredAt: "2026-07-11T00:00:08.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({
        type: "goal.node-cancellation-requested",
        payload: {
          goalId,
          graphVersionId: graph.id,
          node,
          status: "cancelled",
          activeAttemptId: attemptId,
          blocker: "superseded by a newer plan",
          updatedAt: "2026-07-11T00:00:10.000Z",
        },
      });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.artifact-published",
            payload: {
              ...artifact,
              id: GoalArtifactId.make("artifact:late-after-cancel"),
              createdAt: "2026-07-11T00:00:11.000Z",
            },
          }),
        )).reason,
        "stale_active_run_target",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.evidence-submitted",
            payload: {
              ...evidence,
              id: GoalEvidenceId.make("evidence:late-after-cancel"),
              createdAt: "2026-07-11T00:00:11.000Z",
            },
          }),
        )).reason,
        "stale_active_run_target",
      );
      const detail = yield* store.getDetail(goalId);
      assert.equal(detail.attempts[0]?.graphVersionId, graph.id);
      assert.equal(detail.attempts[0]?.executionThreadId, ThreadId.make("thread:attempt:records"));
      assert.equal(detail.writerCommits[0]?.state, "conflicted");
      assert.equal(detail.failures[0]?.recoveryState, "resolved");
      assert.equal(detail.artifacts[0]?.uri, artifact.uri);
      assert.equal(detail.evidence[0]?.integrationSha, evidence.integrationSha);
      assert.equal(detail.evidence[0]?.verdict, "rejected");
    }),
  );
});
