import { assert, it } from "@effect/vitest";
import {
  CommandId,
  GoalEdgeId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type GoalGraphNode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { EventSinkV2, layer as eventSinkLayer } from "./EventSink.ts";
import { layer as eventStoreLayer } from "./EventStore.ts";
import { canonicalGoalLeadPublisherId } from "./GoalGraphSemantics.ts";
import {
  GoalProjectionStore,
  layer as goalProjectionStoreLayer,
  type GoalProjectionStoreShape,
} from "./GoalProjectionStore.ts";
import { GoalRoutingService } from "./GoalRoutingService.ts";
import {
  GoalScheduler,
  layerWithOptions as goalSchedulerLayerWithOptions,
} from "./GoalScheduler.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "./IdAllocator.ts";
import { layer as projectionStoreLayer } from "./ProjectionStore.ts";

const database = SqlitePersistenceMemory;
const eventStore = eventStoreLayer.pipe(Layer.provideMerge(database));
const projectionStore = projectionStoreLayer.pipe(Layer.provideMerge(database));
const goalStore = goalProjectionStoreLayer.pipe(Layer.provideMerge(database));
const stores = Layer.mergeAll(database, eventStore, projectionStore, goalStore);
const eventSink = eventSinkLayer.pipe(Layer.provide(stores));
const routing = Layer.succeed(
  GoalRoutingService,
  GoalRoutingService.of({
    catalog: Effect.succeed([]),
    route: (input) =>
      Effect.succeed({
        type: "resolved",
        route: {
          requested: input.requested,
          providerInstanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
          capabilitySnapshot: ["tools.shell"],
          rationale: "test route",
        },
      }),
  }),
);
const scheduler = goalSchedulerLayerWithOptions({
  workerCapacity: 14,
  writerCapacity: 4,
  workerId: "scheduler:test",
}).pipe(Layer.provide(Layer.mergeAll(stores, eventSink, routing, idAllocatorLayer)));
const TestLayer = Layer.mergeAll(stores, eventSink, routing, idAllocatorLayer, scheduler);
const capacityScheduler = goalSchedulerLayerWithOptions({
  workerCapacity: 2,
  writerCapacity: 1,
  workerId: "scheduler:capacity-test",
}).pipe(Layer.provide(Layer.mergeAll(stores, eventSink, routing, idAllocatorLayer)));
const CapacityTestLayer = Layer.mergeAll(
  stores,
  eventSink,
  routing,
  idAllocatorLayer,
  capacityScheduler,
);

const policy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};
const readOnlyPolicy = { ...policy, sandboxMode: "read-only" as const, writableRoots: [] };

function schedulerNode(
  id: GoalNodeId,
  workspaceMode: "read_only" | "writer" = "read_only",
): GoalGraphNode {
  return {
    id,
    role: "worker",
    persona: "implementer",
    objective: "do work",
    successCriteria: ["done"],
    contextPacket: {
      schemaVersion: 1,
      digest: null,
      objective: "do work",
      artifacts: [],
      dependencyOutputs: [],
      notes: [],
    },
    outputContract:
      workspaceMode === "writer"
        ? { kind: "commit", description: "commit", requiredFields: [] }
        : { kind: "structured_result", description: "report", requiredFields: [] },
    requiredCapabilities: ["tools.shell"],
    workspaceMode,
    routingRequest: {
      type: "exact",
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    evidenceRequirements: [],
    policy: workspaceMode === "writer" ? policy : readOnlyPolicy,
  };
}

function schedulerVerifierNode(id: GoalNodeId): GoalGraphNode {
  return {
    ...schedulerNode(id),
    outputContract: {
      kind: "verification",
      description: "verify integrated result",
      requiredFields: ["verdict"],
    },
    evidenceRequirements: [{ kind: "command", description: "durable test log", required: true }],
  };
}

function completionValidSchedulerGraph(goalId: GoalId, nodes: ReadonlyArray<GoalGraphNode>) {
  const writers = nodes.filter((node) => node.workspaceMode === "writer");
  if (writers.length === 0) throw new Error(`Scheduler graph ${goalId} needs an explicit writer.`);
  const verifier = schedulerVerifierNode(GoalNodeId.make(`node:verifier:${goalId}`));
  return {
    nodes: [...nodes, verifier],
    edges: writers.map((writer, index) => ({
      id: GoalEdgeId.make(`edge:${goalId}:writer-verifier:${index}`),
      fromNodeId: writer.id,
      toNodeId: verifier.id,
    })),
  };
}

function createGoalWithGraph(input: {
  readonly goals: GoalProjectionStoreShape;
  readonly goalId: GoalId;
  readonly graphVersionId: GoalGraphVersionId;
  readonly nodes: ReadonlyArray<GoalGraphNode>;
  readonly createdAt: string;
}) {
  return Effect.gen(function* () {
    const rootThreadId = ThreadId.make(`thread:goal:${input.goalId}`);
    const graph = completionValidSchedulerGraph(input.goalId, input.nodes);
    yield* input.goals.create({
      id: input.goalId,
      projectId: ProjectId.make(`project:${input.goalId}`),
      objective: "schedule work",
      status: "planning",
      sourceThreadId: ThreadId.make(`thread:source:${input.goalId}`),
      rootThreadId,
      policy,
      currentGraphVersionId: null,
      currentRevision: 0,
      integrationBranch: null,
      integrationWorktreePath: null,
      integrationSha: "sha:base",
      verifiedSha: null,
      createdAt: input.createdAt,
      updatedAt: input.createdAt,
    });
    yield* input.goals.activateGraph({
      goalId: input.goalId,
      expectedRevision: 0,
      graph: {
        id: input.graphVersionId,
        goalId: input.goalId,
        revision: 1,
        publishedByNodeId: canonicalGoalLeadPublisherId(rootThreadId),
        nodes: graph.nodes,
        edges: graph.edges,
        createdAt: input.createdAt,
      },
    });
  });
}

it.layer(TestLayer)("GoalScheduler durable leasing", (it) => {
  it.effect("leases a ready node exactly once through the event/projection transaction", () =>
    Effect.gen(function* () {
      const goals = yield* GoalProjectionStore;
      const scheduler = yield* GoalScheduler;
      const sink = yield* EventSinkV2;
      const ids = yield* IdAllocatorV2;
      const goalId = GoalId.make("goal:scheduler-integration");
      const graphVersionId = GoalGraphVersionId.make("graph:scheduler-integration");
      const rootThreadId = ThreadId.make("thread:goal:scheduler-integration");
      const createdAt = "2026-07-12T00:00:00.000Z";
      yield* goals.create({
        id: goalId,
        projectId: ProjectId.make("project:scheduler-integration"),
        objective: "schedule work",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:source:scheduler-integration"),
        rootThreadId,
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: "sha:base",
        verifiedSha: null,
        createdAt,
        updatedAt: createdAt,
      });
      const node = {
        id: GoalNodeId.make("node:worker"),
        role: "worker",
        persona: "implementer",
        objective: "do work",
        successCriteria: ["done"],
        contextPacket: {
          schemaVersion: 1,
          digest: null,
          objective: "do work",
          artifacts: [],
          dependencyOutputs: [],
          notes: [],
        },
        outputContract: {
          kind: "commit" as const,
          description: "commit",
          requiredFields: [],
        },
        requiredCapabilities: ["tools.shell"],
        workspaceMode: "writer" as const,
        routingRequest: {
          type: "exact" as const,
          providerInstanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        evidenceRequirements: [],
        policy,
      };
      const verifier = schedulerVerifierNode(GoalNodeId.make("node:verifier"));
      yield* goals.activateGraph({
        goalId,
        expectedRevision: 0,
        graph: {
          id: graphVersionId,
          goalId,
          revision: 1,
          publishedByNodeId: canonicalGoalLeadPublisherId(rootThreadId),
          nodes: [node, verifier],
          edges: [
            {
              id: GoalEdgeId.make("edge:worker-verifier"),
              fromNodeId: node.id,
              toNodeId: verifier.id,
            },
          ],
          createdAt: "2026-07-12T00:00:01.000Z",
        },
      });

      const first = yield* scheduler.tick;
      assert.equal(first.leasedAttempts.length, 1);
      const afterFirst = yield* goals.getDetail(goalId);
      assert.equal(afterFirst.goal.status, "running");
      assert.equal(
        afterFirst.nodes.find((candidate) => candidate.node.id === node.id)?.status,
        "running",
      );
      assert.equal(afterFirst.attempts[0]?.status, "leased");
      assert.equal(afterFirst.attempts[0]?.resolvedRoute?.providerInstanceId, "codex");
      assert.equal(afterFirst.attempts[0]?.baseIntegrationSha, "sha:base");
      const leasedAttempt = afterFirst.attempts[0]!;
      const now = yield* DateTime.now;
      const transitionCommandId = CommandId.make("goal-attempt-cas:commit");
      const transitioned = {
        ...leasedAttempt,
        status: "launching" as const,
        updatedAt: DateTime.formatIso(now),
      };
      const transition = {
        id: yield* ids.allocate.event({ threadId: rootThreadId, commandId: transitionCommandId }),
        threadId: rootThreadId,
        type: "goal.attempt-transitioned" as const,
        payload: transitioned,
        occurredAt: now,
      };
      const committed = yield* sink.commitGoalAttemptCommand({
        commandId: transitionCommandId,
        threadId: rootThreadId,
        commandType: "test.attempt-cas",
        acceptedAt: now,
        goalId,
        graphVersionId,
        nodeId: node.id,
        attemptId: leasedAttempt.id,
        expectedStatuses: ["leased"],
        events: [transition],
        effects: [],
      });
      assert.isTrue(committed.committed);
      const stale = yield* sink.commitGoalAttemptCommand({
        commandId: CommandId.make("goal-attempt-cas:stale"),
        threadId: rootThreadId,
        commandType: "test.attempt-cas",
        acceptedAt: now,
        goalId,
        graphVersionId,
        nodeId: node.id,
        attemptId: leasedAttempt.id,
        expectedStatuses: ["leased"],
        events: [
          {
            ...transition,
            id: yield* ids.allocate.event({
              threadId: rootThreadId,
              commandId: CommandId.make("goal-attempt-cas:stale"),
            }),
          },
        ],
        effects: [],
      });
      assert.isTrue(stale.stale);

      const second = yield* scheduler.tick;
      assert.equal(second.leasedAttempts.length, 0);
      assert.equal((yield* goals.getDetail(goalId)).attempts.length, 1);

      // A new graph revision must not orphan a running node from the prior
      // immutable graph. The lead can still supersede it under a node CAS.
      const replacementGraphVersionId = GoalGraphVersionId.make(
        "graph:scheduler-integration:replacement",
      );
      const replacementNode = {
        ...node,
        id: GoalNodeId.make("node:replacement"),
      };
      const replacementVerifier = schedulerVerifierNode(
        GoalNodeId.make("node:replacement-verifier"),
      );
      yield* goals.activateGraph({
        goalId,
        expectedRevision: 1,
        graph: {
          id: replacementGraphVersionId,
          goalId,
          revision: 2,
          publishedByNodeId: canonicalGoalLeadPublisherId(rootThreadId),
          nodes: [replacementNode, replacementVerifier],
          edges: [
            {
              id: GoalEdgeId.make("edge:replacement-writer-verifier"),
              fromNodeId: replacementNode.id,
              toNodeId: replacementVerifier.id,
            },
          ],
          createdAt: "2026-07-12T00:01:00.000Z",
        },
      });
      const historicalNode = (yield* goals.getDetail(goalId)).nodes.find(
        (candidate) => candidate.graphVersionId === graphVersionId && candidate.node.id === node.id,
      );
      assert.isTrue(historicalNode !== undefined);
      if (historicalNode === undefined) return;
      const historicalCommandId = CommandId.make("goal-node-cas:historical-supersede");
      const historicalNow = yield* DateTime.now;
      const historical = yield* sink.commitGoalNodeCommand({
        commandId: historicalCommandId,
        threadId: rootThreadId,
        commandType: "test.historical-node-cas",
        acceptedAt: historicalNow,
        goalId,
        graphVersionId,
        nodeId: node.id,
        expectedStatuses: ["running"],
        events: [
          {
            id: yield* ids.allocate.event({
              threadId: rootThreadId,
              commandId: historicalCommandId,
            }),
            threadId: rootThreadId,
            type: "goal.node-cancellation-requested",
            payload: {
              ...historicalNode,
              status: "superseded",
              blocker: "replacement graph accepted",
              updatedAt: DateTime.formatIso(historicalNow),
            },
            occurredAt: historicalNow,
          },
        ],
        effects: [],
      });
      assert.isTrue(historical.committed);
      const afterHistoricalCancellation = yield* goals.getDetail(goalId);
      assert.equal(
        afterHistoricalCancellation.goal.currentGraphVersionId,
        replacementGraphVersionId,
      );
      assert.equal(
        afterHistoricalCancellation.nodes.find(
          (candidate) =>
            candidate.graphVersionId === graphVersionId && candidate.node.id === node.id,
        )?.status,
        "superseded",
      );
    }),
  );
});

it.layer(CapacityTestLayer)("GoalScheduler global capacity", (it) => {
  it.effect("keeps paused reader attempts in the global worker capacity", () =>
    Effect.gen(function* () {
      const goals = yield* GoalProjectionStore;
      const scheduler = yield* GoalScheduler;
      const pausedGoalId = GoalId.make("goal:scheduler-paused-capacity");
      const pausedGraphVersionId = GoalGraphVersionId.make("graph:scheduler-paused-capacity");
      const pausedNodes = [
        schedulerNode(GoalNodeId.make("node:paused-reader-a")),
        schedulerNode(GoalNodeId.make("node:paused-reader-b")),
        schedulerNode(GoalNodeId.make("node:paused-completion-writer"), "writer"),
      ];
      yield* createGoalWithGraph({
        goals,
        goalId: pausedGoalId,
        graphVersionId: pausedGraphVersionId,
        nodes: pausedNodes,
        createdAt: "2026-07-12T00:00:00.000Z",
      });
      assert.equal((yield* scheduler.tick).leasedAttempts.length, 2);
      const pausedDetail = yield* goals.getDetail(pausedGoalId);
      yield* goals.apply({
        type: "goal.updated",
        payload: {
          ...pausedDetail.goal,
          status: "paused",
          updatedAt: "2026-07-12T00:00:02.000Z",
        },
      });
      assert.isTrue(
        (yield* goals.listSchedulable).some((detail) => detail.goal.id === pausedGoalId),
      );

      const readyGoalId = GoalId.make("goal:scheduler-ready-after-pause");
      const readyGraphVersionId = GoalGraphVersionId.make("graph:scheduler-ready-after-pause");
      const readyNode = schedulerNode(GoalNodeId.make("node:ready-reader"));
      yield* createGoalWithGraph({
        goals,
        goalId: readyGoalId,
        graphVersionId: readyGraphVersionId,
        nodes: [
          readyNode,
          schedulerNode(GoalNodeId.make("node:ready-completion-writer"), "writer"),
        ],
        createdAt: "2026-07-12T00:00:03.000Z",
      });
      const blockedByPausedWorkers = yield* scheduler.tick;
      assert.equal(blockedByPausedWorkers.leasedAttempts.length, 0);
      assert.isTrue(
        blockedByPausedWorkers.plan.transitions.some(
          (transition) => transition.nodeId === readyNode.id && transition.reason === "capacity",
        ),
      );
      assert.equal((yield* goals.getDetail(readyGoalId)).nodes[0]?.status, "queued");
    }),
  );
});

it.layer(CapacityTestLayer)("GoalScheduler global writer capacity", (it) => {
  it.effect("keeps blocked writer attempts in the global writer capacity", () =>
    Effect.gen(function* () {
      const goals = yield* GoalProjectionStore;
      const scheduler = yield* GoalScheduler;
      const blockedGoalId = GoalId.make("goal:scheduler-blocked-writer");
      const blockedGraphVersionId = GoalGraphVersionId.make("graph:scheduler-blocked-writer");
      const blockedWriter = schedulerNode(GoalNodeId.make("node:blocked-writer"), "writer");
      yield* createGoalWithGraph({
        goals,
        goalId: blockedGoalId,
        graphVersionId: blockedGraphVersionId,
        nodes: [blockedWriter],
        createdAt: "2026-07-12T00:01:00.000Z",
      });
      assert.equal((yield* scheduler.tick).leasedAttempts.length, 1);
      const blockedDetail = yield* goals.getDetail(blockedGoalId);
      yield* goals.apply({
        type: "goal.updated",
        payload: {
          ...blockedDetail.goal,
          status: "blocked",
          updatedAt: "2026-07-12T00:01:02.000Z",
        },
      });
      assert.isTrue(
        (yield* goals.listSchedulable).some((detail) => detail.goal.id === blockedGoalId),
      );

      const readyGoalId = GoalId.make("goal:scheduler-ready-after-block");
      const readyGraphVersionId = GoalGraphVersionId.make("graph:scheduler-ready-after-block");
      const readyWriter = schedulerNode(GoalNodeId.make("node:ready-writer"), "writer");
      yield* createGoalWithGraph({
        goals,
        goalId: readyGoalId,
        graphVersionId: readyGraphVersionId,
        nodes: [readyWriter],
        createdAt: "2026-07-12T00:01:03.000Z",
      });
      const blockedByWriter = yield* scheduler.tick;
      assert.equal(blockedByWriter.leasedAttempts.length, 0);
      assert.isTrue(
        blockedByWriter.plan.transitions.some(
          (transition) =>
            transition.nodeId === readyWriter.id && transition.reason === "writer_capacity",
        ),
      );
      assert.equal((yield* goals.getDetail(readyGoalId)).nodes[0]?.status, "queued");
    }),
  );
});
