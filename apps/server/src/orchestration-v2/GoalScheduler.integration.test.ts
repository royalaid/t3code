import { assert, it } from "@effect/vitest";
import {
  CommandId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { EventSinkV2, layer as eventSinkLayer } from "./EventSink.ts";
import { layer as eventStoreLayer } from "./EventStore.ts";
import { GoalProjectionStore, layer as goalProjectionStoreLayer } from "./GoalProjectionStore.ts";
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

const policy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};

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
          kind: "structured_result" as const,
          description: "report",
          requiredFields: [],
        },
        requiredCapabilities: ["tools.shell"],
        workspaceMode: "read_only" as const,
        routingRequest: {
          type: "exact" as const,
          providerInstanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        evidenceRequirements: [],
        policy,
      };
      yield* goals.activateGraph({
        goalId,
        expectedRevision: 0,
        graph: {
          id: graphVersionId,
          goalId,
          revision: 1,
          publishedByNodeId: GoalNodeId.make("lead"),
          nodes: [node],
          edges: [],
          createdAt: "2026-07-12T00:00:01.000Z",
        },
      });

      const first = yield* scheduler.tick;
      assert.equal(first.leasedAttempts.length, 1);
      const afterFirst = yield* goals.getDetail(goalId);
      assert.equal(afterFirst.goal.status, "running");
      assert.equal(afterFirst.nodes[0]?.status, "running");
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
    }),
  );
});
