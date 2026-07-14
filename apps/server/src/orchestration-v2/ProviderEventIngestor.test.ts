import { assert, it } from "@effect/vitest";
import {
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { EventSinkV2, layer as eventSinkLayer } from "./EventSink.ts";
import { EventStoreV2, layer as eventStoreLayer } from "./EventStore.ts";
import {
  IdAllocatorV2,
  type IdAllocatorV2Error,
  layer as idAllocatorLayer,
} from "./IdAllocator.ts";
import type {
  ProviderAdapterV2RuntimeRequestResponseInput,
  ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { ProjectionStoreV2, layer as projectionStoreLayer } from "./ProjectionStore.ts";
import {
  ProviderEventIngestorV2,
  ProviderRuntimeRequestBindingAmbiguousError,
  ProviderRuntimeRequestBindingMissingError,
  resolveRuntimeRequestThreadBinding,
  layer as providerEventIngestorLayer,
} from "./ProviderEventIngestor.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import {
  RuntimeRequestServiceV2,
  layer as runtimeRequestServiceLayer,
} from "./RuntimeRequestService.ts";

const TestDatabaseLayer = SqlitePersistenceMemory;
const TestStoresLayer = Layer.merge(eventStoreLayer, projectionStoreLayer).pipe(
  Layer.provide(TestDatabaseLayer),
);

const TestEventSinkLayer = eventSinkLayer.pipe(
  Layer.provide(Layer.mergeAll(TestStoresLayer, TestDatabaseLayer)),
);

const TestLayer = Layer.mergeAll(
  TestStoresLayer,
  TestEventSinkLayer,
  idAllocatorLayer,
  providerEventIngestorLayer.pipe(
    Layer.provide(Layer.mergeAll(TestStoresLayer, TestEventSinkLayer, idAllocatorLayer)),
  ),
);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

function threadCreatedEvent(
  now: DateTime.Utc,
): Effect.Effect<OrchestrationV2DomainEvent, IdAllocatorV2Error, IdAllocatorV2> {
  return Effect.gen(function* () {
    const idAllocator = yield* IdAllocatorV2;
    const projectId = yield* idAllocator.allocate.project({
      fixtureName: "provider-event-ingestor",
    });
    const threadId = yield* idAllocator.allocate.thread({
      fixtureName: "provider-event-ingestor",
      projectId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Provider event ingestor",
      providerInstanceId: modelSelection.instanceId,
      modelSelection: modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
    };

    return {
      id: yield* idAllocator.allocate.event({ threadId }),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    };
  });
}

const layer = it.layer(TestLayer);

layer("ProviderEventIngestorV2", (it) => {
  it.effect("normalizes provider events through the real event log and projection store", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSinkV2;
      const eventStore = yield* EventStoreV2;
      const projectionStore = yield* ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThread: OrchestrationV2ProviderThread = {
        id: idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: "native-thread",
        }),
        driver: CODEX_DRIVER,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId,
        appThreadId: threadEvent.threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: CODEX_DRIVER,
          nativeId: "native-thread",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      const storedEvents = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread,
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const storedDomainEvents = yield* eventStore.read({}).pipe(Stream.runCollect);
      const afterFirstEvent = yield* eventStore
        .read({ afterSequence: 1, threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const latestThreadSequence = yield* eventStore.latestSequence({
        threadId: threadEvent.threadId,
      });

      assert.equal(storedEvents.length, 1);
      assert.equal(storedEvents[0]?.event.type, "provider-thread.updated");
      assert.deepEqual(
        projection.providerThreads.map((thread) => thread.id),
        [providerThread.id],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.event.type),
        ["thread.created", "provider-thread.updated"],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.sequence),
        [1, 2],
      );
      assert.deepEqual(
        Array.from(afterFirstEvent).map((stored) => stored.event.type),
        ["provider-thread.updated"],
      );
      assert.equal(latestThreadSequence, 2);
    }),
  );

  it.effect(
    "treats successful provider terminal markers as non-persisted orchestration control signals",
    () =>
      Effect.gen(function* () {
        const ingestor = yield* ProviderEventIngestorV2;
        const idAllocator = yield* IdAllocatorV2;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-event-terminal",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-event-terminal",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const normalized = yield* ingestor.normalize({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event: {
            type: "turn.terminal",
            driver: CODEX_DRIVER,
            providerThreadId: idAllocator.derive.providerThread({
              driver: CODEX_DRIVER,
              nativeThreadId: "native-thread",
            }),
            providerTurnId: idAllocator.derive.providerTurn({
              driver: CODEX_DRIVER,
              nativeTurnId: "native-turn",
            }),
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        });

        assert.deepEqual(normalized, []);
      }),
  );

  it.effect("persists a failed provider terminal as one expected error item", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSinkV2;
      const projectionStore = yield* ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-failed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-failed",
      });

      yield* eventSink.write({ events: [threadEvent] });
      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId,
          providerTurnId,
          runOrdinal: 1,
          failureItemOrdinal: 102,
          status: "failed",
          failure: makeProviderFailure({
            message: "Invalid reasoning effort.",
            code: "invalid_request",
            class: "validation_error",
          }),
          threadDisposition: "reusable",
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const errorItems = projection.visibleTurnItems.filter(
        (candidate) => candidate.item.type === "error",
      );

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(errorItems.length, 1);
      const errorItem = errorItems[0]?.item;
      assert.equal(errorItem?.type, "error");
      if (errorItem?.type !== "error") return;
      assert.equal(errorItem.failure.message, "Invalid reasoning effort.");
      assert.equal(errorItem.failure.code, "invalid_request");
      assert.equal(errorItem.providerThreadId, providerThreadId);
      assert.equal(errorItem.providerTurnId, providerTurnId);
    }),
  );

  it.effect("routes provider-owned child artifacts to their child app thread", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const ingestor = yield* ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-subagent-thread",
      });
      const childRootNodeId = NodeId.make("node:subagent-root");
      const childThread: OrchestrationV2AppThread = {
        ...rootEvent.payload,
        id: childThreadId,
        title: "inspect package",
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: rootEvent.threadId,
          relationshipToParent: "subagent",
          rootThreadId: rootEvent.threadId,
        },
        forkedFrom: {
          type: "node",
          nodeId: NodeId.make("node:parent-subagent"),
        },
      };
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });

      const threadEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "app_thread.created",
          driver: CODEX_DRIVER,
          appThread: childThread,
        },
      });
      const messageEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "message.updated",
          driver: CODEX_DRIVER,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make("message:subagent-response"),
            threadId: childThreadId,
            runId: null,
            nodeId: childRootNodeId,
            role: "assistant",
            text: "Subagent result",
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
      });

      assert.equal(threadEvents[0]?.type, "thread.created");
      assert.equal(threadEvents[0]?.threadId, childThreadId);
      assert.equal(messageEvents[0]?.type, "message.updated");
      assert.equal(messageEvents[0]?.threadId, childThreadId);
    }),
  );

  it.effect("routes runtime requests through provider turn and thread bindings", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSinkV2;
      const projectionStore = yield* ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = ProviderThreadId.make("provider-thread:runtime-request:root");
      const providerTurnId = ProviderTurnId.make("provider-turn:runtime-request:root");
      const requestId = RuntimeRequestId.make("runtime-request:root");
      const rawProviderThreadId = ThreadId.make("thread:provider:codex:native-thread:root");
      const runtimeRequest = {
        id: requestId,
        nodeId: NodeId.make("node:runtime-request:root"),
        providerTurnId,
        nativeRequestRef: null,
        kind: "command" as const,
        status: "pending" as const,
        responseCapability: { type: "live" as const, providerSessionId },
        createdAt: now,
        resolvedAt: null,
      };

      yield* eventSink.write({ events: [threadEvent] });
      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runtimeRequestRouting: {
          providerThreadIdsByProviderTurnId: new Map([
            [providerTurnId, new Set([providerThreadId])],
          ]),
          appThreadIdsByProviderThreadId: new Map([
            [providerThreadId, new Set([threadEvent.threadId])],
          ]),
        },
        event: {
          type: "runtime_request.updated",
          driver: CODEX_DRIVER,
          threadId: rawProviderThreadId,
          runtimeRequest,
        },
      });

      assert.equal(stored[0]?.event.threadId, threadEvent.threadId);
      assert.notEqual(stored[0]?.event.threadId, rawProviderThreadId);
      assert.deepEqual(stored[0]?.event.payload, runtimeRequest);
      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      assert.equal(projection.runtimeRequests[0]?.id, requestId);
      assert.equal(projection.runtimeRequests[0]?.providerTurnId, providerTurnId);
      assert.deepEqual(
        projection.runtimeRequests
          .filter((request) => request.status === "pending")
          .map((request) => request.id),
        [requestId],
      );

      const deliveredResponses = yield* Ref.make<
        ReadonlyArray<ProviderAdapterV2RuntimeRequestResponseInput>
      >([]);
      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: modelSelection.instanceId,
        driver: CODEX_DRIVER,
        providerSessionId,
        providerSession: {
          id: providerSessionId,
          driver: CODEX_DRIVER,
          providerInstanceId: modelSelection.instanceId,
          status: "ready",
          cwd: process.cwd(),
          model: modelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
        events: Stream.empty,
        ensureThread: () => Effect.die("unused ensureThread"),
        resumeThread: () => Effect.die("unused resumeThread"),
        startTurn: () => Effect.die("unused startTurn"),
        steerTurn: () => Effect.die("unused steerTurn"),
        interruptTurn: () => Effect.die("unused interruptTurn"),
        respondToRuntimeRequest: (response: ProviderAdapterV2RuntimeRequestResponseInput) =>
          Ref.update(deliveredResponses, (current) => [...current, response]),
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
      const sessionManagerLayer = Layer.succeed(
        ProviderSessionManagerV2,
        ProviderSessionManagerV2.of({
          shutdown: Effect.void,
          open: () => Effect.die("unused open"),
          get: (requestedSessionId) =>
            Effect.succeed(
              requestedSessionId === providerSessionId ? Option.some(runtime) : Option.none(),
            ),
          close: () => Effect.void,
          release: () => Effect.void,
          detach: () => Effect.void,
        }),
      );
      const responseServiceLayer = runtimeRequestServiceLayer.pipe(
        Layer.provide(
          Layer.merge(Layer.succeed(ProjectionStoreV2, projectionStore), sessionManagerLayer),
        ),
      );
      yield* Effect.gen(function* () {
        const runtimeRequests = yield* RuntimeRequestServiceV2;
        yield* runtimeRequests.respond({
          threadId: threadEvent.threadId,
          providerSessionId,
          requestId,
          decision: "accept",
        });
      }).pipe(Effect.provide(responseServiceLayer));
      assert.deepEqual(yield* Ref.get(deliveredResponses), [{ requestId, decision: "accept" }]);
    }),
  );

  it.effect("rejects missing and ambiguous runtime request bindings before persistence", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventStore = yield* EventStoreV2;
      const ingestor = yield* ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocatorV2;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-event-runtime-request-binding",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-event-runtime-request-binding",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerTurnId = ProviderTurnId.make("provider-turn:runtime-request:binding-error");
      const providerThreadId = ProviderThreadId.make(
        "provider-thread:runtime-request:binding-error",
      );
      const otherProviderThreadId = ProviderThreadId.make(
        "provider-thread:runtime-request:binding-error:other",
      );
      const appThreadA = ThreadId.make("thread:runtime-request:ambiguous:a");
      const appThreadB = ThreadId.make("thread:runtime-request:ambiguous:b");
      const runtimeRequest = {
        id: RuntimeRequestId.make("runtime-request:binding-error"),
        nodeId: NodeId.make("node:runtime-request:binding-error"),
        providerTurnId,
        nativeRequestRef: null,
        kind: "command" as const,
        status: "pending" as const,
        responseCapability: { type: "live" as const, providerSessionId },
        createdAt: now,
        resolvedAt: null,
      };
      const event = {
        type: "runtime_request.updated" as const,
        driver: CODEX_DRIVER,
        runtimeRequest,
      };
      const before = yield* eventStore.latestSequence();

      const resolutionCases = [
        {
          providerTurnId: null,
          routing: undefined,
          expected: { type: "missing" as const, binding: "provider_turn" as const },
        },
        {
          providerTurnId,
          routing: {
            providerThreadIdsByProviderTurnId: new Map(),
            appThreadIdsByProviderThreadId: new Map(),
          },
          expected: { type: "missing" as const, binding: "provider_thread" as const },
        },
        {
          providerTurnId,
          routing: {
            providerThreadIdsByProviderTurnId: new Map([
              [providerTurnId, new Set([providerThreadId, otherProviderThreadId])],
            ]),
            appThreadIdsByProviderThreadId: new Map(),
          },
          expected: {
            type: "ambiguous" as const,
            binding: "provider_thread" as const,
            candidateIds: [providerThreadId, otherProviderThreadId],
          },
        },
        {
          providerTurnId,
          routing: {
            providerThreadIdsByProviderTurnId: new Map([
              [providerTurnId, new Set([providerThreadId])],
            ]),
            appThreadIdsByProviderThreadId: new Map(),
          },
          expected: { type: "missing" as const, binding: "app_thread" as const },
        },
        {
          providerTurnId,
          routing: {
            providerThreadIdsByProviderTurnId: new Map([
              [providerTurnId, new Set([providerThreadId])],
            ]),
            appThreadIdsByProviderThreadId: new Map([
              [providerThreadId, new Set([appThreadA, appThreadB])],
            ]),
          },
          expected: {
            type: "ambiguous" as const,
            binding: "app_thread" as const,
            candidateIds: [appThreadA, appThreadB],
          },
        },
      ];

      for (const resolutionCase of resolutionCases) {
        const failure = yield* Effect.flip(
          resolveRuntimeRequestThreadBinding({
            providerSessionId,
            requestId: runtimeRequest.id,
            providerTurnId: resolutionCase.providerTurnId,
            ...(resolutionCase.routing === undefined ? {} : { routing: resolutionCase.routing }),
          }),
        );
        if (resolutionCase.expected.type === "missing") {
          assert.isTrue(Schema.is(ProviderRuntimeRequestBindingMissingError)(failure));
          if (Schema.is(ProviderRuntimeRequestBindingMissingError)(failure)) {
            assert.equal(failure.missingBinding, resolutionCase.expected.binding);
          }
        } else {
          assert.isTrue(Schema.is(ProviderRuntimeRequestBindingAmbiguousError)(failure));
          if (Schema.is(ProviderRuntimeRequestBindingAmbiguousError)(failure)) {
            assert.equal(failure.ambiguousBinding, resolutionCase.expected.binding);
            assert.deepEqual(failure.candidateIds, resolutionCase.expected.candidateIds);
          }
        }
      }

      const missing = yield* Effect.flip(
        ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event,
        }),
      );
      assert.isTrue(Schema.is(ProviderRuntimeRequestBindingMissingError)(missing));
      if (Schema.is(ProviderRuntimeRequestBindingMissingError)(missing)) {
        assert.equal(missing.missingBinding, "provider_thread");
      }

      const ambiguous = yield* Effect.flip(
        ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          runtimeRequestRouting: {
            providerThreadIdsByProviderTurnId: new Map([
              [providerTurnId, new Set([providerThreadId])],
            ]),
            appThreadIdsByProviderThreadId: new Map([
              [providerThreadId, new Set([appThreadA, appThreadB])],
            ]),
          },
          event,
        }),
      );
      assert.isTrue(Schema.is(ProviderRuntimeRequestBindingAmbiguousError)(ambiguous));
      if (Schema.is(ProviderRuntimeRequestBindingAmbiguousError)(ambiguous)) {
        assert.equal(ambiguous.ambiguousBinding, "app_thread");
        assert.lengthOf(ambiguous.candidateIds, 2);
      }
      assert.equal(yield* eventStore.latestSequence(), before);
    }),
  );
});
