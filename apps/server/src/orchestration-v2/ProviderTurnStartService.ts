import {
  CommandId,
  type GoalDetail,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type ProviderSessionId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EventSinkV2, type EventSinkV2Shape } from "./EventSink.ts";
import {
  ContextHandoffServiceV2,
  providerMessageWithContextHandoffs,
} from "./ContextHandoffService.ts";
import { IdAllocatorV2, type IdAllocatorV2AllocateShape } from "./IdAllocator.ts";
import { GoalProjectionStore, type GoalProjectionStoreShape } from "./GoalProjectionStore.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RunExecutionServiceV2 } from "./RunExecutionService.ts";
import {
  GoalRuntimePolicyResolveError,
  isGoalRuntimePolicyResolveError,
  resolveGoalWorkerRuntimePolicy,
  RuntimePolicyV2,
} from "./RuntimePolicy.ts";

export class ProviderTurnStartError extends Schema.TaggedErrorClass<ProviderTurnStartError>()(
  "ProviderTurnStartError",
  {
    runId: RunId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isProviderTurnStartError = Schema.is(ProviderTurnStartError);

const providerTurnStartError = (runId: RunId) => (cause: unknown) =>
  new ProviderTurnStartError({ runId, cause });

type GoalWorkerContextInput = {
  readonly goals: Pick<GoalProjectionStoreShape, "getDetail" | "resolveMcpBinding">;
  readonly threadId: ThreadId;
  readonly run: Pick<OrchestrationV2Run, "id" | "modelSelection">;
};

type GoalWorkerContext = {
  readonly detail: GoalDetail;
  readonly attempt: GoalDetail["attempts"][number];
  readonly node: GoalDetail["nodes"][number];
};

const resolveGoalWorkerContext = Effect.fn("ProviderTurnStartService.resolveGoalWorkerContext")(
  function* (input: GoalWorkerContextInput) {
    const binding = yield* input.goals
      .resolveMcpBinding(input.threadId)
      .pipe(Effect.mapError(providerTurnStartError(input.run.id)));
    if (binding?.kind !== "worker") return null;

    const detail = yield* input.goals
      .getDetail(binding.goalId)
      .pipe(Effect.mapError(providerTurnStartError(input.run.id)));
    const attempt = detail.attempts.find((candidate) => candidate.id === binding.attemptId);
    const node =
      attempt === undefined
        ? undefined
        : detail.nodes.find(
            (candidate) =>
              candidate.graphVersionId === attempt.graphVersionId &&
              candidate.node.id === attempt.nodeId,
          );
    if (
      attempt === undefined ||
      node === undefined ||
      attempt.executionThreadId !== input.threadId ||
      (attempt.runId !== null && attempt.runId !== input.run.id) ||
      attempt.resolvedRoute === null ||
      attempt.resolvedRoute.providerInstanceId !== input.run.modelSelection.instanceId ||
      attempt.resolvedRoute.model !== input.run.modelSelection.model ||
      node.activeAttemptId !== attempt.id ||
      node.status === "cancelled" ||
      node.status === "superseded"
    ) {
      return yield* new ProviderTurnStartError({
        runId: input.run.id,
        cause: "Goal worker run is not bound to its active durable attempt and node.",
      });
    }
    return { detail, attempt, node } satisfies GoalWorkerContext;
  },
);

function resolveGoalWorkerRuntimePolicyFromContext(input: {
  readonly context: GoalWorkerContext | null;
  readonly run: GoalWorkerContextInput["run"];
  readonly inherited: import("./ProviderAdapter.ts").ProviderAdapterV2RuntimePolicy;
}): Effect.Effect<
  import("./ProviderAdapter.ts").ProviderAdapterV2RuntimePolicy,
  ProviderTurnStartError
> {
  if (input.context === null) return Effect.succeed(input.inherited);
  return resolveGoalWorkerRuntimePolicy({
    inherited: input.inherited,
    goalId: input.context.detail.goal.id,
    rootPolicy: input.context.detail.goal.policy,
    nodePolicy: input.context.node.node.policy,
    workspaceMode: input.context.node.node.workspaceMode,
    workspacePath: input.context.attempt.workspacePath,
    prohibitedWorkspacePaths: [
      input.context.detail.goal.repositoryRoot,
      input.context.detail.goal.sourceWorkspacePath,
      input.context.detail.goal.integrationWorktreePath,
    ].filter((path): path is string => path !== undefined && path !== null),
    providerInstanceId: input.run.modelSelection.instanceId,
  }).pipe(Effect.mapError(providerTurnStartError(input.run.id)));
}

/** Returns deterministic policy rejection that must terminalize its durable launch effect. */
export function terminalGoalPolicyFailureForProviderTurnStart(
  cause: unknown,
): GoalRuntimePolicyResolveError | undefined {
  if (!isProviderTurnStartError(cause) || !isGoalRuntimePolicyResolveError(cause.cause)) {
    return undefined;
  }
  return cause.cause;
}

/**
 * Goal worker threads are ordinary V2 threads at the provider boundary. Bind
 * them back to their active durable attempt before opening a provider session
 * so a provider receives the graph node's concrete, narrowed policy rather
 * than only the thread's coarse runtime mode.
 */
export function resolveGoalAttemptRuntimePolicy(
  input: GoalWorkerContextInput & {
    readonly inherited: import("./ProviderAdapter.ts").ProviderAdapterV2RuntimePolicy;
  },
): Effect.Effect<
  import("./ProviderAdapter.ts").ProviderAdapterV2RuntimePolicy,
  ProviderTurnStartError
> {
  return resolveGoalWorkerContext(input).pipe(
    Effect.flatMap((context) =>
      resolveGoalWorkerRuntimePolicyFromContext({
        context,
        run: input.run,
        inherited: input.inherited,
      }),
    ),
  );
}

type GoalWorkerProviderSessionBindingInput = {
  readonly context: GoalWorkerContext | null;
  readonly run: GoalWorkerContextInput["run"];
  readonly providerSessionId: ProviderSessionId;
  readonly allocateEvent: IdAllocatorV2AllocateShape["event"];
  readonly commitGoalAttemptCommand: EventSinkV2Shape["commitGoalAttemptCommand"];
};

const bindResolvedGoalWorkerProviderSession = Effect.fn(
  "ProviderTurnStartService.bindResolvedGoalWorkerProviderSession",
)(function* (input: GoalWorkerProviderSessionBindingInput) {
  const { context } = input;
  if (context === null || context.attempt.providerSessionId === input.providerSessionId) return;
  if (context.attempt.providerSessionId !== null) {
    return yield* new ProviderTurnStartError({
      runId: input.run.id,
      cause: "Goal worker attempt is already bound to a different provider session.",
    });
  }

  const now = yield* DateTime.now;
  const commandId = CommandId.make(
    `goal-attempt-provider-session:${context.attempt.id}:${input.providerSessionId}`,
  );
  const event = {
    id: yield* input
      .allocateEvent({ threadId: context.detail.goal.rootThreadId, commandId })
      .pipe(Effect.mapError(providerTurnStartError(input.run.id))),
    threadId: context.detail.goal.rootThreadId,
    type: "goal.attempt-transitioned" as const,
    payload: {
      ...context.attempt,
      providerSessionId: input.providerSessionId,
      updatedAt: DateTime.formatIso(now),
    },
    occurredAt: now,
  } satisfies OrchestrationV2DomainEvent;
  const committed = yield* input
    .commitGoalAttemptCommand({
      commandId,
      threadId: context.detail.goal.rootThreadId,
      commandType: "goal.attempt.bind-provider-session",
      acceptedAt: now,
      goalId: context.attempt.goalId,
      graphVersionId: context.attempt.graphVersionId,
      nodeId: context.attempt.nodeId,
      attemptId: context.attempt.id,
      expectedStatuses: [context.attempt.status],
      expectedNodeStatuses: [context.node.status],
      events: [event],
      effects: [],
    })
    .pipe(Effect.mapError(providerTurnStartError(input.run.id)));
  if (!committed.committed) {
    return yield* new ProviderTurnStartError({
      runId: input.run.id,
      cause: "Goal worker provider-session binding lost its active-attempt fence.",
    });
  }
});

/** Durably bind the worker credential to its exact provider session before adapter startup. */
export function bindGoalWorkerProviderSession(
  input: GoalWorkerContextInput & {
    readonly providerSessionId: ProviderSessionId;
    readonly allocateEvent: IdAllocatorV2AllocateShape["event"];
    readonly commitGoalAttemptCommand: EventSinkV2Shape["commitGoalAttemptCommand"];
  },
): Effect.Effect<void, ProviderTurnStartError> {
  return resolveGoalWorkerContext(input).pipe(
    Effect.flatMap((context) => bindResolvedGoalWorkerProviderSession({ ...input, context })),
  );
}

export interface ProviderTurnStartServiceV2Shape {
  readonly start: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<void, ProviderTurnStartError>;
}

export class ProviderTurnStartServiceV2 extends Context.Service<
  ProviderTurnStartServiceV2,
  ProviderTurnStartServiceV2Shape
>()("t3/orchestration-v2/ProviderTurnStartService/ProviderTurnStartServiceV2") {}

export const layer: Layer.Layer<
  ProviderTurnStartServiceV2,
  never,
  | EventSinkV2
  | ContextHandoffServiceV2
  | GoalProjectionStore
  | IdAllocatorV2
  | ProjectionStoreV2
  | ProviderSessionManagerV2
  | RunExecutionServiceV2
  | RuntimePolicyV2
> = Layer.effect(
  ProviderTurnStartServiceV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const contextHandoffService = yield* ContextHandoffServiceV2;
    const goals = yield* GoalProjectionStore;
    const idAllocator = yield* IdAllocatorV2;
    const projectionStore = yield* ProjectionStoreV2;
    const providerSessions = yield* ProviderSessionManagerV2;
    const runExecution = yield* RunExecutionServiceV2;
    const runtimePolicy = yield* RuntimePolicyV2;

    const start = Effect.fn("orchestrationV2.providerTurnStart.start")(function* (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }) {
      const { runId } = input;
      const projection = yield* projectionStore.getThreadProjection(input.threadId);
      const run = projection.runs.find((candidate) => candidate.id === runId);
      if (run === undefined) {
        return yield* new ProviderTurnStartError({ runId, cause: `Run ${runId} was not found.` });
      }
      if (run.status !== "starting") {
        // The effect is idempotent once the run has advanced or terminalized.
        return;
      }
      const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === run.providerThreadId,
      );
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      const checkpointScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === rootNode?.checkpointScopeId,
      );
      const handoffs = projection.contextHandoffs.filter(
        (handoff) => handoff.targetRunId === run.id && handoff.status === "ready",
      );
      const nativeForkTransfer = projection.contextTransfers.find(
        (transfer) =>
          transfer.type === "fork" &&
          transfer.targetThreadId === input.threadId &&
          transfer.targetRunId === run.id &&
          transfer.status === "pending" &&
          transfer.resolution === null,
      );
      const existingResumeFallback = projection.contextTransfers.find(
        (transfer) =>
          transfer.type === "provider_handoff" &&
          transfer.sourceThreadId === projection.thread.id &&
          transfer.targetThreadId === projection.thread.id &&
          transfer.targetRunId === run.id &&
          transfer.status === "resolved_portable" &&
          transfer.resolution?.strategy === "portable_context",
      );
      if (
        rootNode === undefined ||
        attempt === undefined ||
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        message === undefined ||
        checkpointScope === undefined
      ) {
        return yield* new ProviderTurnStartError({
          runId,
          cause: `Run ${runId} is missing its execution projection state.`,
        });
      }
      const providerSessionId = providerThread.providerSessionId;
      const isCurrentAttemptInStatus = (
        expectedStatus: OrchestrationV2Run["status"],
      ): Effect.Effect<boolean, never> =>
        projectionStore.getThreadProjection(projection.thread.id).pipe(
          Effect.map((current) => {
            const currentRun = current.runs.find((candidate) => candidate.id === run.id);
            return (
              currentRun?.activeAttemptId === attempt.id && currentRun.status === expectedStatus
            );
          }),
          Effect.catchCause(() => Effect.succeed(false)),
        );

      const inheritedRuntimePolicy = yield* runtimePolicy.resolve({
        thread: projection.thread,
        modelSelection: run.modelSelection,
      });
      const goalWorkerContext = yield* resolveGoalWorkerContext({
        goals,
        threadId: projection.thread.id,
        run,
      });
      const resolvedRuntimePolicy = yield* resolveGoalWorkerRuntimePolicyFromContext({
        context: goalWorkerContext,
        run,
        inherited: inheritedRuntimePolicy,
      });
      yield* bindResolvedGoalWorkerProviderSession({
        context: goalWorkerContext,
        run,
        providerSessionId,
        allocateEvent: idAllocator.allocate.event,
        commitGoalAttemptCommand: eventSink.commitGoalAttemptCommand,
      });
      const existingSessionProjection = projection.providerSessions.find(
        (candidate) => candidate.id === providerSessionId,
      );
      const session = yield* providerSessions.open({
        threadId: projection.thread.id,
        providerSessionId,
        modelSelection: run.modelSelection,
        runtimePolicy: resolvedRuntimePolicy,
        ...(existingSessionProjection === undefined
          ? {}
          : { resumeFromSession: existingSessionProjection }),
      });
      let effectiveHandoffs = handoffs;
      const loadedProviderThread = yield* Effect.gen(function* () {
        if (nativeForkTransfer !== undefined) {
          const sourceProjection = yield* projectionStore.getThreadProjection(
            nativeForkTransfer.sourceThreadId,
          );
          const sourceRun = sourceProjection.runs.find(
            (candidate) => candidate.id === nativeForkTransfer.sourcePoint.runId,
          );
          const sourceProviderThread = sourceProjection.providerThreads.find(
            (candidate) => candidate.id === sourceRun?.providerThreadId,
          );
          const sourceAttempt = sourceProjection.attempts.find(
            (candidate) => candidate.id === sourceRun?.activeAttemptId,
          );
          const sourceProviderTurn = sourceProjection.providerTurns.find(
            (candidate) =>
              candidate.id === sourceAttempt?.providerTurnId ||
              candidate.runAttemptId === sourceAttempt?.id,
          );
          if (sourceRun === undefined || sourceProviderThread === undefined) {
            return yield* new ProviderTurnStartError({
              runId,
              cause: `Native fork transfer ${nativeForkTransfer.id} has no source provider execution.`,
            });
          }
          return yield* session.forkThread({
            sourceProviderThread,
            sourceProviderTurns: sourceProjection.providerTurns,
            targetThreadId: projection.thread.id,
            modelSelection: run.modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
            ...(sourceProviderTurn === undefined ? {} : { providerTurnId: sourceProviderTurn.id }),
          });
        }
        if (providerThread.nativeThreadRef === null) {
          return yield* session.ensureThread({
            threadId: projection.thread.id,
            modelSelection: run.modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
            providerSessionId,
          });
        }
        const resumed = yield* Effect.result(
          session.resumeThread({
            providerThread,
            threadId: projection.thread.id,
            modelSelection: run.modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
          }),
        );
        if (resumed._tag === "Success") {
          return resumed.success;
        }

        const replacement = yield* session.ensureThread({
          threadId: projection.thread.id,
          modelSelection: run.modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
          providerSessionId,
        });
        if (existingResumeFallback !== undefined) {
          return replacement;
        }
        const transferId = yield* idAllocator.allocate.contextTransfer({
          sourceThreadId: projection.thread.id,
          targetThreadId: projection.thread.id,
          type: "provider_resume_fallback",
        });
        const createdAt = yield* DateTime.now;
        const handoff = yield* contextHandoffService.prepareProviderHandoff({
          threadId: projection.thread.id,
          targetRunId: run.id,
          transferId,
          fromProviderThreadIds: [providerThread.id],
          toProviderThreadId: providerThread.id,
          fromProviderInstanceId: providerThread.providerInstanceId,
          toProviderInstanceId: run.providerInstanceId,
          coveredRunOrdinals: { from: 1, to: Math.max(1, run.ordinal - 1) },
          strategy: "full_thread_summary",
          items: projection.turnItems,
          createdAt,
        });
        effectiveHandoffs = [...handoffs, handoff];
        yield* eventSink.write({
          events: [
            {
              id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
              type: "context-handoff.updated",
              threadId: projection.thread.id,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: createdAt,
              payload: handoff,
            },
            {
              id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
              type: "context-transfer.updated",
              threadId: projection.thread.id,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: createdAt,
              payload: {
                id: transferId,
                type: "provider_handoff",
                sourceThreadId: projection.thread.id,
                targetThreadId: projection.thread.id,
                sourcePoint: { threadId: projection.thread.id },
                basePoint: null,
                sourceProviderInstanceId: providerThread.providerInstanceId,
                targetProviderInstanceId: run.providerInstanceId,
                targetRunId: run.id,
                status: "resolved_portable",
                resolution: { strategy: "portable_context", contextHandoffId: handoff.id },
                createdBy: "system",
                error: null,
                createdAt,
                updatedAt: createdAt,
                consumedAt: null,
              },
            },
          ],
        });
        return replacement;
      });
      if (!(yield* isCurrentAttemptInStatus("starting"))) {
        return;
      }
      const now = yield* DateTime.now;
      const runningProviderThread: OrchestrationV2ProviderThread = {
        ...loadedProviderThread,
        id: providerThread.id,
        driver: session.driver,
        providerInstanceId: run.providerInstanceId,
        providerSessionId,
        appThreadId: projection.thread.id,
        ownerNodeId: providerThread.ownerNodeId,
        firstRunOrdinal: providerThread.firstRunOrdinal ?? run.ordinal,
        lastRunOrdinal: run.ordinal,
        handoffIds: providerThread.handoffIds,
        forkedFrom: providerThread.forkedFrom,
        status: "active",
        createdAt: providerThread.createdAt,
        updatedAt: now,
      };
      const runningRun: OrchestrationV2Run = {
        ...run,
        status: "running",
        startedAt: now,
      };
      const runningAttempt: OrchestrationV2RunAttempt = {
        ...attempt,
        status: "running",
        startedAt: now,
      };
      const runningRootNode: OrchestrationV2ExecutionNode = {
        ...rootNode,
        status: "running",
        startedAt: now,
      };
      const events: Array<OrchestrationV2DomainEvent> = [
        {
          id: yield* idAllocator.allocate.event({
            threadId: projection.thread.id,
            providerSessionId,
          }),
          type: "provider-session.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: session.providerSession,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "provider-thread.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningProviderThread,
        },
        ...(nativeForkTransfer === undefined || runningProviderThread.nativeThreadRef === null
          ? []
          : [
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "context-transfer.updated" as const,
                threadId: projection.thread.id,
                runId: run.id,
                driver: session.driver,
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...nativeForkTransfer,
                  targetProviderInstanceId: run.providerInstanceId,
                  targetRunId: run.id,
                  status: "consumed" as const,
                  resolution: {
                    strategy: "native_fork" as const,
                    providerThreadRef: runningProviderThread.nativeThreadRef,
                  },
                  error: null,
                  updatedAt: now,
                  consumedAt: now,
                },
              },
            ]),
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRun,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run-attempt.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningAttempt,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "node.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRootNode,
        },
      ];
      const runningWrite = yield* eventSink.writeIfRunCurrent({
        threadId: projection.thread.id,
        runId: run.id,
        activeAttemptId: attempt.id,
        expectedStatus: "starting",
        events,
      });
      if (!runningWrite.committed) {
        return;
      }
      yield* runExecution.startRootRun({
        commandId: CommandId.make(`command:effect:provider-turn.start:${run.id}`),
        appThread: projection.thread,
        providerSessionId,
        session,
        run: runningRun,
        rootNode: runningRootNode,
        checkpointScope,
        providerThread: runningProviderThread,
        attempt: runningAttempt,
        attemptId: attempt.id,
        relatedThreadIds: projection.subagents.flatMap((subagent) =>
          subagent.childThreadId === null ? [] : [subagent.childThreadId],
        ),
        relatedProviderThreadIds: projection.subagents.flatMap((subagent) =>
          subagent.providerThreadId === null ? [] : [subagent.providerThreadId],
        ),
        providerTurnOrdinal:
          Math.max(
            0,
            ...projection.providerTurns
              .filter((turn) => turn.providerThreadId === providerThread.id)
              .map((turn) => turn.ordinal),
          ) + 1,
        shouldStartProviderTurn: () => isCurrentAttemptInStatus("running"),
        shouldFinalizeRun: () =>
          projectionStore.getThreadProjection(projection.thread.id).pipe(
            Effect.map((current) => {
              const currentRun = current.runs.find((candidate) => candidate.id === run.id);
              return (
                currentRun?.activeAttemptId === attempt.id &&
                (currentRun.status === "starting" || currentRun.status === "running")
              );
            }),
            Effect.catchCause(() => Effect.succeed(false)),
          ),
        message: {
          messageId: message.id,
          text:
            effectiveHandoffs.length === 0
              ? message.text
              : providerMessageWithContextHandoffs({
                  handoffs: effectiveHandoffs,
                  userText: message.text,
                }),
          attachments: message.attachments,
          createdBy: message.createdBy,
          creationSource: message.creationSource,
        },
        modelSelection: run.modelSelection,
        runtimePolicy: resolvedRuntimePolicy,
      });
    });

    return ProviderTurnStartServiceV2.of({
      start: (input) =>
        start(input).pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
    });
  }),
);
