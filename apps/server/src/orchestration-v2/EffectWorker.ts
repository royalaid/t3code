import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { RunFinalizationService } from "./RunFinalizationService.ts";
import { ResourceCleanupService } from "./ResourceCleanupService.ts";
import { EffectOutboxV2, type OrchestrationEffectV2 } from "./EffectOutbox.ts";
import { CheckpointRollbackServiceV2 } from "./CheckpointRollbackService.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ProviderTurnControlServiceV2 } from "./ProviderTurnControlService.ts";
import {
  ProviderTurnStartServiceV2,
  terminalGoalPolicyFailureForProviderTurnStart,
} from "./ProviderTurnStartService.ts";
import { RuntimeRequestServiceV2 } from "./RuntimeRequestService.ts";
import { GoalAttemptExecutionService } from "./GoalAttemptExecutionService.ts";

export class OrchestrationEffectExecutionError extends Schema.TaggedErrorClass<OrchestrationEffectExecutionError>()(
  "OrchestrationEffectExecutionError",
  {
    effectId: Schema.String,
    effectType: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const isOrchestrationEffectExecutionError = Schema.is(OrchestrationEffectExecutionError);

/**
 * A provider-start effect normally retries infrastructure failures. A goal
 * policy rejection is durable state, not infrastructure, so fail it once with
 * a structured outbox error for lead recovery.
 */
export function terminalGoalPolicyFailureForEffectCause(cause: Cause.Cause<unknown>) {
  for (const reason of cause.reasons) {
    if (!Cause.isFailReason(reason) || !isOrchestrationEffectExecutionError(reason.error)) {
      continue;
    }
    const policyFailure = terminalGoalPolicyFailureForProviderTurnStart(reason.error.cause);
    if (policyFailure !== undefined) return policyFailure;
  }
  return undefined;
}

export function goalPolicyFailureOutboxError(input: {
  readonly reason: string;
  readonly detail: string;
  readonly toolAllowlist?: ReadonlyArray<string>;
}): string {
  return JSON.stringify({
    type: "goal_policy_rejected",
    reason: input.reason,
    detail: input.detail,
    ...(input.toolAllowlist === undefined ? {} : { toolAllowlist: [...input.toolAllowlist] }),
  });
}

export interface OrchestrationEffectExecutorV2Shape {
  readonly execute: (
    effect: OrchestrationEffectV2,
  ) => Effect.Effect<void, OrchestrationEffectExecutionError>;
}

export class OrchestrationEffectExecutorV2 extends Context.Service<
  OrchestrationEffectExecutorV2,
  OrchestrationEffectExecutorV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectExecutorV2") {}

export const executorLayer: Layer.Layer<
  OrchestrationEffectExecutorV2,
  never,
  | ProviderSessionManagerV2
  | RunFinalizationService
  | CheckpointRollbackServiceV2
  | ProviderTurnControlServiceV2
  | ProviderTurnStartServiceV2
  | RuntimeRequestServiceV2
  | GoalAttemptExecutionService
> = Layer.effect(
  OrchestrationEffectExecutorV2,
  Effect.gen(function* () {
    const runFinalization = yield* RunFinalizationService;
    const resourceCleanup = yield* ResourceCleanupService;
    const checkpointRollback = yield* CheckpointRollbackServiceV2;
    const providerSessions = yield* ProviderSessionManagerV2;
    const providerTurnControl = yield* ProviderTurnControlServiceV2;
    const providerTurnStart = yield* ProviderTurnStartServiceV2;
    const runtimeRequests = yield* RuntimeRequestServiceV2;
    const goalAttempts = yield* GoalAttemptExecutionService;
    return OrchestrationEffectExecutorV2.of({
      execute: (effect) => {
        switch (effect.request.type) {
          case "goal-attempt.launch":
            return goalAttempts
              .launch({
                goalId: effect.request.goalId,
                attemptId: effect.request.attemptId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-session.detach":
            return providerSessions
              .detach({
                providerSessionId: effect.request.providerSessionId,
                threadId: effect.threadId,
                ...(effect.request.detail === undefined ? {} : { detail: effect.request.detail }),
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.start":
            return providerTurnStart
              .start({ threadId: effect.threadId, runId: effect.request.runId })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.interrupt":
            return providerTurnControl
              .interrupt({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.steer":
            return providerTurnControl
              .steer({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                messageId: effect.request.messageId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.restart":
            return providerTurnControl
              .interruptAndAwaitTerminal({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                interruptedAttemptId: effect.request.interruptedAttemptId,
                ...(effect.request.sessionTransition?.type === "replace"
                  ? {
                      replacementProviderSessionId:
                        effect.request.sessionTransition.replacementProviderSessionId,
                    }
                  : {}),
              })
              .pipe(
                Effect.andThen(
                  effect.request.sessionTransition?.type === "replace"
                    ? providerSessions.detach({
                        providerSessionId: effect.request.providerSessionId,
                        threadId: effect.threadId,
                        detail: "Selection change requires a provider session restart.",
                      })
                    : effect.request.sessionTransition?.type === "detach"
                      ? providerSessions.detach({
                          providerSessionId: effect.request.providerSessionId,
                          threadId: effect.threadId,
                          detail: "Provider thread handoff replaced this session binding.",
                        })
                      : Effect.void,
                ),
                Effect.andThen(
                  providerTurnStart.start({
                    threadId: effect.threadId,
                    runId: effect.request.runId,
                  }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "runtime-request.respond":
            return runtimeRequests
              .respond({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                requestId: effect.request.requestId,
                ...(effect.request.decision === undefined
                  ? {}
                  : { decision: effect.request.decision }),
                ...(effect.request.answers === undefined
                  ? {}
                  : { answers: effect.request.answers }),
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-thread.rollback":
            return checkpointRollback
              .execute({
                threadId: effect.threadId,
                providerThreadId: effect.request.providerThreadId,
                checkpointId: effect.request.checkpointId,
                scopeId: effect.request.scopeId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "checkpoint.capture":
            return runFinalization
              .finalize({
                threadId: effect.threadId,
                runId: effect.request.runId,
                scopeId: effect.request.scopeId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "terminal.cleanup":
            return resourceCleanup.cleanupTerminals(effect.threadId).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          case "attachment.cleanup":
            return resourceCleanup.cleanupAttachments(effect.request.attachmentIds).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
        }
      },
    });
  }),
);

export class OrchestrationEffectWorkerError extends Schema.TaggedErrorClass<OrchestrationEffectWorkerError>()(
  "OrchestrationEffectWorkerError",
  {
    operation: Schema.String,
    effectId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isOrchestrationEffectWorkerError = Schema.is(OrchestrationEffectWorkerError);

export interface OrchestrationEffectWorkerV2Shape {
  readonly awaitWork: Effect.Effect<void>;
  readonly runOnce: Effect.Effect<boolean, OrchestrationEffectWorkerError>;
  readonly drain: (maxEffects?: number) => Effect.Effect<number, OrchestrationEffectWorkerError>;
}

export class OrchestrationEffectWorkerV2 extends Context.Service<
  OrchestrationEffectWorkerV2,
  OrchestrationEffectWorkerV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectWorkerV2") {}

export interface OrchestrationEffectWorkerOptions {
  readonly workerId?: string;
  readonly leaseDurationMs?: number;
  readonly maxAttempts?: number;
}

export const orchestrationEffectAttemptLimit = (
  effectType: OrchestrationEffectV2["request"]["type"],
  defaultLimit: number,
): number => (effectType === "goal-attempt.launch" ? 2 : defaultLimit);

export const layerWithOptions = (
  options: OrchestrationEffectWorkerOptions = {},
): Layer.Layer<
  OrchestrationEffectWorkerV2,
  never,
  EffectOutboxV2 | OrchestrationEffectExecutorV2
> =>
  Layer.effect(
    OrchestrationEffectWorkerV2,
    Effect.gen(function* () {
      const outbox = yield* EffectOutboxV2;
      const executor = yield* OrchestrationEffectExecutorV2;
      const workerId = options.workerId ?? `orchestration-v2:${process.pid}`;
      const leaseDurationMs = Math.max(1, options.leaseDurationMs ?? 30_000);
      const maxAttempts = Math.max(1, options.maxAttempts ?? 5);
      const wasCancelled = (effectId: string) =>
        outbox.get(effectId).pipe(
          Effect.map(
            Option.match({
              onNone: () => false,
              onSome: (effect) => effect.status === "cancelled",
            }),
          ),
        );

      const runOnce = Effect.gen(function* () {
        const claimed = yield* outbox.claimNext({ workerId, leaseDurationMs });
        if (Option.isNone(claimed)) {
          return false;
        }
        const effect = claimed.value;
        // Cancellation can commit after the durable claim but before the
        // process-local Deferred is registered. Re-read the authoritative row
        // once before starting external work; later cancellations use the
        // Deferred raced below.
        if (yield* wasCancelled(effect.id)) {
          yield* outbox.clearCancellation(effect.id);
          return true;
        }
        const execution = executor.execute(effect).pipe(Effect.as("executed" as const));
        const cancellation = outbox
          .awaitCancellation(effect.id)
          .pipe(Effect.as("cancelled" as const));
        const exit = yield* Effect.exit(Effect.raceFirst(execution, cancellation)).pipe(
          Effect.ensuring(outbox.clearCancellation(effect.id)),
        );
        if (Exit.isSuccess(exit) && exit.value === "cancelled") {
          return true;
        }
        if (Exit.isSuccess(exit)) {
          const completed = yield* outbox.succeed({ effectId: effect.id, workerId });
          if (!completed) {
            if (yield* wasCancelled(effect.id)) return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "complete",
              effectId: effect.id,
              cause: "The worker no longer owns the effect lease.",
            });
          }
          return true;
        }

        const policyFailure =
          effect.request.type === "provider-turn.start"
            ? terminalGoalPolicyFailureForEffectCause(exit.cause)
            : undefined;
        const error =
          policyFailure === undefined
            ? Cause.pretty(exit.cause)
            : goalPolicyFailureOutboxError({
                reason: policyFailure.reason,
                detail: policyFailure.detail,
                ...(policyFailure.toolAllowlist === undefined
                  ? {}
                  : { toolAllowlist: policyFailure.toolAllowlist }),
              });
        yield* Effect.logWarning("Orchestration effect execution failed", {
          effectId: effect.id,
          effectType: effect.request.type,
          attemptCount: effect.attemptCount,
          error,
        });
        const updated =
          policyFailure !== undefined ||
          effect.attemptCount >= orchestrationEffectAttemptLimit(effect.request.type, maxAttempts)
            ? yield* outbox.fail({ effectId: effect.id, workerId, error })
            : yield* outbox.retry({
                effectId: effect.id,
                workerId,
                error,
                delayMs: Math.min(30_000, 100 * 2 ** Math.max(0, effect.attemptCount - 1)),
              });
        if (!updated) {
          if (yield* wasCancelled(effect.id)) return true;
          return yield* new OrchestrationEffectWorkerError({
            operation: "reschedule",
            effectId: effect.id,
            cause: "The worker no longer owns the effect lease.",
          });
        }
        return true;
      }).pipe(
        Effect.mapError((cause) =>
          isOrchestrationEffectWorkerError(cause)
            ? cause
            : new OrchestrationEffectWorkerError({ operation: "run", cause }),
        ),
      );

      return OrchestrationEffectWorkerV2.of({
        awaitWork: outbox.awaitAvailable,
        runOnce,
        drain: (maxEffects = Number.MAX_SAFE_INTEGER) =>
          Effect.gen(function* () {
            let completed = 0;
            while (completed < maxEffects && (yield* runOnce)) {
              completed += 1;
            }
            return completed;
          }),
      });
    }),
  );

export const layer = layerWithOptions();

export interface OrchestrationEffectDaemonOptions {
  readonly concurrency?: number;
}

export const DEFAULT_EFFECT_WORKER_CONCURRENCY = 4;

export const runDaemonWithOptions = (options: OrchestrationEffectDaemonOptions = {}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const worker = yield* OrchestrationEffectWorkerV2;
      const requestedConcurrency = options.concurrency ?? DEFAULT_EFFECT_WORKER_CONCURRENCY;
      const concurrency = Number.isFinite(requestedConcurrency)
        ? Math.max(1, Math.floor(requestedConcurrency))
        : DEFAULT_EFFECT_WORKER_CONCURRENCY;
      // Notifications only reduce latency; the durable outbox remains authoritative.
      // Every slot polls after a bounded delay so a missed or coalesced wakeup can
      // never strand committed work. Consuming the outbox signal directly also
      // avoids lifecycle coupling between a separate fan-out fiber and subscribers.
      const runWorker = Effect.forever(
        worker.runOnce.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Orchestration effect worker failed", cause).pipe(Effect.as(false)),
          ),
          Effect.flatMap((worked) =>
            worked
              ? Effect.yieldNow
              : Effect.raceFirst(worker.awaitWork, Effect.sleep(Duration.millis(50))),
          ),
        ),
      );

      return yield* Effect.all(
        Array.from({ length: concurrency }, () => runWorker),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
    }),
  );

export const runDaemon = runDaemonWithOptions();

export const daemonLayer: Layer.Layer<never, never, OrchestrationEffectWorkerV2> =
  Layer.effectDiscard(runDaemon.pipe(Effect.forkScoped));
