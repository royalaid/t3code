import {
  CommandId,
  GoalEvidenceId,
  type GoalAttempt,
  type GoalDetail,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { EventSinkV2 } from "./EventSink.ts";
import { EffectOutboxV2 } from "./EffectOutbox.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { GoalScheduler } from "./GoalScheduler.ts";
import {
  planGoalRecovery,
  shouldRefreshExpiredGoalLease,
  type GoalRecoveryAction,
} from "./GoalRecoveryService.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

const ACTIVE_RUN_STATUSES = new Set(["preparing", "queued", "starting", "running", "waiting"]);
const ATTEMPT_ACTIVITY_EVENTS = new Set<OrchestrationV2DomainEvent["type"]>([
  "run.updated",
  "node.updated",
  "subagent.updated",
  "provider-turn.updated",
  "runtime-request.updated",
  "message.updated",
  "turn-item.updated",
  "plan.updated",
]);

export class GoalWorkflowServiceError extends Schema.TaggedErrorClass<GoalWorkflowServiceError>()(
  "GoalWorkflowServiceError",
  { operation: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

export class GoalWorkflowService extends Context.Service<
  GoalWorkflowService,
  { readonly reconcile: Effect.Effect<void, GoalWorkflowServiceError> }
>()("t3/orchestration-v2/GoalWorkflowService") {}

const workflowError = (operation: string) => (cause: unknown) =>
  new GoalWorkflowServiceError({ operation, cause });

export const layer = Layer.effect(
  GoalWorkflowService,
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const outbox = yield* EffectOutboxV2;
    const goals = yield* GoalProjectionStore;
    const scheduler = yield* GoalScheduler;
    const ids = yield* IdAllocatorV2;
    const threads = yield* ThreadManagementService;

    const schedule = scheduler.tick.pipe(Effect.asVoid, Effect.mapError(workflowError("schedule")));

    const applyRecovery = Effect.fn("GoalWorkflowService.applyRecovery")(function* (
      detail: GoalDetail,
      action: GoalRecoveryAction,
    ) {
      const attempt = detail.attempts.find((candidate) => candidate.id === action.attemptId);
      if (attempt === undefined) return;
      const node = detail.nodes.find(
        (candidate) =>
          candidate.graphVersionId === attempt.graphVersionId &&
          candidate.node.id === attempt.nodeId,
      );
      if (node === undefined) return;
      if (action.type === "native_descendant_overage") return;
      const launchEffect =
        action.type === "reconcile_required"
          ? yield* outbox
              .get(`goal-attempt-launch:${attempt.id}`)
              .pipe(Effect.mapError(workflowError("read-launch-effect")))
          : Option.none();
      const launchStillPending =
        Option.isSome(launchEffect) && shouldRefreshExpiredGoalLease(launchEffect.value.status);
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const commandId = CommandId.make(
        launchStillPending
          ? `goal-recovery:lease-refresh:${attempt.id}:${attempt.leaseExpiresAt ?? "none"}`
          : `goal-recovery:${action.type}:${attempt.id}`,
      );
      const makeEvent = Effect.fn("GoalWorkflowService.recoveryEvent")(function* (
        type: OrchestrationV2DomainEvent["type"],
        payload: unknown,
      ) {
        const id = yield* ids.allocate
          .event({ threadId: detail.goal.rootThreadId, commandId })
          .pipe(Effect.mapError(workflowError("allocate-recovery-event")));
        return {
          id,
          threadId: detail.goal.rootThreadId,
          type,
          payload,
          occurredAt: now,
        } as OrchestrationV2DomainEvent;
      });
      const retrying = action.type === "retry";
      const blocked =
        action.type === "retry_exhausted" ||
        (action.type === "reconcile_required" && !launchStillPending);
      const updatedAttempt: GoalAttempt = {
        ...attempt,
        status: launchStillPending
          ? attempt.status
          : action.type === "mark_stalled" ||
              action.type === "terminate_ceiling" ||
              action.type === "reconcile_required"
            ? "stalled"
            : attempt.status,
        failureReason: launchStillPending
          ? attempt.failureReason
          : retrying
            ? "retry_scheduled"
            : action.type === "retry_exhausted"
              ? "retry_exhausted"
              : action.type,
        leaseExpiresAt: launchStillPending
          ? DateTime.formatIso(DateTime.add(now, { minutes: 2 }))
          : attempt.leaseExpiresAt,
        updatedAt: timestamp,
      };
      const nodeStatus = retrying ? "ready" : blocked ? "blocked" : node.status;
      const recoveryEvents = [
        yield* makeEvent("goal.attempt-transitioned", updatedAttempt),
        yield* makeEvent("goal.node-transitioned", {
          ...node,
          status: nodeStatus,
          activeAttemptId: retrying || blocked ? null : node.activeAttemptId,
          blocker: retrying || launchStillPending ? null : action.type,
          updatedAt: timestamp,
        }),
        ...(blocked
          ? [
              yield* makeEvent("goal.updated", {
                ...detail.goal,
                status: "blocked",
                updatedAt: timestamp,
              }),
            ]
          : []),
      ];
      const committed = yield* eventSink
        .commitGoalAttemptCommand({
          commandId,
          threadId: detail.goal.rootThreadId,
          commandType: `goal.recovery.${action.type}`,
          acceptedAt: now,
          goalId: detail.goal.id,
          graphVersionId: attempt.graphVersionId,
          nodeId: attempt.nodeId,
          attemptId: attempt.id,
          expectedStatuses: [attempt.status],
          events: recoveryEvents,
          effects: [],
        })
        .pipe(Effect.mapError(workflowError("persist-recovery")));
      if (
        committed.committed &&
        action.type === "terminate_ceiling" &&
        attempt.executionThreadId !== null &&
        attempt.runId !== null
      ) {
        yield* threads
          .dispatch({
            type: "run.interrupt",
            commandId: CommandId.make(`goal-worker-ceiling-interrupt:${attempt.id}`),
            threadId: attempt.executionThreadId,
            runId: attempt.runId,
            reason: "Goal worker exceeded the 60-minute ceiling.",
          })
          .pipe(Effect.mapError(workflowError("interrupt-ceiling")));
      }
    });

    const recover = Effect.gen(function* () {
      const details = yield* goals.listSchedulable.pipe(
        Effect.mapError(workflowError("list-recovery-goals")),
      );
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* Effect.forEach(
        details,
        (detail) =>
          Effect.forEach(
            planGoalRecovery({ attempts: detail.attempts, now }).actions,
            (action) => applyRecovery(detail, action),
            { concurrency: 1 },
          ),
        { concurrency: 1 },
      );
    });
    const reconcile = recover.pipe(Effect.andThen(schedule));

    const updateAttempt = Effect.fn("GoalWorkflowService.updateAttempt")(function* (
      domainEvent: OrchestrationV2DomainEvent,
    ) {
      if (!ATTEMPT_ACTIVITY_EVENTS.has(domainEvent.type)) return;
      const binding = yield* goals
        .resolveMcpBinding(domainEvent.threadId)
        .pipe(Effect.mapError(workflowError("resolve-attempt")));
      if (binding === null || binding.kind !== "worker") return;
      const detail = yield* goals
        .getDetail(binding.goalId)
        .pipe(Effect.mapError(workflowError("read-goal")));
      const attempt = detail.attempts.find((candidate) => candidate.id === binding.attemptId);
      if (attempt === undefined) return;
      const node = detail.nodes.find(
        (candidate) =>
          candidate.graphVersionId === attempt.graphVersionId &&
          candidate.node.id === attempt.nodeId,
      );
      if (node === undefined) return;
      let status: GoalAttempt["status"] = attempt.status;
      if (domainEvent.type === "run.updated" && domainEvent.payload.id === attempt.runId) {
        status = ACTIVE_RUN_STATUSES.has(domainEvent.payload.status)
          ? "running"
          : domainEvent.payload.status === "completed"
            ? "succeeded"
            : domainEvent.payload.status === "cancelled" ||
                domainEvent.payload.status === "interrupted"
              ? "cancelled"
              : domainEvent.payload.status === "failed"
                ? "failed"
                : attempt.status;
      }
      const workerProjection = yield* threads
        .getThreadProjection(domainEvent.threadId)
        .pipe(Effect.mapError(workflowError("read-worker")));
      const nativeDescendantCount = workerProjection.subagents.filter(
        (subagent) => subagent.origin === "provider_native",
      ).length;
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const updated: GoalAttempt = {
        ...attempt,
        status,
        rootExecutionNodeId:
          domainEvent.type === "run.updated"
            ? domainEvent.payload.rootNodeId
            : attempt.rootExecutionNodeId,
        usage: { ...attempt.usage, nativeDescendantCount },
        leaseExpiresAt: status === "running" ? null : attempt.leaseExpiresAt,
        failureReason:
          status === "failed" && attempt.failureReason === null
            ? "provider_run_failed"
            : attempt.failureReason,
        updatedAt: timestamp,
      };
      const commandId = CommandId.make(`goal-attempt-observe:${attempt.id}:${domainEvent.id}`);
      const makeEvent = Effect.fn("GoalWorkflowService.event")(function* (
        type: OrchestrationV2DomainEvent["type"],
        payload: unknown,
      ) {
        const id = yield* ids.allocate
          .event({ threadId: detail.goal.rootThreadId, commandId })
          .pipe(Effect.mapError(workflowError("allocate-event")));
        return {
          id,
          threadId: detail.goal.rootThreadId,
          type,
          payload,
          occurredAt: now,
        } as OrchestrationV2DomainEvent;
      });
      const attemptEvent = yield* makeEvent("goal.attempt-transitioned", updated);
      const terminal = new Set(["succeeded", "failed", "cancelled"]).has(status);
      const nextNodeStatus =
        status === "succeeded"
          ? "processing"
          : status === "failed"
            ? "failed"
            : status === "cancelled"
              ? "cancelled"
              : node.status;
      const nodeEvents = terminal
        ? [
            yield* makeEvent("goal.node-transitioned", {
              ...node,
              status: nextNodeStatus,
              activeAttemptId: null,
              blocker: status === "failed" ? updated.failureReason : null,
              updatedAt: timestamp,
            }),
          ]
        : [];
      const totalAgents =
        detail.attempts.length -
        attempt.usage.nativeDescendantCount +
        detail.attempts.reduce(
          (total, candidate) => total + candidate.usage.nativeDescendantCount,
          0,
        ) +
        nativeDescendantCount;
      const overage = totalAgents > 1_000;
      const overageEvents = overage
        ? [
            yield* makeEvent("goal.failure-recorded", {
              id: GoalEvidenceId.make(`goal-native-overage:${attempt.id}:${totalAgents}`),
              goalId: detail.goal.id,
              graphVersionId: attempt.graphVersionId,
              nodeId: attempt.nodeId,
              attemptId: attempt.id,
              reason: { type: "native_descendant_overage", limit: 1_000, observed: totalAgents },
              recoveryState: "unresolved",
              blocker: "Native descendant lifetime backstop exceeded.",
              occurredAt: timestamp,
            }),
            yield* makeEvent("goal.updated", {
              ...detail.goal,
              status: "paused",
              updatedAt: timestamp,
            }),
          ]
        : [];
      const committed = yield* eventSink
        .commitGoalAttemptCommand({
          commandId,
          threadId: detail.goal.rootThreadId,
          commandType: "goal.attempt.observe",
          acceptedAt: now,
          goalId: detail.goal.id,
          graphVersionId: attempt.graphVersionId,
          nodeId: attempt.nodeId,
          attemptId: attempt.id,
          expectedStatuses: [attempt.status],
          events: [attemptEvent, ...nodeEvents, ...overageEvents],
          effects: [],
        })
        .pipe(Effect.mapError(workflowError("persist-attempt")));
      if (committed.committed && overage && attempt.runId !== null) {
        yield* threads
          .dispatch({
            type: "run.interrupt",
            commandId: CommandId.make(`goal-native-overage-interrupt:${attempt.id}`),
            threadId: domainEvent.threadId,
            runId: attempt.runId,
            reason: "Native descendant lifetime backstop exceeded.",
          })
          .pipe(Effect.mapError(workflowError("interrupt-overage")));
      }
    });

    const observe = eventSink.stream().pipe(
      Stream.runForEach((stored) =>
        updateAttempt(stored.event).pipe(
          Effect.andThen(stored.event.type.startsWith("goal.") ? reconcile : Effect.void),
          Effect.catchCause((cause) =>
            Effect.logError("Goal workflow event processing failed", {
              sequence: stored.sequence,
              eventType: stored.event.type,
              cause,
            }),
          ),
        ),
      ),
      Effect.catchCause((cause) => Effect.logError("Goal workflow stream failed", { cause })),
    );
    yield* reconcile.pipe(
      Effect.catchCause((cause) =>
        Effect.logError("Initial goal reconciliation failed", { cause }),
      ),
    );
    yield* observe.pipe(Effect.forkScoped);
    yield* Effect.forever(Effect.sleep("30 seconds").pipe(Effect.andThen(reconcile))).pipe(
      Effect.catchCause((cause) => Effect.logError("Goal recovery loop failed", { cause })),
      Effect.forkScoped,
    );
    return GoalWorkflowService.of({ reconcile });
  }),
);
