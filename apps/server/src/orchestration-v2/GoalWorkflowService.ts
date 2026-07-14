import {
  CommandId,
  EventId,
  type Goal,
  GoalEvidenceId,
  type GoalAttempt,
  type GoalDetail,
  type GoalFailureReason,
  type GoalId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RunStatus,
  type RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { EventSinkV2 } from "./EventSink.ts";
import { EffectOutboxV2 } from "./EffectOutbox.ts";
import { goalRootLaunchClaimId } from "./GoalLaunchService.ts";
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
const ACTIVE_ATTEMPT_STATUSES = new Set(["leased", "launching", "running", "stalled"]);
const NODE_CANCELLATION_STATUSES = new Set(["cancelled", "superseded"]);
type RootNoGraphTerminalStatus = Extract<
  GoalFailureReason,
  { readonly type: "root_lead_no_graph" }
>["terminalStatus"];
const ROOT_NO_GRAPH_TERMINAL_STATUSES = new Set<RootNoGraphTerminalStatus>([
  "completed",
  "failed",
  "interrupted",
  "rolled_back",
]);
export function rootNoGraphTerminalStatus(
  status: OrchestrationV2RunStatus,
): RootNoGraphTerminalStatus | null {
  return ROOT_NO_GRAPH_TERMINAL_STATUSES.has(status as RootNoGraphTerminalStatus)
    ? (status as RootNoGraphTerminalStatus)
    : null;
}
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
    const trackedBlockedRootRuns = yield* Ref.make(new Map<RunId, GoalId>());

    const trackBlockedRootRun = (runId: RunId, goalId: GoalId) =>
      Ref.update(trackedBlockedRootRuns, (current) => {
        const next = new Map(current);
        next.set(runId, goalId);
        return next;
      });

    const takeBlockedRootRun = (runId: RunId) =>
      Ref.modify(trackedBlockedRootRuns, (current) => {
        if (!current.has(runId)) return [false, current] as const;
        const next = new Map(current);
        next.delete(runId);
        return [true, next] as const;
      });

    const clearBlockedRootRunsForGoal = (goalId: GoalId) =>
      Ref.update(trackedBlockedRootRuns, (current) => {
        const next = new Map(current);
        for (const [runId, trackedGoalId] of current) {
          if (trackedGoalId === goalId) next.delete(runId);
        }
        return next;
      });

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
        action.type === "native_descendant_overage"
          ? `goal-recovery:native-descendant-overage:${attempt.id}:${action.observed}`
          : launchStillPending
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
      if (action.type === "native_descendant_overage") {
        const committed = yield* eventSink
          .commitGoalAttemptCommand({
            commandId,
            threadId: detail.goal.rootThreadId,
            commandType: "goal.recovery.native_descendant_overage",
            acceptedAt: now,
            goalId: detail.goal.id,
            graphVersionId: attempt.graphVersionId,
            nodeId: attempt.nodeId,
            attemptId: attempt.id,
            expectedStatuses: [attempt.status],
            events: [
              yield* makeEvent("goal.failure-recorded", {
                id: GoalEvidenceId.make(`goal-native-overage:${attempt.id}:${action.observed}`),
                goalId: detail.goal.id,
                graphVersionId: attempt.graphVersionId,
                nodeId: attempt.nodeId,
                attemptId: attempt.id,
                reason: {
                  type: "native_descendant_overage",
                  limit: action.limit,
                  observed: action.observed,
                },
                recoveryState: "unresolved",
                blocker: "Native descendant lifetime backstop exceeded.",
                occurredAt: timestamp,
              }),
              yield* makeEvent("goal.updated", {
                ...detail.goal,
                status: "paused",
                updatedAt: timestamp,
              }),
            ],
            effects: [],
          })
          .pipe(Effect.mapError(workflowError("persist-native-overage")));
        if (committed.committed && attempt.executionThreadId !== null && attempt.runId !== null) {
          yield* threads
            .dispatch({
              type: "run.interrupt",
              createdBy: "system",
              creationSource: "server",
              commandId: CommandId.make(`goal-native-overage-interrupt:${attempt.id}`),
              threadId: attempt.executionThreadId,
              runId: attempt.runId,
              reason: "Native descendant lifetime backstop exceeded.",
            })
            .pipe(Effect.mapError(workflowError("interrupt-native-overage")));
        }
        return;
      }
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
            createdBy: "system",
            creationSource: "server",
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

    const persistRootLeadNoGraph = Effect.fn("GoalWorkflowService.persistRootLeadNoGraph")(
      function* (input: {
        readonly goal: Goal;
        readonly runId: RunId;
        readonly terminalStatus: RootNoGraphTerminalStatus;
        readonly initial: boolean;
      }) {
        const goal = input.goal;
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const commandId = CommandId.make(`goal-root-no-graph:${goal.id}:${input.runId}`);
        const diagnostic = input.initial
          ? `Root lead run ${input.runId} ended with ${input.terminalStatus} before publishing a valid graph.`
          : `Corrective root lead run ${input.runId} ended with ${input.terminalStatus} without publishing a valid graph.`;
        const failureId = GoalEvidenceId.make(`goal-root-no-graph:${goal.id}:${input.runId}`);
        yield* eventSink
          .commitGoalNoGraphCommand({
            commandId,
            threadId: goal.rootThreadId,
            commandType: input.initial
              ? "goal.root-lead.initial-no-graph"
              : "goal.root-lead.retry-no-graph",
            acceptedAt: now,
            goalId: goal.id,
            runId: input.runId,
            pendingLaunchClaimId: goal.pendingLaunchClaimId ?? goalRootLaunchClaimId(goal.id),
            expectedStatus: input.initial ? "planning" : "blocked",
            requireInitialRootRun: input.initial,
            events: [
              {
                id: EventId.make(`event:${failureId}:failure`),
                threadId: goal.rootThreadId,
                type: "goal.failure-recorded",
                payload: {
                  id: failureId,
                  goalId: goal.id,
                  graphVersionId: null,
                  nodeId: null,
                  attemptId: null,
                  reason: {
                    type: "root_lead_no_graph",
                    runId: input.runId,
                    terminalStatus: input.terminalStatus,
                    detail: diagnostic.slice(0, 4_000),
                  },
                  recoveryState: "retryable",
                  blocker: "Send a corrective message in the root thread.",
                  occurredAt: timestamp,
                },
                occurredAt: now,
              },
              {
                id: EventId.make(`event:${failureId}:goal`),
                threadId: goal.rootThreadId,
                type: "goal.updated",
                payload: {
                  ...goal,
                  status: "blocked",
                  updatedAt: timestamp,
                },
                occurredAt: now,
              },
            ],
            effects: [],
          })
          .pipe(Effect.mapError(workflowError("persist-root-lead-no-graph")));
      },
    );

    const superviseRootLeadNoGraph = Effect.fn("GoalWorkflowService.superviseRootLeadNoGraph")(
      function* (domainEvent: OrchestrationV2DomainEvent) {
        if (domainEvent.type === "goal.graph-version-activated") {
          yield* clearBlockedRootRunsForGoal(domainEvent.payload.goalId);
          return;
        }
        if (
          domainEvent.type === "goal.cancelled" ||
          domainEvent.type === "goal.completed" ||
          domainEvent.type === "goal.reopened" ||
          domainEvent.type === "goal.integration-updated" ||
          domainEvent.type === "goal.integration-conflicted"
        ) {
          yield* clearBlockedRootRunsForGoal(domainEvent.payload.id);
          return;
        }
        if (domainEvent.type === "goal.updated") {
          const projected = domainEvent.payload;
          if (
            projected.status !== "planning" ||
            projected.currentRevision !== 0 ||
            projected.currentGraphVersionId !== null ||
            projected.initialRootRunId === undefined
          ) {
            if (
              projected.status !== "blocked" ||
              projected.currentRevision !== 0 ||
              projected.currentGraphVersionId !== null
            ) {
              yield* clearBlockedRootRunsForGoal(projected.id);
            }
            return;
          }
          const root = yield* threads
            .getThreadProjection(projected.rootThreadId)
            .pipe(Effect.mapError(workflowError("read-planning-no-graph-root")));
          const run = root.runs.find((candidate) => candidate.id === projected.initialRootRunId);
          const settled = run === undefined ? null : rootNoGraphTerminalStatus(run.status);
          if (run === undefined || settled === null) return;
          yield* persistRootLeadNoGraph({
            goal: projected,
            runId: run.id,
            terminalStatus: settled,
            initial: true,
          });
          return;
        }

        if (domainEvent.type !== "run.created" && domainEvent.type !== "run.updated") return;
        const settled =
          domainEvent.type === "run.updated"
            ? rootNoGraphTerminalStatus(domainEvent.payload.status)
            : null;
        if (domainEvent.type === "run.updated" && settled === null) {
          if (domainEvent.payload.status === "cancelled") {
            yield* takeBlockedRootRun(domainEvent.payload.id);
          }
          return;
        }
        const binding = yield* goals
          .resolveMcpBinding(domainEvent.threadId)
          .pipe(Effect.mapError(workflowError("resolve-root-no-graph")));
        if (binding?.kind !== "lead") return;
        const detail = yield* goals
          .getDetail(binding.goalId)
          .pipe(Effect.mapError(workflowError("read-root-no-graph-goal")));
        const noGraphAtRevisionZero =
          detail.goal.currentRevision === 0 && detail.goal.currentGraphVersionId === null;

        if (
          domainEvent.type === "run.created" &&
          detail.goal.status === "blocked" &&
          noGraphAtRevisionZero &&
          domainEvent.payload.id !== detail.goal.initialRootRunId
        ) {
          yield* trackBlockedRootRun(domainEvent.payload.id, detail.goal.id);
          return;
        }
        if (domainEvent.type !== "run.updated") return;
        if (settled === null) return;
        const tracked = yield* takeBlockedRootRun(domainEvent.payload.id);
        if (
          detail.goal.status === "planning" &&
          noGraphAtRevisionZero &&
          detail.goal.initialRootRunId === domainEvent.payload.id
        ) {
          yield* persistRootLeadNoGraph({
            goal: detail.goal,
            runId: domainEvent.payload.id,
            terminalStatus: settled,
            initial: true,
          });
          return;
        }
        if (tracked && detail.goal.status === "blocked" && noGraphAtRevisionZero) {
          yield* persistRootLeadNoGraph({
            goal: detail.goal,
            runId: domainEvent.payload.id,
            terminalStatus: settled,
            initial: false,
          });
        }
      },
    );

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
      const nodeCancelled = NODE_CANCELLATION_STATUSES.has(node.status);
      let status: GoalAttempt["status"] = attempt.status;
      if (domainEvent.type === "run.updated" && domainEvent.payload.id === attempt.runId) {
        const observedStatus = ACTIVE_RUN_STATUSES.has(domainEvent.payload.status)
          ? "running"
          : domainEvent.payload.status === "completed"
            ? "succeeded"
            : domainEvent.payload.status === "cancelled" ||
                domainEvent.payload.status === "interrupted"
              ? "cancelled"
              : domainEvent.payload.status === "failed"
                ? "failed"
                : attempt.status;
        // A cancellation is a durable workflow decision. Provider events may
        // arrive after the interrupt request (including a late successful
        // completion), but they must only terminalize the owning attempt and
        // never revive the node into processing/succeeded.
        status =
          nodeCancelled &&
          (attempt.status === "cancelled" ||
            observedStatus === "succeeded" ||
            observedStatus === "failed" ||
            observedStatus === "cancelled")
            ? "cancelled"
            : observedStatus;
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
      const nextNodeStatus = nodeCancelled
        ? node.status
        : status === "succeeded"
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
              blocker: nodeCancelled
                ? node.blocker
                : status === "failed"
                  ? updated.failureReason
                  : null,
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
      const overage = !nodeCancelled && totalAgents > 1_000;
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
            createdBy: "system",
            creationSource: "server",
            commandId: CommandId.make(`goal-native-overage-interrupt:${attempt.id}`),
            threadId: domainEvent.threadId,
            runId: attempt.runId,
            reason: "Native descendant lifetime backstop exceeded.",
          })
          .pipe(Effect.mapError(workflowError("interrupt-overage")));
      }
    });

    const cancelNodeAttempt = Effect.fn("GoalWorkflowService.cancelNodeAttempt")(function* (
      domainEvent: OrchestrationV2DomainEvent,
    ) {
      if (domainEvent.type !== "goal.node-cancellation-requested") return;
      const detail = yield* goals
        .getDetail(domainEvent.payload.goalId)
        .pipe(Effect.mapError(workflowError("read-cancelled-node-goal")));
      const node = detail.nodes.find(
        (candidate) =>
          candidate.graphVersionId === domainEvent.payload.graphVersionId &&
          candidate.node.id === domainEvent.payload.node.id,
      );
      if (node === undefined || !NODE_CANCELLATION_STATUSES.has(node.status)) return;
      const attempt =
        (node.activeAttemptId === null
          ? undefined
          : detail.attempts.find((candidate) => candidate.id === node.activeAttemptId)) ??
        detail.attempts
          .filter(
            (candidate) =>
              candidate.graphVersionId === node.graphVersionId && candidate.nodeId === node.node.id,
          )
          .findLast((candidate) => ACTIVE_ATTEMPT_STATUSES.has(candidate.status));
      if (attempt === undefined || !ACTIVE_ATTEMPT_STATUSES.has(attempt.status)) return;

      // A worker thread can have created its durable run between the launch
      // fence and the run-binding projection update. Reconcile that tiny
      // window before treating the attempt as unbound so the interruption is
      // still a normal durable `run.interrupt` command.
      if (attempt.executionThreadId !== null && attempt.runId === null) {
        const worker = yield* Effect.option(
          threads
            .getThreadProjection(attempt.executionThreadId)
            .pipe(Effect.mapError(workflowError("read-cancelled-node-worker"))),
        );
        const run = Option.isNone(worker)
          ? undefined
          : worker.value.runs.findLast((candidate) => ACTIVE_RUN_STATUSES.has(candidate.status));
        if (run !== undefined) {
          const now = yield* DateTime.now;
          const timestamp = DateTime.formatIso(now);
          const commandId = CommandId.make(`goal-node-cancel-bind-run:${attempt.id}:${run.id}`);
          const eventId = yield* ids.allocate
            .event({ threadId: detail.goal.rootThreadId, commandId })
            .pipe(Effect.mapError(workflowError("allocate-node-cancel-run-bind-event")));
          const boundAttempt: GoalAttempt = {
            ...attempt,
            status: "running",
            runId: run.id,
            leaseExpiresAt: null,
            updatedAt: timestamp,
          };
          const committed = yield* eventSink
            .commitGoalAttemptCommand({
              commandId,
              threadId: detail.goal.rootThreadId,
              commandType: "goal.node-cancel.bind-observed-run",
              acceptedAt: now,
              goalId: detail.goal.id,
              graphVersionId: attempt.graphVersionId,
              nodeId: attempt.nodeId,
              attemptId: attempt.id,
              expectedStatuses: [attempt.status],
              events: [
                {
                  id: eventId,
                  threadId: detail.goal.rootThreadId,
                  type: "goal.attempt-transitioned",
                  payload: boundAttempt,
                  occurredAt: now,
                },
              ],
              effects: [],
            })
            .pipe(Effect.mapError(workflowError("bind-observed-cancelled-node-run")));
          if (committed.committed)
            yield* threads
              .dispatch({
                type: "run.interrupt",
                createdBy: "system",
                creationSource: "server",
                commandId: CommandId.make(`goal-node-cancel-interrupt:${attempt.id}:${run.id}`),
                threadId: attempt.executionThreadId,
                runId: run.id,
                reason: node.blocker ?? `Node ${node.status} by root lead.`,
              })
              .pipe(Effect.mapError(workflowError("interrupt-observed-cancelled-node-run")));
          return;
        }
      }

      // A leased/launching attempt has no provider run to interrupt. Mark it
      // terminal under an attempt CAS so an already-queued launch effect sees
      // the cancellation fence and becomes a no-op.
      if (attempt.executionThreadId === null || attempt.runId === null) {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const commandId = CommandId.make(
          `goal-node-cancel-terminalize:${attempt.id}:${node.status}`,
        );
        const attemptEventId = yield* ids.allocate
          .event({ threadId: detail.goal.rootThreadId, commandId })
          .pipe(Effect.mapError(workflowError("allocate-node-cancel-attempt-event")));
        const nodeEventId = yield* ids.allocate
          .event({ threadId: detail.goal.rootThreadId, commandId })
          .pipe(Effect.mapError(workflowError("allocate-node-cancel-node-event")));
        const cancelledAttempt: GoalAttempt = {
          ...attempt,
          status: "cancelled",
          leaseOwner: null,
          leaseExpiresAt: null,
          failureReason: `node_${node.status}`,
          updatedAt: timestamp,
        };
        const committed = yield* eventSink
          .commitGoalAttemptCommand({
            commandId,
            threadId: detail.goal.rootThreadId,
            commandType: "goal.node-cancel.terminalize-unbound",
            acceptedAt: now,
            goalId: detail.goal.id,
            graphVersionId: attempt.graphVersionId,
            nodeId: attempt.nodeId,
            attemptId: attempt.id,
            expectedStatuses: [attempt.status],
            events: [
              {
                id: attemptEventId,
                threadId: detail.goal.rootThreadId,
                type: "goal.attempt-transitioned",
                payload: cancelledAttempt,
                occurredAt: now,
              },
              {
                id: nodeEventId,
                threadId: detail.goal.rootThreadId,
                type: "goal.node-transitioned",
                payload: {
                  ...node,
                  activeAttemptId: null,
                  updatedAt: timestamp,
                },
                occurredAt: now,
              },
            ],
            effects: [],
          })
          .pipe(Effect.mapError(workflowError("terminalize-cancelled-unbound-attempt")));
        if (!committed.committed) return;
        return;
      }

      yield* threads
        .dispatch({
          type: "run.interrupt",
          createdBy: "system",
          creationSource: "server",
          commandId: CommandId.make(`goal-node-cancel-interrupt:${attempt.id}:${node.status}`),
          threadId: attempt.executionThreadId,
          runId: attempt.runId,
          reason: node.blocker ?? `Node ${node.status} by root lead.`,
        })
        .pipe(Effect.mapError(workflowError("interrupt-cancelled-node")));
    });

    const cancelGoalRuns = Effect.fn("GoalWorkflowService.cancelGoalRuns")(function* (
      domainEvent: OrchestrationV2DomainEvent,
    ) {
      if (domainEvent.type !== "goal.cancelled") return;
      const detail = yield* goals
        .getDetail(domainEvent.payload.id)
        .pipe(Effect.mapError(workflowError("read-cancelled-goal")));
      const root = yield* threads
        .getThreadProjection(detail.goal.rootThreadId)
        .pipe(Effect.mapError(workflowError("read-cancelled-root")));
      const targets = [
        ...root.runs
          .filter((run) => ACTIVE_RUN_STATUSES.has(run.status))
          .map((run) => ({ threadId: detail.goal.rootThreadId, runId: run.id })),
        ...detail.attempts.flatMap((attempt) =>
          attempt.executionThreadId !== null &&
          attempt.runId !== null &&
          ["leased", "launching", "running", "stalled"].includes(attempt.status)
            ? [{ threadId: attempt.executionThreadId, runId: attempt.runId }]
            : [],
        ),
      ];
      yield* Effect.forEach(
        targets,
        (target) =>
          threads.dispatch({
            type: "run.interrupt",
            createdBy: "system",
            creationSource: "server",
            commandId: CommandId.make(`goal-cancel-interrupt:${detail.goal.id}:${target.runId}`),
            threadId: target.threadId,
            runId: target.runId,
            reason: "Goal cancelled by the user.",
          }),
        { concurrency: 4, discard: true },
      );
    });

    const pauseForRootControl = Effect.fn("GoalWorkflowService.pauseForRootControl")(function* (
      domainEvent: OrchestrationV2DomainEvent,
    ) {
      if (domainEvent.type !== "run.updated") return;
      if (
        !ACTIVE_RUN_STATUSES.has(domainEvent.payload.status) &&
        domainEvent.payload.status !== "interrupted"
      )
        return;
      const binding = yield* goals
        .resolveMcpBinding(domainEvent.threadId)
        .pipe(Effect.mapError(workflowError("resolve-root-control")));
      if (binding?.kind !== "lead") return;
      const detail = yield* goals
        .getDetail(binding.goalId)
        .pipe(Effect.mapError(workflowError("read-root-control-goal")));
      if (detail.goal.status !== "running") return;
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const commandId = CommandId.make(`goal-root-control-pause:${domainEvent.id}`);
      const eventId = yield* ids.allocate
        .event({ threadId: detail.goal.rootThreadId, commandId })
        .pipe(Effect.mapError(workflowError("allocate-root-control-event")));
      yield* eventSink
        .commitCommand({
          commandId,
          threadId: detail.goal.rootThreadId,
          commandType: "goal.root-control.pause",
          acceptedAt: now,
          events: [
            {
              id: eventId,
              threadId: detail.goal.rootThreadId,
              type: "goal.updated",
              payload: { ...detail.goal, status: "paused", updatedAt: timestamp },
              occurredAt: now,
            },
          ],
          effects: [],
        })
        .pipe(Effect.mapError(workflowError("persist-root-control-pause")));
    });

    const liveBoundary = yield* eventSink
      .latestSequence()
      .pipe(Effect.mapError(workflowError("read-live-boundary")));
    const observe = eventSink.stream({ afterSequence: liveBoundary }).pipe(
      Stream.runForEach((stored) =>
        superviseRootLeadNoGraph(stored.event).pipe(
          Effect.andThen(cancelGoalRuns(stored.event)),
          Effect.andThen(cancelNodeAttempt(stored.event)),
          Effect.andThen(pauseForRootControl(stored.event)),
          Effect.andThen(updateAttempt(stored.event)),
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
