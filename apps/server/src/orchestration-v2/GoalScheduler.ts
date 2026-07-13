import {
  CommandId,
  GoalAttemptId,
  GoalEvidenceId,
  GoalFailureReason,
  type GoalDetail,
  type GoalAttempt,
  type GoalNodeId,
  type GoalNodeProjection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as NodeOS from "node:os";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { GoalRoutingService } from "./GoalRoutingService.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";

const encodeGoalFailureReason = Schema.encodeSync(Schema.fromJsonString(GoalFailureReason));

const ACTIVE_ATTEMPT_STATUSES = new Set(["leased", "launching", "running", "stalled"]);
const DEPENDENCY_FAILURE_STATUSES = new Set(["failed", "cancelled"]);
const SCHEDULABLE_GOAL_STATUSES = new Set(["planning", "running"]);
export const GOAL_AGENT_LIFETIME_LIMIT = 1_000;
export const GOAL_AGENT_WARNING_THRESHOLD = 25;

export function goalWorkerCapacity(logicalCpuCount: number): number {
  return Math.min(16, Math.max(2, Math.floor(logicalCpuCount) - 2));
}

export interface GoalSchedulerLaunch {
  readonly goal: GoalDetail;
  readonly node: GoalNodeProjection;
}

export interface GoalSchedulerTransition {
  readonly goalId: GoalDetail["goal"]["id"];
  readonly graphVersionId: GoalNodeProjection["graphVersionId"];
  readonly nodeId: GoalNodeId;
  readonly status: "ready" | "queued" | "blocked";
  readonly reason: "capacity" | "writer_capacity" | "dependency_failure" | "resource_backstop";
  readonly queuePosition: number | null;
  readonly dependencyNodeId: GoalNodeId | null;
}

export interface GoalSchedulerWarning {
  readonly goalId: GoalDetail["goal"]["id"];
  readonly createdAgents: number;
  readonly limit: number;
}

export interface GoalSchedulingPlan {
  readonly launches: ReadonlyArray<GoalSchedulerLaunch>;
  readonly transitions: ReadonlyArray<GoalSchedulerTransition>;
  readonly warnings: ReadonlyArray<GoalSchedulerWarning>;
}

function currentNodes(detail: GoalDetail): ReadonlyArray<GoalNodeProjection> {
  return detail.nodes.filter(
    (projection) => projection.graphVersionId === detail.goal.currentGraphVersionId,
  );
}

function roundRobin<T>(groups: ReadonlyArray<ReadonlyArray<T>>): ReadonlyArray<T> {
  const output: T[] = [];
  const maximum = Math.max(0, ...groups.map((group) => group.length));
  for (let index = 0; index < maximum; index += 1) {
    for (const group of groups) {
      const item = group[index];
      if (item !== undefined) output.push(item);
    }
  }
  return output;
}

function createdAgentCount(detail: GoalDetail): number {
  return (
    detail.attempts.length +
    detail.attempts.reduce((total, attempt) => total + attempt.usage.nativeDescendantCount, 0)
  );
}

function workspaceModeForAttempt(
  detail: GoalDetail,
  attempt: GoalAttempt,
): GoalNodeProjection["node"]["workspaceMode"] | null {
  return (
    detail.nodes.find(
      (projection) =>
        projection.graphVersionId === attempt.graphVersionId &&
        projection.node.id === attempt.nodeId,
    )?.node.workspaceMode ?? null
  );
}

export function planGoalScheduling(input: {
  readonly goals: ReadonlyArray<GoalDetail>;
  readonly workerCapacity: number;
  readonly writerCapacity: number;
}): GoalSchedulingPlan {
  // Goal lifecycle gates new work only. An in-flight provider attempt owns a
  // global worker slot until it reaches a terminal state, even if Queue,
  // Steer, Stop, a conflict, or a replacement graph pauses its goal.
  const activeAttempts = input.goals.flatMap((detail) =>
    detail.attempts
      .filter((attempt) => ACTIVE_ATTEMPT_STATUSES.has(attempt.status))
      .map((attempt) => ({ detail, attempt })),
  );
  const activeWriterAttempts = activeAttempts.filter(({ detail, attempt }) => {
    const workspaceMode = workspaceModeForAttempt(detail, attempt);
    // Goal graph/attempt referential integrity normally makes this impossible,
    // but fail closed if a partially recovered projection cannot identify the
    // writer mode: undercounting could violate the global writer semaphore.
    return workspaceMode !== "read_only";
  });
  const orderedGoals = input.goals
    .filter((detail) => SCHEDULABLE_GOAL_STATUSES.has(detail.goal.status))
    .toSorted((left, right) => {
      const activeDifference =
        left.attempts.filter((attempt) => ACTIVE_ATTEMPT_STATUSES.has(attempt.status)).length -
        right.attempts.filter((attempt) => ACTIVE_ATTEMPT_STATUSES.has(attempt.status)).length;
      return activeDifference === 0
        ? left.goal.createdAt.localeCompare(right.goal.createdAt)
        : activeDifference;
    });
  let remainingWorkers = Math.max(0, input.workerCapacity - activeAttempts.length);
  let remainingWriters = Math.max(0, input.writerCapacity - activeWriterAttempts.length);
  const transitions: GoalSchedulerTransition[] = [];
  const warnings: GoalSchedulerWarning[] = [];
  const remainingAgentBudget = new Map(
    orderedGoals.map((detail) => [
      detail.goal.id,
      Math.max(0, GOAL_AGENT_LIFETIME_LIMIT - createdAgentCount(detail)),
    ]),
  );
  const candidatesByGoal = orderedGoals.map((detail) => {
    const nodes = currentNodes(detail);
    const statusByNode = new Map(
      nodes.map((projection) => [projection.node.id, projection.status]),
    );
    const graph = detail.graphVersions.find(
      (version) => version.id === detail.goal.currentGraphVersionId,
    );
    const createdAgents = createdAgentCount(detail);
    if (createdAgents >= GOAL_AGENT_WARNING_THRESHOLD) {
      warnings.push({ goalId: detail.goal.id, createdAgents, limit: GOAL_AGENT_LIFETIME_LIMIT });
    }
    return nodes.flatMap((projection): ReadonlyArray<GoalSchedulerLaunch> => {
      if (!new Set(["pending", "ready", "queued"]).has(projection.status)) return [];
      const dependencies =
        graph?.edges
          .filter((edge) => edge.toNodeId === projection.node.id)
          .map((edge) => edge.fromNodeId) ?? [];
      const failedDependency = dependencies.find((dependency) =>
        DEPENDENCY_FAILURE_STATUSES.has(statusByNode.get(dependency) ?? "pending"),
      );
      if (failedDependency !== undefined) {
        transitions.push({
          goalId: detail.goal.id,
          graphVersionId: projection.graphVersionId,
          nodeId: projection.node.id,
          status: "blocked",
          reason: "dependency_failure",
          queuePosition: null,
          dependencyNodeId: failedDependency,
        });
        return [];
      }
      if (!dependencies.every((dependency) => statusByNode.get(dependency) === "succeeded")) {
        return [];
      }
      return [{ goal: detail, node: projection }];
    });
  });

  const fairCandidates = roundRobin(candidatesByGoal);
  const launches: GoalSchedulerLaunch[] = [];
  const queued: Array<{
    readonly launch: GoalSchedulerLaunch;
    readonly reason: "capacity" | "writer_capacity";
  }> = [];
  for (const launch of fairCandidates) {
    const isWriter = launch.node.node.workspaceMode === "writer";
    const remainingForGoal = remainingAgentBudget.get(launch.goal.goal.id) ?? 0;
    if (remainingForGoal <= 0) {
      transitions.push({
        goalId: launch.goal.goal.id,
        graphVersionId: launch.node.graphVersionId,
        nodeId: launch.node.node.id,
        status: "queued",
        reason: "resource_backstop",
        queuePosition: null,
        dependencyNodeId: null,
      });
      continue;
    }
    if (remainingWorkers <= 0) {
      queued.push({ launch, reason: "capacity" });
      continue;
    }
    if (isWriter && remainingWriters <= 0) {
      queued.push({ launch, reason: "writer_capacity" });
      continue;
    }
    launches.push(launch);
    remainingAgentBudget.set(launch.goal.goal.id, remainingForGoal - 1);
    remainingWorkers -= 1;
    if (isWriter) remainingWriters -= 1;
  }
  queued.forEach(({ launch, reason }, index) => {
    transitions.push({
      goalId: launch.goal.goal.id,
      graphVersionId: launch.node.graphVersionId,
      nodeId: launch.node.node.id,
      status: "queued",
      reason,
      queuePosition: index + 1,
      dependencyNodeId: null,
    });
  });
  return { launches, transitions, warnings };
}

export class GoalSchedulerError extends Schema.TaggedErrorClass<GoalSchedulerError>()(
  "GoalSchedulerError",
  { operation: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

export interface GoalSchedulerTickResult {
  readonly plan: GoalSchedulingPlan;
  readonly leasedAttempts: ReadonlyArray<GoalAttempt>;
}

export class GoalScheduler extends Context.Service<
  GoalScheduler,
  { readonly tick: Effect.Effect<GoalSchedulerTickResult, GoalSchedulerError> }
>()("t3/orchestration-v2/GoalScheduler") {}

const schedulerFailure =
  (operation: string) =>
  (cause: unknown): GoalSchedulerError =>
    new GoalSchedulerError({ operation, cause });

export const layerWithOptions = (options?: {
  readonly workerCapacity?: number;
  readonly writerCapacity?: number;
  readonly leaseDurationMs?: number;
  readonly workerId?: string;
}) =>
  Layer.effect(
    GoalScheduler,
    Effect.gen(function* () {
      const goals = yield* GoalProjectionStore;
      const routes = yield* GoalRoutingService;
      const events = yield* EventSinkV2;
      const ids = yield* IdAllocatorV2;
      const workerCapacity =
        options?.workerCapacity ?? goalWorkerCapacity(NodeOS.availableParallelism());
      const writerCapacity = options?.writerCapacity ?? 4;
      const workerId = options?.workerId ?? "goal-scheduler";
      const leaseDurationMs = options?.leaseDurationMs ?? 2 * 60 * 1_000;

      const event = Effect.fn("GoalScheduler.event")(function* (
        rootThreadId: GoalDetail["goal"]["rootThreadId"],
        commandId: CommandId,
        type: OrchestrationV2DomainEvent["type"],
        payload: unknown,
        occurredAt: DateTime.Utc,
      ) {
        const id = yield* ids.allocate
          .event({ threadId: rootThreadId, commandId })
          .pipe(Effect.mapError(schedulerFailure("allocate-event")));
        return {
          id,
          threadId: rootThreadId,
          type,
          payload,
          occurredAt,
        } as OrchestrationV2DomainEvent;
      });

      const commitTransition = Effect.fn("GoalScheduler.commitTransition")(function* (
        transition: GoalSchedulerTransition,
        detail: GoalDetail,
      ) {
        const projection = currentNodes(detail).find(
          (candidate) => candidate.node.id === transition.nodeId,
        );
        if (projection === undefined) return;
        const commandId = CommandId.make(
          `goal-scheduler:${transition.graphVersionId}:${transition.nodeId}:${transition.status}:${transition.reason}:${transition.queuePosition ?? "none"}`,
        );
        const now = yield* DateTime.now;
        const updatedAt = DateTime.formatIso(now);
        const blocker =
          transition.status === "queued"
            ? `queue:${transition.reason}:position=${transition.queuePosition ?? "held"}`
            : transition.reason === "dependency_failure"
              ? `dependency_failure:${transition.dependencyNodeId ?? "unknown"}`
              : transition.reason;
        const nodeEvent = yield* event(
          detail.goal.rootThreadId,
          commandId,
          "goal.node-transitioned",
          { ...projection, status: transition.status, blocker, updatedAt },
          now,
        );
        yield* events
          .commitGoalNodeCommand({
            commandId,
            threadId: detail.goal.rootThreadId,
            commandType: "goal.scheduler.transition",
            acceptedAt: now,
            goalId: detail.goal.id,
            graphVersionId: transition.graphVersionId,
            nodeId: transition.nodeId,
            expectedStatuses: [projection.status],
            events: [nodeEvent],
            effects: [],
          })
          .pipe(Effect.mapError(schedulerFailure("transition")));
      });

      const lease = Effect.fn("GoalScheduler.lease")(function* (launch: GoalSchedulerLaunch) {
        const { goal: detail, node: projection } = launch;
        const ordinal =
          Math.max(
            0,
            ...detail.attempts
              .filter((attempt) => attempt.nodeId === projection.node.id)
              .map((attempt) => attempt.ordinal),
          ) + 1;
        const attemptId = GoalAttemptId.make(
          `goal-attempt:${projection.graphVersionId}:${projection.node.id}:${ordinal}`,
        );
        const commandId = CommandId.make(`goal-scheduler:lease:${attemptId}`);
        const decision = yield* routes
          .route({
            requested: projection.node.routingRequest,
            requiredCapabilities: projection.node.requiredCapabilities,
            providerAllowlist: projection.node.policy.providerAllowlist,
          })
          .pipe(Effect.mapError(schedulerFailure("route")));
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const baseAttempt: GoalAttempt = {
          id: attemptId,
          goalId: detail.goal.id,
          graphVersionId: projection.graphVersionId,
          nodeId: projection.node.id,
          ordinal,
          status: decision.type === "resolved" ? "leased" : "failed",
          requestedRoute: projection.node.routingRequest,
          resolvedRoute: decision.type === "resolved" ? decision.route : null,
          providerSessionId: null,
          executionThreadId: null,
          runId: null,
          rootExecutionNodeId: null,
          baseIntegrationSha: detail.goal.integrationSha,
          workspacePath: null,
          leaseOwner: decision.type === "resolved" ? workerId : null,
          leaseExpiresAt:
            decision.type === "resolved"
              ? DateTime.formatIso(DateTime.add(now, { milliseconds: leaseDurationMs }))
              : null,
          usage: {
            inputTokens: null,
            outputTokens: null,
            cachedTokens: null,
            costMicros: null,
            nativeDescendantCount: 0,
          },
          failureReason:
            decision.type === "ambiguous"
              ? encodeGoalFailureReason({
                  type: "ambiguous_routing",
                  candidates: decision.candidates,
                  unmetConstraints: decision.unmetConstraints,
                })
              : null,
          createdAt: timestamp,
          updatedAt: timestamp,
        };
        const attemptCreated = yield* event(
          detail.goal.rootThreadId,
          commandId,
          "goal.attempt-created",
          { ...baseAttempt, resolvedRoute: null },
          now,
        );
        const attemptRouted = yield* event(
          detail.goal.rootThreadId,
          commandId,
          "goal.route-resolved",
          baseAttempt,
          now,
        );
        const nextNode = {
          ...projection,
          status: decision.type === "resolved" ? ("running" as const) : ("blocked" as const),
          activeAttemptId: decision.type === "resolved" ? attemptId : null,
          blocker:
            decision.type === "resolved"
              ? null
              : `ambiguous_routing:${decision.unmetConstraints.join(",")}`,
          updatedAt: timestamp,
        };
        const nodeEvent = yield* event(
          detail.goal.rootThreadId,
          commandId,
          "goal.node-transitioned",
          nextNode,
          now,
        );
        const goalEvent = yield* event(
          detail.goal.rootThreadId,
          commandId,
          "goal.updated",
          {
            ...detail.goal,
            status: decision.type === "resolved" ? "running" : "blocked",
            updatedAt: timestamp,
          },
          now,
        );
        const failureEvents =
          decision.type === "resolved"
            ? []
            : [
                yield* event(
                  detail.goal.rootThreadId,
                  commandId,
                  "goal.failure-recorded",
                  {
                    id: GoalEvidenceId.make(`goal-routing-failure:${attemptId}`),
                    goalId: detail.goal.id,
                    graphVersionId: projection.graphVersionId,
                    nodeId: projection.node.id,
                    attemptId,
                    reason: {
                      type: "ambiguous_routing",
                      candidates: decision.candidates,
                      unmetConstraints: decision.unmetConstraints,
                    },
                    recoveryState: "unresolved",
                    blocker: nextNode.blocker,
                    occurredAt: timestamp,
                  },
                  now,
                ),
              ];
        const committed = yield* events
          .commitGoalNodeCommand({
            commandId,
            threadId: detail.goal.rootThreadId,
            commandType: "goal.scheduler.lease",
            acceptedAt: now,
            goalId: detail.goal.id,
            graphVersionId: projection.graphVersionId,
            nodeId: projection.node.id,
            expectedStatuses: [projection.status],
            events: [attemptCreated, attemptRouted, nodeEvent, goalEvent, ...failureEvents],
            effects:
              decision.type === "resolved"
                ? [
                    {
                      id: `goal-attempt-launch:${attemptId}`,
                      commandId,
                      threadId: detail.goal.rootThreadId,
                      request: {
                        type: "goal-attempt.launch",
                        goalId: detail.goal.id,
                        graphVersionId: projection.graphVersionId,
                        nodeId: projection.node.id,
                        attemptId,
                      },
                    },
                  ]
                : [],
          })
          .pipe(Effect.mapError(schedulerFailure("lease")));
        return committed.committed && decision.type === "resolved" ? baseAttempt : null;
      });

      const tick = Effect.gen(function* () {
        const details = yield* goals.listSchedulable.pipe(
          Effect.mapError(schedulerFailure("list-goals")),
        );
        const plan = planGoalScheduling({ goals: details, workerCapacity, writerCapacity });
        const byGoal = new Map(details.map((detail) => [detail.goal.id, detail]));
        yield* Effect.forEach(
          plan.transitions,
          (transition) => {
            const detail = byGoal.get(transition.goalId);
            return detail === undefined ? Effect.void : commitTransition(transition, detail);
          },
          { concurrency: 1 },
        );
        yield* Effect.forEach(
          plan.warnings,
          (warning) => {
            const detail = byGoal.get(warning.goalId);
            if (detail === undefined) return Effect.void;
            return Effect.gen(function* () {
              const exceeded = warning.createdAgents >= warning.limit;
              const threshold = exceeded ? warning.limit : GOAL_AGENT_WARNING_THRESHOLD;
              const commandId = CommandId.make(`goal-agent-warning:${warning.goalId}:${threshold}`);
              const now = yield* DateTime.now;
              const timestamp = DateTime.formatIso(now);
              const warningEvent = yield* event(
                detail.goal.rootThreadId,
                commandId,
                "goal.failure-recorded",
                {
                  id: GoalEvidenceId.make(`goal-agent-warning:${warning.goalId}:${threshold}`),
                  goalId: warning.goalId,
                  graphVersionId: null,
                  nodeId: null,
                  attemptId: null,
                  reason: {
                    type: "resource_backstop",
                    limit: warning.limit,
                    observed: warning.createdAgents,
                    warning: !exceeded,
                  },
                  recoveryState: exceeded ? "terminal" : "resolved",
                  blocker: exceeded ? "Goal agent lifetime backstop exhausted." : null,
                  occurredAt: timestamp,
                },
                now,
              );
              const lifecycleEvents = exceeded
                ? [
                    yield* event(
                      detail.goal.rootThreadId,
                      commandId,
                      "goal.updated",
                      { ...detail.goal, status: "blocked", updatedAt: timestamp },
                      now,
                    ),
                  ]
                : [];
              yield* events
                .commitCommand({
                  commandId,
                  threadId: detail.goal.rootThreadId,
                  commandType: "goal.scheduler.resource-warning",
                  acceptedAt: now,
                  events: [warningEvent, ...lifecycleEvents],
                  effects: [],
                })
                .pipe(Effect.mapError(schedulerFailure("persist-resource-warning")));
            });
          },
          { concurrency: 1 },
        );
        const leased = yield* Effect.forEach(plan.launches, lease, { concurrency: 4 });
        return {
          plan,
          leasedAttempts: leased.filter((attempt): attempt is GoalAttempt => attempt !== null),
        };
      });
      return GoalScheduler.of({ tick });
    }),
  );

export const layer = layerWithOptions();
