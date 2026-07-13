import {
  CommandId,
  type GoalAttempt,
  type GoalAttemptId,
  type GoalDetail,
  type GoalGraphNode,
  type GoalNodeStatus,
  MessageId,
  type OrchestrationV2DomainEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore, validateGoalNodeWorkspace } from "./GoalProjectionStore.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import {
  goalBranchName,
  GoalWorkspaceService,
  type GoalPreparedWorkspace,
} from "./GoalWorkspaceService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

export function goalWorkerPrompt(input: {
  readonly goal: GoalDetail["goal"];
  readonly node: GoalGraphNode;
}): string {
  return [
    `Goal: ${input.goal.objective}`,
    `Role: ${input.node.role}`,
    `Persona: ${input.node.persona}`,
    `Node objective: ${input.node.objective}`,
    "",
    "Success criteria:",
    ...input.node.successCriteria.map((criterion) => `- ${criterion}`),
    "",
    `Output contract (${input.node.outputContract.kind}): ${input.node.outputContract.description}`,
    `Required fields: ${input.node.outputContract.requiredFields.join(", ") || "none"}`,
    "",
    "Context packet:",
    `- schema version: ${input.node.contextPacket.schemaVersion}`,
    `- digest: ${input.node.contextPacket.digest ?? "none"}`,
    `- objective: ${input.node.contextPacket.objective}`,
    `- dependency nodes: ${input.node.contextPacket.dependencyOutputs.join(", ") || "none"}`,
    `- artifact refs: ${input.node.contextPacket.artifacts.join(", ") || "none"}`,
    ...input.node.contextPacket.notes.map((note) => `- note: ${note}`),
  ].join("\n");
}

export class GoalAttemptExecutionError extends Schema.TaggedErrorClass<GoalAttemptExecutionError>()(
  "GoalAttemptExecutionError",
  {
    operation: Schema.String,
    attemptId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class GoalAttemptExecutionService extends Context.Service<
  GoalAttemptExecutionService,
  {
    readonly launch: (input: {
      readonly goalId: GoalDetail["goal"]["id"];
      readonly attemptId: GoalAttemptId;
    }) => Effect.Effect<void, GoalAttemptExecutionError>;
  }
>()("t3/orchestration-v2/GoalAttemptExecutionService") {}

export function goalAttemptWorkspaceBindingError(input: {
  readonly node: GoalGraphNode;
  readonly attemptId: GoalAttemptId;
  readonly workspace: GoalPreparedWorkspace;
  readonly integrationWorktreePath: string | null;
}): string | null {
  if (input.node.workspaceMode === "integration")
    return "Goal integration worktrees are reserved for server-controlled assembly.";
  if (
    input.integrationWorktreePath !== null &&
    input.workspace.path === input.integrationWorktreePath
  )
    return "Worker attempts must not receive the retained integration worktree.";
  if (input.node.workspaceMode === "read_only")
    return input.workspace.sharedReadOnly
      ? null
      : "Read-only nodes must receive a shared read-only workspace.";
  if (input.workspace.sharedReadOnly)
    return "Writer nodes must receive a mutable isolated writer workspace.";
  return input.workspace.branch === goalBranchName("worker", input.attemptId)
    ? null
    : "Writer nodes must receive their attempt-specific writer branch.";
}

/**
 * A provider start is permitted only while the goal is actively executing and
 * the exact leased attempt still owns a live, non-superseded node. This is
 * deliberately re-evaluated around every durable/external launch boundary:
 * cancelling a goal is terminal even if an outbox worker claimed its launch
 * effect a moment earlier.
 */
export function goalAttemptLaunchIsFenced(input: {
  readonly goalStatus: GoalDetail["goal"]["status"];
  readonly attemptStatus: GoalAttempt["status"] | undefined;
  readonly nodeStatus: GoalNodeStatus | undefined;
}): boolean {
  return (
    input.goalStatus === "running" &&
    (input.attemptStatus === "leased" || input.attemptStatus === "launching") &&
    input.nodeStatus !== undefined &&
    input.nodeStatus !== "cancelled" &&
    input.nodeStatus !== "superseded"
  );
}

const failure = (operation: string, attemptId?: GoalAttemptId) => (cause: unknown) =>
  new GoalAttemptExecutionError({
    operation,
    ...(attemptId === undefined ? {} : { attemptId }),
    cause,
  });

export const layer = Layer.effect(
  GoalAttemptExecutionService,
  Effect.gen(function* () {
    const goals = yield* GoalProjectionStore;
    const threads = yield* ThreadManagementService;
    const events = yield* EventSinkV2;
    const ids = yield* IdAllocatorV2;
    const workspaces = yield* GoalWorkspaceService;

    const launch = Effect.fn("GoalAttemptExecutionService.launch")(function* (input: {
      readonly goalId: GoalDetail["goal"]["id"];
      readonly attemptId: GoalAttemptId;
    }) {
      const detail = yield* goals
        .getDetail(input.goalId)
        .pipe(Effect.mapError(failure("read-goal", input.attemptId)));
      const persistedAttempt = detail.attempts.find(
        (candidate) => candidate.id === input.attemptId,
      );
      if (persistedAttempt === undefined) {
        return yield* failure("read-attempt", input.attemptId)("Attempt does not exist.");
      }
      if (persistedAttempt.status !== "leased" && persistedAttempt.status !== "launching") return;
      if (detail.goal.status !== "running") return;
      if (persistedAttempt.resolvedRoute === null) {
        return yield* failure("resolve-route", input.attemptId)("Attempt has no resolved route.");
      }
      const resolvedRoute = persistedAttempt.resolvedRoute;
      let attempt = persistedAttempt;
      const projection = detail.nodes.find(
        (candidate) =>
          candidate.graphVersionId === attempt.graphVersionId &&
          candidate.node.id === attempt.nodeId,
      );
      if (projection === undefined) {
        return yield* failure("read-node", input.attemptId)("Attempt node does not exist.");
      }
      if (projection.status === "cancelled" || projection.status === "superseded") return;
      yield* Effect.try({
        try: () => validateGoalNodeWorkspace(projection.node),
        catch: failure("validate-workspace-mode", input.attemptId),
      });
      const launchStillFenced = Effect.fn("GoalAttemptExecutionService.launchStillFenced")(
        function* () {
          const fresh = yield* goals
            .getDetail(input.goalId)
            .pipe(Effect.mapError(failure("refresh-launch-fence", input.attemptId)));
          const freshAttempt = fresh.attempts.find((candidate) => candidate.id === input.attemptId);
          const freshNode = fresh.nodes.find(
            (candidate) =>
              candidate.graphVersionId === attempt.graphVersionId &&
              candidate.node.id === attempt.nodeId,
          );
          return goalAttemptLaunchIsFenced({
            goalStatus: fresh.goal.status,
            attemptStatus: freshAttempt?.status,
            nodeStatus: freshNode?.status,
          });
        },
      );
      // Do not create a mutable writer worktree for an attempt that lost its
      // goal-level cancellation race before we reached the workspace boundary.
      if (!(yield* launchStillFenced())) return;
      const workspace = yield* workspaces
        .prepareAttempt(input)
        .pipe(Effect.mapError(failure("resolve-workspace", input.attemptId)));
      const workspaceBindingError = goalAttemptWorkspaceBindingError({
        node: projection.node,
        attemptId: input.attemptId,
        workspace,
        integrationWorktreePath: detail.goal.integrationWorktreePath,
      });
      if (workspaceBindingError !== null)
        return yield* failure("validate-workspace-binding", input.attemptId)(workspaceBindingError);
      // Workspace creation is an external side effect and may take long enough
      // for Cancel Goal to win. Fence again before changing durable attempt
      // ownership from leased to launching.
      if (!(yield* launchStillFenced())) return;
      const projectId = detail.goal.projectId;
      if (projectId === undefined) {
        return yield* failure("resolve-project", input.attemptId)("Goal has no project identity.");
      }
      const executionThreadId = ThreadId.make(`goal-worker:${input.attemptId}`);
      const modelSelection = {
        instanceId: resolvedRoute.providerInstanceId,
        model: resolvedRoute.model,
      };
      const persistAttempt = Effect.fn("GoalAttemptExecutionService.persistAttempt")(function* (
        updated: GoalAttempt,
        phase: "thread-bound" | "run-bound",
        expectedStatus: GoalAttempt["status"],
        expectedNodeStatuses?: ReadonlyArray<GoalNodeStatus>,
      ) {
        const now = yield* DateTime.now;
        const commandId = CommandId.make(`goal-attempt-${phase}:${input.attemptId}`);
        const eventId = yield* ids.allocate
          .event({ threadId: detail.goal.rootThreadId, commandId })
          .pipe(Effect.mapError(failure("allocate-event", input.attemptId)));
        const transition = {
          id: eventId,
          threadId: detail.goal.rootThreadId,
          type: "goal.attempt-transitioned",
          payload: updated,
          occurredAt: now,
        } satisfies OrchestrationV2DomainEvent;
        return yield* events
          .commitGoalAttemptCommand({
            commandId,
            threadId: detail.goal.rootThreadId,
            commandType: `goal.attempt.${phase}`,
            acceptedAt: now,
            goalId: detail.goal.id,
            graphVersionId: attempt.graphVersionId,
            nodeId: attempt.nodeId,
            attemptId: attempt.id,
            expectedStatuses: [expectedStatus],
            ...(expectedNodeStatuses === undefined ? {} : { expectedNodeStatuses }),
            events: [transition],
            effects: [],
          })
          .pipe(Effect.mapError(failure(`persist-${phase}`, input.attemptId)));
      });
      if (attempt.status === "leased") {
        const now = yield* DateTime.now;
        const threadBound: GoalAttempt = {
          ...attempt,
          status: "launching",
          executionThreadId,
          baseIntegrationSha: workspace.baseSha,
          workspacePath: workspace.path,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 10 })),
          updatedAt: DateTime.formatIso(now),
        };
        const committed = yield* persistAttempt(threadBound, "thread-bound", "leased", ["running"]);
        if (!committed.committed) return;
        attempt = threadBound;
      }
      if (attempt.executionThreadId !== executionThreadId) {
        return yield* failure(
          "bind-thread",
          input.attemptId,
        )("Attempt is bound to a different execution thread.");
      }
      if (attempt.runId !== null) return;
      if (!(yield* launchStillFenced())) return;
      yield* threads
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make(`goal-attempt-thread:${input.attemptId}`),
          threadId: executionThreadId,
          projectId,
          title: `${projection.node.role}: ${projection.node.objective}`.slice(0, 512),
          modelSelection,
          runtimeMode:
            projection.node.workspaceMode === "read_only"
              ? "approval-required"
              : "auto-accept-edits",
          interactionMode: detail.goal.rootInteractionMode ?? "default",
          branch: workspace.branch,
          worktreePath: workspace.path,
          createdBy: "system",
          creationSource: "server",
        })
        .pipe(Effect.mapError(failure("create-thread", input.attemptId)));
      if (!(yield* launchStillFenced())) return;
      yield* threads
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`goal-attempt-message:${input.attemptId}`),
          threadId: executionThreadId,
          messageId: MessageId.make(`goal-worker-message:${input.attemptId}`),
          text: goalWorkerPrompt({ goal: detail.goal, node: projection.node }),
          attachments: [],
          modelSelection: {
            instanceId: resolvedRoute.providerInstanceId,
            model: resolvedRoute.model,
          },
          dispatchMode: { type: "start_immediately" },
          createdBy: "system",
          creationSource: "server",
        })
        .pipe(Effect.mapError(failure("dispatch-message", input.attemptId)));
      const launched = yield* threads
        .getThreadProjection(executionThreadId)
        .pipe(Effect.mapError(failure("read-worker-thread", input.attemptId)));
      const run = launched.runs.at(-1);
      if (run === undefined) {
        return yield* failure(
          "launch-thread",
          input.attemptId,
        )("Worker thread launched without a durable run.");
      }
      // If cancellation committed in the narrow interval around the message
      // transaction, the worker run may exist before its attempt can be
      // durably bound. Compensate immediately rather than leaving a provider
      // turn alive without an owning active goal attempt.
      if (!(yield* launchStillFenced())) {
        yield* threads
          .dispatch({
            type: "run.interrupt",
            commandId: CommandId.make(`goal-attempt-cancel-race-interrupt:${input.attemptId}`),
            threadId: executionThreadId,
            runId: run.id,
            reason: "Goal cancelled before the worker launch could be bound.",
            createdBy: "system",
            creationSource: "server",
          })
          .pipe(Effect.mapError(failure("interrupt-cancel-race", input.attemptId)));
        return;
      }
      const now = yield* DateTime.now;
      const updated: GoalAttempt = {
        ...attempt,
        status: "launching",
        executionThreadId,
        runId: run.id,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 10 })),
        updatedAt: DateTime.formatIso(now),
      };
      yield* persistAttempt(updated, "run-bound", "launching", ["running"]);
    });
    return GoalAttemptExecutionService.of({ launch });
  }),
);
