import {
  CommandId,
  type GoalAttempt,
  type GoalAttemptId,
  type GoalDetail,
  type GoalGraphNode,
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
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
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
      if (projection.node.workspaceMode !== "read_only") {
        return yield* failure(
          "resolve-workspace",
          input.attemptId,
        )(
          "Writer and integration attempts require an isolated workspace from GoalWorkspaceService.",
        );
      }
      const projectId = detail.goal.projectId;
      if (projectId === undefined) {
        return yield* failure("resolve-project", input.attemptId)("Goal has no project identity.");
      }
      const executionThreadId = ThreadId.make(`goal-worker:${input.attemptId}`);
      const modelSelection = {
        instanceId: resolvedRoute.providerInstanceId,
        model: resolvedRoute.model,
      };
      yield* threads
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make(`goal-attempt-thread:${input.attemptId}`),
          threadId: executionThreadId,
          projectId,
          title: `${projection.node.role}: ${projection.node.objective}`.slice(0, 512),
          modelSelection,
          runtimeMode:
            projection.node.policy.sandboxMode === "read-only"
              ? "approval-required"
              : projection.node.policy.sandboxMode === "workspace-write"
                ? "auto-accept-edits"
                : "full-access",
          interactionMode: detail.goal.rootInteractionMode ?? "default",
          branch: null,
          worktreePath: null,
          createdBy: "system",
          creationSource: "server",
        })
        .pipe(Effect.mapError(failure("create-thread", input.attemptId)));
      const persistAttempt = Effect.fn("GoalAttemptExecutionService.persistAttempt")(function* (
        updated: GoalAttempt,
        phase: "thread-bound" | "run-bound",
        expectedStatus: GoalAttempt["status"],
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
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 10 })),
          updatedAt: DateTime.formatIso(now),
        };
        yield* persistAttempt(threadBound, "thread-bound", "leased");
        attempt = threadBound;
      }
      if (attempt.executionThreadId !== executionThreadId) {
        return yield* failure(
          "bind-thread",
          input.attemptId,
        )("Attempt is bound to a different execution thread.");
      }
      if (attempt.runId !== null) return;
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
      const now = yield* DateTime.now;
      const updated: GoalAttempt = {
        ...attempt,
        status: "launching",
        executionThreadId,
        runId: run.id,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 10 })),
        updatedAt: DateTime.formatIso(now),
      };
      yield* persistAttempt(updated, "run-bound", "launching");
    });
    return GoalAttemptExecutionService.of({ launch });
  }),
);
