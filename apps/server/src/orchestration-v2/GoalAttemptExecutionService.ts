import {
  CommandId,
  type GoalAttempt,
  type GoalAttemptId,
  type GoalDetail,
  type GoalGraphNode,
  type GoalNodeId,
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
import { isGoalVerifierNode, transitiveAncestorNodeIds } from "./GoalGraphSemantics.ts";
import { buildGoalWorkerPrompt, type GoalWorkerExecutionContext } from "./GoalPrompts.ts";
import { GoalProjectionStore, validateGoalNodeWorkspace } from "./GoalProjectionStore.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import {
  goalBranchName,
  GoalWorkspaceService,
  type GoalPreparedWorkspace,
} from "./GoalWorkspaceService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

const GoalAttemptExecutionReason = Schema.Literals([
  "stale_graph",
  "attempt_binding_mismatch",
  "workspace_binding_mismatch",
  "verification_sha_mismatch",
  "ancestor_attempt_missing",
  "producer_context_missing",
  "producer_context_ambiguous",
]);
type GoalAttemptExecutionReason = typeof GoalAttemptExecutionReason.Type;

export class GoalAttemptExecutionError extends Schema.TaggedErrorClass<GoalAttemptExecutionError>()(
  "GoalAttemptExecutionError",
  {
    operation: Schema.String,
    attemptId: Schema.optional(Schema.String),
    reason: Schema.optional(GoalAttemptExecutionReason),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isGoalAttemptExecutionError = Schema.is(GoalAttemptExecutionError);

function executionContractError(input: {
  readonly reason: GoalAttemptExecutionReason;
  readonly operation: string;
  readonly attemptId: GoalAttemptId;
  readonly cause: string;
}): GoalAttemptExecutionError {
  return new GoalAttemptExecutionError(input);
}

function throwExecutionContractError(input: {
  readonly reason: GoalAttemptExecutionReason;
  readonly operation: string;
  readonly attemptId: GoalAttemptId;
  readonly cause: string;
}): never {
  throw executionContractError(input);
}

export function buildGoalWorkerExecutionCapsule(input: {
  readonly detail: GoalDetail;
  readonly attemptId: GoalAttemptId;
  readonly workspace: GoalPreparedWorkspace;
}): GoalWorkerExecutionContext {
  const { detail, attemptId, workspace } = input;
  const attempt = detail.attempts.find((candidate) => candidate.id === attemptId);
  if (attempt === undefined) {
    return throwExecutionContractError({
      reason: "attempt_binding_mismatch",
      operation: "validate-fresh-attempt",
      attemptId,
      cause: "The bound attempt is absent from the fresh goal projection.",
    });
  }
  const graph = detail.graphVersions.find(
    (candidate) => candidate.id === detail.goal.currentGraphVersionId,
  );
  if (
    graph === undefined ||
    graph.id !== attempt.graphVersionId ||
    graph.revision !== detail.goal.currentRevision
  ) {
    return throwExecutionContractError({
      reason: "stale_graph",
      operation: "validate-fresh-graph",
      attemptId,
      cause: "The attempt no longer belongs to the current graph revision.",
    });
  }
  const graphNode = graph.nodes.find((candidate) => candidate.id === attempt.nodeId);
  const nodeProjection = detail.nodes.find(
    (candidate) => candidate.graphVersionId === graph.id && candidate.node.id === attempt.nodeId,
  );
  if (
    attempt.status !== "launching" ||
    graphNode === undefined ||
    nodeProjection === undefined ||
    nodeProjection.node.id !== attempt.nodeId ||
    nodeProjection.activeAttemptId !== attempt.id ||
    nodeProjection.status !== "running"
  ) {
    return throwExecutionContractError({
      reason: "attempt_binding_mismatch",
      operation: "validate-fresh-attempt",
      attemptId,
      cause:
        "The fresh projection does not show this launching attempt as the active node binding.",
    });
  }
  const workspaceBindingIssue = goalAttemptWorkspaceBindingError({
    node: graphNode,
    attemptId,
    workspace,
    integrationWorktreePath: detail.goal.integrationWorktreePath,
  });
  if (
    workspaceBindingIssue !== null ||
    attempt.workspacePath !== workspace.path ||
    attempt.baseIntegrationSha !== workspace.baseSha
  ) {
    return throwExecutionContractError({
      reason: "workspace_binding_mismatch",
      operation: "validate-fresh-workspace",
      attemptId,
      cause:
        workspaceBindingIssue ??
        "The prepared workspace no longer matches the attempt's durable path and base SHA.",
    });
  }
  if (isGoalVerifierNode(graphNode) && detail.goal.integrationSha !== workspace.baseSha) {
    return throwExecutionContractError({
      reason: "verification_sha_mismatch",
      operation: "validate-verification-sha",
      attemptId,
      cause: "A verifier must launch against the current final integration SHA.",
    });
  }

  const ancestorIds = transitiveAncestorNodeIds(graph, graphNode.id);
  const ancestorNodes = graph.nodes.filter((candidate) => ancestorIds.has(candidate.id));
  const latestSucceededAttempts = new Map<GoalNodeId, GoalDetail["attempts"][number]>();
  for (const candidate of detail.attempts) {
    if (
      candidate.graphVersionId !== graph.id ||
      candidate.status !== "succeeded" ||
      !ancestorIds.has(candidate.nodeId)
    ) {
      continue;
    }
    const current = latestSucceededAttempts.get(candidate.nodeId);
    if (
      current === undefined ||
      candidate.ordinal > current.ordinal ||
      (candidate.ordinal === current.ordinal &&
        String(candidate.id).localeCompare(String(current.id)) > 0)
    ) {
      latestSucceededAttempts.set(candidate.nodeId, candidate);
    }
  }
  const ancestorAttempts = ancestorNodes.map((ancestorNode) => {
    const selected = latestSucceededAttempts.get(ancestorNode.id);
    if (selected === undefined) {
      return throwExecutionContractError({
        reason: "ancestor_attempt_missing",
        operation: "select-ancestor-attempt",
        attemptId,
        cause: `Ancestor node ${ancestorNode.id} has no succeeded attempt in the active graph.`,
      });
    }
    return {
      node: ancestorNode,
      attempt: selected,
    };
  });
  const selectedAttemptsByNode = new Map(
    ancestorAttempts.map(({ node: ancestorNode, attempt: selected }) => [
      ancestorNode.id,
      selected,
    ]),
  );
  const ancestorArtifacts = detail.artifacts
    .filter((artifact) => selectedAttemptsByNode.get(artifact.nodeId)?.id === artifact.attemptId)
    .toSorted(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) ||
        String(left.id).localeCompare(String(right.id)),
    )
    .map((artifact) => ({
      id: artifact.id,
      nodeId: artifact.nodeId,
      attemptId: artifact.attemptId,
      kind: artifact.kind,
      uri: artifact.uri,
      digest: artifact.digest,
    }));

  const integratedWriterAttemptsByNode = new Map<GoalNodeId, Set<GoalAttemptId>>();
  for (const record of detail.writerCommits) {
    if (
      record.graphVersionId !== graph.id ||
      record.state !== "integrated" ||
      record.integrationAfterSha !== workspace.baseSha
    ) {
      continue;
    }
    const attemptIds = integratedWriterAttemptsByNode.get(record.nodeId);
    if (attemptIds === undefined) {
      integratedWriterAttemptsByNode.set(record.nodeId, new Set([record.attemptId]));
    } else {
      attemptIds.add(record.attemptId);
    }
  }
  const producerCandidates = isGoalVerifierNode(graphNode)
    ? ancestorAttempts.filter(
        ({ node: ancestorNode, attempt: selected }) =>
          ancestorNode.workspaceMode === "writer" &&
          integratedWriterAttemptsByNode.get(ancestorNode.id)?.has(selected.id) === true,
      )
    : [];
  const seenProducerAttempts = new Set<GoalAttemptId>();
  const distinctProducerCandidates = producerCandidates.filter(({ attempt: candidate }) => {
    if (seenProducerAttempts.has(candidate.id)) return false;
    seenProducerAttempts.add(candidate.id);
    return true;
  });
  if (isGoalVerifierNode(graphNode) && distinctProducerCandidates.length === 0) {
    return throwExecutionContractError({
      reason: "producer_context_missing",
      operation: "select-producer-context",
      attemptId,
      cause: "No succeeded ancestor writer integrated the verifier workspace base SHA.",
    });
  }
  if (distinctProducerCandidates.length > 1) {
    return throwExecutionContractError({
      reason: "producer_context_ambiguous",
      operation: "select-producer-context",
      attemptId,
      cause: "Multiple succeeded ancestor writers claim the verifier workspace base SHA.",
    });
  }
  const producer = distinctProducerCandidates[0];

  return {
    node: graphNode,
    capsule: {
      goalId: detail.goal.id,
      graphVersionId: graph.id,
      graphRevision: graph.revision,
      nodeId: graphNode.id,
      attemptId: attempt.id,
      workspaceMode: graphNode.workspaceMode,
      branch: workspace.branch,
      baseSha: workspace.baseSha,
      ancestorNodeIds: ancestorNodes.map((ancestorNode) => ancestorNode.id),
      ancestorAttempts: ancestorAttempts.map(({ node: ancestorNode, attempt: selected }) => ({
        nodeId: ancestorNode.id,
        attemptId: selected.id,
        ordinal: selected.ordinal,
      })),
      ancestorArtifacts,
      preferredProducerAttempt:
        producer === undefined
          ? null
          : {
              nodeId: producer.node.id,
              attemptId: producer.attempt.id,
              integrationSha: workspace.baseSha,
            },
    },
  };
}

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
          if (fresh.goal.currentGraphVersionId !== attempt.graphVersionId) return false;
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
      const projectId = detail.goal.projectId;
      if (projectId === undefined) {
        return yield* failure("resolve-project", input.attemptId)("Goal has no project identity.");
      }
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
        return yield* executionContractError({
          reason: "attempt_binding_mismatch",
          operation: "bind-thread",
          attemptId: input.attemptId,
          cause: "Attempt is bound to a different execution thread.",
        });
      }
      if (attempt.runId !== null) return;
      const freshDetail = yield* goals
        .getDetail(input.goalId)
        .pipe(Effect.mapError(failure("refresh-execution-capsule", input.attemptId)));
      if (freshDetail.goal.status !== "running") return;
      const execution = yield* Effect.try({
        try: () =>
          buildGoalWorkerExecutionCapsule({
            detail: freshDetail,
            attemptId: input.attemptId,
            workspace,
          }),
        catch: (cause) =>
          isGoalAttemptExecutionError(cause)
            ? cause
            : failure("build-execution-capsule", input.attemptId)(cause),
      });
      if (!(yield* launchStillFenced())) return;
      yield* threads
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make(`goal-attempt-thread:${input.attemptId}`),
          threadId: executionThreadId,
          projectId,
          title: `${execution.node.role}: ${execution.node.objective}`.slice(0, 512),
          modelSelection,
          runtimeMode:
            execution.node.workspaceMode === "read_only"
              ? "approval-required"
              : "auto-accept-edits",
          interactionMode: freshDetail.goal.rootInteractionMode ?? "default",
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
          text: buildGoalWorkerPrompt({
            objective: freshDetail.goal.objective,
            execution,
          }),
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
