import type { GoalDetail, GoalNodeId } from "@t3tools/contracts";

import type { McpGoalAuthority } from "./McpInvocationContext.ts";

const ACTIVE_WORKER_STATUSES = new Set(["leased", "launching", "running", "stalled"]);

type OwnedRecord = {
  readonly id?: string;
  readonly goalId: string;
  readonly nodeId: string;
  readonly attemptId: string;
};

export function selectGoalNodeForAuthority(
  detail: GoalDetail,
  authority: Exclude<McpGoalAuthority, { readonly kind: "ordinary" }>,
  nodeId: GoalNodeId,
) {
  const graphVersionId =
    authority.kind === "goal_worker"
      ? detail.attempts.find((attempt) => attempt.id === authority.attemptId)?.graphVersionId
      : detail.goal.currentGraphVersionId;
  return graphVersionId === null || graphVersionId === undefined
    ? undefined
    : detail.nodes.find(
        (candidate) => candidate.graphVersionId === graphVersionId && candidate.node.id === nodeId,
      );
}

export function validateGoalWorkerBinding(input: {
  readonly authority: {
    readonly goalId: string;
    readonly executionThreadId: string;
    readonly nodeId: string;
    readonly attemptId: string;
    readonly nativeOwnerAttemptId: string;
  };
  readonly scope: {
    readonly threadId: string;
    readonly providerSessionId: string;
    readonly providerInstanceId: string;
  };
  readonly attempt: {
    readonly id: string;
    readonly goalId: string;
    readonly nodeId: string;
    readonly status: string;
    readonly executionThreadId: string | null;
    readonly providerSessionId: string | null;
    readonly resolvedRoute: { readonly providerInstanceId: string } | null;
  };
  readonly artifacts?: ReadonlyArray<OwnedRecord>;
  readonly evidence?: OwnedRecord & {
    readonly producerAttemptId: string;
    readonly artifacts: ReadonlyArray<string>;
    readonly commands: ReadonlyArray<{ readonly logArtifactId: string | null }>;
  };
  readonly resolvedArtifacts?: ReadonlyArray<OwnedRecord & { readonly id: string }>;
}): string | null {
  const { authority, scope, attempt } = input;
  if (
    attempt.id !== authority.attemptId ||
    attempt.goalId !== authority.goalId ||
    attempt.nodeId !== authority.nodeId
  ) {
    return "Attempt does not belong to this goal worker authority.";
  }
  if (!ACTIVE_WORKER_STATUSES.has(attempt.status)) {
    return "Goal worker attempt is no longer active.";
  }
  if (
    scope.threadId !== authority.executionThreadId ||
    attempt.executionThreadId !== authority.executionThreadId
  ) {
    return "Goal worker execution thread does not match the active attempt.";
  }
  if (attempt.providerSessionId !== scope.providerSessionId) {
    return "Goal worker provider session does not match the active attempt.";
  }
  if (attempt.resolvedRoute?.providerInstanceId !== scope.providerInstanceId) {
    return "Goal worker provider instance does not match the resolved route.";
  }
  for (const artifact of input.artifacts ?? []) {
    if (
      artifact.goalId !== authority.goalId ||
      artifact.nodeId !== authority.nodeId ||
      artifact.attemptId !== authority.attemptId
    ) {
      return "Published artifact embeds authority outside this goal worker scope.";
    }
  }
  if (
    input.evidence !== undefined &&
    (input.evidence.goalId !== authority.goalId ||
      input.evidence.nodeId !== authority.nodeId ||
      input.evidence.attemptId !== authority.attemptId)
  ) {
    return "Submitted evidence embeds authority outside this goal worker scope.";
  }
  if (input.evidence !== undefined) {
    const referencedIds = new Set([
      ...input.evidence.artifacts,
      ...input.evidence.commands.flatMap((command) =>
        command.logArtifactId === null ? [] : [command.logArtifactId],
      ),
    ]);
    const resolvedById = new Map(
      (input.resolvedArtifacts ?? []).map((artifact) => [artifact.id, artifact]),
    );
    for (const artifactId of referencedIds) {
      const artifact = resolvedById.get(artifactId);
      if (
        artifact === undefined ||
        artifact.goalId !== authority.goalId ||
        artifact.nodeId !== authority.nodeId ||
        artifact.attemptId !== authority.attemptId
      ) {
        return `Referenced evidence artifact ${artifactId} is missing or outside this worker scope.`;
      }
    }
  }
  return null;
}
