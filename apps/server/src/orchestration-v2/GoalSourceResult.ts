import {
  CommandId,
  type GoalDetail,
  type GoalFailureReason,
  type GoalSourceTerminalResult,
  type GoalSourceTerminalStatus,
} from "@t3tools/contracts";

const SUMMARY_LIMIT = 4_000;
const OBJECTIVE_LIMIT = 20_000;
const DIGEST_LIMIT = 512;
const FAILURE_LIMIT = 5;
const ARTIFACT_LIMIT = 10;

const TERMINAL_STATUSES = new Set<GoalSourceTerminalStatus>(["completed", "failed", "cancelled"]);

export function isGoalSourceTerminalStatus(
  status: GoalDetail["goal"]["status"],
): status is GoalSourceTerminalStatus {
  return TERMINAL_STATUSES.has(status as GoalSourceTerminalStatus);
}

export function goalSourceResultCommandId(detail: GoalDetail): CommandId {
  return CommandId.make(
    `goal-source-result:${detail.goal.id}:${detail.goal.status}:${detail.goal.updatedAt}`,
  );
}

function truncate(value: string, limit: number): string {
  return value.length <= limit ? value : `${value.slice(0, Math.max(0, limit - 3))}...`;
}

function truncateNullable(value: string | null, limit: number): string | null {
  return value === null ? null : truncate(value, limit);
}

function stableReasonText(reason: GoalFailureReason): string {
  switch (reason.type) {
    case "stale_revision":
      return `Stale revision: expected ${reason.expected}, current ${reason.actual}.`;
    case "policy_rejection":
      return reason.detail;
    case "ambiguous_routing":
      return `Ambiguous routing: ${reason.unmetConstraints.join(", ") || "no constraints"}.`;
    case "dependency_failure":
      return `Dependency failed: ${reason.dependencyNodeId}.`;
    case "worker_failure":
      return reason.detail;
    case "native_descendant_overage":
      return `Native descendant limit ${reason.limit} exceeded with ${reason.observed}.`;
    case "integration_conflict":
      return reason.detail;
    case "stale_evidence":
      return `Stale evidence for ${reason.evidenceSha}; current integration SHA is ${reason.integrationSha}.`;
    case "unsupported_queue_steer":
      return reason.detail;
    case "stale_active_run_target":
      return reason.detail;
    case "root_lead_no_graph":
      return reason.detail;
    case "resource_backstop":
      return `Resource backstop ${reason.limit} observed ${reason.observed}.`;
  }
}

export function buildGoalSourceTerminalResult(detail: GoalDetail): GoalSourceTerminalResult | null {
  const terminalStatus = detail.goal.status;
  if (!isGoalSourceTerminalStatus(terminalStatus)) return null;

  const failureSummaries = detail.failures
    .toSorted((left, right) => {
      const timeOrder = left.occurredAt.localeCompare(right.occurredAt);
      return timeOrder === 0 ? left.id.localeCompare(right.id) : timeOrder;
    })
    .slice(-FAILURE_LIMIT)
    .map((failure) => ({
      id: failure.id,
      reasonType: failure.reason.type,
      detail: truncate(stableReasonText(failure.reason), SUMMARY_LIMIT),
      recoveryState: failure.recoveryState,
      blocker: truncateNullable(failure.blocker, SUMMARY_LIMIT),
      occurredAt: failure.occurredAt,
    }));

  const artifactSummaries = detail.artifacts
    .toSorted((left, right) => {
      const timeOrder = left.createdAt.localeCompare(right.createdAt);
      return timeOrder === 0 ? left.id.localeCompare(right.id) : timeOrder;
    })
    .slice(-ARTIFACT_LIMIT)
    .map((artifact) => ({
      id: artifact.id,
      kind: artifact.kind,
      digest: truncateNullable(artifact.digest, DIGEST_LIMIT),
      createdAt: artifact.createdAt,
    }));

  const acceptedEvidence = detail.evidence
    .filter((evidence) => evidence.verdict === "accepted")
    .toSorted((left, right) => {
      const timeOrder = left.createdAt.localeCompare(right.createdAt);
      return timeOrder === 0 ? left.id.localeCompare(right.id) : timeOrder;
    })
    .at(-1);

  const summary = (() => {
    if (terminalStatus === "completed") {
      const verified =
        detail.goal.verifiedSha !== null && detail.goal.verifiedSha === detail.goal.integrationSha;
      return verified
        ? `Goal completed and verified at ${detail.goal.verifiedSha}.`
        : `Goal completed without an accepted verification for the current integration SHA.`;
    }
    if (terminalStatus === "failed") {
      const latestFailure = failureSummaries.at(-1);
      return latestFailure === undefined
        ? "Goal failed without a structured failure record."
        : `Goal failed: ${latestFailure.detail}`;
    }
    return "Goal was cancelled before completion.";
  })();

  return {
    schemaVersion: 1,
    goalId: detail.goal.id,
    terminalStatus,
    terminalAt: detail.goal.updatedAt,
    objective: truncate(detail.goal.objective, OBJECTIVE_LIMIT),
    currentRevision: detail.goal.currentRevision,
    integrationSha: truncateNullable(detail.goal.integrationSha, DIGEST_LIMIT),
    verifiedSha: truncateNullable(
      acceptedEvidence === undefined ? detail.goal.verifiedSha : acceptedEvidence.integrationSha,
      DIGEST_LIMIT,
    ),
    summary: truncate(summary, SUMMARY_LIMIT),
    failureSummaries,
    artifactSummaries,
  };
}
