import type { GoalAttempt, GoalAttemptId } from "@t3tools/contracts";

export const GOAL_STALL_TIMEOUT_MS = 10 * 60 * 1_000;
export const GOAL_WORKER_CEILING_MS = 60 * 60 * 1_000;
export const GOAL_AGENT_BACKSTOP = 1_000;

export type GoalRecoveryAction =
  | {
      readonly type: "mark_stalled";
      readonly attemptId: GoalAttemptId;
      readonly silentForMs: number;
    }
  | {
      readonly type: "terminate_ceiling";
      readonly attemptId: GoalAttemptId;
      readonly elapsedMs: number;
    }
  | {
      readonly type: "reconcile_required";
      readonly attemptId: GoalAttemptId;
      readonly resolvedProviderInstanceId: string | null;
      readonly resolvedModel: string | null;
    }
  | { readonly type: "retry"; readonly attemptId: GoalAttemptId }
  | { readonly type: "retry_exhausted"; readonly attemptId: GoalAttemptId }
  | {
      readonly type: "native_descendant_overage";
      readonly attemptId: GoalAttemptId;
      readonly observed: number;
      readonly limit: number;
    };

export interface GoalRecoveryPlan {
  readonly actions: ReadonlyArray<GoalRecoveryAction>;
  readonly pauseNewLaunches: boolean;
}

export const shouldRefreshExpiredGoalLease = (status: string | null): boolean =>
  status === "pending" || status === "running";

const timestamp = (value: string): number => Date.parse(value);
const retryKey = (attempt: GoalAttempt): string => `${attempt.goalId}:${attempt.nodeId}`;

export function planGoalRecovery(input: {
  readonly attempts: ReadonlyArray<GoalAttempt>;
  readonly now: string;
}): GoalRecoveryPlan {
  const now = timestamp(input.now);
  const actions: GoalRecoveryAction[] = [];
  const latestByNode = new Map<string, GoalAttempt>();
  for (const attempt of input.attempts) {
    const key = retryKey(attempt);
    const latest = latestByNode.get(key);
    if (latest === undefined || attempt.ordinal > latest.ordinal) latestByNode.set(key, attempt);
  }
  for (const attempt of input.attempts) {
    const elapsedMs = now - timestamp(attempt.createdAt);
    const silentForMs = now - timestamp(attempt.updatedAt);
    if (new Set(["running", "stalled"]).has(attempt.status)) {
      if (elapsedMs >= GOAL_WORKER_CEILING_MS) {
        actions.push({ type: "terminate_ceiling", attemptId: attempt.id, elapsedMs });
      } else if (attempt.status === "running" && silentForMs >= GOAL_STALL_TIMEOUT_MS) {
        actions.push({ type: "mark_stalled", attemptId: attempt.id, silentForMs });
      }
    }
    if (
      new Set(["leased", "launching"]).has(attempt.status) &&
      attempt.leaseExpiresAt !== null &&
      timestamp(attempt.leaseExpiresAt) <= now
    ) {
      actions.push({
        type: "reconcile_required",
        attemptId: attempt.id,
        resolvedProviderInstanceId: attempt.resolvedRoute?.providerInstanceId ?? null,
        resolvedModel: attempt.resolvedRoute?.model ?? null,
      });
    }
    if (
      attempt.status === "failed" &&
      attempt.failureReason?.startsWith("retryable:") === true &&
      latestByNode.get(retryKey(attempt))?.id === attempt.id
    ) {
      actions.push({
        type: attempt.ordinal < 2 ? "retry" : "retry_exhausted",
        attemptId: attempt.id,
      });
    }
  }
  const observedAgents =
    input.attempts.length +
    input.attempts.reduce((total, attempt) => total + attempt.usage.nativeDescendantCount, 0);
  if (observedAgents > GOAL_AGENT_BACKSTOP) {
    const owner = input.attempts
      .filter((attempt) => attempt.usage.nativeDescendantCount > 0)
      .toSorted(
        (left, right) => right.usage.nativeDescendantCount - left.usage.nativeDescendantCount,
      )[0];
    if (owner !== undefined) {
      actions.push({
        type: "native_descendant_overage",
        attemptId: owner.id,
        observed: observedAgents,
        limit: GOAL_AGENT_BACKSTOP,
      });
    }
  }
  return { actions, pauseNewLaunches: observedAgents > GOAL_AGENT_BACKSTOP };
}
