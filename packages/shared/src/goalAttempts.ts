import type { GoalAttempt, ThreadId } from "@t3tools/contracts";

const INTERACTIVE_WORKER_ATTEMPT_STATUSES = new Set<GoalAttempt["status"]>([
  "launching",
  "running",
  "stalled",
]);

/** Worker threads that can currently own a live runtime request. */
export function interactiveGoalWorkerThreadIds(
  attempts: ReadonlyArray<Pick<GoalAttempt, "executionThreadId" | "status">>,
): ReadonlyArray<ThreadId> {
  return [
    ...new Set(
      attempts.flatMap((attempt) =>
        attempt.executionThreadId !== null &&
        INTERACTIVE_WORKER_ATTEMPT_STATUSES.has(attempt.status)
          ? [attempt.executionThreadId]
          : [],
      ),
    ),
  ];
}
