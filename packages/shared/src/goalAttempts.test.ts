import { GoalAttemptId, ThreadId, type GoalAttempt } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { interactiveGoalWorkerThreadIds } from "./goalAttempts.js";

function attempt(
  id: string,
  status: GoalAttempt["status"],
  executionThreadId: ThreadId | null,
): Pick<GoalAttempt, "executionThreadId" | "status"> & { readonly id: GoalAttemptId } {
  return { id: GoalAttemptId.make(id), status, executionThreadId };
}

describe("interactiveGoalWorkerThreadIds", () => {
  it("selects request-capable workers once and excludes inactive attempts", () => {
    const worker = ThreadId.make("thread:worker");
    expect(
      interactiveGoalWorkerThreadIds([
        attempt("attempt:launching", "launching", worker),
        attempt("attempt:running", "running", worker),
        attempt("attempt:stalled", "stalled", ThreadId.make("thread:stalled")),
        attempt("attempt:leased", "leased", ThreadId.make("thread:leased")),
        attempt("attempt:failed", "failed", ThreadId.make("thread:failed")),
        attempt("attempt:unbound", "running", null),
      ]),
    ).toEqual([worker, ThreadId.make("thread:stalled")]);
  });
});
