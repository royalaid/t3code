import { assert, it } from "@effect/vitest";
import { CommandId, GoalId } from "@t3tools/contracts";

import { EventSinkWriteError } from "../orchestration-v2/EventSink.ts";
import { GoalProjectionValidationError } from "../orchestration-v2/GoalProjectionStore.ts";
import { OrchestratorDispatchError } from "../orchestration-v2/Orchestrator.ts";
import {
  goalGraphIdentityIssue,
  goalGraphMutationCommandId,
  goalMutationFailure,
} from "./OrchestratorMcpService.ts";

function nestedGoalError(reason: GoalProjectionValidationError["reason"], detail: string) {
  return new OrchestratorDispatchError({
    commandId: CommandId.make(`command:mcp-goal-error:${reason}`),
    commandType: "goal.graph.replace",
    cause: new EventSinkWriteError({
      eventCount: 1,
      cause: new GoalProjectionValidationError({ reason, detail }),
    }),
  });
}

it("preserves caller-correctable goal mutation error codes through wrapped failures", () => {
  assert.deepInclude(goalMutationFailure(nestedGoalError("stale_revision", "read revision 4")), {
    code: "stale_revision",
    message: "read revision 4",
  });
  assert.deepInclude(
    goalMutationFailure(nestedGoalError("non_terminal_graph", "add a terminal verifier")),
    {
      code: "invalid_request",
      message: "add a terminal verifier",
    },
  );
});

it("requires goal-scoped graph ids and goal-scopes graph mutation receipts", () => {
  const firstGoalId = GoalId.make("goal:first");
  const secondGoalId = GoalId.make("goal:second");

  assert.equal(
    goalGraphIdentityIssue(firstGoalId, "graph-rev-1"),
    "Graph id graph-rev-1 must include its goal id goal:first.",
  );
  assert.equal(goalGraphIdentityIssue(firstGoalId, "graph:goal:first:revision:1"), null);
  assert.notEqual(
    goalGraphMutationCommandId("provider-session:shared", firstGoalId, "graph-rev-1"),
    goalGraphMutationCommandId("provider-session:shared", secondGoalId, "graph-rev-1"),
  );
});
