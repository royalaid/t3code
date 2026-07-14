import { assert, it } from "@effect/vitest";
import { CommandId } from "@t3tools/contracts";

import { EventSinkWriteError } from "../orchestration-v2/EventSink.ts";
import { GoalProjectionValidationError } from "../orchestration-v2/GoalProjectionStore.ts";
import { OrchestratorDispatchError } from "../orchestration-v2/Orchestrator.ts";
import { goalMutationFailure } from "./OrchestratorMcpService.ts";

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
