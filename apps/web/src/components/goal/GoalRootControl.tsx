import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, GoalDetail, ThreadId } from "@t3tools/contracts";
import { GitFork, OctagonX, RotateCcw } from "lucide-react";

import { useRightPanelStore } from "../../rightPanelStore";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";

export function GoalRootControl(props: {
  readonly environmentId: EnvironmentId;
  readonly panelThreadId: ThreadId;
  readonly goalThreadId: ThreadId;
  readonly detail: GoalDetail;
}) {
  const cancelGoal = useAtomCommand(threadEnvironment.cancelGoal, "cancel goal");
  const reopenGoal = useAtomCommand(threadEnvironment.reopenGoal, "reopen goal");
  const terminal = ["completed", "failed", "cancelled"].includes(props.detail.goal.status);
  const openWorkflow = () =>
    useRightPanelStore
      .getState()
      .open(scopeThreadRef(props.environmentId, props.panelThreadId), "goal");
  const cancel = async () => {
    if (!window.confirm("Cancel this goal and request cancellation of its running workers?"))
      return;
    await cancelGoal({
      environmentId: props.environmentId,
      input: {
        threadId: props.goalThreadId,
        goalId: props.detail.goal.id,
        reason: "Cancelled from the goal root controls.",
      },
    });
  };
  const reopen = async () => {
    if (
      !window.confirm(
        "Reopen this completed goal and require a new workflow revision and verification?",
      )
    )
      return;
    await reopenGoal({
      environmentId: props.environmentId,
      input: {
        threadId: props.goalThreadId,
        goalId: props.detail.goal.id,
      },
    });
  };

  return (
    <div className="mb-2 flex items-center gap-2 rounded-xl border border-border bg-background/96 px-3 py-2 shadow-xs">
      <GitFork className="size-3.5 text-sky-600" />
      <button type="button" onClick={openWorkflow} className="min-w-0 flex-1 text-left">
        <span className="block truncate text-xs font-medium">Goal workflow</span>
        <span className="block text-[10px] text-muted-foreground">
          {props.detail.goal.status.replaceAll("_", " ")} · revision{" "}
          {props.detail.goal.currentRevision}
        </span>
      </button>
      {props.detail.goal.status === "completed" ? (
        <Button size="xs" variant="ghost" aria-label="Reopen Goal" onClick={() => void reopen()}>
          <RotateCcw className="size-3.5" />
          Reopen Goal
        </Button>
      ) : !terminal ? (
        <Button size="xs" variant="ghost" aria-label="Cancel Goal" onClick={() => void cancel()}>
          <OctagonX className="size-3.5" />
          Cancel Goal
        </Button>
      ) : null}
    </div>
  );
}
