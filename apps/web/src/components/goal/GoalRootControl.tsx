import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, GoalDetail, ThreadId } from "@t3tools/contracts";
import { GitFork, OctagonX } from "lucide-react";

import { useRightPanelStore } from "../../rightPanelStore";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";

export function GoalRootControl(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly detail: GoalDetail;
}) {
  const cancelGoal = useAtomCommand(threadEnvironment.cancelGoal, "cancel goal");
  const terminal = ["completed", "failed", "cancelled"].includes(props.detail.goal.status);
  const openWorkflow = () =>
    useRightPanelStore.getState().open(scopeThreadRef(props.environmentId, props.threadId), "goal");
  const cancel = async () => {
    if (!window.confirm("Cancel this goal and request cancellation of its running workers?"))
      return;
    await cancelGoal({
      environmentId: props.environmentId,
      input: {
        threadId: props.threadId,
        goalId: props.detail.goal.id,
        reason: "Cancelled from the goal root controls.",
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
      {!terminal ? (
        <Button size="xs" variant="ghost" aria-label="Cancel Goal" onClick={() => void cancel()}>
          <OctagonX className="size-3.5" />
          Cancel Goal
        </Button>
      ) : null}
    </div>
  );
}
