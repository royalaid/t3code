import type { OrchestrationV2Command, ThreadId } from "@t3tools/contracts";

export type GoalClientThreadBindingKind = "lead" | "worker" | null;

export interface GoalClientCommandBinding {
  readonly threadId: ThreadId;
  readonly kind: GoalClientThreadBindingKind;
}

const SERVER_OWNED_GOAL_COMMAND_TYPES = new Set<OrchestrationV2Command["type"]>([
  "goal.create",
  "goal.pending-launch.cancel",
  "goal.pending-launch.fail",
  "goal.pending-launch.claim",
  "goal.pending-launch.complete",
  "goal.graph.replace",
  "goal.node.cancel",
  "goal.result.publish",
  "goal.evidence.publish",
]);

const ROOT_HUMAN_COMMAND_TYPES = new Set<OrchestrationV2Command["type"]>([
  "message.dispatch",
  "run.interrupt",
  "queued-message.promote-to-steer",
  "queued-run.reorder",
  "runtime-request.respond",
  "goal.cancel",
  "goal.reopen",
]);

export function commandThreadIds(command: OrchestrationV2Command): ReadonlyArray<ThreadId> {
  switch (command.type) {
    case "thread.fork":
    case "thread.merge_back":
      return [command.sourceThreadId, command.targetThreadId];
    case "delegated_task.request":
      return [command.parentThreadId];
    case "thread.created.record":
      return [command.parentThreadId, command.targetThreadId];
    default:
      return [command.threadId];
  }
}

/**
 * The websocket transport represents a human client. Goal worker mutation is
 * intentionally only available through scoped MCP or server-owned effects;
 * the one exception is responding to a worker-originated runtime request.
 * This closes the generic command endpoint as an authority bypass.
 */
export function clientGoalCommandRejection(
  command: OrchestrationV2Command,
  bindings: ReadonlyArray<GoalClientCommandBinding>,
): string | null {
  if (SERVER_OWNED_GOAL_COMMAND_TYPES.has(command.type)) {
    return `Goal command ${command.type} is not available to clients.`;
  }

  const bindingKinds = bindings.map((binding) => binding.kind);
  if (command.type === "goal.launch") {
    return bindingKinds.some((kind) => kind !== null)
      ? "A goal must be launched from an ordinary source thread."
      : null;
  }

  if (command.type === "goal.cancel" || command.type === "goal.reopen") {
    return bindingKinds.includes("lead") && !bindingKinds.includes("worker")
      ? null
      : `Goal command ${command.type} must target its root thread.`;
  }

  if (bindingKinds.includes("worker")) {
    // Child threads remain non-conversational, but a worker can surface a
    // runtime approval or user-input request to the human. Responding to that
    // request is not a steer/queue/send mutation and must stay available on
    // compact mobile controls as well as web.
    if (command.type === "runtime-request.respond") {
      return null;
    }
    return "Goal child threads are inspect-only; send, Queue, Steer, Stop, and thread mutation are root-only operations.";
  }

  if (bindingKinds.includes("lead") && !ROOT_HUMAN_COMMAND_TYPES.has(command.type)) {
    return `Goal root thread does not allow client command ${command.type}. Use Queue, Steer, Stop, approvals, Cancel Goal, or Reopen Goal.`;
  }

  return null;
}
