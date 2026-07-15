import { CommandId, type ThreadId } from "@t3tools/contracts";

const RUNTIME_RECONCILIATION_PREFIX = "command:runtime-reconcile:";

export function runtimeReconciliationCommandId(input: {
  readonly trigger: "startup" | "shutdown";
  readonly threadId: ThreadId;
  readonly occurredAt: string;
}): CommandId {
  return CommandId.make(
    `${RUNTIME_RECONCILIATION_PREFIX}${input.trigger}:${input.threadId}:${input.occurredAt}`,
  );
}

export function isRuntimeReconciliationCommand(commandId: CommandId | null): boolean {
  return commandId !== null && String(commandId).startsWith(RUNTIME_RECONCILIATION_PREFIX);
}

export function isStartupRuntimeReconciliationCommand(commandId: CommandId | null): boolean {
  return (
    commandId !== null && String(commandId).startsWith(`${RUNTIME_RECONCILIATION_PREFIX}startup:`)
  );
}
