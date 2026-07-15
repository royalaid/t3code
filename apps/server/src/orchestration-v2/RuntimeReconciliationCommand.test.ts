import { CommandId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  isRuntimeReconciliationCommand,
  isStartupRuntimeReconciliationCommand,
  runtimeReconciliationCommandId,
} from "./RuntimeReconciliationCommand.ts";

describe("runtime reconciliation command ids", () => {
  const startup = runtimeReconciliationCommandId({
    trigger: "startup",
    threadId: ThreadId.make("thread:test"),
    occurredAt: "2026-07-15T00:00:00.000Z",
  });

  it("classifies startup reconciliation without matching ordinary commands", () => {
    expect(isRuntimeReconciliationCommand(startup)).toBe(true);
    expect(isStartupRuntimeReconciliationCommand(startup)).toBe(true);
    expect(isRuntimeReconciliationCommand(CommandId.make("command:ordinary"))).toBe(false);
  });
});
