import { CommandId, ThreadId, type OrchestrationV2Command } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  clientGoalCommandRejection,
  type GoalClientCommandBinding,
} from "./GoalClientCommandPolicy.ts";

const rootThreadId = ThreadId.make("thread:goal-root");
const workerThreadId = ThreadId.make("thread:goal-worker");

function command(type: OrchestrationV2Command["type"], threadId = rootThreadId) {
  return {
    type,
    commandId: CommandId.make(`command:${type}`),
    threadId,
  } as OrchestrationV2Command;
}

function binding(
  threadId: ThreadId,
  kind: GoalClientCommandBinding["kind"],
): GoalClientCommandBinding {
  return { threadId, kind };
}

describe("goal client command policy", () => {
  it("rejects server- and MCP-owned goal mutations at the client boundary", () => {
    expect(clientGoalCommandRejection(command("goal.graph.replace"), [])).toContain(
      "not available to clients",
    );
    expect(clientGoalCommandRejection(command("goal.node.cancel"), [])).toContain(
      "not available to clients",
    );
    expect(clientGoalCommandRejection(command("goal.result.publish"), [])).toContain(
      "not available to clients",
    );
    expect(clientGoalCommandRejection(command("goal.evidence.publish"), [])).toContain(
      "not available to clients",
    );
  });

  it("keeps goal children inspect-only except for human approval and input responses", () => {
    expect(
      clientGoalCommandRejection(command("message.dispatch", workerThreadId), [
        binding(workerThreadId, "worker"),
      ]),
    ).toContain("inspect-only");
    expect(
      clientGoalCommandRejection(command("thread.model-selection.set", workerThreadId), [
        binding(workerThreadId, "worker"),
      ]),
    ).toContain("inspect-only");
    expect(
      clientGoalCommandRejection(command("runtime-request.respond", workerThreadId), [
        binding(workerThreadId, "worker"),
      ]),
    ).toBeNull();
  });

  it("limits a goal root to human workflow controls while leaving ordinary threads unchanged", () => {
    expect(
      clientGoalCommandRejection(command("message.dispatch"), [binding(rootThreadId, "lead")]),
    ).toBeNull();
    expect(
      clientGoalCommandRejection(command("goal.cancel"), [binding(rootThreadId, "lead")]),
    ).toBeNull();
    expect(
      clientGoalCommandRejection(command("thread.model-selection.set"), [
        binding(rootThreadId, "lead"),
      ]),
    ).toContain("does not allow");
    expect(clientGoalCommandRejection(command("thread.model-selection.set"), [])).toBeNull();
  });
});
