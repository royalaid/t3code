import { describe, expect, it } from "vite-plus/test";

import {
  buildGoalSourceHandoff,
  makeGoalLaunchCoordinator,
  shouldFinalizeGoalAfterEvent,
  sourceRunIsSettled,
} from "./GoalLaunchService.ts";

describe("goal source handoff", () => {
  it("derives summary, branch, checkpoints, and instructions from settled server state", () => {
    expect(
      buildGoalSourceHandoff({
        objective: "ship it",
        attachments: [],
        selectedContextText: ["terminal context"],
        messages: [
          { role: "user", text: "first" },
          { role: "assistant", text: "settled answer" },
        ],
        projectInstructions: ["root instructions"],
        branch: "feature/goal",
        worktreePath: "C:/repo",
        checkpoints: [
          { ref: "refs/checkpoints/1", status: "ready" },
          { ref: "refs/checkpoints/stale", status: "stale" },
        ],
      }),
    ).toEqual({
      objective: "ship it",
      attachments: [],
      selectedContextText: ["terminal context"],
      sourceSummary:
        "Source conversation: 2 non-empty messages (1 user, 1 assistant, 0 system).\nLatest user request: first\nLatest assistant outcome: settled answer",
      projectInstructions: ["root instructions"],
      branchState: "branch=feature/goal; worktree=C:/repo",
      relevantCheckpoints: ["refs/checkpoints/1"],
    });
  });

  it("preserves the settled conclusion without copying earlier transcript messages", () => {
    const handoff = buildGoalSourceHandoff({
      objective: "ship it",
      attachments: [],
      selectedContextText: [],
      messages: [
        { role: "user", text: "OLD_TRANSCRIPT_MARKER should not be copied" },
        { role: "assistant", text: "an old response" },
        { role: "user", text: "please finish the current implementation" },
        {
          role: "assistant",
          text: `${"implementation detail ".repeat(100)}FINAL_CONCLUSION_MARKER`,
        },
      ],
      projectInstructions: [],
      branch: null,
      worktreePath: null,
      checkpoints: [],
    });

    expect(handoff.sourceSummary).toContain(
      "Source conversation: 4 non-empty messages (2 user, 2 assistant, 0 system).",
    );
    expect(handoff.sourceSummary).toContain("please finish the current implementation");
    expect(handoff.sourceSummary).toContain("FINAL_CONCLUSION_MARKER");
    expect(handoff.sourceSummary).not.toContain("OLD_TRANSCRIPT_MARKER");
    expect(handoff.sourceSummary?.length).toBeLessThanOrEqual(4_000);
  });
});

describe("GoalLaunchService settlement gate", () => {
  it("recognizes a captured run that settled before startup rescan", () => {
    expect(sourceRunIsSettled("run:source", [{ id: "run:source", status: "completed" }])).toBe(
      true,
    );
    expect(sourceRunIsSettled("run:source", [{ id: "run:source", status: "running" }])).toBe(false);
  });
  it("finalizes an idle source immediately", () => {
    expect(shouldFinalizeGoalAfterEvent(null, null)).toBe(true);
  });

  it("waits for the captured source run and ignores unrelated terminal events", () => {
    expect(shouldFinalizeGoalAfterEvent("run:source", null)).toBe(false);
    expect(
      shouldFinalizeGoalAfterEvent("run:source", {
        type: "run.updated",
        runId: "run:other",
        status: "completed",
      }),
    ).toBe(false);
    expect(
      shouldFinalizeGoalAfterEvent("run:source", {
        type: "run.updated",
        runId: "run:source",
        status: "completed",
      }),
    ).toBe(true);
  });
});

describe("GoalLaunchService durable rescan coordinator", () => {
  it("continues later pending goals when one goal is poison", async () => {
    const launched: string[] = [];
    const failures: string[] = [];
    const coordinator = makeGoalLaunchCoordinator({
      listPending: async () => [
        { goalId: "goal:poison", sourceActiveRunId: null },
        { goalId: "goal:healthy", sourceActiveRunId: null },
      ],
      claim: async () => true,
      finalize: async (goal) => {
        if (goal.goalId === "goal:poison") throw new Error("poison");
        launched.push(goal.goalId);
      },
      onError: async (goal) => {
        failures.push(goal.goalId);
      },
    });
    await coordinator.rescan(null);
    expect(failures).toEqual(["goal:poison"]);
    expect(launched).toEqual(["goal:healthy"]);
  });

  it("claims a pending launch exactly once and waits for its captured run", async () => {
    const launched: string[] = [];
    const pending = [{ goalId: "goal:1", sourceActiveRunId: "run:source" }];
    const claimed = new Set<string>();
    const makeCoordinator = () =>
      makeGoalLaunchCoordinator({
        listPending: async () => pending,
        claim: async (goal) => {
          if (claimed.has(goal.goalId)) return false;
          claimed.add(goal.goalId);
          return true;
        },
        finalize: async (goal) => {
          launched.push(goal.goalId);
        },
      });
    const coordinator = makeCoordinator();
    await coordinator.rescan(null);
    await coordinator.rescan({ type: "run.updated", runId: "run:other", status: "completed" });
    expect(launched).toEqual([]);
    const terminal = { type: "run.updated" as const, runId: "run:source", status: "completed" };
    await Promise.all([coordinator.rescan(terminal), makeCoordinator().rescan(terminal)]);
    expect(launched).toEqual(["goal:1"]);
  });

  it("does not finalize when cancellation wins the durable claim race", async () => {
    const launched: string[] = [];
    let status: "waiting_for_source" | "cancelled" = "waiting_for_source";
    const coordinator = makeGoalLaunchCoordinator({
      listPending: async () => [{ goalId: "goal:race", sourceActiveRunId: null }],
      claim: async () => {
        status = "cancelled";
        return false;
      },
      recheckClaim: async () => status === "waiting_for_source",
      finalize: async (goal) => {
        launched.push(goal.goalId);
      },
    });
    await coordinator.rescan(null);
    expect(launched).toEqual([]);
  });
});
