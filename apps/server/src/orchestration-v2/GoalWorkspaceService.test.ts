import { describe, expect, it } from "@effect/vitest";

import { goalBranchName, parseGitWorktreeList } from "./GoalWorkspaceService.ts";

describe("GoalWorkspaceService helpers", () => {
  it("derives stable collision-resistant branch names", () => {
    expect(goalBranchName("integration", "Goal: My Important Work")).toBe(
      goalBranchName("integration", "Goal: My Important Work"),
    );
    expect(goalBranchName("integration", "Goal: My Important Work")).not.toBe(
      goalBranchName("integration", "Goal: My Important Work 2"),
    );
    expect(goalBranchName("worker", "attempt:1")).toMatch(/^goal-worker\/attempt-1-[a-f0-9]{12}$/u);
  });

  it("parses attached and detached worktrees from porcelain output", () => {
    expect(
      parseGitWorktreeList(
        [
          "worktree C:/repo",
          "HEAD aaaaa",
          "branch refs/heads/main",
          "",
          "worktree C:/worktrees/goal",
          "HEAD bbbbb",
          "detached",
          "",
        ].join("\n"),
      ),
    ).toEqual([
      { path: "C:/repo", branch: "main" },
      { path: "C:/worktrees/goal", branch: null },
    ]);
  });
});
