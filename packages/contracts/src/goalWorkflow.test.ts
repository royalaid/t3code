import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  Goal,
  GoalGraphVersion,
  GoalSourceHandoff,
  GoalWorkflowCommand,
  GoalWorkflowEvent,
  GoalWorkflowPolicy,
} from "./goalWorkflow.ts";

const decodeGoal = Schema.decodeUnknownSync(Goal);
const decodeGoalGraphVersion = Schema.decodeUnknownSync(GoalGraphVersion);
const decodeGoalWorkflowCommand = Schema.decodeUnknownSync(GoalWorkflowCommand);
const decodeGoalWorkflowEvent = Schema.decodeUnknownSync(GoalWorkflowEvent);
const decodeGoalWorkflowPolicy = Schema.decodeUnknownSync(GoalWorkflowPolicy);
const decodeGoalSourceHandoff = Schema.decodeUnknownSync(GoalSourceHandoff);

const policy = {
  sandboxMode: "workspace-write",
  approvalPolicy: "on-request",
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["shell", "apply_patch"],
} as const;

const node = (id: string, workspaceMode: "read_only" | "writer" = "read_only") => ({
  id,
  role: `role-${id}`,
  persona: `persona-${id}`,
  objective: `objective-${id}`,
  successCriteria: ["done"],
  contextPacket: {
    schemaVersion: 1,
    digest: null,
    objective: `objective-${id}`,
    artifacts: [],
    dependencyOutputs: [],
    notes: [],
  },
  outputContract: { kind: "structured_result", description: "report", requiredFields: ["summary"] },
  requiredCapabilities: ["tools"],
  workspaceMode,
  routingRequest: {
    type: "requirements",
    capabilities: ["tools"],
    latencyClass: "standard",
    costClass: "standard",
  },
  evidenceRequirements: [{ kind: "test", description: "focused test", required: true }],
  policy,
});

describe("goal workflow contracts", () => {
  it("persists waiting goals before an integration workspace exists", () => {
    const waiting = decodeGoal({
      id: "goal:waiting",
      objective: "wait durably",
      status: "waiting_for_source",
      sourceThreadId: "thread:source",
      rootThreadId: "thread:root",
      policy,
      currentGraphVersionId: null,
      currentRevision: 0,
      integrationBranch: null,
      integrationWorktreePath: null,
      integrationSha: null,
      verifiedSha: null,
      createdAt: "2026-07-11T00:00:00.000Z",
      updatedAt: "2026-07-11T00:00:00.000Z",
    });
    expect(waiting.integrationWorktreePath).toBeNull();
    expect(waiting.createdAt).toBe("2026-07-11T00:00:00.000Z");
    expect(() =>
      decodeGoal({
        ...waiting,
        updatedAt: "2026-02-30T00:00:00.000Z",
      }),
    ).toThrow();
  });
  it("decodes immutable graph versions and root graph replacement commands", () => {
    const graph = decodeGoalGraphVersion({
      id: "goal-graph:1",
      goalId: "goal:1",
      revision: 1,
      publishedByNodeId: "goal-node:lead",
      nodes: [node("goal-node:lead"), node("goal-node:writer", "writer")],
      edges: [{ id: "goal-edge:1", fromNodeId: "goal-node:lead", toNodeId: "goal-node:writer" }],
      createdAt: "2026-07-11T00:00:00.000Z",
    });
    expect(graph.nodes).toHaveLength(2);
    expect(
      decodeGoalWorkflowCommand({
        type: "goal.graph.replace",
        commandId: "command:1",
        threadId: "thread:goal",
        goalId: "goal:1",
        expectedRevision: 0,
        graph,
      }).type,
    ).toBe("goal.graph.replace");
  });

  it("decodes goal lifecycle events and policy constraints", () => {
    expect(decodeGoalWorkflowPolicy(policy).writableRoots).toEqual(["/repo"]);
    expect(
      decodeGoalWorkflowEvent({
        type: "goal.graph-version-activated",
        payload: {
          goalId: "goal:1",
          expectedRevision: 0,
          graph: {
            id: "goal-graph:1",
            goalId: "goal:1",
            revision: 1,
            publishedByNodeId: "goal-node:lead",
            nodes: [node("goal-node:lead")],
            edges: [],
            createdAt: "2026-07-11T00:00:00.000Z",
          },
          activatedAt: "2026-07-11T00:00:00.000Z",
        },
      }).type,
    ).toBe("goal.graph-version-activated");
  });

  it("bounds the portable source handoff at the transport boundary", () => {
    const valid = {
      objective: "ship it",
      attachments: [],
      selectedContextText: ["context"],
      sourceSummary: "summary",
      projectInstructions: ["instructions"],
      branchState: "main @ abc123",
      relevantCheckpoints: ["checkpoint"],
    } as const;
    expect(decodeGoalSourceHandoff(valid).objective).toBe("ship it");
    expect(() =>
      decodeGoalSourceHandoff({ ...valid, selectedContextText: Array(33).fill("context") }),
    ).toThrow();
    expect(() =>
      decodeGoalSourceHandoff({ ...valid, selectedContextText: ["x".repeat(20_001)] }),
    ).toThrow();
    expect(() =>
      decodeGoalSourceHandoff({ ...valid, projectInstructions: Array(17).fill("instruction") }),
    ).toThrow();
    expect(() =>
      decodeGoalSourceHandoff({ ...valid, sourceSummary: "x".repeat(40_001) }),
    ).toThrow();
    expect(() =>
      decodeGoalSourceHandoff({ ...valid, relevantCheckpoints: Array(33).fill("checkpoint") }),
    ).toThrow();
  });
});
