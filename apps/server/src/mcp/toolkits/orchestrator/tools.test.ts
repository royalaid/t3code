import { describe, expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import { Tool } from "effect/unstable/ai";

import {
  GoalCapabilitiesTool,
  GoalEvidenceReadTool,
  GoalEvidenceSubmitTool,
  GoalNodeCancelTool,
  GoalNodeReadTool,
  GoalReadTool,
  GoalReplaceGraphTool,
  GoalResultPublishTool,
} from "./tools.ts";

const readTools = [
  {
    tool: GoalReadTool,
    title: "Read durable goal state",
    descriptionFragments: ["Root-lead", "currentRevision", "expectedRevision"],
  },
  {
    tool: GoalCapabilitiesTool,
    title: "Read goal capabilities",
    descriptionFragments: ["authenticated role", "node scope", "route candidates"],
  },
  {
    tool: GoalNodeReadTool,
    title: "Read a goal node",
    descriptionFragments: ["credential-owned node", "goalId", "nodeId"],
  },
  {
    tool: GoalEvidenceReadTool,
    title: "Read goal evidence",
    descriptionFragments: ["visible", "evidenceId", "own attempt"],
  },
] as const;

const mutationTools = [
  {
    tool: GoalReplaceGraphTool,
    title: "Replace the goal graph",
    descriptionFragments: ["Root-lead only", "complete next goal graph", "stale"],
  },
  {
    tool: GoalNodeCancelTool,
    title: "Cancel a goal node",
    descriptionFragments: ["Root-lead only", "immutable graphVersionId", "Never infer"],
  },
  {
    tool: GoalResultPublishTool,
    title: "Publish a goal result",
    descriptionFragments: ["Worker-only", "exact attempt", "goal, node, and attempt IDs"],
  },
  {
    tool: GoalEvidenceSubmitTool,
    title: "Submit goal evidence",
    descriptionFragments: ["Worker-only", "integration SHA", "producer attempt"],
  },
] as const;

describe("orchestrator goal tool metadata", () => {
  for (const { tool, title, descriptionFragments } of readTools) {
    it(`${tool.name} exposes read guidance and safety metadata`, () => {
      const description = Tool.getDescription(tool);
      expect(Context.getOrUndefined(tool.annotations, Tool.Title)).toBe(title);
      expect(description?.length).toBeGreaterThan(80);
      for (const fragment of descriptionFragments) expect(description).toContain(fragment);
      expect(Context.get(tool.annotations, Tool.Readonly)).toBe(true);
    });
  }

  for (const { tool, title, descriptionFragments } of mutationTools) {
    it(`${tool.name} exposes mutation guidance and safety metadata`, () => {
      const description = Tool.getDescription(tool);
      expect(Context.getOrUndefined(tool.annotations, Tool.Title)).toBe(title);
      expect(description?.length).toBeGreaterThan(80);
      for (const fragment of descriptionFragments) expect(description).toContain(fragment);
      expect(Context.get(tool.annotations, Tool.Destructive)).toBe(true);
    });
  }
});
