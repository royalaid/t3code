import type { GoalDetail } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { GoalWorkflowPanel } from "./GoalWorkflowPanel";

describe("GoalWorkflowPanel", () => {
  it("explains a recoverable revision-zero root lead no-graph failure", () => {
    const detail = {
      goal: {
        id: "goal:no-graph",
        objective: "Publish a worker graph",
        status: "blocked",
        currentRevision: 0,
        currentGraphVersionId: null,
        integrationSha: "abcdef1234567890",
        verifiedSha: null,
      },
      nodes: [],
      attempts: [],
      writerCommits: [],
      evidence: [],
      failures: [
        {
          id: "failure:no-graph",
          reason: {
            type: "root_lead_no_graph",
            runId: "run:root",
            terminalStatus: "failed",
            detail: "Provider exited before the graph call completed.",
          },
        },
      ],
    } as unknown as GoalDetail;

    const markup = renderToStaticMarkup(<GoalWorkflowPanel detail={detail} />);
    expect(markup).toContain("lead ended before publishing a worker graph");
    expect(markup).toContain("Provider exited before the graph call completed.");
    expect(markup).toContain("corrective message");
    expect(markup).toContain("Queue or Steer");
    expect(markup).toContain("resumes automatically");
  });

  it("renders authoritative revision, role, route, blocker, and evidence state", () => {
    const detail = {
      goal: {
        id: "goal:ui",
        objective: "Ship the repository deliverable",
        status: "blocked",
        currentRevision: 3,
        currentGraphVersionId: "graph:ui",
        integrationSha: "abcdef1234567890",
        verifiedSha: null,
      },
      nodes: [
        {
          graphVersionId: "graph:ui",
          status: "blocked",
          blocker: "Waiting for policy approval",
          node: {
            id: "node:writer",
            role: "release engineer",
            objective: "Create one clean commit",
            workspaceMode: "writer",
          },
        },
      ],
      attempts: [
        {
          graphVersionId: "graph:ui",
          nodeId: "node:writer",
          resolvedRoute: { providerInstanceId: "claude", model: "sonnet" },
          usage: {
            inputTokens: 120,
            outputTokens: 30,
            nativeDescendantCount: 1,
          },
        },
      ],
      writerCommits: [],
      evidence: [],
    } as unknown as GoalDetail;

    const markup = renderToStaticMarkup(<GoalWorkflowPanel detail={detail} />);
    expect(markup).toContain("graph revision 3");
    expect(markup).toContain("release engineer");
    expect(markup).toContain("claude/sonnet");
    expect(markup).toContain("Waiting for policy approval");
    expect(markup).toContain("Verification");
  });
});
