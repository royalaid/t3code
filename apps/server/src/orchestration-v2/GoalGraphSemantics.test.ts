import {
  GoalEdgeId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ThreadId,
  type GoalGraphNode,
  type GoalGraphVersion,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  canonicalGoalLeadPublisherId,
  goalGraphCompletionPathIssue,
  isGoalVerifierNode,
  isStrictTransitiveAncestor,
} from "./GoalGraphSemantics.ts";

const writerPolicy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["shell"],
};
const readOnlyPolicy = {
  ...writerPolicy,
  sandboxMode: "read-only" as const,
  writableRoots: [],
};
const node = (id: string, overrides: Partial<GoalGraphNode> = {}): GoalGraphNode => ({
  id: GoalNodeId.make(id),
  role: id,
  persona: id,
  objective: id,
  successCriteria: ["done"],
  contextPacket: {
    schemaVersion: 1,
    digest: null,
    objective: id,
    artifacts: [],
    dependencyOutputs: [],
    notes: [],
  },
  outputContract: { kind: "commit", description: "commit", requiredFields: [] },
  requiredCapabilities: ["tools"],
  workspaceMode: "writer",
  routingRequest: {
    type: "requirements",
    capabilities: ["tools"],
    latencyClass: "standard",
    costClass: "standard",
  },
  evidenceRequirements: [],
  policy: writerPolicy,
  ...overrides,
});
const verifier = (id: string, overrides: Partial<GoalGraphNode> = {}): GoalGraphNode =>
  node(id, {
    workspaceMode: "read_only",
    outputContract: {
      kind: "verification",
      description: "verification",
      requiredFields: ["verdict"],
    },
    evidenceRequirements: [{ kind: "command", description: "test log", required: true }],
    policy: readOnlyPolicy,
    ...overrides,
  });
const graph = (
  nodes: ReadonlyArray<GoalGraphNode>,
  dependencies: ReadonlyArray<readonly [string, string]>,
): GoalGraphVersion => ({
  id: GoalGraphVersionId.make("graph:semantics"),
  goalId: GoalId.make("goal:semantics"),
  revision: 1,
  publishedByNodeId: GoalNodeId.make("thread:root"),
  nodes: [...nodes],
  edges: dependencies.map(([from, to], index) => ({
    id: GoalEdgeId.make(`edge:${index}`),
    fromNodeId: GoalNodeId.make(from),
    toNodeId: GoalNodeId.make(to),
  })),
  createdAt: "2026-07-13T00:00:00.000Z",
});

describe("goal graph completion semantics", () => {
  it("derives the external publisher from the immutable root thread", () => {
    expect(canonicalGoalLeadPublisherId(ThreadId.make("thread:root"))).toBe("thread:root");
  });

  it("classifies only read-only verification nodes with required evidence", () => {
    expect(isGoalVerifierNode(verifier("verifier"))).toBe(true);
    expect(
      isGoalVerifierNode(
        verifier("optional", {
          evidenceRequirements: [
            { kind: "command", description: "optional test", required: false },
          ],
        }),
      ),
    ).toBe(false);
    expect(
      isGoalVerifierNode(
        verifier("wrong-output", {
          outputContract: { kind: "structured_result", description: "report", requiredFields: [] },
        }),
      ),
    ).toBe(false);
    expect(
      isGoalVerifierNode(
        verifier("writer-verifier", {
          workspaceMode: "writer",
          policy: writerPolicy,
        }),
      ),
    ).toBe(false);
  });

  it("accepts minimal, parallel, and staged writer completion paths", () => {
    expect(
      goalGraphCompletionPathIssue(
        graph([node("writer"), verifier("verifier")], [["writer", "verifier"]]),
      ),
    ).toBeNull();
    expect(
      goalGraphCompletionPathIssue(
        graph(
          [node("writer-a"), node("writer-b"), verifier("verifier")],
          [
            ["writer-a", "verifier"],
            ["writer-b", "verifier"],
          ],
        ),
      ),
    ).toBeNull();
    const staged = graph(
      [node("writer-a"), node("writer-b"), verifier("verifier")],
      [
        ["writer-a", "writer-b"],
        ["writer-b", "verifier"],
      ],
    );
    expect(goalGraphCompletionPathIssue(staged)).toBeNull();
    expect(
      isStrictTransitiveAncestor(staged, GoalNodeId.make("writer-a"), GoalNodeId.make("verifier")),
    ).toBe(true);
  });

  it("rejects empty, lone-writer, verifier-without-producer, and pseudo-verifier graphs", () => {
    expect(goalGraphCompletionPathIssue(graph([], []))).toMatch(/at least one node/iu);
    expect(goalGraphCompletionPathIssue(graph([node("writer")], []))).toMatch(/verifier/iu);
    expect(goalGraphCompletionPathIssue(graph([verifier("verifier")], []))).toMatch(/producer/iu);
    expect(
      goalGraphCompletionPathIssue(
        graph(
          [
            node("researcher", {
              workspaceMode: "read_only",
              outputContract: {
                kind: "structured_result",
                description: "research",
                requiredFields: [],
              },
              policy: readOnlyPolicy,
            }),
            verifier("verifier"),
          ],
          [["researcher", "verifier"]],
        ),
      ),
    ).toMatch(/writer producer/iu);
    expect(
      goalGraphCompletionPathIssue(
        graph(
          [
            node("writer"),
            verifier("optional", {
              evidenceRequirements: [{ kind: "command", description: "optional", required: false }],
            }),
          ],
          [["writer", "optional"]],
        ),
      ),
    ).toMatch(/verifier/iu);
  });

  it("rejects a verifier that can run before every writer finishes", () => {
    expect(
      goalGraphCompletionPathIssue(
        graph(
          [node("covered"), node("uncovered"), verifier("verifier")],
          [["covered", "verifier"]],
        ),
      ),
    ).toMatch(/every writer/iu);
  });
});
