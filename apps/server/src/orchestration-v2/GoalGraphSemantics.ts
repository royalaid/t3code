import {
  GoalNodeId,
  type GoalGraphEdge,
  type GoalGraphNode,
  type GoalGraphVersion,
  type ThreadId,
} from "@t3tools/contracts";

type GoalGraphEdges = {
  readonly edges: ReadonlyArray<Pick<GoalGraphEdge, "fromNodeId" | "toNodeId">>;
};

export function canonicalGoalLeadPublisherId(rootThreadId: ThreadId): GoalNodeId {
  return GoalNodeId.make(rootThreadId);
}

export function goalGraphPublisherIssue(
  graph: GoalGraphVersion,
  rootThreadId: ThreadId,
): string | null {
  const canonicalPublisherId = canonicalGoalLeadPublisherId(rootThreadId);
  if (graph.publishedByNodeId !== canonicalPublisherId) {
    return `Graph publisher ${graph.publishedByNodeId} does not match authenticated root lead ${canonicalPublisherId}.`;
  }
  if (graph.nodes.some((node) => node.id === canonicalPublisherId)) {
    return `Root lead ${canonicalPublisherId} is the external graph publisher and cannot be scheduled as a graph node.`;
  }
  return null;
}

export function isGoalVerifierNode(node: GoalGraphNode): boolean {
  return (
    node.workspaceMode === "read_only" &&
    node.outputContract.kind === "verification" &&
    node.evidenceRequirements.some((requirement) => requirement.required)
  );
}

function incomingGoalEdges(graph: GoalGraphEdges): ReadonlyMap<GoalNodeId, Array<GoalNodeId>> {
  const incoming = new Map<GoalNodeId, Array<GoalNodeId>>();
  for (const edge of graph.edges) {
    const predecessors = incoming.get(edge.toNodeId);
    if (predecessors === undefined) incoming.set(edge.toNodeId, [edge.fromNodeId]);
    else predecessors.push(edge.fromNodeId);
  }
  return incoming;
}

function transitiveAncestorsFromIncoming(
  incoming: ReadonlyMap<GoalNodeId, ReadonlyArray<GoalNodeId>>,
  nodeId: GoalNodeId,
): ReadonlySet<GoalNodeId> {
  const ancestors = new Set<GoalNodeId>();
  const pending = [...(incoming.get(nodeId) ?? [])];
  while (pending.length > 0) {
    const candidate = pending.pop();
    if (candidate === undefined || ancestors.has(candidate)) continue;
    ancestors.add(candidate);
    pending.push(...(incoming.get(candidate) ?? []));
  }
  ancestors.delete(nodeId);
  return ancestors;
}

export function transitiveAncestorNodeIds(
  graph: GoalGraphEdges,
  nodeId: GoalNodeId,
): ReadonlySet<GoalNodeId> {
  return transitiveAncestorsFromIncoming(incomingGoalEdges(graph), nodeId);
}

export function isStrictTransitiveAncestor(
  graph: GoalGraphEdges,
  ancestorNodeId: GoalNodeId,
  descendantNodeId: GoalNodeId,
): boolean {
  return transitiveAncestorNodeIds(graph, descendantNodeId).has(ancestorNodeId);
}

export function goalGraphCompletionPathIssue(graph: GoalGraphVersion): string | null {
  if (graph.nodes.length === 0) {
    return "A completion-valid graph must contain at least one node and a qualifying verifier.";
  }
  const writerIds = graph.nodes
    .filter((node) => node.workspaceMode === "writer")
    .map((node) => node.id);
  const verifiers = graph.nodes.filter(isGoalVerifierNode);
  if (verifiers.length === 0) {
    return "Add a read-only verifier with verification output and at least one required evidence requirement.";
  }
  const incoming = incomingGoalEdges(graph);
  const verifierAncestors = verifiers.map((verifier) => ({
    verifier,
    ancestors: transitiveAncestorsFromIncoming(incoming, verifier.id),
  }));
  const verifierWithCompletionPath = verifierAncestors.find(({ ancestors }) => {
    return writerIds.length > 0 && writerIds.every((writerId) => ancestors.has(writerId));
  });
  if (verifierWithCompletionPath !== undefined) return null;
  if (writerIds.length === 0) {
    return "Add at least one writer producer and make it a dependency of a qualifying verifier.";
  }
  if (verifierAncestors.every(({ ancestors }) => ancestors.size === 0)) {
    return "Add at least one producer dependency before a qualifying verifier.";
  }
  return "Add a qualifying verifier that transitively depends on every writer so it runs after final integration.";
}
