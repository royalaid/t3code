import {
  Goal,
  type Goal as GoalType,
  GoalDetail,
  type GoalDetail as GoalDetailType,
  type GoalGraphNode,
  type GoalGraphVersion,
  GoalGraphVersion as GoalGraphVersionSchema,
  GoalGraphEdge,
  GoalNodeProjection,
  GoalAttempt,
  type GoalAttempt as GoalAttemptType,
  GoalArtifact,
  GoalEvidence,
  GoalFailureRecord,
  GoalId,
  GoalNodeId,
  GoalAttemptId,
  GoalSummary,
  GoalWriterCommit,
  ThreadId,
  type GoalWorkflowEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { hasDurableCommandEvidence } from "./GoalEvidenceValidation.ts";

const MAX_GRAPH_NODES = 1_000;
const MAX_GRAPH_EDGES = 5_000;
type GoalArtifactType = typeof GoalArtifact.Type;
type GoalSummaryType = typeof GoalSummary.Type;

export class GoalProjectionValidationError extends Schema.TaggedErrorClass<GoalProjectionValidationError>()(
  "GoalProjectionValidationError",
  {
    reason: Schema.Literals([
      "goal_not_found",
      "stale_revision",
      "invalid_revision",
      "duplicate_node",
      "duplicate_edge",
      "missing_dependency",
      "cycle",
      "structural_limit",
      "policy_expansion",
      "stale_evidence",
      "machine_evidence_required",
      "independent_verification_required",
      "stale_active_run_target",
      "referential_integrity",
      "persistence_error",
    ]),
    detail: Schema.String,
  },
) {}

const subsetOf = (child: ReadonlyArray<string>, parent: ReadonlyArray<string>) =>
  parent.includes("*") || child.every((value) => parent.includes(value));
const isWindowsRoot = (root: string) => /^[a-zA-Z]:[\\/]/.test(root) || root.startsWith("\\\\");
const resolveSegments = (segments: ReadonlyArray<string>) => {
  const resolved: Array<string> = [];
  for (const segment of segments) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") resolved.pop();
    else resolved.push(segment);
  }
  return resolved;
};
const rootContains = (allowed: string, child: string) => {
  if (isWindowsRoot(allowed)) {
    if (!isWindowsRoot(child)) return false;
    const allowedParts = resolveSegments(allowed.replaceAll("/", "\\").split("\\")).map((part) =>
      part.toLocaleLowerCase("en-US"),
    );
    const childParts = resolveSegments(child.replaceAll("/", "\\").split("\\")).map((part) =>
      part.toLocaleLowerCase("en-US"),
    );
    return allowedParts.every((part, index) => childParts[index] === part);
  }
  if (isWindowsRoot(child)) return false;
  const allowedParts = resolveSegments(allowed.split("/"));
  const childParts = resolveSegments(child.split("/"));
  return (
    allowed.startsWith("/") === child.startsWith("/") &&
    allowedParts.every((part, index) => childParts[index] === part)
  );
};
const writableRootsNarrow = (child: ReadonlyArray<string>, parent: ReadonlyArray<string>) =>
  child.every((root) => parent.some((allowed) => rootContains(allowed, root)));
const sandboxRank = { "read-only": 0, "workspace-write": 1, "danger-full-access": 2 } as const;
// Lower rank is less authority. A child may require more approvals, never fewer.
const approvalRank = { untrusted: 0, "on-request": 1, never: 2 } as const;

/**
 * Provider workers never receive the retained integration worktree. Integration is a
 * server-side assembly step; a conflict resolver must still be a normal writer so its
 * one clean commit passes through the same serial integration path.
 */
export function validateGoalNodeWorkspace(node: GoalGraphNode): void {
  if (node.workspaceMode === "integration") {
    throw new GoalProjectionValidationError({
      reason: "policy_expansion",
      detail: `Node ${node.id} requests the server-reserved integration workspace. Use an isolated writer node for conflict resolution.`,
    });
  }
  if (
    node.workspaceMode === "read_only" &&
    (node.policy.sandboxMode !== "read-only" || node.policy.writableRoots.length > 0)
  ) {
    throw new GoalProjectionValidationError({
      reason: "policy_expansion",
      detail: `Read-only node ${node.id} must use a read-only sandbox with no writable roots.`,
    });
  }
  if (node.workspaceMode === "writer" && node.policy.sandboxMode !== "workspace-write") {
    throw new GoalProjectionValidationError({
      reason: "policy_expansion",
      detail: `Writer node ${node.id} must use the isolated workspace-write sandbox.`,
    });
  }
}

export function validateGoalGraph(graph: GoalGraphVersion, rootPolicy: GoalType["policy"]): void {
  if (graph.nodes.length > MAX_GRAPH_NODES || graph.edges.length > MAX_GRAPH_EDGES) {
    throw new GoalProjectionValidationError({
      reason: "structural_limit",
      detail: "Graph exceeds 1,000 nodes or 5,000 edges.",
    });
  }
  const nodeIds = new Set<string>();
  for (const node of graph.nodes) {
    if (nodeIds.has(node.id))
      throw new GoalProjectionValidationError({
        reason: "duplicate_node",
        detail: `Duplicate node ${node.id}.`,
      });
    nodeIds.add(node.id);
    const policy = node.policy;
    if (
      sandboxRank[policy.sandboxMode] > sandboxRank[rootPolicy.sandboxMode] ||
      approvalRank[policy.approvalPolicy] > approvalRank[rootPolicy.approvalPolicy] ||
      !writableRootsNarrow(policy.writableRoots, rootPolicy.writableRoots) ||
      !subsetOf(policy.providerAllowlist, rootPolicy.providerAllowlist) ||
      !subsetOf(policy.toolAllowlist, rootPolicy.toolAllowlist)
    ) {
      throw new GoalProjectionValidationError({
        reason: "policy_expansion",
        detail: `Node ${node.id} expands root policy.`,
      });
    }
    validateGoalNodeWorkspace(node);
  }
  const edgeIds = new Set<string>();
  const outgoing = new Map<string, Array<string>>();
  const indegree = new Map<string, number>(Array.from(nodeIds, (id) => [id, 0]));
  for (const edge of graph.edges) {
    if (edgeIds.has(edge.id))
      throw new GoalProjectionValidationError({
        reason: "duplicate_edge",
        detail: `Duplicate edge ${edge.id}.`,
      });
    edgeIds.add(edge.id);
    if (!nodeIds.has(edge.fromNodeId) || !nodeIds.has(edge.toNodeId))
      throw new GoalProjectionValidationError({
        reason: "missing_dependency",
        detail: `Edge ${edge.id} references a missing node.`,
      });
    outgoing.set(edge.fromNodeId, [...(outgoing.get(edge.fromNodeId) ?? []), edge.toNodeId]);
    indegree.set(edge.toNodeId, (indegree.get(edge.toNodeId) ?? 0) + 1);
  }
  const queue = Array.from(indegree)
    .filter(([, degree]) => degree === 0)
    .map(([id]) => id);
  let visited = 0;
  while (queue.length > 0) {
    const id = queue.shift()!;
    visited += 1;
    for (const next of outgoing.get(id) ?? []) {
      const degree = (indegree.get(next) ?? 0) - 1;
      indegree.set(next, degree);
      if (degree === 0) queue.push(next);
    }
  }
  if (visited !== nodeIds.size)
    throw new GoalProjectionValidationError({
      reason: "cycle",
      detail: "Goal graph must be acyclic.",
    });
}

type GoalRow = { readonly payload_json: string };
type PayloadRow = { readonly payload_json: string };
const encodeGoal = Schema.encodeEffect(Schema.fromJsonString(Goal));
const encodeGraph = Schema.encodeEffect(Schema.fromJsonString(GoalGraphVersionSchema));
const encodeNode = Schema.encodeEffect(Schema.fromJsonString(GoalNodeProjection));
const encodeEdge = Schema.encodeEffect(Schema.fromJsonString(GoalGraphEdge));
const encodeAttempt = Schema.encodeEffect(Schema.fromJsonString(GoalAttempt));
const encodeArtifact = Schema.encodeEffect(Schema.fromJsonString(GoalArtifact));
const encodeEvidence = Schema.encodeEffect(Schema.fromJsonString(GoalEvidence));
const encodeWriterCommit = Schema.encodeEffect(Schema.fromJsonString(GoalWriterCommit));
const encodeFailure = Schema.encodeEffect(Schema.fromJsonString(GoalFailureRecord));
const decodeGoal = Schema.decodeUnknownEffect(Schema.fromJsonString(Goal));
const decodeGraph = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalGraphVersionSchema));
const decodeNode = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalNodeProjection));
const decodeAttempt = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalAttempt));
const decodeArtifact = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalArtifact));
const decodeEvidence = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalEvidence));
const decodeWriterCommit = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalWriterCommit));
const decodeFailure = Schema.decodeUnknownEffect(Schema.fromJsonString(GoalFailureRecord));
const decodeGoalDetail = Schema.decodeUnknownEffect(GoalDetail);
const isGoalProjectionValidationError = Schema.is(GoalProjectionValidationError);
const mapStoreError = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((error) =>
      isGoalProjectionValidationError(error)
        ? error
        : new GoalProjectionValidationError({ reason: "persistence_error", detail: String(error) }),
    ),
  );

export const readGoalDetailByRootThread = Effect.fn("readGoalDetailByRootThread")(function* (
  sql: SqlClient.SqlClient,
  rootThreadId: ThreadId,
) {
  const goalRows =
    yield* sql<GoalRow>`SELECT payload_json FROM goals WHERE root_thread_id=${rootThreadId} LIMIT 1`;
  const goalRow = goalRows[0];
  if (goalRow === undefined) return null;
  const goal = yield* decodeGoal(goalRow.payload_json);
  const [graphRows, nodeRows, attemptRows, artifactRows, evidenceRows, writerRows, failureRows] =
    yield* Effect.all([
      sql<PayloadRow>`SELECT payload_json FROM goal_graph_versions WHERE goal_id=${goal.id} ORDER BY revision`,
      sql<PayloadRow>`SELECT payload_json FROM goal_nodes WHERE goal_id=${goal.id} ORDER BY graph_version_id, node_id`,
      sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE goal_id=${goal.id} ORDER BY node_id, ordinal`,
      sql<PayloadRow>`SELECT payload_json FROM goal_artifacts WHERE goal_id=${goal.id} ORDER BY created_at, artifact_id`,
      sql<PayloadRow>`SELECT payload_json FROM goal_evidence WHERE goal_id=${goal.id} ORDER BY created_at, evidence_id`,
      sql<PayloadRow>`SELECT payload_json FROM goal_writer_commits WHERE goal_id=${goal.id} ORDER BY created_at, writer_commit_id`,
      sql<PayloadRow>`SELECT payload_json FROM goal_failures WHERE goal_id=${goal.id} ORDER BY occurred_at, failure_id`,
    ]);
  return yield* decodeGoalDetail({
    goal,
    graphVersions: yield* Effect.forEach(graphRows, (row) => decodeGraph(row.payload_json)),
    nodes: yield* Effect.forEach(nodeRows, (row) => decodeNode(row.payload_json)),
    attempts: yield* Effect.forEach(attemptRows, (row) => decodeAttempt(row.payload_json)),
    artifacts: yield* Effect.forEach(artifactRows, (row) => decodeArtifact(row.payload_json)),
    evidence: yield* Effect.forEach(evidenceRows, (row) => decodeEvidence(row.payload_json)),
    writerCommits: yield* Effect.forEach(writerRows, (row) => decodeWriterCommit(row.payload_json)),
    failures: yield* Effect.forEach(failureRows, (row) => decodeFailure(row.payload_json)),
  });
});

export const readGoalSummaryByRootThread = Effect.fn("readGoalSummaryByRootThread")(function* (
  sql: SqlClient.SqlClient,
  rootThreadId: ThreadId,
): Effect.fn.Return<GoalSummaryType | null, unknown> {
  const detail = yield* readGoalDetailByRootThread(sql, rootThreadId);
  if (detail === null) return null;
  return goalSummaryFromDetail(detail);
});

export function goalSummaryFromDetail(detail: GoalDetailType): GoalSummaryType {
  const currentNodes = detail.nodes.filter(
    (node) => node.graphVersionId === detail.goal.currentGraphVersionId,
  );
  const blockedCount = currentNodes.filter((node) => node.status === "blocked").length;
  return {
    id: detail.goal.id,
    status: detail.goal.status,
    currentRevision: detail.goal.currentRevision,
    readyCount: currentNodes.filter((node) => node.status === "ready").length,
    runningCount: currentNodes.filter(
      (node) => node.status === "running" || node.status === "processing",
    ).length,
    blockedCount,
    attentionRequired:
      blockedCount > 0 ||
      detail.goal.status === "blocked" ||
      detail.goal.status === "failed" ||
      detail.goal.status === "paused",
    verified:
      detail.goal.integrationSha !== null && detail.goal.verifiedSha === detail.goal.integrationSha,
  };
}

type GoalSummaryRow = {
  readonly root_thread_id: string;
  readonly payload_json: string;
  readonly ready_count: number;
  readonly running_count: number;
  readonly blocked_count: number;
};
export const readGoalSummariesByRootThread = Effect.fn("readGoalSummariesByRootThread")(function* (
  sql: SqlClient.SqlClient,
) {
  const rows = yield* sql<GoalSummaryRow>`
    SELECT g.root_thread_id, g.payload_json,
      SUM(CASE WHEN n.status = 'ready' THEN 1 ELSE 0 END) AS ready_count,
      SUM(CASE WHEN n.status IN ('running', 'processing') THEN 1 ELSE 0 END) AS running_count,
      SUM(CASE WHEN n.status = 'blocked' THEN 1 ELSE 0 END) AS blocked_count
    FROM goals g
    LEFT JOIN goal_nodes n
      ON n.goal_id = g.goal_id AND n.graph_version_id = g.current_graph_version_id
    GROUP BY g.goal_id, g.root_thread_id, g.payload_json
  `;
  const entries = yield* Effect.forEach(rows, (row) =>
    decodeGoal(row.payload_json).pipe(
      Effect.map((goal) => {
        const summary: GoalSummaryType = {
          id: goal.id,
          status: goal.status,
          currentRevision: goal.currentRevision,
          readyCount: row.ready_count,
          runningCount: row.running_count,
          blockedCount: row.blocked_count,
          attentionRequired:
            row.blocked_count > 0 ||
            goal.status === "blocked" ||
            goal.status === "failed" ||
            goal.status === "paused",
          verified: goal.integrationSha !== null && goal.verifiedSha === goal.integrationSha,
        };
        return [ThreadId.make(row.root_thread_id), summary] as const;
      }),
    ),
  );
  return new Map(entries);
});

export interface GoalProjectionStoreShape {
  readonly create: (goal: GoalType) => Effect.Effect<void, GoalProjectionValidationError>;
  readonly activateGraph: (input: {
    readonly goalId: GoalId;
    readonly expectedRevision: number;
    readonly graph: GoalGraphVersion;
  }) => Effect.Effect<void, GoalProjectionValidationError>;
  readonly apply: (event: GoalWorkflowEvent) => Effect.Effect<void, GoalProjectionValidationError>;
  readonly getDetail: (
    goalId: GoalId,
  ) => Effect.Effect<GoalDetailType, GoalProjectionValidationError>;
  readonly listPendingLaunches: Effect.Effect<
    ReadonlyArray<GoalDetailType>,
    GoalProjectionValidationError
  >;
  /**
   * Includes lifecycle-schedulable goals plus any goal with an active attempt.
   * Callers must still gate *new* work on lifecycle status; the extra rows are
   * capacity and recovery context for workers that continue while paused or
   * blocked.
   */
  readonly listSchedulable: Effect.Effect<
    ReadonlyArray<GoalDetailType>,
    GoalProjectionValidationError
  >;
  readonly resolveMcpBinding: (threadId: ThreadId) => Effect.Effect<
    | { readonly kind: "lead"; readonly goalId: GoalId; readonly rootThreadId: ThreadId }
    | {
        readonly kind: "worker";
        readonly goalId: GoalId;
        readonly rootThreadId: ThreadId;
        readonly nodeId: GoalNodeId;
        readonly attemptId: GoalAttemptId;
      }
    | null,
    GoalProjectionValidationError
  >;
}
export class GoalProjectionStore extends Context.Service<
  GoalProjectionStore,
  GoalProjectionStoreShape
>()("t3/orchestration-v2/GoalProjectionStore") {}

export const layer: Layer.Layer<GoalProjectionStore, never, SqlClient.SqlClient> = Layer.effect(
  GoalProjectionStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const create = Effect.fn("GoalProjectionStore.create")(function* (goal: GoalType) {
      const payload = yield* encodeGoal(goal);
      const existingRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goals WHERE goal_id=${goal.id}`;
      if (existingRows[0] !== undefined) {
        const existing = yield* decodeGoal(existingRows[0].payload_json);
        if ((yield* encodeGoal(existing)) !== payload)
          return yield* new GoalProjectionValidationError({
            reason: "referential_integrity",
            detail: `Goal ${goal.id} reuses an existing ID with different immutable identity.`,
          });
        return;
      }
      yield* sql`INSERT INTO goals (
        goal_id, root_thread_id, source_thread_id, status, current_graph_version_id, current_revision,
        integration_sha, verified_sha, payload_json, created_at, updated_at
      ) VALUES (${goal.id}, ${goal.rootThreadId}, ${goal.sourceThreadId}, ${goal.status}, ${goal.currentGraphVersionId},
        ${goal.currentRevision}, ${goal.integrationSha}, ${goal.verifiedSha}, ${payload}, ${goal.createdAt}, ${goal.updatedAt})
      ON CONFLICT(goal_id) DO NOTHING`;
    });

    const readGoal = Effect.fn("GoalProjectionStore.readGoal")(function* (goalId: GoalId) {
      const rows =
        yield* sql<GoalRow>`SELECT payload_json FROM goals WHERE goal_id=${goalId} LIMIT 1`;
      const row = rows[0];
      if (row === undefined)
        return yield* new GoalProjectionValidationError({
          reason: "goal_not_found",
          detail: `Goal ${goalId} does not exist.`,
        });
      return yield* decodeGoal(row.payload_json).pipe(
        Effect.mapError(
          (error) =>
            new GoalProjectionValidationError({
              reason: "persistence_error",
              detail: String(error),
            }),
        ),
      );
    });

    const persistLifecycle = Effect.fn("GoalProjectionStore.persistLifecycle")(function* (
      event: Extract<GoalWorkflowEvent, { readonly payload: GoalType }> & {
        readonly type:
          | "goal.updated"
          | "goal.reopened"
          | "goal.cancelled"
          | "goal.completed"
          | "goal.integration-updated"
          | "goal.integration-conflicted";
      },
    ) {
      const existing = yield* readGoal(event.payload.id);
      const integrationChanged = existing.integrationSha !== event.payload.integrationSha;
      const next = {
        ...existing,
        objective: event.payload.objective,
        status: event.payload.status,
        ...(event.payload.sourceActiveRunId === undefined
          ? {}
          : { sourceActiveRunId: event.payload.sourceActiveRunId }),
        ...(event.payload.pendingLaunchClaimId === undefined
          ? {}
          : { pendingLaunchClaimId: event.payload.pendingLaunchClaimId }),
        ...(event.payload.sourceHandoff === undefined
          ? {}
          : { sourceHandoff: event.payload.sourceHandoff }),
        currentGraphVersionId:
          event.type === "goal.reopened" ? null : existing.currentGraphVersionId,
        integrationBranch: event.payload.integrationBranch,
        integrationWorktreePath: event.payload.integrationWorktreePath,
        integrationSha: event.payload.integrationSha,
        verifiedSha:
          event.type === "goal.reopened" || integrationChanged
            ? null
            : event.type === "goal.completed" &&
                event.payload.verifiedSha === existing.integrationSha
              ? event.payload.verifiedSha
              : existing.verifiedSha,
        updatedAt: event.payload.updatedAt,
      } satisfies GoalType;
      const payload = yield* encodeGoal(next);
      yield* sql`UPDATE goals SET status=${next.status}, current_graph_version_id=${next.currentGraphVersionId},
        integration_sha=${next.integrationSha}, verified_sha=${next.verifiedSha}, payload_json=${payload}, updated_at=${next.updatedAt}
        WHERE goal_id=${next.id}`;
    });

    const requireNode = Effect.fn("GoalProjectionStore.requireNode")(function* (
      goalId: GoalId,
      graphVersionId: GoalGraphVersion["id"],
      nodeId: GoalAttemptType["nodeId"],
    ) {
      const rows = yield* sql<{ readonly goal_id: string; readonly payload_json: string }>`
        SELECT goal_id, payload_json FROM goal_nodes
        WHERE graph_version_id=${graphVersionId} AND node_id=${nodeId} LIMIT 1`;
      if (rows[0]?.goal_id !== goalId)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Node ${nodeId} does not belong to graph ${graphVersionId} for goal ${goalId}.`,
        });
      return yield* decodeNode(rows[0].payload_json);
    });

    const requireAttempt = Effect.fn("GoalProjectionStore.requireAttempt")(function* (
      attemptId: GoalAttemptType["id"],
      goalId: GoalId,
      nodeId: GoalAttemptType["nodeId"],
    ) {
      const rows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE attempt_id=${attemptId} LIMIT 1`;
      const row = rows[0];
      if (row === undefined)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Attempt ${attemptId} does not exist.`,
        });
      const attempt = yield* decodeAttempt(row.payload_json);
      if (attempt.goalId !== goalId || attempt.nodeId !== nodeId)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Attempt ${attemptId} belongs to a different goal or node.`,
        });
      return attempt;
    });
    const requireAttemptForGoal = Effect.fn("GoalProjectionStore.requireAttemptForGoal")(function* (
      attemptId: GoalAttemptType["id"],
      goalId: GoalId,
    ) {
      const rows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE attempt_id=${attemptId} LIMIT 1`;
      const row = rows[0];
      if (row === undefined)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Attempt ${attemptId} does not exist.`,
        });
      const attempt = yield* decodeAttempt(row.payload_json);
      if (attempt.goalId !== goalId)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Attempt ${attemptId} belongs to a different goal.`,
        });
      return attempt;
    });
    const requireOwnedArtifact = Effect.fn("GoalProjectionStore.requireOwnedArtifact")(function* (
      artifactId: GoalArtifactType["id"],
      goalId: GoalId,
      nodeId: GoalNodeId,
      attemptId: GoalAttemptType["id"],
    ) {
      const rows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_artifacts WHERE artifact_id=${artifactId} LIMIT 1`;
      if (rows[0] === undefined)
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Evidence artifact ${artifactId} does not exist.`,
        });
      const artifact = yield* decodeArtifact(rows[0].payload_json);
      if (
        artifact.goalId !== goalId ||
        artifact.nodeId !== nodeId ||
        artifact.attemptId !== attemptId
      )
        return yield* new GoalProjectionValidationError({
          reason: "referential_integrity",
          detail: `Evidence artifact ${artifactId} belongs to a different worker scope.`,
        });
      return artifact;
    });

    const identityConflict = (entity: string, id: string) =>
      new GoalProjectionValidationError({
        reason: "referential_integrity",
        detail: `${entity} ${id} reuses an existing ID with different immutable identity.`,
      });

    const rejectCancelledNodePublication = (node: GoalNodeProjection, publication: string) => {
      if (node.status !== "cancelled" && node.status !== "superseded") return null;
      return new GoalProjectionValidationError({
        reason: "stale_active_run_target",
        detail: `${publication} is late because node ${node.node.id} is ${node.status}.`,
      });
    };

    const activateGraph = Effect.fn("GoalProjectionStore.activateGraph")(function* (
      input: Parameters<GoalProjectionStoreShape["activateGraph"]>[0],
    ) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const goal = yield* readGoal(input.goalId);
          if (goal.currentRevision !== input.expectedRevision)
            return yield* new GoalProjectionValidationError({
              reason: "stale_revision",
              detail: `Expected revision ${input.expectedRevision}, current revision is ${goal.currentRevision}.`,
            });
          if (
            input.graph.goalId !== input.goalId ||
            input.graph.revision !== input.expectedRevision + 1
          )
            return yield* new GoalProjectionValidationError({
              reason: "invalid_revision",
              detail: "Graph identity or revision does not follow the current goal revision.",
            });
          yield* Effect.try({
            try: () => validateGoalGraph(input.graph, goal.policy),
            catch: (error) => error as GoalProjectionValidationError,
          });
          const nextGoal = {
            ...goal,
            status: "running" as const,
            currentGraphVersionId: input.graph.id,
            currentRevision: input.graph.revision,
            verifiedSha: null,
            updatedAt: input.graph.createdAt,
          };
          const nextGoalPayload = yield* encodeGoal(nextGoal);
          const claimed = yield* sql<{ readonly goal_id: string }>`UPDATE goals
            SET status='running', current_graph_version_id=${input.graph.id}, current_revision=${input.graph.revision},
              verified_sha=NULL, payload_json=${nextGoalPayload}, updated_at=${input.graph.createdAt}
            WHERE goal_id=${input.goalId} AND current_revision=${input.expectedRevision}
            RETURNING goal_id`;
          if (claimed.length !== 1)
            return yield* new GoalProjectionValidationError({
              reason: "stale_revision",
              detail: "Goal revision changed during activation.",
            });
          const graphPayload = yield* encodeGraph(input.graph);
          yield* sql`INSERT INTO goal_graph_versions (graph_version_id, goal_id, revision, published_by_node_id, payload_json, created_at)
          VALUES (${input.graph.id}, ${input.goalId}, ${input.graph.revision}, ${input.graph.publishedByNodeId}, ${graphPayload}, ${input.graph.createdAt})`;
          for (const node of input.graph.nodes) {
            const projection = {
              goalId: input.goalId,
              graphVersionId: input.graph.id,
              node,
              status: "pending",
              activeAttemptId: null,
              blocker: null,
              updatedAt: input.graph.createdAt,
            } as const;
            const nodePayload = yield* encodeNode(projection);
            yield* sql`INSERT INTO goal_nodes (goal_id, graph_version_id, node_id, status, active_attempt_id, blocker, payload_json, updated_at)
            VALUES (${input.goalId}, ${input.graph.id}, ${node.id}, 'pending', NULL, NULL, ${nodePayload}, ${input.graph.createdAt})`;
          }
          for (const edge of input.graph.edges) {
            const edgePayload = yield* encodeEdge(edge);
            yield* sql`INSERT INTO goal_edges (graph_version_id, edge_id, from_node_id, to_node_id, payload_json)
            VALUES (${input.graph.id}, ${edge.id}, ${edge.fromNodeId}, ${edge.toNodeId}, ${edgePayload})`;
          }
        }),
      );
    });

    const getDetail = Effect.fn("GoalProjectionStore.getDetail")(function* (goalId: GoalId) {
      const goal = yield* readGoal(goalId);
      const graphRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_graph_versions WHERE goal_id=${goalId} ORDER BY revision`;
      const nodeRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_nodes WHERE goal_id=${goalId} ORDER BY graph_version_id, node_id`;
      const attemptRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE goal_id=${goalId} ORDER BY node_id, ordinal`;
      const artifactRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_artifacts WHERE goal_id=${goalId} ORDER BY created_at, artifact_id`;
      const evidenceRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_evidence WHERE goal_id=${goalId} ORDER BY created_at, evidence_id`;
      const writerRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_writer_commits WHERE goal_id=${goalId} ORDER BY created_at, writer_commit_id`;
      const failureRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_failures WHERE goal_id=${goalId} ORDER BY occurred_at, failure_id`;
      return yield* decodeGoalDetail({
        goal,
        graphVersions: yield* Effect.forEach(graphRows, (row) => decodeGraph(row.payload_json)),
        nodes: yield* Effect.forEach(nodeRows, (row) => decodeNode(row.payload_json)),
        attempts: yield* Effect.forEach(attemptRows, (row) => decodeAttempt(row.payload_json)),
        artifacts: yield* Effect.forEach(artifactRows, (row) => decodeArtifact(row.payload_json)),
        evidence: yield* Effect.forEach(evidenceRows, (row) => decodeEvidence(row.payload_json)),
        writerCommits: yield* Effect.forEach(writerRows, (row) =>
          decodeWriterCommit(row.payload_json),
        ),
        failures: yield* Effect.forEach(failureRows, (row) => decodeFailure(row.payload_json)),
      }).pipe(
        Effect.mapError(
          (error) =>
            new GoalProjectionValidationError({
              reason: "persistence_error",
              detail: String(error),
            }),
        ),
      );
    });

    const apply = Effect.fn("GoalProjectionStore.apply")(function* (event: GoalWorkflowEvent) {
      switch (event.type) {
        case "goal.created":
          return yield* create(event.payload);
        case "goal.updated":
        case "goal.reopened":
        case "goal.cancelled":
        case "goal.completed":
        case "goal.integration-updated":
        case "goal.integration-conflicted":
          return yield* persistLifecycle(event);
        case "goal.graph-version-activated":
          return yield* activateGraph({
            goalId: event.payload.goalId,
            expectedRevision: event.payload.expectedRevision,
            graph: event.payload.graph,
          });
        case "goal.node-transitioned":
        case "goal.node-cancellation-requested": {
          const existingNode = yield* requireNode(
            event.payload.goalId,
            event.payload.graphVersionId,
            event.payload.node.id,
          );
          const nextNode = {
            ...existingNode,
            status: event.payload.status,
            activeAttemptId: event.payload.activeAttemptId,
            blocker: event.payload.blocker,
            updatedAt: event.payload.updatedAt,
          };
          yield* sql`UPDATE goal_nodes SET status=${nextNode.status}, active_attempt_id=${nextNode.activeAttemptId}, blocker=${nextNode.blocker}, payload_json=${yield* encodeNode(nextNode)}, updated_at=${nextNode.updatedAt}
            WHERE graph_version_id=${event.payload.graphVersionId} AND node_id=${event.payload.node.id}`;
          return;
        }
        case "goal.attempt-created":
        case "goal.attempt-transitioned":
        case "goal.route-resolved": {
          yield* requireNode(
            event.payload.goalId,
            event.payload.graphVersionId,
            event.payload.nodeId,
          );
          const existingRows =
            yield* sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE attempt_id=${event.payload.id}`;
          let payload = event.payload;
          if (existingRows[0] !== undefined) {
            const existingAttempt = yield* decodeAttempt(existingRows[0].payload_json);
            const incomingWithExistingMutable = {
              ...event.payload,
              status: existingAttempt.status,
              resolvedRoute: existingAttempt.resolvedRoute,
              providerSessionId: existingAttempt.providerSessionId,
              executionThreadId: existingAttempt.executionThreadId,
              runId: existingAttempt.runId,
              rootExecutionNodeId: existingAttempt.rootExecutionNodeId,
              workspacePath: existingAttempt.workspacePath,
              leaseOwner: existingAttempt.leaseOwner,
              leaseExpiresAt: existingAttempt.leaseExpiresAt,
              usage: existingAttempt.usage,
              failureReason: existingAttempt.failureReason,
              updatedAt: existingAttempt.updatedAt,
            };
            if (
              (yield* encodeAttempt(existingAttempt)) !==
              (yield* encodeAttempt(incomingWithExistingMutable))
            )
              return yield* new GoalProjectionValidationError({
                reason: "referential_integrity",
                detail: "Attempt event cannot change immutable attempt identity.",
              });
            payload = {
              ...existingAttempt,
              status: event.payload.status,
              resolvedRoute: event.payload.resolvedRoute,
              providerSessionId: event.payload.providerSessionId,
              executionThreadId: event.payload.executionThreadId,
              runId: event.payload.runId,
              rootExecutionNodeId: event.payload.rootExecutionNodeId,
              workspacePath: event.payload.workspacePath,
              leaseOwner: event.payload.leaseOwner,
              leaseExpiresAt: event.payload.leaseExpiresAt,
              usage: event.payload.usage,
              failureReason: event.payload.failureReason,
              updatedAt: event.payload.updatedAt,
            };
          }
          yield* sql`INSERT INTO goal_attempts (attempt_id, goal_id, graph_version_id, node_id, ordinal, execution_thread_id, run_id, root_execution_node_id, status, lease_owner, lease_expires_at, payload_json, created_at, updated_at)
            VALUES (${payload.id}, ${payload.goalId}, ${payload.graphVersionId}, ${payload.nodeId}, ${payload.ordinal}, ${payload.executionThreadId}, ${payload.runId}, ${payload.rootExecutionNodeId}, ${payload.status}, ${payload.leaseOwner}, ${payload.leaseExpiresAt}, ${yield* encodeAttempt(payload)}, ${payload.createdAt}, ${payload.updatedAt})
            ON CONFLICT(attempt_id) DO UPDATE SET execution_thread_id=excluded.execution_thread_id,
              run_id=excluded.run_id, root_execution_node_id=excluded.root_execution_node_id,
              status=excluded.status, lease_owner=excluded.lease_owner,
              lease_expires_at=excluded.lease_expires_at, payload_json=excluded.payload_json,
              updated_at=excluded.updated_at`;
          return;
        }
        case "goal.artifact-published": {
          const owningAttempt = yield* requireAttempt(
            event.payload.attemptId,
            event.payload.goalId,
            event.payload.nodeId,
          );
          const owningNode = yield* requireNode(
            event.payload.goalId,
            owningAttempt.graphVersionId,
            event.payload.nodeId,
          );
          const cancellation = rejectCancelledNodePublication(
            owningNode,
            `Artifact ${event.payload.id}`,
          );
          if (cancellation !== null) return yield* cancellation;
          const payload = yield* encodeArtifact(event.payload);
          const existingRows =
            yield* sql<PayloadRow>`SELECT payload_json FROM goal_artifacts WHERE artifact_id=${event.payload.id}`;
          if (existingRows[0] !== undefined) {
            const existing = yield* decodeArtifact(existingRows[0].payload_json);
            if ((yield* encodeArtifact(existing)) !== payload)
              return yield* identityConflict("Artifact", event.payload.id);
            return;
          }
          yield* sql`INSERT INTO goal_artifacts (artifact_id, goal_id, node_id, attempt_id, kind, payload_json, created_at) VALUES (${event.payload.id}, ${event.payload.goalId}, ${event.payload.nodeId}, ${event.payload.attemptId}, ${event.payload.kind}, ${payload}, ${event.payload.createdAt})`;
          return;
        }
        case "goal.writer-commit-recorded": {
          const owningAttempt = yield* requireAttempt(
            event.payload.attemptId,
            event.payload.goalId,
            event.payload.nodeId,
          );
          const owningNode = yield* requireNode(
            event.payload.goalId,
            event.payload.graphVersionId,
            event.payload.nodeId,
          );
          if (owningAttempt.graphVersionId !== event.payload.graphVersionId)
            return yield* identityConflict("Writer commit", event.payload.id);
          if (owningNode.node.workspaceMode !== "writer")
            return yield* new GoalProjectionValidationError({
              reason: "referential_integrity",
              detail: `Writer commit ${event.payload.id} belongs to non-writer node ${event.payload.nodeId}.`,
            });
          const cancellation = rejectCancelledNodePublication(
            owningNode,
            `Writer commit ${event.payload.id}`,
          );
          if (cancellation !== null) return yield* cancellation;
          if (!event.payload.cleanSingleCommit)
            return yield* new GoalProjectionValidationError({
              reason: "referential_integrity",
              detail: `Writer commit ${event.payload.id} is not a clean single-commit publication.`,
            });
          if (
            owningAttempt.baseIntegrationSha === null ||
            event.payload.baseSha !== owningAttempt.baseIntegrationSha
          )
            return yield* new GoalProjectionValidationError({
              reason: "referential_integrity",
              detail: `Writer commit ${event.payload.id} does not match attempt ${owningAttempt.id}'s recorded base SHA.`,
            });
          const existingRows =
            yield* sql<PayloadRow>`SELECT payload_json FROM goal_writer_commits WHERE writer_commit_id=${event.payload.id}`;
          let payload = event.payload;
          if (existingRows[0] !== undefined) {
            const existing = yield* decodeWriterCommit(existingRows[0].payload_json);
            const incomingWithExistingMutable = {
              ...event.payload,
              integrationAfterSha: existing.integrationAfterSha,
              state: existing.state,
              updatedAt: existing.updatedAt,
            };
            if (
              (yield* encodeWriterCommit(existing)) !==
              (yield* encodeWriterCommit(incomingWithExistingMutable))
            )
              return yield* identityConflict("Writer commit", event.payload.id);
            payload = {
              ...existing,
              integrationAfterSha: event.payload.integrationAfterSha,
              state: event.payload.state,
              updatedAt: event.payload.updatedAt,
            };
          }
          yield* sql`INSERT INTO goal_writer_commits (writer_commit_id, goal_id, graph_version_id, node_id, attempt_id, base_sha, commit_sha, integration_before_sha, integration_after_sha, state, payload_json, created_at, updated_at)
            VALUES (${payload.id}, ${payload.goalId}, ${payload.graphVersionId}, ${payload.nodeId}, ${payload.attemptId}, ${payload.baseSha}, ${payload.commitSha}, ${payload.integrationBeforeSha}, ${payload.integrationAfterSha}, ${payload.state}, ${yield* encodeWriterCommit(payload)}, ${payload.createdAt}, ${payload.updatedAt})
            ON CONFLICT(writer_commit_id) DO UPDATE SET integration_after_sha=excluded.integration_after_sha, state=excluded.state, payload_json=excluded.payload_json, updated_at=excluded.updated_at`;
          return;
        }
        case "goal.evidence-submitted":
        case "goal.verdict-recorded": {
          const verifierAttempt = yield* requireAttempt(
            event.payload.attemptId,
            event.payload.goalId,
            event.payload.nodeId,
          );
          const producerAttempt = yield* requireAttemptForGoal(
            event.payload.producerAttemptId,
            event.payload.goalId,
          );
          const verifierNode = yield* requireNode(
            event.payload.goalId,
            verifierAttempt.graphVersionId,
            verifierAttempt.nodeId,
          );
          const producerNode = yield* requireNode(
            event.payload.goalId,
            producerAttempt.graphVersionId,
            producerAttempt.nodeId,
          );
          const verifierCancellation = rejectCancelledNodePublication(
            verifierNode,
            `Evidence ${event.payload.id}`,
          );
          if (verifierCancellation !== null) return yield* verifierCancellation;
          const producerCancellation = rejectCancelledNodePublication(
            producerNode,
            `Evidence ${event.payload.id}`,
          );
          if (producerCancellation !== null) return yield* producerCancellation;
          if (event.payload.verdict === "accepted") {
            if (!hasDurableCommandEvidence(event.payload))
              return yield* new GoalProjectionValidationError({
                reason: "machine_evidence_required",
                detail:
                  "Accepted evidence requires a recorded command with a durable log artifact reference.",
              });
            const goal = yield* readGoal(event.payload.goalId);
            if (
              goal.integrationSha === null ||
              event.payload.integrationSha !== goal.integrationSha
            )
              return yield* new GoalProjectionValidationError({
                reason: "stale_evidence",
                detail: `Accepted evidence targets ${event.payload.integrationSha}, current integration SHA is ${goal.integrationSha ?? "unavailable"}.`,
              });
            if (
              verifierAttempt.id === producerAttempt.id ||
              verifierAttempt.nodeId === producerAttempt.nodeId ||
              verifierAttempt.executionThreadId === null ||
              producerAttempt.executionThreadId === null ||
              verifierAttempt.executionThreadId === producerAttempt.executionThreadId
            )
              return yield* new GoalProjectionValidationError({
                reason: "independent_verification_required",
                detail: "Accepted evidence requires a distinct verifier attempt, node, and thread.",
              });
          }
          for (const artifactId of new Set([
            ...event.payload.artifacts,
            ...event.payload.commands.flatMap((command) =>
              command.logArtifactId === null ? [] : [command.logArtifactId],
            ),
          ])) {
            yield* requireOwnedArtifact(
              artifactId,
              event.payload.goalId,
              event.payload.nodeId,
              event.payload.attemptId,
            );
          }
          const existingRows =
            yield* sql<PayloadRow>`SELECT payload_json FROM goal_evidence WHERE evidence_id=${event.payload.id}`;
          let payload = event.payload;
          if (existingRows[0] !== undefined) {
            const existing = yield* decodeEvidence(existingRows[0].payload_json);
            if (
              (yield* encodeEvidence(existing)) !==
              (yield* encodeEvidence({ ...event.payload, verdict: existing.verdict }))
            )
              return yield* identityConflict("Evidence", event.payload.id);
            payload = { ...existing, verdict: event.payload.verdict };
          }
          yield* sql`INSERT INTO goal_evidence (evidence_id, goal_id, node_id, attempt_id, integration_sha, verdict, payload_json, created_at) VALUES (${payload.id}, ${payload.goalId}, ${payload.nodeId}, ${payload.attemptId}, ${payload.integrationSha}, ${payload.verdict}, ${yield* encodeEvidence(payload)}, ${payload.createdAt}) ON CONFLICT(evidence_id) DO UPDATE SET verdict=excluded.verdict, payload_json=excluded.payload_json`;
          return;
        }
        case "goal.failure-recorded": {
          if ((event.payload.graphVersionId === null) !== (event.payload.nodeId === null))
            return yield* identityConflict("Failure", event.payload.id);
          if (event.payload.graphVersionId !== null && event.payload.nodeId !== null)
            yield* requireNode(
              event.payload.goalId,
              event.payload.graphVersionId,
              event.payload.nodeId,
            );
          if (event.payload.attemptId !== null) {
            if (event.payload.graphVersionId === null || event.payload.nodeId === null)
              return yield* identityConflict("Failure", event.payload.id);
            const attempt = yield* requireAttempt(
              event.payload.attemptId,
              event.payload.goalId,
              event.payload.nodeId,
            );
            if (attempt.graphVersionId !== event.payload.graphVersionId)
              return yield* identityConflict("Failure", event.payload.id);
          }
          const existingRows =
            yield* sql<PayloadRow>`SELECT payload_json FROM goal_failures WHERE failure_id=${event.payload.id}`;
          let payload = event.payload;
          if (existingRows[0] !== undefined) {
            const existing = yield* decodeFailure(existingRows[0].payload_json);
            const incomingWithExistingMutable = {
              ...event.payload,
              recoveryState: existing.recoveryState,
              blocker: existing.blocker,
            };
            if (
              (yield* encodeFailure(existing)) !==
              (yield* encodeFailure(incomingWithExistingMutable))
            )
              return yield* identityConflict("Failure", event.payload.id);
            payload = {
              ...existing,
              recoveryState: event.payload.recoveryState,
              blocker: event.payload.blocker,
            };
          }
          yield* sql`INSERT INTO goal_failures (failure_id, goal_id, graph_version_id, node_id, attempt_id, recovery_state, blocker, payload_json, occurred_at)
            VALUES (${payload.id}, ${payload.goalId}, ${payload.graphVersionId}, ${payload.nodeId}, ${payload.attemptId}, ${payload.recoveryState}, ${payload.blocker}, ${yield* encodeFailure(payload)}, ${payload.occurredAt})
            ON CONFLICT(failure_id) DO UPDATE SET recovery_state=excluded.recovery_state, blocker=excluded.blocker, payload_json=excluded.payload_json`;
          return;
        }
      }
    });
    const listPendingLaunches = Effect.gen(function* () {
      const rows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goals WHERE status IN ('waiting_for_source', 'provisioning') ORDER BY created_at`;
      return yield* Effect.forEach(rows, (row) =>
        decodeGoal(row.payload_json).pipe(Effect.flatMap((goal) => getDetail(goal.id))),
      );
    });
    const listSchedulable = Effect.gen(function* () {
      const rows = yield* sql<PayloadRow>`SELECT payload_json FROM goals
          WHERE current_graph_version_id IS NOT NULL
            AND (
              status IN ('planning', 'running')
              OR EXISTS (
                SELECT 1 FROM goal_attempts
                WHERE goal_attempts.goal_id = goals.goal_id
                  AND goal_attempts.status IN ('leased', 'launching', 'running', 'stalled')
              )
            )
          ORDER BY created_at, goal_id`;
      return yield* Effect.forEach(rows, (row) =>
        decodeGoal(row.payload_json).pipe(Effect.flatMap((goal) => getDetail(goal.id))),
      );
    });
    const resolveMcpBinding = Effect.fn("GoalProjectionStore.resolveMcpBinding")(function* (
      threadId: ThreadId,
    ) {
      const rootRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goals WHERE root_thread_id=${threadId} LIMIT 1`;
      if (rootRows[0] !== undefined) {
        const goal = yield* decodeGoal(rootRows[0].payload_json);
        return { kind: "lead" as const, goalId: goal.id, rootThreadId: goal.rootThreadId };
      }
      const attemptRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goal_attempts WHERE execution_thread_id=${threadId} ORDER BY updated_at DESC LIMIT 1`;
      if (attemptRows[0] === undefined) return null;
      const attempt = yield* decodeAttempt(attemptRows[0].payload_json);
      const goalRows =
        yield* sql<PayloadRow>`SELECT payload_json FROM goals WHERE goal_id=${attempt.goalId} LIMIT 1`;
      if (goalRows[0] === undefined) return null;
      const goal = yield* decodeGoal(goalRows[0].payload_json);
      return {
        kind: "worker" as const,
        goalId: goal.id,
        rootThreadId: goal.rootThreadId,
        nodeId: attempt.nodeId,
        attemptId: attempt.id,
      };
    });
    return GoalProjectionStore.of({
      create: (goal) => mapStoreError(create(goal)),
      activateGraph: (input) => mapStoreError(activateGraph(input)),
      apply: (event) => mapStoreError(apply(event)),
      getDetail: (goalId) => mapStoreError(getDetail(goalId)),
      listPendingLaunches: mapStoreError(listPendingLaunches),
      listSchedulable: mapStoreError(listSchedulable),
      resolveMcpBinding: (threadId) => mapStoreError(resolveMcpBinding(threadId)),
    });
  }),
);
