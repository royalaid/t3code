import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE goals (
    goal_id TEXT PRIMARY KEY, root_thread_id TEXT NOT NULL UNIQUE, source_thread_id TEXT NOT NULL,
    status TEXT NOT NULL, current_graph_version_id TEXT, current_revision INTEGER NOT NULL DEFAULT 0,
    integration_sha TEXT, verified_sha TEXT, payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX idx_goals_source_thread ON goals(source_thread_id, updated_at)`;
  yield* sql`CREATE TABLE goal_graph_versions (
    graph_version_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, revision INTEGER NOT NULL,
    published_by_node_id TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    UNIQUE(goal_id, revision), FOREIGN KEY(goal_id) REFERENCES goals(goal_id)
  )`;
  yield* sql`CREATE TABLE goal_nodes (
    goal_id TEXT NOT NULL, graph_version_id TEXT NOT NULL, node_id TEXT NOT NULL,
    status TEXT NOT NULL, active_attempt_id TEXT, blocker TEXT, payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL, PRIMARY KEY(graph_version_id, node_id),
    FOREIGN KEY(goal_id) REFERENCES goals(goal_id),
    FOREIGN KEY(graph_version_id) REFERENCES goal_graph_versions(graph_version_id)
  )`;
  yield* sql`CREATE INDEX idx_goal_nodes_ready ON goal_nodes(goal_id, status, updated_at)`;
  yield* sql`CREATE TABLE goal_edges (
    graph_version_id TEXT NOT NULL, edge_id TEXT NOT NULL, from_node_id TEXT NOT NULL,
    to_node_id TEXT NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(graph_version_id, edge_id),
    FOREIGN KEY(graph_version_id, from_node_id) REFERENCES goal_nodes(graph_version_id, node_id),
    FOREIGN KEY(graph_version_id, to_node_id) REFERENCES goal_nodes(graph_version_id, node_id)
  )`;
  yield* sql`CREATE INDEX idx_goal_edges_dependency ON goal_edges(graph_version_id, to_node_id)`;
  yield* sql`CREATE TABLE goal_attempts (
    attempt_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, graph_version_id TEXT NOT NULL,
    node_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
    execution_thread_id TEXT, run_id TEXT, root_execution_node_id TEXT,
    status TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT, payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(graph_version_id, node_id, ordinal),
    FOREIGN KEY(goal_id) REFERENCES goals(goal_id),
    FOREIGN KEY(graph_version_id, node_id) REFERENCES goal_nodes(graph_version_id, node_id)
  )`;
  yield* sql`CREATE INDEX idx_goal_attempts_leases ON goal_attempts(status, lease_expires_at)`;
  yield* sql`CREATE INDEX idx_goal_attempts_execution ON goal_attempts(execution_thread_id, run_id)`;
  yield* sql`CREATE TABLE goal_artifacts (
    artifact_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, node_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
    kind TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY(attempt_id) REFERENCES goal_attempts(attempt_id)
  )`;
  yield* sql`CREATE TABLE goal_evidence (
    evidence_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, node_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
    integration_sha TEXT NOT NULL, verdict TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY(attempt_id) REFERENCES goal_attempts(attempt_id)
  )`;
  yield* sql`CREATE INDEX idx_goal_evidence_sha ON goal_evidence(goal_id, integration_sha, verdict)`;
  yield* sql`CREATE TABLE goal_writer_commits (
    writer_commit_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, graph_version_id TEXT NOT NULL,
    node_id TEXT NOT NULL, attempt_id TEXT NOT NULL, base_sha TEXT NOT NULL, commit_sha TEXT NOT NULL,
    integration_before_sha TEXT NOT NULL, integration_after_sha TEXT, state TEXT NOT NULL,
    payload_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    FOREIGN KEY(attempt_id) REFERENCES goal_attempts(attempt_id)
  )`;
  yield* sql`CREATE TABLE goal_failures (
    failure_id TEXT PRIMARY KEY, goal_id TEXT NOT NULL, graph_version_id TEXT, node_id TEXT, attempt_id TEXT,
    recovery_state TEXT NOT NULL, blocker TEXT, payload_json TEXT NOT NULL, occurred_at TEXT NOT NULL,
    FOREIGN KEY(goal_id) REFERENCES goals(goal_id), FOREIGN KEY(attempt_id) REFERENCES goal_attempts(attempt_id)
  )`;
});
