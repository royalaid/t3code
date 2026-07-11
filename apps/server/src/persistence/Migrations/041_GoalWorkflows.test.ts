import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()))("041_GoalWorkflows", (it) => {
  it.effect("adds durable immutable graph and execution projection tables", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 40 });
      yield* runMigrations({ toMigrationInclusive: 41 });
      const rows = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'goal_%'
        ORDER BY name
      `;
      assert.deepEqual(
        rows.map((row) => row.name),
        [
          "goal_artifacts",
          "goal_attempts",
          "goal_edges",
          "goal_evidence",
          "goal_failures",
          "goal_graph_versions",
          "goal_nodes",
          "goal_writer_commits",
          "goals",
        ],
      );
      const attemptColumns = yield* sql<{
        readonly name: string;
        readonly notnull: number;
      }>`PRAGMA table_info(goal_attempts)`;
      assert.includeMembers(
        attemptColumns.map((column) => column.name),
        ["graph_version_id", "execution_thread_id", "run_id", "root_execution_node_id"],
      );
      const goalColumns = yield* sql<{
        readonly name: string;
        readonly notnull: number;
      }>`PRAGMA table_info(goals)`;
      assert.equal(goalColumns.find((column) => column.name === "integration_sha")?.notnull, 0);
      const foreignKeys = yield* sql<{
        readonly table: string;
      }>`PRAGMA foreign_key_list(goal_attempts)`;
      assert.includeMembers(
        foreignKeys.map((row) => row.table),
        ["goal_nodes", "goals"],
      );
    }),
  );
});
