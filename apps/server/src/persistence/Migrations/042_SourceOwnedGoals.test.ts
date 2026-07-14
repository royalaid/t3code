import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()))("042_SourceOwnedGoals", (it) => {
  it.effect("enforces at most one nonterminal goal per source thread", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 42 });
      const insert = (goalId: string, status: string) =>
        sql`INSERT INTO goals (
          goal_id, root_thread_id, source_thread_id, status, current_revision,
          payload_json, created_at, updated_at
        ) VALUES (${goalId}, ${`root:${goalId}`}, 'source:one', ${status}, 0, '{}',
          '2026-07-14T00:00:00.000Z', '2026-07-14T00:00:00.000Z')`;

      yield* insert("goal:first", "planning");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(insert("goal:concurrent", "running"))));
      yield* sql`UPDATE goals SET status='completed' WHERE goal_id='goal:first'`;
      yield* insert("goal:second", "planning");
    }),
  );
});
