import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE UNIQUE INDEX idx_goals_one_active_per_source
    ON goals(source_thread_id)
    WHERE status NOT IN ('completed', 'failed', 'cancelled')`;
});
