import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Legacy builds allowed several active goals to share a source. Preserve
  // every episode, but retire older duplicates before installing the new
  // transactional invariant. The most recently updated episode remains the
  // active conversational target.
  yield* sql`WITH ranked_nonterminal_goals AS (
    SELECT goal_id,
      ROW_NUMBER() OVER (
        PARTITION BY source_thread_id
        ORDER BY updated_at DESC, created_at DESC, goal_id DESC
      ) AS source_rank
    FROM goals
    WHERE status NOT IN ('completed', 'failed', 'cancelled')
  )
  UPDATE goals
  SET status = 'failed',
      payload_json = json_set(payload_json, '$.status', 'failed')
  WHERE goal_id IN (
    SELECT goal_id FROM ranked_nonterminal_goals WHERE source_rank > 1
  )`;
  yield* sql`CREATE UNIQUE INDEX idx_goals_one_active_per_source
    ON goals(source_thread_id)
    WHERE status NOT IN ('completed', 'failed', 'cancelled')`;
});
