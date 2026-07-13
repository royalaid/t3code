import {
  type GoalWorkflowEvent,
  GoalId,
  type OrchestrationV2StoredEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { EventStoreV2 } from "./EventStore.ts";
import {
  ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
  ProjectionStoreV2,
} from "./ProjectionStore.ts";
import { GoalProjectionStore, layer as goalProjectionStoreLayer } from "./GoalProjectionStore.ts";

export interface ProjectionVerificationV2 {
  readonly valid: boolean;
  readonly schemaVersion: number;
  readonly expectedSequence: number;
  readonly projectionSequence: number;
  readonly unreadableThreadIds: ReadonlyArray<ThreadId>;
  readonly missingThreadIds: ReadonlyArray<ThreadId>;
  readonly unexpectedThreadIds: ReadonlyArray<ThreadId>;
  readonly unreadableGoalIds: ReadonlyArray<GoalId>;
  readonly inconsistentGoalIds: ReadonlyArray<GoalId>;
  readonly missingGoalIds: ReadonlyArray<GoalId>;
  readonly unexpectedGoalIds: ReadonlyArray<GoalId>;
}

export class ProjectionMaintenanceError extends Schema.TaggedErrorClass<ProjectionMaintenanceError>()(
  "ProjectionMaintenanceError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ProjectionMaintenanceV2Shape {
  readonly verify: Effect.Effect<ProjectionVerificationV2, ProjectionMaintenanceError>;
  readonly rebuild: Effect.Effect<ProjectionVerificationV2, ProjectionMaintenanceError>;
}

export class ProjectionMaintenanceV2 extends Context.Service<
  ProjectionMaintenanceV2,
  ProjectionMaintenanceV2Shape
>()("t3/orchestration-v2/ProjectionMaintenance/ProjectionMaintenanceV2") {}

type ProjectionMetadataRow = {
  readonly schema_version: number;
  readonly last_sequence: number;
};

const baseLayer: Layer.Layer<
  ProjectionMaintenanceV2,
  never,
  EventStoreV2 | GoalProjectionStore | ProjectionStoreV2 | SqlClient.SqlClient
> = Layer.effect(
  ProjectionMaintenanceV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const eventStore = yield* EventStoreV2;
    const projectionStore = yield* ProjectionStoreV2;
    const goalProjectionStore = yield* GoalProjectionStore;

    const readAllEvents = Effect.gen(function* () {
      const events: Array<OrchestrationV2StoredEvent> = [];
      const pageSize = 500;
      let afterSequence = 0;
      while (true) {
        const page = yield* eventStore.read({ afterSequence, limit: pageSize }).pipe(
          Stream.runCollect,
          Effect.map((chunk) => Array.from(chunk)),
        );
        events.push(...page);
        if (page.length < pageSize) {
          break;
        }
        afterSequence = page.at(-1)?.sequence ?? afterSequence;
      }
      return events;
    });

    /**
     * EventSink commits the event, its projection updates, and projection metadata in one SQL
     * transaction. Startup verification therefore checks that transaction boundary and that every
     * stored projection can be decoded. It intentionally does not replay domain events through a
     * second projector: doing so creates another implementation of projection semantics that must
     * evolve in lockstep with ProjectionStore.
     */
    const verify = Effect.gen(function* () {
      const expectedThreadRows = yield* sql<{ readonly thread_id: string }>`
        SELECT DISTINCT stream_id AS thread_id
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND event_type = 'thread.created'
        ORDER BY stream_id ASC
      `;
      const projectionRows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id
        FROM orchestration_v2_projection_threads
        ORDER BY thread_id ASC
      `;
      const actualIds = projectionRows.map((row) => ThreadId.make(row.thread_id));
      const expectedIds = expectedThreadRows.map((row) => ThreadId.make(row.thread_id));
      const actualSet = new Set(actualIds);
      const expectedSet = new Set(expectedIds);
      const missingThreadIds = expectedIds.filter((threadId) => !actualSet.has(threadId));
      const unexpectedThreadIds = actualIds.filter((threadId) => !expectedSet.has(threadId));
      const unreadableThreadIds = (yield* Effect.forEach(
        actualIds,
        (threadId) =>
          projectionStore.getThreadProjection(threadId).pipe(
            Effect.as<ThreadId | null>(null),
            Effect.orElseSucceed((): ThreadId | null => threadId),
          ),
        { concurrency: 8 },
      )).filter((threadId): threadId is ThreadId => threadId !== null);
      const expectedGoalRows = yield* sql<{ readonly goal_id: string }>`
        SELECT DISTINCT json_extract(payload_json, '$.id') AS goal_id
        FROM orchestration_events
        WHERE application_event_version=2 AND event_type='goal.created'
        ORDER BY goal_id`;
      const goalRows = yield* sql<{ readonly goal_id: string; readonly root_thread_id: string }>`
        SELECT goal_id, root_thread_id FROM goals ORDER BY goal_id`;
      const expectedGoalIds = expectedGoalRows.map((row) => GoalId.make(row.goal_id));
      const actualGoalIds = goalRows.map((row) => GoalId.make(row.goal_id));
      const expectedGoalSet = new Set(expectedGoalIds);
      const actualGoalSet = new Set(actualGoalIds);
      const missingGoalIds = expectedGoalIds.filter((goalId) => !actualGoalSet.has(goalId));
      const unexpectedGoalIds = actualGoalIds.filter((goalId) => !expectedGoalSet.has(goalId));
      const goalChecks = yield* Effect.forEach(goalRows, (row) =>
        goalProjectionStore.getDetail(GoalId.make(row.goal_id)).pipe(
          Effect.map((detail) => {
            const current = detail.goal.currentGraphVersionId;
            const consistent =
              actualSet.has(ThreadId.make(row.root_thread_id)) &&
              ((current === null && detail.goal.currentRevision === 0) ||
                detail.graphVersions.some(
                  (graph) => graph.id === current && graph.revision === detail.goal.currentRevision,
                ));
            return { goalId: detail.goal.id, readable: true, consistent } as const;
          }),
          Effect.orElseSucceed(
            () =>
              ({
                goalId: GoalId.make(row.goal_id),
                readable: false,
                consistent: false,
              }) as const,
          ),
        ),
      );
      const unreadableGoalIds = goalChecks.filter((row) => !row.readable).map((row) => row.goalId);
      const inconsistentGoalIdSet = new Set(
        goalChecks.filter((row) => row.readable && !row.consistent).map((row) => row.goalId),
      );
      const expectedNodeRows = yield* sql<{
        readonly goal_id: string;
        readonly record_key: string;
      }>`
        SELECT json_extract(events.payload_json, '$.goalId') AS goal_id,
          json_extract(events.payload_json, '$.graph.id') || ':' || json_extract(nodes.value, '$.id') AS record_key
        FROM orchestration_events events, json_each(events.payload_json, '$.graph.nodes') nodes
        WHERE events.application_event_version=2 AND events.event_type='goal.graph-version-activated'`;
      const actualNodeRows = yield* sql<{ readonly goal_id: string; readonly record_key: string }>`
        SELECT goal_id, graph_version_id || ':' || node_id AS record_key FROM goal_nodes`;
      const actualNodeKeys = new Set(actualNodeRows.map((row) => row.record_key));
      const expectedNodeKeys = new Set(expectedNodeRows.map((row) => row.record_key));
      for (const row of expectedNodeRows)
        if (!actualNodeKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      for (const row of actualNodeRows)
        if (!expectedNodeKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      const expectedGraphRows = yield* sql<{
        readonly goal_id: string;
        readonly record_key: string;
      }>`
        SELECT json_extract(payload_json, '$.goalId') AS goal_id,
          json_extract(payload_json, '$.graph.id') AS record_key
        FROM orchestration_events
        WHERE application_event_version=2 AND event_type='goal.graph-version-activated'`;
      const actualGraphRows = yield* sql<{ readonly goal_id: string; readonly record_key: string }>`
        SELECT goal_id, graph_version_id AS record_key FROM goal_graph_versions`;
      const actualGraphKeys = new Set(actualGraphRows.map((row) => row.record_key));
      const expectedGraphKeys = new Set(expectedGraphRows.map((row) => row.record_key));
      for (const row of expectedGraphRows)
        if (!actualGraphKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      for (const row of actualGraphRows)
        if (!expectedGraphKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      const expectedEdgeRows = yield* sql<{
        readonly goal_id: string;
        readonly record_key: string;
      }>`
        SELECT json_extract(events.payload_json, '$.goalId') AS goal_id,
          json_extract(events.payload_json, '$.graph.id') || ':' || json_extract(edges.value, '$.id') AS record_key
        FROM orchestration_events events, json_each(events.payload_json, '$.graph.edges') edges
        WHERE events.application_event_version=2 AND events.event_type='goal.graph-version-activated'`;
      const actualEdgeRows = yield* sql<{ readonly goal_id: string; readonly record_key: string }>`
        SELECT versions.goal_id, edges.graph_version_id || ':' || edges.edge_id AS record_key
        FROM goal_edges edges JOIN goal_graph_versions versions USING(graph_version_id)`;
      const actualEdgeKeys = new Set(actualEdgeRows.map((row) => row.record_key));
      const expectedEdgeKeys = new Set(expectedEdgeRows.map((row) => row.record_key));
      for (const row of expectedEdgeRows)
        if (!actualEdgeKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      for (const row of actualEdgeRows)
        if (!expectedEdgeKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      const expectedRecordRows = yield* sql<{
        readonly goal_id: string;
        readonly record_key: string;
      }>`
        SELECT DISTINCT json_extract(payload_json, '$.goalId') AS goal_id,
          event_type || ':' || json_extract(payload_json, '$.id') AS record_key
        FROM orchestration_events
        WHERE application_event_version=2 AND event_type IN (
          'goal.attempt-created', 'goal.artifact-published', 'goal.evidence-submitted',
          'goal.verdict-recorded', 'goal.writer-commit-recorded', 'goal.failure-recorded'
        )`;
      const actualRecordRows = yield* sql<{
        readonly goal_id: string;
        readonly record_key: string;
      }>`
        SELECT goal_id, 'goal.attempt-created:' || attempt_id AS record_key FROM goal_attempts
        UNION ALL SELECT goal_id, 'goal.artifact-published:' || artifact_id FROM goal_artifacts
        UNION ALL SELECT goal_id, 'goal.evidence-submitted:' || evidence_id FROM goal_evidence
        UNION ALL SELECT goal_id, 'goal.writer-commit-recorded:' || writer_commit_id FROM goal_writer_commits
        UNION ALL SELECT goal_id, 'goal.failure-recorded:' || failure_id FROM goal_failures`;
      const normalizeRecordKey = (key: string) =>
        key.replace("goal.verdict-recorded:", "goal.evidence-submitted:");
      const actualRecordKeys = new Set(actualRecordRows.map((row) => row.record_key));
      const expectedRecordKeys = new Set(
        expectedRecordRows.map((row) => normalizeRecordKey(row.record_key)),
      );
      for (const row of expectedRecordRows)
        if (!actualRecordKeys.has(normalizeRecordKey(row.record_key)))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      for (const row of actualRecordRows)
        if (!expectedRecordKeys.has(row.record_key))
          inconsistentGoalIdSet.add(GoalId.make(row.goal_id));
      const inconsistentGoalIds = Array.from(inconsistentGoalIdSet).sort((a, b) =>
        String(a).localeCompare(String(b)),
      );
      const metadata = yield* sql<ProjectionMetadataRow>`
        SELECT schema_version, last_sequence
        FROM orchestration_v2_projection_metadata
        WHERE projection_name = 'thread-projections'
        LIMIT 1
      `;
      const expectedSequence = yield* eventStore.latestSequence();
      const schemaVersion = metadata[0]?.schema_version ?? 0;
      const projectionSequence = metadata[0]?.last_sequence ?? 0;
      return {
        valid:
          schemaVersion === ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION &&
          projectionSequence === expectedSequence &&
          missingThreadIds.length === 0 &&
          unexpectedThreadIds.length === 0 &&
          unreadableThreadIds.length === 0 &&
          unreadableGoalIds.length === 0 &&
          inconsistentGoalIds.length === 0 &&
          missingGoalIds.length === 0 &&
          unexpectedGoalIds.length === 0,
        schemaVersion,
        expectedSequence,
        projectionSequence,
        unreadableThreadIds,
        missingThreadIds,
        unexpectedThreadIds,
        unreadableGoalIds,
        inconsistentGoalIds,
        missingGoalIds,
        unexpectedGoalIds,
      } satisfies ProjectionVerificationV2;
    });

    const rebuild = Effect.gen(function* () {
      const events = yield* readAllEvents;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM goal_evidence`;
          yield* sql`DELETE FROM goal_failures`;
          yield* sql`DELETE FROM goal_writer_commits`;
          yield* sql`DELETE FROM goal_artifacts`;
          yield* sql`DELETE FROM goal_attempts`;
          yield* sql`DELETE FROM goal_edges`;
          yield* sql`DELETE FROM goal_nodes`;
          yield* sql`DELETE FROM goal_graph_versions`;
          yield* sql`DELETE FROM goals`;
          yield* sql`DELETE FROM orchestration_v2_projection_context_transfers`;
          yield* sql`DELETE FROM orchestration_v2_projection_context_handoffs`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoints`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoint_scopes`;
          yield* sql`DELETE FROM orchestration_v2_projection_turn_items`;
          yield* sql`DELETE FROM orchestration_v2_projection_plans`;
          yield* sql`DELETE FROM orchestration_v2_projection_messages`;
          yield* sql`DELETE FROM orchestration_v2_projection_runtime_requests`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_turns`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_threads`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_sessions`;
          yield* sql`DELETE FROM orchestration_v2_projection_subagents`;
          yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
          yield* sql`DELETE FROM orchestration_v2_projection_run_attempts`;
          yield* sql`DELETE FROM orchestration_v2_projection_runs`;
          yield* sql`DELETE FROM orchestration_v2_projection_threads`;
          yield* sql`DELETE FROM orchestration_v2_turn_item_positions`;

          for (const stored of events) {
            yield* projectionStore.apply(stored.event);
            if (stored.event.type.startsWith("goal.")) {
              yield* goalProjectionStore.applyTrustedReplay(stored.event as GoalWorkflowEvent);
            }
            if (stored.event.type === "turn-item.updated") {
              yield* sql`
                INSERT INTO orchestration_v2_turn_item_positions (
                  thread_id,
                  turn_item_id,
                  ordinal
                )
                VALUES (
                  ${stored.event.threadId},
                  ${stored.event.payload.id},
                  ${stored.event.payload.ordinal}
                )
                ON CONFLICT(thread_id, turn_item_id) DO UPDATE SET
                  ordinal = excluded.ordinal
              `;
            }
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          const lastSequence = events.at(-1)?.sequence ?? 0;
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${lastSequence},
              ${now}
            )
            ON CONFLICT(projection_name) DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }),
      );
      return yield* verify;
    });

    const mapError =
      (operation: string) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError((cause) => new ProjectionMaintenanceError({ operation, cause })),
        );

    return ProjectionMaintenanceV2.of({
      verify: mapError("verify")(verify),
      rebuild: mapError("rebuild")(rebuild),
    });
  }),
);

export const layer: Layer.Layer<
  ProjectionMaintenanceV2,
  never,
  EventStoreV2 | ProjectionStoreV2 | SqlClient.SqlClient
> = baseLayer.pipe(Layer.provide(goalProjectionStoreLayer));
