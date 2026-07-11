import { assert, it } from "@effect/vitest";
import {
  GoalArtifactId,
  GoalAttemptId,
  GoalEvidenceId,
  GoalGraphVersionId,
  GoalEdgeId,
  GoalId,
  GoalNodeId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import {
  GoalProjectionStore,
  GoalProjectionValidationError,
  layer as goalProjectionStoreLayer,
} from "./GoalProjectionStore.ts";

const TestLayer = goalProjectionStoreLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory));
const policy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["shell"],
};
const makeNode = (id: string) => ({
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
  outputContract: { kind: "structured_result" as const, description: "report", requiredFields: [] },
  requiredCapabilities: ["tools"],
  workspaceMode: "read_only" as const,
  routingRequest: {
    type: "requirements" as const,
    capabilities: ["tools"],
    latencyClass: "standard" as const,
    costClass: "standard" as const,
  },
  evidenceRequirements: [],
  policy,
});

it.layer(TestLayer)("GoalProjectionStore", (it) => {
  it.effect("activates immutable graph versions with revision CAS and exact replay", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:projection");
      const rootThreadId = ThreadId.make("thread:goal-root");
      yield* store.create({
        id: goalId,
        objective: "ship it",
        status: "planning",
        sourceThreadId: ThreadId.make("thread:source"),
        policy,
        rootThreadId,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/projection",
        integrationWorktreePath: "/repo-goal",
        integrationSha: "abc",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const graph = {
        id: GoalGraphVersionId.make("goal-graph:1"),
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("node:lead"),
        nodes: [makeNode("node:lead"), makeNode("node:worker")],
        edges: [
          {
            id: GoalEdgeId.make("edge:1"),
            fromNodeId: GoalNodeId.make("node:lead"),
            toNodeId: GoalNodeId.make("node:worker"),
          },
        ],
        createdAt: "2026-07-11T00:00:01.000Z",
      } as const;
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      const replayed = yield* store.getDetail(goalId);
      assert.equal(replayed.goal.currentRevision, 1);
      assert.deepEqual(replayed.graphVersions, [graph]);
      const stale = yield* Effect.flip(
        store.activateGraph({
          goalId,
          expectedRevision: 0,
          graph: { ...graph, id: GoalGraphVersionId.make("goal-graph:2"), revision: 2 },
        }),
      );
      assert.instanceOf(stale, GoalProjectionValidationError);
      assert.equal(stale.reason, "stale_revision");
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 1);
    }),
  );

  it.effect("rejects cycles, missing dependencies, duplicates, and policy expansion", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:validation");
      yield* store.create({
        id: goalId,
        objective: "validate",
        status: "planning",
        sourceThreadId: ThreadId.make("source"),
        rootThreadId: ThreadId.make("root"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/validation",
        integrationWorktreePath: "/goal",
        integrationSha: "abc",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const base = {
        id: GoalGraphVersionId.make("graph:bad"),
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("a"),
        createdAt: "2026-07-11T00:00:00.000Z",
      };
      const cases = [
        { nodes: [makeNode("a"), makeNode("a")], edges: [] },
        {
          nodes: [makeNode("a")],
          edges: [
            {
              id: GoalEdgeId.make("e"),
              fromNodeId: GoalNodeId.make("missing"),
              toNodeId: GoalNodeId.make("a"),
            },
          ],
        },
        {
          nodes: [makeNode("a"), makeNode("b")],
          edges: [
            {
              id: GoalEdgeId.make("e1"),
              fromNodeId: GoalNodeId.make("a"),
              toNodeId: GoalNodeId.make("b"),
            },
            {
              id: GoalEdgeId.make("e2"),
              fromNodeId: GoalNodeId.make("b"),
              toNodeId: GoalNodeId.make("a"),
            },
          ],
        },
        {
          nodes: [
            { ...makeNode("a"), policy: { ...policy, writableRoots: ["/repo", "/outside"] } },
          ],
          edges: [],
        },
      ];
      for (const graph of cases) {
        assert.instanceOf(
          yield* Effect.flip(
            store.activateGraph({ goalId, expectedRevision: 0, graph: { ...base, ...graph } }),
          ),
          GoalProjectionValidationError,
        );
      }
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 0);
    }),
  );

  it.effect("treats Windows roots case-insensitively with either separator", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:windows-policy");
      const windowsPolicy = { ...policy, writableRoots: ["C:\\Repo"] };
      yield* store.create({
        id: goalId,
        objective: "windows",
        status: "planning",
        sourceThreadId: ThreadId.make("source:windows"),
        rootThreadId: ThreadId.make("root:windows"),
        policy: windowsPolicy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const graph = {
        id: GoalGraphVersionId.make("graph:windows"),
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("external-root-lead"),
        nodes: [
          {
            ...makeNode("worker:windows"),
            policy: { ...windowsPolicy, writableRoots: ["c:/repo/packages"] },
          },
        ],
        edges: [],
        createdAt: "2026-07-11T00:00:01.000Z",
      } as const;
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      assert.equal((yield* store.getDetail(goalId)).goal.currentRevision, 1);
    }),
  );

  it.effect("does not confuse Windows path prefixes with containment", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:windows-prefix");
      const windowsPolicy = { ...policy, writableRoots: ["C:\\Repo"] };
      yield* store.create({
        id: goalId,
        objective: "windows",
        status: "planning",
        sourceThreadId: ThreadId.make("source:prefix"),
        rootThreadId: ThreadId.make("root:prefix"),
        policy: windowsPolicy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      for (const [index, writableRoot] of ["C:\\Repository", "C:\\Repo\\..\\outside"].entries()) {
        const error = yield* Effect.flip(
          store.activateGraph({
            goalId,
            expectedRevision: 0,
            graph: {
              id: GoalGraphVersionId.make(`graph:prefix:${index}`),
              goalId,
              revision: 1,
              publishedByNodeId: GoalNodeId.make("lead"),
              nodes: [
                {
                  ...makeNode("worker:prefix"),
                  policy: { ...windowsPolicy, writableRoots: [writableRoot] },
                },
              ],
              edges: [],
              createdAt: "2026-07-11T00:00:01.000Z",
            },
          }),
        );
        assert.equal(error.reason, "policy_expansion");
      }
    }),
  );

  it.effect("rejects POSIX traversal outside an allowed root", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:posix-traversal");
      yield* store.create({
        id: goalId,
        objective: "posix",
        status: "planning",
        sourceThreadId: ThreadId.make("source:posix"),
        rootThreadId: ThreadId.make("root:posix"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const error = yield* Effect.flip(
        store.activateGraph({
          goalId,
          expectedRevision: 0,
          graph: {
            id: GoalGraphVersionId.make("graph:posix-traversal"),
            goalId,
            revision: 1,
            publishedByNodeId: GoalNodeId.make("lead"),
            nodes: [
              {
                ...makeNode("worker:posix"),
                policy: { ...policy, writableRoots: ["/repo/../outside"] },
              },
            ],
            edges: [],
            createdAt: "2026-07-11T00:00:01.000Z",
          },
        }),
      );
      assert.equal(error.reason, "policy_expansion");
    }),
  );

  it.effect("claims graph revisions once under concurrent CAS", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:concurrent-cas");
      yield* store.create({
        id: goalId,
        objective: "cas",
        status: "planning",
        sourceThreadId: ThreadId.make("source:cas"),
        rootThreadId: ThreadId.make("root:cas"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: null,
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const makeGraph = (suffix: string) =>
        ({
          id: GoalGraphVersionId.make(`graph:cas:${suffix}`),
          goalId,
          revision: 1,
          publishedByNodeId: GoalNodeId.make("lead"),
          nodes: [makeNode(`worker:${suffix}`)],
          edges: [],
          createdAt: "2026-07-11T00:00:01.000Z",
        }) as const;
      const exits = yield* Effect.all(
        [makeGraph("a"), makeGraph("b")].map((graph) =>
          Effect.exit(store.activateGraph({ goalId, expectedRevision: 0, graph })),
        ),
        { concurrency: "unbounded" },
      );
      assert.equal(exits.filter(Exit.isSuccess).length, 1);
      assert.equal(exits.filter(Exit.isFailure).length, 1);
      assert.equal((yield* store.getDetail(goalId)).graphVersions.length, 1);
    }),
  );

  it.effect(
    "preserves aggregate identity and invalidates verification on integration changes",
    () =>
      Effect.gen(function* () {
        const store = yield* GoalProjectionStore;
        const goalId = GoalId.make("goal:lifecycle-invariants");
        const original = {
          id: goalId,
          objective: "original",
          status: "completed" as const,
          sourceThreadId: ThreadId.make("source:original"),
          rootThreadId: ThreadId.make("root:original"),
          policy,
          currentGraphVersionId: null,
          currentRevision: 0,
          integrationBranch: "goal/original",
          integrationWorktreePath: "/goal",
          integrationSha: "sha:old",
          verifiedSha: "sha:old",
          createdAt: "2026-07-11T00:00:00.000Z",
          updatedAt: "2026-07-11T00:00:00.000Z",
        };
        yield* store.create(original);
        assert.equal(
          (yield* Effect.flip(
            store.create({ ...original, rootThreadId: ThreadId.make("root:conflict") }),
          )).reason,
          "referential_integrity",
        );
        yield* store.apply({
          type: "goal.integration-updated",
          payload: {
            ...original,
            sourceThreadId: ThreadId.make("source:malicious"),
            rootThreadId: ThreadId.make("root:malicious"),
            policy: { ...policy, writableRoots: ["/outside"] },
            currentRevision: 99,
            integrationSha: "sha:new",
            verifiedSha: "sha:new",
            updatedAt: "2026-07-11T00:01:00.000Z",
          },
        });
        const persisted = (yield* store.getDetail(goalId)).goal;
        assert.equal(persisted.sourceThreadId, original.sourceThreadId);
        assert.equal(persisted.rootThreadId, original.rootThreadId);
        assert.deepEqual(persisted.policy, policy);
        assert.equal(persisted.currentRevision, 0);
        assert.isNull(persisted.verifiedSha);
      }),
  );

  it.effect("persists graph-owned attempts, writer commits, and structured failures", () =>
    Effect.gen(function* () {
      const store = yield* GoalProjectionStore;
      const goalId = GoalId.make("goal:records");
      yield* store.create({
        id: goalId,
        objective: "records",
        status: "planning",
        sourceThreadId: ThreadId.make("source:records"),
        rootThreadId: ThreadId.make("root:records"),
        policy,
        currentGraphVersionId: null,
        currentRevision: 0,
        integrationBranch: "goal/records",
        integrationWorktreePath: "/records",
        integrationSha: "sha:base",
        verifiedSha: null,
        createdAt: "2026-07-11T00:00:00.000Z",
        updatedAt: "2026-07-11T00:00:00.000Z",
      });
      const node = makeNode("worker:records");
      const graph = {
        id: GoalGraphVersionId.make("graph:records"),
        goalId,
        revision: 1,
        publishedByNodeId: GoalNodeId.make("lead"),
        nodes: [node],
        edges: [],
        createdAt: "2026-07-11T00:00:01.000Z",
      } as const;
      yield* store.activateGraph({ goalId, expectedRevision: 0, graph });
      const attemptId = GoalAttemptId.make("attempt:records");
      const attempt = {
        id: attemptId,
        goalId,
        graphVersionId: graph.id,
        nodeId: node.id,
        ordinal: 1,
        status: "running" as const,
        requestedRoute: node.routingRequest,
        resolvedRoute: null,
        providerSessionId: null,
        executionThreadId: ThreadId.make("thread:attempt:records"),
        runId: null,
        rootExecutionNodeId: null,
        baseIntegrationSha: "sha:base",
        workspacePath: "/records-worker",
        leaseOwner: "worker",
        leaseExpiresAt: "2026-07-11T00:10:00.000Z",
        usage: {
          inputTokens: null,
          outputTokens: null,
          cachedTokens: null,
          costMicros: null,
          nativeDescendantCount: 0,
        },
        failureReason: null,
        createdAt: "2026-07-11T00:00:02.000Z",
        updatedAt: "2026-07-11T00:00:02.000Z",
      };
      const dangling = yield* Effect.flip(
        store.apply({
          type: "goal.attempt-created",
          payload: { ...attempt, graphVersionId: GoalGraphVersionId.make("graph:missing") },
        }),
      );
      assert.equal(dangling.reason, "referential_integrity");
      yield* store.apply({ type: "goal.attempt-created", payload: attempt });
      assert.equal(
        (yield* Effect.flip(
          store.apply({ type: "goal.attempt-created", payload: { ...attempt, ordinal: 2 } }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({
        type: "goal.writer-commit-recorded",
        payload: {
          id: GoalArtifactId.make("commit-record:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          baseSha: "sha:base",
          commitSha: "sha:commit",
          cleanSingleCommit: true,
          integrationBeforeSha: "sha:base",
          integrationAfterSha: "sha:after",
          state: "integrated",
          createdAt: "2026-07-11T00:00:03.000Z",
          updatedAt: "2026-07-11T00:00:04.000Z",
        },
      });
      yield* store.apply({
        type: "goal.failure-recorded",
        payload: {
          id: GoalEvidenceId.make("failure:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          reason: {
            type: "integration_conflict",
            artifactId: GoalArtifactId.make("commit-record:1"),
            detail: "conflict",
          },
          recoveryState: "unresolved",
          blocker: "resolve conflict",
          occurredAt: "2026-07-11T00:00:05.000Z",
        },
      });
      const artifact = {
        id: GoalArtifactId.make("artifact:records"),
        goalId,
        nodeId: node.id,
        attemptId,
        kind: "result" as const,
        uri: "artifact://result",
        digest: "digest:1",
        metadata: {},
        createdAt: "2026-07-11T00:00:05.000Z",
      };
      yield* store.apply({ type: "goal.artifact-published", payload: artifact });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.artifact-published",
            payload: { ...artifact, uri: "artifact://different" },
          }),
        )).reason,
        "referential_integrity",
      );
      const evidence = {
        id: GoalEvidenceId.make("evidence:records"),
        goalId,
        nodeId: node.id,
        attemptId,
        integrationSha: "sha:after",
        producerAttemptId: attemptId,
        commands: [],
        artifacts: [artifact.id],
        verdict: "inconclusive" as const,
        summary: "pending",
        createdAt: "2026-07-11T00:00:06.000Z",
      };
      yield* store.apply({ type: "goal.evidence-submitted", payload: evidence });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.verdict-recorded",
            payload: { ...evidence, integrationSha: "sha:different", verdict: "accepted" },
          }),
        )).reason,
        "referential_integrity",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.writer-commit-recorded",
            payload: {
              id: GoalArtifactId.make("commit-record:1"),
              goalId,
              graphVersionId: graph.id,
              nodeId: node.id,
              attemptId,
              baseSha: "sha:base",
              commitSha: "sha:different",
              cleanSingleCommit: true,
              integrationBeforeSha: "sha:base",
              integrationAfterSha: "sha:after",
              state: "integrated",
              createdAt: "2026-07-11T00:00:03.000Z",
              updatedAt: "2026-07-11T00:00:07.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.failure-recorded",
            payload: {
              id: GoalEvidenceId.make("failure:1"),
              goalId,
              graphVersionId: graph.id,
              nodeId: node.id,
              attemptId,
              reason: { type: "dependency_failure", dependencyNodeId: node.id },
              recoveryState: "resolved",
              blocker: null,
              occurredAt: "2026-07-11T00:00:05.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      yield* store.apply({
        type: "goal.verdict-recorded",
        payload: { ...evidence, verdict: "accepted" },
      });
      yield* store.apply({
        type: "goal.writer-commit-recorded",
        payload: {
          id: GoalArtifactId.make("commit-record:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          baseSha: "sha:base",
          commitSha: "sha:commit",
          cleanSingleCommit: true,
          integrationBeforeSha: "sha:base",
          integrationAfterSha: null,
          state: "conflicted",
          createdAt: "2026-07-11T00:00:03.000Z",
          updatedAt: "2026-07-11T00:00:09.000Z",
        },
      });
      yield* store.apply({
        type: "goal.failure-recorded",
        payload: {
          id: GoalEvidenceId.make("failure:1"),
          goalId,
          graphVersionId: graph.id,
          nodeId: node.id,
          attemptId,
          reason: {
            type: "integration_conflict",
            artifactId: GoalArtifactId.make("commit-record:1"),
            detail: "conflict",
          },
          recoveryState: "resolved",
          blocker: null,
          occurredAt: "2026-07-11T00:00:05.000Z",
        },
      });
      assert.equal(
        (yield* Effect.flip(
          store.apply({
            type: "goal.failure-recorded",
            payload: {
              id: GoalEvidenceId.make("failure:bad-owner"),
              goalId,
              graphVersionId: graph.id,
              nodeId: GoalNodeId.make("node:other"),
              attemptId: null,
              reason: { type: "dependency_failure", dependencyNodeId: node.id },
              recoveryState: "unresolved",
              blocker: null,
              occurredAt: "2026-07-11T00:00:08.000Z",
            },
          }),
        )).reason,
        "referential_integrity",
      );
      const detail = yield* store.getDetail(goalId);
      assert.equal(detail.attempts[0]?.graphVersionId, graph.id);
      assert.equal(detail.attempts[0]?.executionThreadId, ThreadId.make("thread:attempt:records"));
      assert.equal(detail.writerCommits[0]?.state, "conflicted");
      assert.equal(detail.failures[0]?.recoveryState, "resolved");
      assert.equal(detail.artifacts[0]?.uri, artifact.uri);
      assert.equal(detail.evidence[0]?.integrationSha, evidence.integrationSha);
      assert.equal(detail.evidence[0]?.verdict, "accepted");
    }),
  );
});
