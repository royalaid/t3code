import { assert, it } from "@effect/vitest";
import {
  GoalArtifactId,
  GoalAttemptId,
  GoalEdgeId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type GoalDetail,
  type GoalGraphNode,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import { EventSinkV2 } from "./EventSink.ts";
import {
  GoalAttemptExecutionError,
  GoalAttemptExecutionService,
  buildGoalWorkerExecutionCapsule,
  goalAttemptLaunchIsFenced,
  goalAttemptWorkspaceBindingError,
  layer,
} from "./GoalAttemptExecutionService.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { layer as idAllocatorLayer } from "./IdAllocator.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import { GoalWorkspaceService } from "./GoalWorkspaceService.ts";

it("rejects integration and non-isolated writer workspace bindings", () => {
  const attemptId = GoalAttemptId.make("attempt:binding");
  const workspace = {
    path: "C:/worktrees/goal-worker",
    branch: "goal-worker/not-the-attempt",
    baseSha: "sha:base",
    sharedReadOnly: true,
  };
  const writer = { workspaceMode: "writer" } as GoalGraphNode;
  const reader = { workspaceMode: "read_only" } as GoalGraphNode;
  const integration = { workspaceMode: "integration" } as GoalGraphNode;

  assert.equal(
    goalAttemptWorkspaceBindingError({
      node: writer,
      attemptId,
      workspace,
      integrationWorktreePath: null,
    }),
    "Writer nodes must receive a mutable isolated writer workspace.",
  );
  assert.equal(
    goalAttemptWorkspaceBindingError({
      node: reader,
      attemptId,
      workspace: { ...workspace, path: "C:/worktrees/integration", sharedReadOnly: true },
      integrationWorktreePath: "C:/worktrees/integration",
    }),
    "Worker attempts must not receive the retained integration worktree.",
  );
  assert.equal(
    goalAttemptWorkspaceBindingError({
      node: integration,
      attemptId,
      workspace,
      integrationWorktreePath: null,
    }),
    "Goal integration worktrees are reserved for server-controlled assembly.",
  );
});

it("requires an actively running goal at every attempt launch fence", () => {
  assert.isTrue(
    goalAttemptLaunchIsFenced({
      goalStatus: "running",
      attemptStatus: "leased",
      nodeStatus: "running",
    }),
  );
  assert.isFalse(
    goalAttemptLaunchIsFenced({
      goalStatus: "cancelled",
      attemptStatus: "launching",
      nodeStatus: "running",
    }),
  );
  assert.isFalse(
    goalAttemptLaunchIsFenced({
      goalStatus: "running",
      attemptStatus: "cancelled",
      nodeStatus: "running",
    }),
  );
});

it("builds a canonical ancestor-only capsule from selected succeeded active-graph attempts", () => {
  const goalId = GoalId.make("goal:capsule");
  const graphVersionId = GoalGraphVersionId.make("graph:capsule");
  const writerNodeId = GoalNodeId.make("node:writer");
  const researchNodeId = GoalNodeId.make("node:research");
  const siblingNodeId = GoalNodeId.make("node:sibling");
  const verifierNodeId = GoalNodeId.make("node:verifier");
  const writerAttemptId = GoalAttemptId.make("attempt:writer:2");
  const researchAttemptId = GoalAttemptId.make("attempt:research");
  const verifierAttemptId = GoalAttemptId.make("attempt:verifier");
  const policy = {
    sandboxMode: "workspace-write" as const,
    approvalPolicy: "on-request" as const,
    writableRoots: ["/repo"],
    providerAllowlist: ["codex"],
    toolAllowlist: ["shell"],
  };
  const routeRequest = {
    type: "exact" as const,
    providerInstanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  };
  const node = (
    id: GoalNodeId,
    workspaceMode: "writer" | "read_only",
    outputKind: "commit" | "structured_result" | "verification",
  ): GoalGraphNode => ({
    id,
    role: outputKind,
    persona: "test worker",
    objective: `run ${outputKind}`,
    successCriteria: ["complete"],
    contextPacket: {
      schemaVersion: 1,
      digest: null,
      objective: `run ${outputKind}`,
      artifacts: [],
      dependencyOutputs: [],
      notes: [],
    },
    outputContract: { kind: outputKind, description: outputKind, requiredFields: [] },
    requiredCapabilities: ["tools.shell"],
    workspaceMode,
    routingRequest: routeRequest,
    evidenceRequirements:
      outputKind === "verification"
        ? [{ kind: "command", description: "check", required: true }]
        : [],
    policy,
  });
  const graphNodes = [
    node(writerNodeId, "writer", "commit"),
    node(researchNodeId, "read_only", "structured_result"),
    node(siblingNodeId, "writer", "commit"),
    node(verifierNodeId, "read_only", "verification"),
  ];
  const graph = {
    id: graphVersionId,
    goalId,
    revision: 4,
    publishedByNodeId: GoalNodeId.make("thread:root:capsule"),
    nodes: graphNodes,
    edges: [
      {
        id: GoalEdgeId.make("edge:writer-research"),
        fromNodeId: writerNodeId,
        toNodeId: researchNodeId,
      },
      {
        id: GoalEdgeId.make("edge:research-verifier"),
        fromNodeId: researchNodeId,
        toNodeId: verifierNodeId,
      },
    ],
    createdAt: "2026-07-12T00:00:00.000Z",
  };
  const resolvedRoute = {
    requested: routeRequest,
    providerInstanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
    capabilitySnapshot: ["tools.shell"],
    rationale: "exact",
  };
  const attempt = (
    id: GoalAttemptId,
    nodeId: GoalNodeId,
    ordinal: number,
    status: "succeeded" | "failed" | "launching",
  ): GoalDetail["attempts"][number] => ({
    id,
    goalId,
    graphVersionId,
    nodeId,
    ordinal,
    status,
    requestedRoute: routeRequest,
    resolvedRoute,
    providerSessionId: null,
    executionThreadId: null,
    runId: null,
    rootExecutionNodeId: null,
    baseIntegrationSha: "sha:final",
    workspacePath: null,
    leaseOwner: null,
    leaseExpiresAt: null,
    usage: {
      inputTokens: null,
      outputTokens: null,
      cachedTokens: null,
      costMicros: null,
      nativeDescendantCount: 0,
    },
    failureReason: null,
    createdAt: "2026-07-12T00:00:00.000Z",
    updatedAt: "2026-07-12T00:00:00.000Z",
  });
  const failedRetryAttemptId = GoalAttemptId.make("attempt:writer:3-failed");
  const staleWriterAttemptId = GoalAttemptId.make("attempt:writer:1");
  const siblingAttemptId = GoalAttemptId.make("attempt:sibling");
  const detail = {
    goal: {
      id: goalId,
      projectId: ProjectId.make("project:capsule"),
      objective: "test capsule",
      status: "running" as const,
      sourceThreadId: ThreadId.make("thread:source:capsule"),
      rootThreadId: ThreadId.make("thread:root:capsule"),
      policy,
      currentGraphVersionId: graphVersionId,
      currentRevision: 4,
      integrationBranch: "goal/capsule/integration",
      integrationWorktreePath: "/repo/integration",
      integrationSha: "sha:final",
      verifiedSha: null,
      createdAt: "2026-07-12T00:00:00.000Z",
      updatedAt: "2026-07-12T00:00:00.000Z",
    },
    graphVersions: [graph],
    nodes: graphNodes.map((graphNode) => ({
      goalId,
      graphVersionId,
      node: graphNode,
      status: graphNode.id === verifierNodeId ? ("running" as const) : ("succeeded" as const),
      activeAttemptId:
        graphNode.id === writerNodeId
          ? writerAttemptId
          : graphNode.id === researchNodeId
            ? researchAttemptId
            : graphNode.id === siblingNodeId
              ? siblingAttemptId
              : verifierAttemptId,
      blocker: null,
      updatedAt: "2026-07-12T00:00:00.000Z",
    })),
    attempts: [
      attempt(staleWriterAttemptId, writerNodeId, 1, "succeeded"),
      attempt(writerAttemptId, writerNodeId, 2, "succeeded"),
      attempt(failedRetryAttemptId, writerNodeId, 3, "failed"),
      attempt(researchAttemptId, researchNodeId, 1, "succeeded"),
      attempt(siblingAttemptId, siblingNodeId, 1, "succeeded"),
      {
        ...attempt(verifierAttemptId, verifierNodeId, 1, "launching"),
        workspacePath: "/repo/read-only",
      },
    ],
    artifacts: [
      {
        id: GoalArtifactId.make("artifact:writer:latest"),
        goalId,
        nodeId: writerNodeId,
        attemptId: writerAttemptId,
        kind: "commit" as const,
        uri: "git:sha:writer",
        digest: "digest:writer",
        metadata: { mustNotLeak: "secret" },
        createdAt: "2026-07-12T00:00:00.000Z",
      },
      {
        id: GoalArtifactId.make("artifact:writer:stale"),
        goalId,
        nodeId: writerNodeId,
        attemptId: staleWriterAttemptId,
        kind: "commit" as const,
        uri: "git:sha:stale",
        digest: null,
        metadata: {},
        createdAt: "2026-07-12T00:00:00.000Z",
      },
      {
        id: GoalArtifactId.make("artifact:sibling"),
        goalId,
        nodeId: siblingNodeId,
        attemptId: siblingAttemptId,
        kind: "commit" as const,
        uri: "git:sha:sibling",
        digest: null,
        metadata: {},
        createdAt: "2026-07-12T00:00:00.000Z",
      },
    ],
    evidence: [],
    writerCommits: [
      {
        id: GoalArtifactId.make("record:writer:latest"),
        goalId,
        graphVersionId,
        nodeId: writerNodeId,
        attemptId: writerAttemptId,
        baseSha: "sha:before",
        commitSha: "sha:writer",
        cleanSingleCommit: true,
        integrationBeforeSha: "sha:before",
        integrationAfterSha: "sha:final",
        state: "integrated" as const,
        createdAt: "2026-07-12T00:00:00.000Z",
        updatedAt: "2026-07-12T00:00:00.000Z",
      },
    ],
    failures: [],
  } satisfies GoalDetail;

  const { capsule } = buildGoalWorkerExecutionCapsule({
    detail,
    attemptId: verifierAttemptId,
    workspace: {
      path: "/repo/read-only",
      branch: "goal-read/shared",
      baseSha: "sha:final",
      sharedReadOnly: true,
    },
  });

  assert.deepEqual(capsule.ancestorNodeIds, [writerNodeId, researchNodeId]);
  assert.deepEqual(
    capsule.ancestorAttempts.map((selected) => selected.attemptId),
    [writerAttemptId, researchAttemptId],
  );
  assert.deepEqual(
    capsule.ancestorArtifacts.map((artifact) => artifact.id),
    [GoalArtifactId.make("artifact:writer:latest")],
  );
  assert.deepEqual(capsule.preferredProducerAttempt, {
    nodeId: writerNodeId,
    attemptId: writerAttemptId,
    integrationSha: "sha:final",
  });
  assert.isFalse("metadata" in (capsule.ancestorArtifacts[0] ?? {}));

  const captureReason = (run: () => unknown) => {
    let caught: unknown;
    try {
      run();
    } catch (error) {
      caught = error;
    }
    assert.isTrue(Schema.is(GoalAttemptExecutionError)(caught));
    if (!Schema.is(GoalAttemptExecutionError)(caught)) throw new Error("Expected execution error.");
    return caught.reason;
  };
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail: { ...detail, writerCommits: [] },
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/read-only",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "producer_context_missing",
  );
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail: {
          ...detail,
          attempts: detail.attempts.filter((candidate) => candidate.id !== researchAttemptId),
        },
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/read-only",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "ancestor_attempt_missing",
  );
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail: {
          ...detail,
          graphVersions: [
            {
              ...graph,
              nodes: graph.nodes.map((candidate) =>
                candidate.id === researchNodeId
                  ? { ...candidate, workspaceMode: "writer" as const }
                  : candidate,
              ),
            },
          ],
          writerCommits: [
            ...detail.writerCommits,
            {
              ...detail.writerCommits[0]!,
              id: GoalArtifactId.make("record:research"),
              nodeId: researchNodeId,
              attemptId: researchAttemptId,
            },
          ],
        },
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/read-only",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "producer_context_ambiguous",
  );
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail: { ...detail, goal: { ...detail.goal, currentRevision: 5 } },
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/read-only",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "stale_graph",
  );
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail,
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/a-different-read-only-path",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "workspace_binding_mismatch",
  );
  assert.equal(
    captureReason(() =>
      buildGoalWorkerExecutionCapsule({
        detail: { ...detail, goal: { ...detail.goal, integrationSha: "sha:newer" } },
        attemptId: verifierAttemptId,
        workspace: {
          path: "/repo/read-only",
          branch: "goal-read/shared",
          baseSha: "sha:final",
          sharedReadOnly: true,
        },
      }),
    ),
    "verification_sha_mismatch",
  );
});

it.effect("launches a read-only worker once and persists the durable execution binding", () =>
  Effect.gen(function* () {
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
    const committed = yield* Ref.make<ReadonlyArray<unknown>>([]);
    const threadBound = yield* Ref.make(false);
    const goalId = GoalId.make("goal:execute");
    const attemptId = GoalAttemptId.make("attempt:execute");
    const graphVersionId = GoalGraphVersionId.make("graph:execute");
    const nodeId = GoalNodeId.make("node:execute");
    const rootThreadId = ThreadId.make("thread:root:execute");
    const workerThreadId = ThreadId.make(`goal-worker:${attemptId}`);
    const policy = {
      sandboxMode: "read-only" as const,
      approvalPolicy: "untrusted" as const,
      writableRoots: [],
      providerAllowlist: ["codex"],
      toolAllowlist: ["shell"],
    };
    const routeRequest = {
      type: "exact" as const,
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    };
    const graphNode = {
      id: nodeId,
      role: "researcher",
      persona: "careful analyst",
      objective: "inspect repository",
      successCriteria: ["report findings"],
      contextPacket: {
        schemaVersion: 1,
        digest: null,
        objective: "inspect repository",
        artifacts: [],
        dependencyOutputs: [],
        notes: ["read only"],
      },
      outputContract: {
        kind: "structured_result" as const,
        description: "report",
        requiredFields: ["summary"],
      },
      requiredCapabilities: ["tools.shell"],
      workspaceMode: "read_only" as const,
      routingRequest: routeRequest,
      evidenceRequirements: [],
      policy,
    };
    const graph = {
      id: graphVersionId,
      goalId,
      revision: 1,
      publishedByNodeId: GoalNodeId.make(rootThreadId),
      nodes: [graphNode],
      edges: [],
      createdAt: "2026-07-12T00:00:00.000Z",
    };
    const detail = {
      goal: {
        id: goalId,
        projectId: ProjectId.make("project:execute"),
        objective: "inspect repository",
        status: "running" as const,
        sourceThreadId: ThreadId.make("thread:source:execute"),
        rootThreadId,
        policy,
        currentGraphVersionId: graphVersionId,
        currentRevision: 1,
        integrationBranch: null,
        integrationWorktreePath: null,
        integrationSha: "sha:base",
        verifiedSha: null,
        createdAt: "2026-07-12T00:00:00.000Z",
        updatedAt: "2026-07-12T00:00:00.000Z",
      },
      graphVersions: [graph],
      nodes: [
        {
          goalId,
          graphVersionId,
          node: graphNode,
          status: "running" as const,
          activeAttemptId: attemptId,
          blocker: null,
          updatedAt: "2026-07-12T00:00:00.000Z",
        },
      ],
      attempts: [
        {
          id: attemptId,
          goalId,
          graphVersionId,
          nodeId,
          ordinal: 1,
          status: "leased" as const,
          requestedRoute: routeRequest,
          resolvedRoute: {
            requested: routeRequest,
            providerInstanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.4",
            capabilitySnapshot: ["tools.shell"],
            rationale: "exact",
          },
          providerSessionId: null,
          executionThreadId: null,
          runId: null,
          rootExecutionNodeId: null,
          baseIntegrationSha: "sha:base",
          workspacePath: null,
          leaseOwner: "scheduler:test",
          leaseExpiresAt: "2026-07-12T00:02:00.000Z",
          usage: {
            inputTokens: null,
            outputTokens: null,
            cachedTokens: null,
            costMicros: null,
            nativeDescendantCount: 0,
          },
          failureReason: null,
          createdAt: "2026-07-12T00:00:00.000Z",
          updatedAt: "2026-07-12T00:00:00.000Z",
        },
      ],
      artifacts: [],
      evidence: [],
      writerCommits: [],
      failures: [],
    };
    const postBindingDetail = {
      ...detail,
      attempts: [
        {
          ...detail.attempts[0]!,
          status: "launching" as const,
          executionThreadId: workerThreadId,
          baseIntegrationSha: "sha:base",
          workspacePath: "C:/worktrees/goal-read",
        },
      ],
    };
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        getDetail: () =>
          Ref.get(threadBound).pipe(Effect.map((bound) => (bound ? postBindingDetail : detail))),
      }),
      Layer.mock(ThreadManagementService)({
        dispatch: (command) =>
          Effect.gen(function* () {
            if (command.type === "message.dispatch") {
              const bindings = (yield* Ref.get(committed)) as ReadonlyArray<{
                readonly events: ReadonlyArray<{
                  readonly payload: { readonly executionThreadId: string | null };
                }>;
              }>;
              assert.equal(bindings.length, 1);
              assert.equal(bindings[0]?.events[0]?.payload.executionThreadId, workerThreadId);
            }
            yield* Ref.update(commands, (current) => [...current, command]);
            return { sequence: 1, storedEvents: [], effects: [], cancelledEffectCount: 0 };
          }),
        getThreadProjection: () =>
          Effect.succeed({
            runs: [{ id: RunId.make("run:execute") }],
          } as unknown as OrchestrationV2ThreadProjection),
      }),
      Layer.mock(EventSinkV2)({
        commitGoalAttemptCommand: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(committed, (current) => [...current, input]);
            if (input.commandType === "goal.attempt.thread-bound") {
              yield* Ref.set(threadBound, true);
            }
            return { committed: true, stale: false, storedEvents: [] };
          }),
      }),
      Layer.succeed(
        GoalWorkspaceService,
        GoalWorkspaceService.of({
          provision: () => Effect.succeed(detail),
          prepareRootLead: () =>
            Effect.succeed({
              path: "C:/worktrees/goal-read",
              branch: "goal-read/root",
              baseSha: "sha:base",
              sharedReadOnly: true,
            }),
          prepareAttempt: () =>
            Effect.succeed({
              path: "C:/worktrees/goal-read",
              branch: "goal-read/execute",
              baseSha: "sha:base",
              sharedReadOnly: true,
            }),
        }),
      ),
      idAllocatorLayer,
    );
    yield* Effect.gen(function* () {
      const service = yield* GoalAttemptExecutionService;
      yield* service.launch({ goalId, attemptId });
    }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));

    const dispatched = yield* Ref.get(commands);
    assert.deepEqual(
      dispatched.map((command) => command.type),
      ["thread.create", "message.dispatch"],
    );
    assert.equal(dispatched[0]?.type, "thread.create");
    if (dispatched[0]?.type === "thread.create") {
      assert.equal(dispatched[0].threadId, workerThreadId);
      assert.equal(dispatched[0].branch, "goal-read/execute");
      assert.equal(dispatched[0].worktreePath, "C:/worktrees/goal-read");
    }
    assert.equal(dispatched[1]?.type, "message.dispatch");
    if (dispatched[1]?.type === "message.dispatch") {
      assert.include(dispatched[1].text, "graph version id: graph:execute");
      assert.include(dispatched[1].text, "attempt id: attempt:execute");
      assert.include(dispatched[1].text, "goal_node_read");
      assert.include(dispatched[1].text, "goal_result_publish");
    }
    const persisted = (yield* Ref.get(committed)) as ReadonlyArray<{
      readonly expectedStatuses: ReadonlyArray<string>;
      readonly events: ReadonlyArray<{
        readonly payload: { readonly status: string; readonly runId: string | null };
      }>;
    }>;
    assert.deepEqual(persisted[0]?.expectedStatuses, ["leased"]);
    assert.equal(persisted[0]?.events[0]?.payload.status, "launching");
    assert.isNull(persisted[0]?.events[0]?.payload.runId);
    assert.deepEqual(persisted[1]?.expectedStatuses, ["launching"]);
    assert.equal(persisted[1]?.events[0]?.payload.runId, "run:execute");
  }),
);

it.effect("does not start a provider when goal cancellation wins after the lease CAS", () =>
  Effect.gen(function* () {
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
    const cancelledState = yield* Ref.make(false);
    const prepared = yield* Ref.make(0);
    const goalId = GoalId.make("goal:launch-fence");
    const attemptId = GoalAttemptId.make("attempt:launch-fence");
    const graphVersionId = GoalGraphVersionId.make("graph:launch-fence");
    const nodeId = GoalNodeId.make("node:launch-fence");
    const rootThreadId = ThreadId.make("thread:root:launch-fence");
    const policy = {
      sandboxMode: "read-only" as const,
      approvalPolicy: "untrusted" as const,
      writableRoots: [],
      providerAllowlist: ["codex"],
      toolAllowlist: ["shell"],
    };
    const routeRequest = {
      type: "exact" as const,
      providerInstanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    };
    const workerNode = {
      id: nodeId,
      role: "researcher",
      persona: "careful analyst",
      objective: "inspect repository",
      successCriteria: ["report findings"],
      contextPacket: {
        schemaVersion: 1,
        digest: null,
        objective: "inspect repository",
        artifacts: [],
        dependencyOutputs: [],
        notes: [],
      },
      outputContract: {
        kind: "structured_result" as const,
        description: "report",
        requiredFields: ["summary"],
      },
      requiredCapabilities: ["tools.shell"],
      workspaceMode: "read_only" as const,
      routingRequest: routeRequest,
      evidenceRequirements: [],
      policy,
    };
    const attempt = {
      id: attemptId,
      goalId,
      graphVersionId,
      nodeId,
      ordinal: 1,
      status: "leased" as const,
      requestedRoute: routeRequest,
      resolvedRoute: {
        requested: routeRequest,
        providerInstanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
        capabilitySnapshot: ["tools.shell"],
        rationale: "exact",
      },
      providerSessionId: null,
      executionThreadId: null,
      runId: null,
      rootExecutionNodeId: null,
      baseIntegrationSha: "sha:base",
      workspacePath: null,
      leaseOwner: "scheduler:test",
      leaseExpiresAt: "2026-07-12T00:02:00.000Z",
      usage: {
        inputTokens: null,
        outputTokens: null,
        cachedTokens: null,
        costMicros: null,
        nativeDescendantCount: 0,
      },
      failureReason: null,
      createdAt: "2026-07-12T00:00:00.000Z",
      updatedAt: "2026-07-12T00:00:00.000Z",
    };
    const base = {
      goal: {
        id: goalId,
        projectId: ProjectId.make("project:launch-fence"),
        objective: "inspect repository",
        status: "running" as const,
        currentGraphVersionId: graphVersionId,
        rootThreadId,
        rootInteractionMode: "default",
        integrationWorktreePath: null,
      },
      attempts: [attempt],
      nodes: [
        {
          goalId,
          graphVersionId,
          node: workerNode,
          status: "running" as const,
          activeAttemptId: attemptId,
          blocker: null,
          updatedAt: "2026-07-12T00:00:00.000Z",
        },
      ],
    };
    const cancelled = {
      ...base,
      goal: { ...base.goal, status: "cancelled" as const },
      nodes: [
        {
          ...base.nodes[0],
          status: "cancelled" as const,
          blocker: "lead cancelled this node",
        },
      ],
    };
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        getDetail: () =>
          Ref.get(cancelledState).pipe(
            Effect.map((isCancelled) => (isCancelled ? cancelled : base) as never),
          ),
      }),
      Layer.mock(ThreadManagementService)({
        dispatch: (command) =>
          Ref.update(commands, (current) => [...current, command]).pipe(
            Effect.as({ sequence: 1, storedEvents: [] }),
          ),
      }),
      Layer.mock(EventSinkV2)({
        commitGoalAttemptCommand: () =>
          Ref.set(cancelledState, true).pipe(
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
      }),
      Layer.succeed(
        GoalWorkspaceService,
        GoalWorkspaceService.of({
          provision: () => Effect.succeed(base as never),
          prepareRootLead: () =>
            Effect.succeed({
              path: "C:/worktrees/goal-read",
              branch: "goal-read/root",
              baseSha: "sha:base",
              sharedReadOnly: true,
            }),
          prepareAttempt: () =>
            Ref.update(prepared, (count) => count + 1).pipe(
              Effect.as({
                path: "C:/worktrees/goal-read",
                branch: "goal-read/launch-fence",
                baseSha: "sha:base",
                sharedReadOnly: true,
              }),
            ),
        }),
      ),
      idAllocatorLayer,
    );

    yield* Effect.gen(function* () {
      const service = yield* GoalAttemptExecutionService;
      yield* service.launch({ goalId, attemptId });
    }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));

    assert.deepEqual(yield* Ref.get(commands), []);
    assert.equal(yield* Ref.get(prepared), 1);
  }),
);
