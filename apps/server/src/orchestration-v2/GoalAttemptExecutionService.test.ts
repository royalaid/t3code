import { assert, it } from "@effect/vitest";
import {
  GoalAttemptId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type GoalGraphNode,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { EventSinkV2 } from "./EventSink.ts";
import {
  GoalAttemptExecutionService,
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

it.effect("launches a read-only worker once and persists the durable execution binding", () =>
  Effect.gen(function* () {
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
    const committed = yield* Ref.make<ReadonlyArray<unknown>>([]);
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
      graphVersions: [],
      nodes: [
        {
          goalId,
          graphVersionId,
          node: {
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
          },
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
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({ getDetail: () => Effect.succeed(detail) }),
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
          Ref.update(committed, (current) => [...current, input]).pipe(
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
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
    const reads = yield* Ref.make(0);
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
          Ref.getAndUpdate(reads, (count) => count + 1).pipe(
            // The first read opens the effect; two fences cover the workspace
            // side effect. Cancellation lands after the durable
            // lease-to-launching CAS but before a worker thread/message can
            // start provider work.
            Effect.map((count) => (count < 3 ? base : cancelled) as never),
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
          Effect.succeed({ committed: true, stale: false, storedEvents: [] }),
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
