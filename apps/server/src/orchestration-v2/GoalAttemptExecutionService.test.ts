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
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { EventSinkV2 } from "./EventSink.ts";
import { GoalAttemptExecutionService, layer } from "./GoalAttemptExecutionService.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { layer as idAllocatorLayer } from "./IdAllocator.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

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
