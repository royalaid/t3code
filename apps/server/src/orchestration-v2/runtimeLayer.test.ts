import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  type ApplicationStoredEvent,
  CommandId,
  MessageId,
  type ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { ServerConfig } from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStore } from "../persistence/Services/OrchestrationEventStore.ts";
import { ProjectEnrichmentService } from "../project/ProjectEnrichmentService.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { layer as mcpSessionRegistryTestLayer } from "../mcp/McpSessionRegistry.testkit.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { OrchestrationV2LayerLive } from "./runtimeLayer.ts";
import { shellStreamItemFromSnapshot } from "./ShellStream.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-runtime-layer-",
});

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const VcsDriverRegistryTestLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(NodeServices.layer),
);

const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(VcsDriverRegistryTestLayer),
);

const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  trustedInstructionDelivery: "developer_instructions",
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by lifecycle tests"),
} as ProviderAdapterV2Shape;
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test",
  },
  displayName: "Codex test",
  enabled: true,
  snapshot: {} as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

function withoutTrustedInstructionDelivery(adapter: ProviderAdapterV2Shape) {
  const { trustedInstructionDelivery, ...unsupported } = adapter;
  void trustedInstructionDelivery;
  return unsupported;
}

const unsupportedOrchestrationAdapter: ProviderAdapterV2Shape = {
  ...withoutTrustedInstructionDelivery(orchestrationAdapter),
  instanceId: ProviderInstanceId.make("unsupported-trusted-instructions"),
  driver: ProviderDriverKind.make("unsupported"),
};

const unsupportedProviderInstance = {
  ...providerInstance,
  instanceId: unsupportedOrchestrationAdapter.instanceId,
  driverKind: unsupportedOrchestrationAdapter.driver,
  continuationIdentity: {
    driverKind: unsupportedOrchestrationAdapter.driver,
    continuationKey: "unsupported:test",
  },
  displayName: "Unsupported trusted-instruction test",
  orchestrationAdapter: unsupportedOrchestrationAdapter,
} satisfies ProviderInstance;
const providerInstances: ReadonlyArray<ProviderInstance> = [
  providerInstance,
  unsupportedProviderInstance,
];

const TestProviderInstanceRegistry = Layer.succeed(ProviderInstanceRegistry, {
  getInstance: (instanceId) =>
    Effect.succeed(providerInstances.find((instance) => instance.instanceId === instanceId)),
  listInstances: Effect.succeed(providerInstances),
  listUnavailable: Effect.succeed([]),
  streamChanges: Stream.empty,
  subscribeChanges: Effect.never,
});

const TestLayer = OrchestrationV2LayerLive.pipe(
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService, {
      peek: () => Effect.succeed({ repositoryIdentity: null, faviconPath: null }),
      request: () => Effect.void,
      getAvailable: () => Effect.succeed({ repositoryIdentity: null, faviconPath: null }),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(mcpSessionRegistryTestLayer),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(NodeServices.layer),
);

const SharedApplicationDataPlaneTestLayer = Layer.merge(
  OrchestrationLayerLive,
  OrchestrationV2LayerLive,
).pipe(
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService, {
      peek: () => Effect.succeed({ repositoryIdentity: null, faviconPath: null }),
      request: () => Effect.void,
      getAvailable: () => Effect.succeed({ repositoryIdentity: null, faviconPath: null }),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(mcpSessionRegistryTestLayer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(NodeServices.layer),
);

it.layer(TestLayer)("OrchestrationV2LayerLive", (it) => {
  it.effect("creates and reads a thread through the production V2 composition", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-thread");
      const projectId = ProjectId.make("runtime-layer-project");

      const result = yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-create"),
        threadId,
        projectId,
        title: "Runtime layer thread",
        modelSelection: modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);

      assert.equal(result.sequence, 1);
      assert.equal(projection.thread.id, threadId);
      assert.equal(projection.thread.projectId, projectId);
      assert.equal(projection.thread.providerInstanceId, "codex");
      assert.deepEqual(projection.runs, []);
    }),
  );

  it.effect("persists server-created child lineage and rejects user-authored parent claims", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-child-lineage-project");
      const parentThreadId = ThreadId.make("runtime-layer-parent-thread");
      const childThreadId = ThreadId.make("runtime-layer-child-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-parent-create"),
        threadId: parentThreadId,
        projectId,
        title: "Parent thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "system",
        creationSource: "server",
        commandId: CommandId.make("runtime-layer-child-create"),
        threadId: childThreadId,
        projectId,
        title: "Child thread",
        modelSelection,
        runtimeMode: "approval-required",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        parentThreadId,
      });

      const child = yield* orchestrator.getThreadProjection(childThreadId);
      assert.deepEqual(child.thread.lineage, {
        parentThreadId,
        relationshipToParent: "subagent",
        rootThreadId: parentThreadId,
      });

      const rejected = yield* orchestrator
        .dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("runtime-layer-forged-child-create"),
          threadId: ThreadId.make("runtime-layer-forged-child-thread"),
          projectId,
          title: "Forged child thread",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          parentThreadId,
        })
        .pipe(Effect.flip);
      assert.equal(rejected._tag, "OrchestratorDispatchError");

      const crossProject = yield* orchestrator
        .dispatch({
          type: "thread.create",
          createdBy: "system",
          creationSource: "server",
          commandId: CommandId.make("runtime-layer-cross-project-child-create"),
          threadId: ThreadId.make("runtime-layer-cross-project-child-thread"),
          projectId: ProjectId.make("runtime-layer-other-project"),
          title: "Cross-project child thread",
          modelSelection,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          parentThreadId,
        })
        .pipe(Effect.flip);
      assert.equal(crossProject._tag, "OrchestratorDispatchError");
    }),
  );

  it.effect("does not overwrite a generated branch from a stale worktree sync", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-branch-race-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-branch-race-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-branch-race-project"),
        title: "Branch race thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "t3code/generated-branch-name",
        worktreePath: "/tmp/runtime-layer-branch-race",
      });

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-stale-worktree-sync"),
        threadId,
        branch: "t3code/1234abcd",
        expectedBranch: "t3code/1234abcd",
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.branch, "t3code/generated-branch-name");
    }),
  );

  it.effect("applies lifecycle commands idempotently and emits archive/removal shell deltas", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-lifecycle-thread");
      const create = {
        type: "thread.create" as const,
        createdBy: "user" as const,
        creationSource: "web" as const,
        commandId: CommandId.make("runtime-layer-lifecycle-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-lifecycle-project"),
        title: "Lifecycle thread",
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
      };

      const firstCreate = yield* orchestrator.dispatch(create);
      const retriedCreate = yield* orchestrator.dispatch(create);
      assert.equal(retriedCreate.sequence, firstCreate.sequence);
      assert.lengthOf(retriedCreate.storedEvents, 1);

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-lifecycle-metadata"),
        threadId,
        title: "Renamed lifecycle thread",
        branch: "feature/v2",
        worktreePath: "/tmp/t3-v2-worktree",
      });
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-runtime"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator.dispatch({
        type: "thread.interaction-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-interaction"),
        threadId,
        interactionMode: "plan",
      });
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-lifecycle-model"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-5.5" },
      });

      const archive = yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-lifecycle-archive"),
        threadId,
      });
      const archivedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        archivedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.include(
        archivedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromSnapshot({
          stored: archive.storedEvents[0]!,
          snapshot: archivedShell,
        }),
        {
          kind: "thread.updated",
          sequence: archive.sequence,
          location: "archive",
          thread: archivedShell.archivedThreads[0]!,
        },
      );

      const remove = yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("runtime-layer-lifecycle-delete"),
        threadId,
      });
      const deletedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        deletedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.notInclude(
        deletedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromSnapshot({ stored: remove.storedEvents[0]!, snapshot: deletedShell }),
        {
          kind: "thread.removed",
          sequence: remove.sequence,
          location: "archive",
          threadId,
        },
      );

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Renamed lifecycle thread");
      assert.equal(projection.thread.branch, "feature/v2");
      assert.equal(projection.thread.worktreePath, "/tmp/t3-v2-worktree");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.equal(projection.thread.modelSelection.model, "gpt-5.5");
      assert.isNotNull(projection.thread.archivedAt);
      assert.isNotNull(projection.thread.deletedAt);
    }),
  );

  it.effect("persists rejected command receipts across retries", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const command = {
        type: "thread.archive" as const,
        commandId: CommandId.make("runtime-layer-rejected-command"),
        threadId: ThreadId.make("runtime-layer-missing-thread"),
      };

      const first = yield* orchestrator.dispatch(command).pipe(Effect.flip);
      const retry = yield* orchestrator.dispatch(command).pipe(Effect.flip);

      assert.equal(first._tag, "OrchestratorProjectionError");
      assert.equal(retry._tag, "OrchestratorCommandPreviouslyRejectedError");
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("pending provider interruption", (it) => {
  it.effect("interrupts a pending provider start without launching provider work", () =>
    Effect.gen(function* () {
      const applicationEngine = yield* OrchestrationEngineService;
      const orchestrator = yield* OrchestratorV2;
      const threadManagement = yield* ThreadManagementService;
      const effectWorker = yield* OrchestrationEffectWorkerV2;
      const projectId = ProjectId.make("runtime-layer-pending-interrupt-project");
      const threadId = ThreadId.make("runtime-layer-pending-interrupt-thread");

      yield* applicationEngine.dispatch({
        type: "project.create",
        commandId: CommandId.make("runtime-layer-pending-interrupt-project-create"),
        projectId,
        title: "Pending interrupt project",
        workspaceRoot: "/tmp/runtime-layer-pending-interrupt-project",
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: "2026-06-22T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-create"),
        threadId,
        projectId,
        title: "Pending interrupt",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-pending-interrupt-message"),
        text: "Do not reach the provider.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const starting = yield* orchestrator.getThreadProjection(threadId);
      const run = starting.runs[0];
      assert.isDefined(run);
      assert.equal(run.status, "starting");

      const interrupt = yield* threadManagement.interruptThread({
        projectId,
        commandId: CommandId.make("runtime-layer-pending-interrupt-command"),
        threadId,
        runId: run.id,
        reason: "Cancelled before provider start",
      });
      assert.equal(interrupt.type, "interrupt_requested");

      const interrupted = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(interrupted.runs[0]?.status, "interrupted");
      assert.equal(interrupted.attempts[0]?.status, "interrupted");
      assert.equal(
        interrupted.nodes.find((node) => node.kind === "root_turn")?.status,
        "interrupted",
      );
      assert.deepEqual(
        interrupted.turnItems.filter((item) => item.runId === run.id).map((item) => item.type),
        ["user_message", "run_interrupt_request", "run_interrupt_result"],
      );
      assert.deepEqual(interrupted.providerTurns, []);
      assert.isFalse(yield* effectWorker.runOnce);
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("goal launch invariants", (it) => {
  it.effect("persists server-owned goal-root trusted instructions outside the user message", () =>
    Effect.gen(function* () {
      const applicationEngine = yield* OrchestrationEngineService;
      const orchestrator = yield* OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-goal-trusted-project");
      const sourceThreadId = ThreadId.make("runtime-layer-goal-trusted-source");
      const rootThreadId = ThreadId.make("runtime-layer-goal-trusted-root");
      yield* applicationEngine.dispatch({
        type: "project.create",
        commandId: CommandId.make("runtime-layer-goal-trusted-project-create"),
        projectId,
        title: "Goal trusted instructions project",
        workspaceRoot: process.cwd(),
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: "2026-07-13T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-goal-trusted-source-create"),
        threadId: sourceThreadId,
        projectId,
        title: "Source",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "goal.launch",
        commandId: CommandId.make("runtime-layer-goal-trusted-launch"),
        threadId: sourceThreadId,
        rootThreadId,
        objective: "Implement the trusted prompt transport",
        messageId: MessageId.make("runtime-layer-goal-trusted-goal-message"),
        attachments: [],
        selectedContextText: [],
        createdBy: "user",
        creationSource: "web",
      });
      const created = yield* orchestrator.getThreadProjection(rootThreadId);
      const goalId = created.goal!.goal.id;
      assert.deepEqual(created.thread.lineage, {
        parentThreadId: sourceThreadId,
        relationshipToParent: "subagent",
        rootThreadId: sourceThreadId,
      });
      const sourceAfterGoalLaunch = yield* orchestrator.getThreadProjection(sourceThreadId);
      assert.isNull(sourceAfterGoalLaunch.goal ?? null);
      assert.deepEqual(sourceAfterGoalLaunch.goalSurface, {
        activeGoalId: goalId,
        episodes: [
          {
            goalId,
            rootThreadId,
            objective: "Implement the trusted prompt transport",
            status: "waiting_for_source",
            currentRevision: 0,
            readyCount: 0,
            runningCount: 0,
            blockedCount: 0,
            attentionRequired: false,
            verified: false,
            createdAt: created.goal!.goal.createdAt,
            updatedAt: created.goal!.goal.updatedAt,
          },
        ],
      });
      const claimId = `goal-root-launch:${goalId}`;
      yield* orchestrator.dispatch({
        type: "goal.pending-launch.claim",
        commandId: CommandId.make("runtime-layer-goal-trusted-claim"),
        threadId: rootThreadId,
        goalId,
        claimId,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "system",
        creationSource: "server",
        commandId: CommandId.make("runtime-layer-goal-trusted-message"),
        threadId: rootThreadId,
        messageId: MessageId.make("runtime-layer-goal-trusted-message"),
        text: "UNTRUSTED_TASK_DATA",
        attachments: [],
        modelSelection,
        trustedInstructions: "TRUSTED_GOAL_ROOT_CONTRACT",
        dispatchMode: { type: "defer_start" },
        goalLaunchClaim: { goalId, claimId },
      });

      const launched = yield* orchestrator.getThreadProjection(rootThreadId);
      assert.equal(launched.runs[0]?.trustedInstructions, "TRUSTED_GOAL_ROOT_CONTRACT");
      assert.equal(launched.messages[0]?.text, "UNTRUSTED_TASK_DATA");
      assert.notInclude(launched.messages[0]?.text ?? "", "TRUSTED_GOAL_ROOT_CONTRACT");

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-goal-trusted-corrective-message"),
        threadId: rootThreadId,
        messageId: MessageId.make("runtime-layer-goal-trusted-corrective-message"),
        text: "Publish a corrective graph revision.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
      });
      const corrected = yield* orchestrator.getThreadProjection(rootThreadId);
      assert.include(corrected.runs[1]?.trustedInstructions ?? "", String(goalId));
      assert.include(corrected.runs[1]?.trustedInstructions ?? "", "immutable root lead");

      const rejected = yield* orchestrator
        .dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("runtime-layer-goal-trusted-client-rejected"),
          threadId: sourceThreadId,
          messageId: MessageId.make("runtime-layer-goal-trusted-client-rejected"),
          text: "ordinary user message",
          attachments: [],
          modelSelection,
          trustedInstructions: "CLIENT_CONTROL_PLANE_INJECTION",
          dispatchMode: { type: "start_immediately" },
        })
        .pipe(Effect.flip);
      assert.match(String(rejected.cause), /server-owned goal-root/iu);
      assert.deepEqual((yield* orchestrator.getThreadProjection(sourceThreadId)).runs, []);

      const unsupported = yield* orchestrator
        .dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("runtime-layer-goal-trusted-unsupported"),
          threadId: rootThreadId,
          messageId: MessageId.make("runtime-layer-goal-trusted-unsupported"),
          text: "UNTRUSTED_TASK_DATA",
          attachments: [],
          modelSelection: {
            instanceId: unsupportedProviderInstance.instanceId,
            model: "unsupported-model",
          },
          dispatchMode: { type: "queue_after_active" },
        })
        .pipe(Effect.flip);
      assert.equal(unsupported._tag, "OrchestratorProviderAdapterError");
      if (unsupported._tag === "OrchestratorProviderAdapterError") {
        assert.match(String(unsupported.cause), /trusted instructions/iu);
      }
      assert.lengthOf((yield* orchestrator.getThreadProjection(rootThreadId)).runs, 2);
    }),
  );

  it.effect("atomically rejects a colliding client-selected goal root id", () =>
    Effect.gen(function* () {
      const applicationEngine = yield* OrchestrationEngineService;
      const orchestrator = yield* OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-goal-project");
      const sourceThreadId = ThreadId.make("runtime-layer-goal-source");
      const rootThreadId = ThreadId.make("runtime-layer-goal-root");
      yield* applicationEngine.dispatch({
        type: "project.create",
        commandId: CommandId.make("runtime-layer-goal-project-create"),
        projectId,
        title: "Goal project",
        workspaceRoot: process.cwd(),
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: "2026-07-12T00:00:00.000Z",
      });
      for (const [threadId, commandId] of [
        [sourceThreadId, "runtime-layer-goal-source-create"],
        [rootThreadId, "runtime-layer-goal-collision-create"],
      ] as const) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(commandId),
          threadId,
          projectId,
          title: String(threadId),
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: process.cwd(),
        });
      }
      const failed = yield* orchestrator
        .dispatch({
          type: "goal.launch",
          commandId: CommandId.make("runtime-layer-goal-launch-collision"),
          threadId: sourceThreadId,
          rootThreadId,
          objective: "Do the work",
          messageId: MessageId.make("runtime-layer-goal-message"),
          attachments: [],
          selectedContextText: [],
          createdBy: "user",
          creationSource: "web",
        })
        .pipe(Effect.flip);
      assert.match(String(failed), /root.*already exists|collision/iu);
      const source = yield* orchestrator.getThreadProjection(sourceThreadId);
      assert.isNull(source.goal ?? null);
    }),
  );

  it.effect("keeps a cancelled pending goal from launching after its source run settles", () =>
    Effect.gen(function* () {
      const applicationEngine = yield* OrchestrationEngineService;
      const orchestrator = yield* OrchestratorV2;
      const threadManagement = yield* ThreadManagementService;
      const projectId = ProjectId.make("runtime-layer-goal-cancel-project");
      const sourceThreadId = ThreadId.make("runtime-layer-goal-cancel-source");
      const rootThreadId = ThreadId.make("runtime-layer-goal-cancel-root");
      yield* applicationEngine.dispatch({
        type: "project.create",
        commandId: CommandId.make("runtime-layer-goal-cancel-project-create"),
        projectId,
        title: "Goal cancel project",
        workspaceRoot: process.cwd(),
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: "2026-07-12T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-goal-cancel-source-create"),
        threadId: sourceThreadId,
        projectId,
        title: "Source",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-goal-cancel-source-run"),
        threadId: sourceThreadId,
        messageId: MessageId.make("runtime-layer-goal-cancel-source-message"),
        text: "Source work",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const source = yield* orchestrator.getThreadProjection(sourceThreadId);
      const sourceRun = source.runs[0]!;
      yield* orchestrator.dispatch({
        type: "goal.launch",
        commandId: CommandId.make("runtime-layer-goal-cancel-launch"),
        threadId: sourceThreadId,
        rootThreadId,
        objective: "Never launch after cancel",
        messageId: MessageId.make("runtime-layer-goal-cancel-goal-message"),
        attachments: [],
        selectedContextText: [],
        createdBy: "user",
        creationSource: "web",
      });
      const root = yield* orchestrator.getThreadProjection(rootThreadId);
      yield* orchestrator.dispatch({
        type: "goal.pending-launch.cancel",
        commandId: CommandId.make("runtime-layer-goal-cancel-command"),
        threadId: rootThreadId,
        goalId: root.goal!.goal.id,
      });
      const sourceAfterGoalCancel = yield* orchestrator.getThreadProjection(sourceThreadId);
      assert.equal(sourceAfterGoalCancel.runs[0]?.status, "starting");
      assert.deepEqual(sourceAfterGoalCancel.providerSessions, []);
      const staleClaimStart = yield* orchestrator
        .dispatch({
          type: "message.dispatch",
          createdBy: "system",
          creationSource: "server",
          commandId: CommandId.make("runtime-layer-goal-cancel-stale-claim-start"),
          threadId: rootThreadId,
          messageId: MessageId.make("runtime-layer-goal-cancel-stale-claim-message"),
          text: "must not start",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "defer_start" },
          goalLaunchClaim: {
            goalId: root.goal!.goal.id,
            claimId: `goal-root-launch:${root.goal!.goal.id}`,
          },
        })
        .pipe(Effect.flip);
      assert.match(String(staleClaimStart), /claim|provisioning|cancel/iu);
      const staleClaimFailure = yield* orchestrator
        .dispatch({
          type: "goal.pending-launch.fail",
          commandId: CommandId.make("runtime-layer-goal-cancel-stale-claim-fail"),
          threadId: rootThreadId,
          goalId: root.goal!.goal.id,
          claimId: `goal-root-launch:${root.goal!.goal.id}`,
          detail: "stale launch claim rejected after cancellation",
        })
        .pipe(Effect.flip);
      assert.match(String(staleClaimFailure), /no domain events|failed to dispatch/iu);
      const afterStaleFailure = yield* orchestrator.getThreadProjection(rootThreadId);
      assert.equal(afterStaleFailure.goal?.goal.status, "cancelled");
      yield* threadManagement.interruptThread({
        projectId,
        commandId: CommandId.make("runtime-layer-goal-cancel-source-interrupt"),
        threadId: sourceThreadId,
        runId: sourceRun.id,
        reason: "settle source after goal cancellation",
      });
      yield* Effect.yieldNow;
      const cancelled = yield* orchestrator.getThreadProjection(rootThreadId);
      assert.equal(cancelled.goal?.goal.status, "cancelled");
      assert.deepEqual(cancelled.runs, []);
      assert.deepEqual(cancelled.providerSessions, []);
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("shared application data plane", (it) => {
  it.effect("orders retained project transactions and V2 thread transactions in one source", () =>
    Effect.gen(function* () {
      const applicationEngine = yield* OrchestrationEngineService;
      const applicationEvents = yield* OrchestrationEventStore;
      const orchestrator = yield* OrchestratorV2;
      const projectionSnapshot = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("runtime-layer-shared-project");
      const threadId = ThreadId.make("runtime-layer-shared-thread");
      const projectCommand = {
        type: "project.create" as const,
        commandId: CommandId.make("runtime-layer-shared-project-create"),
        projectId,
        title: "Shared application source",
        workspaceRoot: "/tmp/runtime-layer-shared-project",
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: "2026-06-20T00:00:00.000Z",
      };

      const projectResult = yield* applicationEngine.dispatch(projectCommand);
      const projectRetry = yield* applicationEngine.dispatch(projectCommand);
      assert.equal(projectRetry.sequence, projectResult.sequence);

      const delivered = yield* Queue.unbounded<ApplicationStoredEvent>();
      yield* applicationEvents.streamApplicationEvents().pipe(
        Stream.take(2),
        Stream.runForEach((event) => Queue.offer(delivered, event)),
        Effect.forkScoped,
      );

      const projectEvent = yield* Queue.take(delivered);
      assert.equal(projectEvent.sequence, projectResult.sequence);

      const threadResult = yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-shared-thread-create"),
        threadId,
        projectId,
        title: "Shared thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const threadEvent = yield* Queue.take(delivered);

      assert.equal(threadEvent.sequence, threadResult.sequence);
      assert.isAbove(threadEvent.sequence, projectEvent.sequence);
      assert.isTrue("aggregateKind" in projectEvent);
      assert.isTrue("event" in threadEvent);
      assert.equal((yield* projectionSnapshot.getProjectShellById(projectId))._tag, "Some");

      const retainedReceipts = yield* sql<{
        readonly aggregate_kind: string;
        readonly aggregate_id: string;
      }>`
        SELECT aggregate_kind, aggregate_id
        FROM orchestration_command_receipts
        ORDER BY result_sequence ASC
      `;
      assert.deepEqual(retainedReceipts, [
        { aggregate_kind: "project", aggregate_id: projectId },
        { aggregate_kind: "thread", aggregate_id: threadId },
      ]);

      const retiredWrites = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_v2_events) +
          (SELECT COUNT(*) FROM orchestration_v2_command_receipts) AS count
      `;
      assert.equal(retiredWrites[0]?.count, 0);
    }),
  );
});
