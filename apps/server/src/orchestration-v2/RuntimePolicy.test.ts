import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  type OrchestrationV2AppThread,
  type GoalWorkflowPolicy,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectionProjects from "../persistence/Services/ProjectionProjects.ts";
import {
  isGoalRuntimePolicyResolveError,
  layerFromProjectRepository,
  resolveGoalWorkerRuntimePolicy,
  RuntimePolicyV2,
} from "./RuntimePolicy.ts";

const projectId = ProjectId.make("project:runtime-policy");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.5",
} satisfies ModelSelection;

const rootGoalPolicy = {
  sandboxMode: "danger-full-access",
  approvalPolicy: "never",
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
} satisfies GoalWorkflowPolicy;

const writerPolicy = {
  sandboxMode: "workspace-write",
  approvalPolicy: "untrusted",
  writableRoots: ["/repo/packages/goal"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
} satisfies GoalWorkflowPolicy;

const readerPolicy = {
  sandboxMode: "read-only",
  approvalPolicy: "untrusted",
  writableRoots: [],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
} satisfies GoalWorkflowPolicy;

const unrestrictedRuntimePolicy = {
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  cwd: "/repo/.t3/goals/worker",
};

function makeThread(input: {
  readonly now: DateTime.Utc;
  readonly worktreePath: string | null;
}): OrchestrationV2AppThread {
  const threadId = ThreadId.make("thread:runtime-policy");
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Runtime policy",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.worktreePath,
    activeProviderThreadId: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
    archivedAt: null,
    deletedAt: null,
  };
}

const TestLayer = layerFromProjectRepository.pipe(
  Layer.provide(
    Layer.mock(ProjectionProjects.ProjectionProjectRepository)({
      getById: () =>
        Effect.succeed(
          Option.some({
            projectId,
            title: "Project",
            workspaceRoot: "/project-root",
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-06-21T00:00:00.000Z",
            updatedAt: "2026-06-21T00:00:00.000Z",
            deletedAt: null,
          }),
        ),
    }),
  ),
);

it.layer(TestLayer)("RuntimePolicyV2", (it) => {
  it.effect("uses the project root for local-checkout threads", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicyV2;
      const now = yield* DateTime.now;
      const resolved = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: null }),
        modelSelection,
      });
      assert.equal(resolved.cwd, "/project-root");
    }),
  );

  it.effect("prefers a provisioned worktree over the project root", () =>
    Effect.gen(function* () {
      const policy = yield* RuntimePolicyV2;
      const now = yield* DateTime.now;
      const resolved = yield* policy.resolve({
        thread: makeThread({ now, worktreePath: "/project-worktree" }),
        modelSelection,
      });
      assert.equal(resolved.cwd, "/project-worktree");
    }),
  );

  it.effect("narrows an untrusted writer instead of auto-accepting its edits", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveGoalWorkerRuntimePolicy({
        inherited: unrestrictedRuntimePolicy,
        rootPolicy: rootGoalPolicy,
        nodePolicy: writerPolicy,
        workspaceMode: "writer",
        providerInstanceId,
      });

      assert.equal(resolved.runtimeMode, "approval-required");
      assert.equal(resolved.approvalPolicy, "untrusted");
      assert.deepEqual(resolved.sandboxPolicy, {
        type: "workspaceWrite",
        writableRoots: ["/repo/packages/goal"],
      });
      assert.deepEqual(resolved.toolAllowlist, ["*"]);
    }),
  );

  it.effect("keeps reader nodes read-only even under an inherited full-access thread", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveGoalWorkerRuntimePolicy({
        inherited: unrestrictedRuntimePolicy,
        rootPolicy: rootGoalPolicy,
        nodePolicy: readerPolicy,
        workspaceMode: "read_only",
        providerInstanceId,
      });

      assert.equal(resolved.runtimeMode, "approval-required");
      assert.equal(resolved.approvalPolicy, "untrusted");
      assert.deepEqual(resolved.sandboxPolicy, { type: "readOnly" });
      assert.deepEqual(resolved.toolAllowlist, ["*"]);
    }),
  );

  it.effect("intersects inherited workspace and tool authority without broadening either", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveGoalWorkerRuntimePolicy({
        inherited: {
          ...unrestrictedRuntimePolicy,
          sandboxPolicy: { type: "workspaceWrite", writableRoots: ["/repo"] },
          toolAllowlist: ["*"],
        },
        rootPolicy: rootGoalPolicy,
        nodePolicy: { ...writerPolicy, toolAllowlist: ["*"] },
        workspaceMode: "writer",
        providerInstanceId,
      });

      assert.deepEqual(resolved.sandboxPolicy, {
        type: "workspaceWrite",
        writableRoots: ["/repo/packages/goal"],
      });
      assert.deepEqual(resolved.toolAllowlist, ["*"]);
    }),
  );

  it.effect("rejects a node that expands root authority instead of inheriting it", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        resolveGoalWorkerRuntimePolicy({
          inherited: unrestrictedRuntimePolicy,
          rootPolicy: {
            ...writerPolicy,
            writableRoots: ["/repo"],
            toolAllowlist: ["shell"],
          },
          nodePolicy: {
            ...writerPolicy,
            approvalPolicy: "never",
            writableRoots: ["/outside"],
            toolAllowlist: ["shell", "apply_patch"],
          },
          workspaceMode: "writer",
          providerInstanceId,
        }),
      );

      assert.equal(exit._tag, "Failure");
    }),
  );

  it.effect("rejects a restricted tool allowlist until an adapter can enforce it", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        resolveGoalWorkerRuntimePolicy({
          inherited: unrestrictedRuntimePolicy,
          rootPolicy: rootGoalPolicy,
          nodePolicy: { ...writerPolicy, toolAllowlist: ["shell"] },
          workspaceMode: "writer",
          providerInstanceId,
        }),
      );

      assert.equal(exit._tag, "Failure");
      if (exit._tag === "Failure") {
        const failure = exit.cause.reasons.find(Cause.isFailReason)?.error;
        assert.isTrue(isGoalRuntimePolicyResolveError(failure));
        if (isGoalRuntimePolicyResolveError(failure)) {
          assert.equal(failure.reason, "tool_allowlist_unsupported");
          assert.deepEqual(failure.toolAllowlist, ["shell"]);
        }
      }
    }),
  );
});
