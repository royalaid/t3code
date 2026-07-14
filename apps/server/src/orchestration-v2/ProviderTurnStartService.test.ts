import { assert, it } from "@effect/vitest";
import {
  EventId,
  type GoalDetail,
  GoalAttemptId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { EventSinkV2Shape } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import {
  bindGoalWorkerProviderSession,
  resolveGoalAttemptRuntimePolicy,
} from "./ProviderTurnStartService.ts";
import { ProviderAdapterV2RuntimePolicy } from "./ProviderAdapter.ts";
import { goalWorkspaceAuthorityRoot } from "./RuntimePolicy.ts";

const goalId = GoalId.make("goal:provider-runtime-policy");
const graphVersionId = GoalGraphVersionId.make("goal-graph:provider-runtime-policy");
const nodeId = GoalNodeId.make("goal-node:provider-runtime-policy");
const attemptId = GoalAttemptId.make("goal-attempt:provider-runtime-policy");
const threadId = ThreadId.make("goal-worker:provider-runtime-policy");
const runId = RunId.make("run:provider-runtime-policy");
const providerInstanceId = ProviderInstanceId.make("codex");
const workspacePath = "/repo/.t3/goals/worker";
const workspaceAuthorityRoot = goalWorkspaceAuthorityRoot(goalId);

const rootPolicy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: [workspaceAuthorityRoot, "/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};
const untrustedWriterPolicy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "untrusted" as const,
  writableRoots: [workspaceAuthorityRoot],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};

function goalDetail(input: {
  readonly activeAttemptId?: typeof attemptId | null;
  readonly workspacePath?: string | null;
}): GoalDetail {
  return {
    goal: {
      id: goalId,
      rootThreadId: ThreadId.make("goal-root:provider-runtime-policy"),
      repositoryRoot: "/repo",
      sourceWorkspacePath: "/repo/source",
      integrationWorktreePath: "/repo/.t3/goals/integration",
      policy: rootPolicy,
    },
    attempts: [
      {
        id: attemptId,
        goalId,
        graphVersionId,
        nodeId,
        status: "launching",
        providerSessionId: null,
        executionThreadId: threadId,
        runId,
        workspacePath: input.workspacePath === undefined ? workspacePath : input.workspacePath,
        resolvedRoute: {
          requested: {
            type: "exact",
            providerInstanceId,
            model: "gpt-5.5",
          },
          providerInstanceId,
          model: "gpt-5.5",
          capabilitySnapshot: [],
          rationale: "test",
        },
      },
    ],
    nodes: [
      {
        graphVersionId,
        node: {
          id: nodeId,
          workspaceMode: "writer",
          policy: untrustedWriterPolicy,
        },
        activeAttemptId: input.activeAttemptId === undefined ? attemptId : input.activeAttemptId,
        status: "running",
      },
    ],
  } as unknown as GoalDetail;
}

function goals(
  detail: GoalDetail,
): Pick<GoalProjectionStore["Service"], "getDetail" | "resolveMcpBinding"> {
  return {
    getDetail: () => Effect.succeed(detail),
    resolveMcpBinding: () =>
      Effect.succeed({
        kind: "worker" as const,
        goalId,
        rootThreadId: ThreadId.make("goal-root:provider-runtime-policy"),
        nodeId,
        attemptId,
      }),
  };
}

const run = { id: runId, modelSelection: { instanceId: providerInstanceId, model: "gpt-5.5" } };
const inherited = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "auto-accept-edits",
  interactionMode: "default",
  cwd: workspacePath,
});

it.effect("starts a goal worker with its active node's narrowed provider policy", () =>
  Effect.gen(function* () {
    const resolved = yield* resolveGoalAttemptRuntimePolicy({
      goals: goals(goalDetail({})),
      threadId,
      run,
      inherited,
    });

    assert.equal(resolved.runtimeMode, "approval-required");
    assert.equal(resolved.approvalPolicy, "untrusted");
    assert.deepEqual(resolved.sandboxPolicy, {
      type: "workspaceWrite",
      writableRoots: [workspacePath],
    });
    assert.equal(resolved.cwd, workspacePath);
    assert.deepEqual(resolved.toolAllowlist, ["*"]);
  }),
);

it.effect("refuses to start a writer without its durable isolated workspace", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      resolveGoalAttemptRuntimePolicy({
        goals: goals(goalDetail({ workspacePath: null })),
        threadId,
        run,
        inherited,
      }),
    );

    assert.equal(exit._tag, "Failure");
  }),
);

it.effect("refuses to start a goal worker after its active-attempt binding changes", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      resolveGoalAttemptRuntimePolicy({
        goals: goals(goalDetail({ activeAttemptId: null })),
        threadId,
        run,
        inherited,
      }),
    );

    assert.equal(exit._tag, "Failure");
  }),
);

it.effect("binds the exact worker provider session before issuing its MCP credential", () =>
  Effect.gen(function* () {
    const providerSessionId = ProviderSessionId.make("provider-session:goal-worker");
    const committedInputs: Array<Parameters<EventSinkV2Shape["commitGoalAttemptCommand"]>[0]> = [];

    yield* bindGoalWorkerProviderSession({
      goals: goals(goalDetail({})),
      threadId,
      run,
      providerSessionId,
      allocateEvent: () => Effect.succeed(EventId.make("event:bind-goal-worker-session")),
      commitGoalAttemptCommand: (input) => {
        committedInputs.push(input);
        return Effect.succeed({ committed: true, stale: false, storedEvents: [] });
      },
    });

    const committedInput = committedInputs[0];
    assert.isDefined(committedInput);
    if (committedInput === undefined) return;
    assert.equal(committedInput?.commandType, "goal.attempt.bind-provider-session");
    assert.deepEqual(committedInput?.expectedStatuses, ["launching"]);
    assert.equal(committedInput?.events[0]?.type, "goal.attempt-transitioned");
    assert.equal(
      committedInput?.events[0]?.type === "goal.attempt-transitioned"
        ? committedInput.events[0].payload.providerSessionId
        : null,
      providerSessionId,
    );
  }),
);

it.effect("fails worker startup when provider-session binding loses its active-attempt fence", () =>
  Effect.gen(function* () {
    const exit = yield* bindGoalWorkerProviderSession({
      goals: goals(goalDetail({})),
      threadId,
      run,
      providerSessionId: ProviderSessionId.make("provider-session:stale-goal-worker"),
      allocateEvent: () => Effect.succeed(EventId.make("event:stale-goal-worker-session")),
      commitGoalAttemptCommand: () =>
        Effect.succeed({ committed: false, stale: true, storedEvents: [] }),
    }).pipe(Effect.exit);

    assert.equal(exit._tag, "Failure");
  }),
);
