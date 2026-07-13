import { assert, it } from "@effect/vitest";
import {
  type GoalDetail,
  GoalAttemptId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { resolveGoalAttemptRuntimePolicy } from "./ProviderTurnStartService.ts";
import { ProviderAdapterV2RuntimePolicy } from "./ProviderAdapter.ts";

const goalId = GoalId.make("goal:provider-runtime-policy");
const graphVersionId = GoalGraphVersionId.make("goal-graph:provider-runtime-policy");
const nodeId = GoalNodeId.make("goal-node:provider-runtime-policy");
const attemptId = GoalAttemptId.make("goal-attempt:provider-runtime-policy");
const threadId = ThreadId.make("goal-worker:provider-runtime-policy");
const runId = RunId.make("run:provider-runtime-policy");
const providerInstanceId = ProviderInstanceId.make("codex");

const rootPolicy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "on-request" as const,
  writableRoots: ["/repo"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};
const untrustedWriterPolicy = {
  sandboxMode: "workspace-write" as const,
  approvalPolicy: "untrusted" as const,
  writableRoots: ["/repo/packages/goal"],
  providerAllowlist: ["codex"],
  toolAllowlist: ["*"],
};

function goalDetail(input: { readonly activeAttemptId?: typeof attemptId | null }): GoalDetail {
  return {
    goal: { policy: rootPolicy },
    attempts: [
      {
        id: attemptId,
        graphVersionId,
        nodeId,
        executionThreadId: threadId,
        runId,
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
  cwd: "/repo/.t3/goals/worker",
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
      writableRoots: ["/repo/packages/goal"],
    });
    assert.deepEqual(resolved.toolAllowlist, ["*"]);
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
