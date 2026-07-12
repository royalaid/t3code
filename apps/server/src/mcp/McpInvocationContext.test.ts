import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  GoalAttemptId,
  GoalId,
  GoalNodeId,
  PreviewAutomationUnavailableError,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "./McpInvocationContext.ts";

it("retains ordinary and goal-worker issuance claims", () => {
  const base: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(),
    issuedAt: 1,
  };
  expect(McpInvocationContext.goalAuthority(base)).toEqual({ kind: "ordinary" });
  const authority = {
    kind: "goal_worker" as const,
    goalId: GoalId.make("goal:test"),
    rootThreadId: ThreadId.make("thread:root"),
    executionThreadId: ThreadId.make("thread:worker"),
    nodeId: GoalNodeId.make("node:test"),
    attemptId: GoalAttemptId.make("attempt:test"),
    nativeOwnerAttemptId: GoalAttemptId.make("attempt:test"),
  };
  expect(McpInvocationContext.goalAuthority({ ...base, authority })).toEqual(authority);
});

it.effect("reports the scoped credential context when preview capability is unavailable", () => {
  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(),
    issuedAt: 1,
  };

  return Effect.gen(function* () {
    const error = yield* McpInvocationContext.requireMcpCapability("preview").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.flip,
    );

    expect(error).toBeInstanceOf(PreviewAutomationUnavailableError);
    expect(error).toMatchObject({
      capability: "preview",
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
    expect(error.message).toBe("MCP credential does not grant the preview capability.");
  });
});
