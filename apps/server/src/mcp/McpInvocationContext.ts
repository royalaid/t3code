import {
  type EnvironmentId,
  type GoalAttemptId,
  type GoalId,
  type GoalNodeId,
  PreviewAutomationUnavailableError,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export type McpCapability = "preview" | "orchestration";

export type McpGoalAuthority =
  | { readonly kind: "ordinary" }
  | { readonly kind: "goal_lead"; readonly goalId: GoalId; readonly rootThreadId: ThreadId }
  | {
      readonly kind: "goal_worker";
      readonly goalId: GoalId;
      readonly rootThreadId: ThreadId;
      readonly executionThreadId: ThreadId;
      readonly nodeId: GoalNodeId;
      readonly attemptId: GoalAttemptId;
      readonly nativeOwnerAttemptId: GoalAttemptId;
    };

export interface McpInvocationScope {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly capabilities: ReadonlySet<McpCapability>;
  readonly issuedAt: number;
  readonly authority?: McpGoalAuthority;
}

export const goalAuthority = (scope: McpInvocationScope): McpGoalAuthority =>
  scope.authority ?? { kind: "ordinary" };

export class McpInvocationContext extends Context.Service<
  McpInvocationContext,
  McpInvocationScope
>()("t3/mcp/McpInvocationContext") {}

export const requireMcpCapability = Effect.fn("mcp.requireCapability")(function* (
  capability: "preview",
) {
  const invocation = yield* McpInvocationContext;
  if (!invocation.capabilities.has(capability)) {
    return yield* new PreviewAutomationUnavailableError({
      capability,
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
  }
  return invocation;
});
