import {
  CommandId,
  type GoalDetail,
  type GoalId,
  type GoalMcpCapabilitiesResult,
  type GoalMcpEvidenceReadInput,
  type GoalMcpEvidenceReadResult,
  type GoalMcpEvidenceSubmitInput,
  type GoalMcpMutationResult,
  type GoalMcpNodeCancelInput,
  type GoalMcpNodeReadInput,
  type GoalMcpNodeReadResult,
  type GoalMcpReplaceGraphInput,
  type GoalMcpResultPublishInput,
  isProviderAvailable,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2TurnItem,
  OrchestratorMcpFailure,
  type OrchestratorMcpCapabilitiesResult,
  type OrchestratorMcpCreateThreadsInput,
  type OrchestratorMcpCreateThreadsResult,
  type OrchestratorMcpCreatedThread,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpDelegateTaskResult,
  type OrchestratorMcpInteractionMode,
  type OrchestratorMcpDeleteScheduledTaskInput,
  type OrchestratorMcpDeleteScheduledTaskResult,
  type OrchestratorMcpListScheduledTasksResult,
  type OrchestratorMcpRuntimeMode,
  type OrchestratorMcpScheduledTask,
  type OrchestratorMcpScheduleTaskInput,
  type OrchestratorMcpScheduleTaskResult,
  type OrchestratorMcpTarget,
  type OrchestratorMcpTaskCancelInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestratorMcpUpdateScheduledTaskInput,
  type OrchestratorMcpThreadDetail,
  type OrchestratorMcpThreadInterruptInput,
  type OrchestratorMcpThreadInterruptResult,
  type OrchestratorMcpThreadListInput,
  type OrchestratorMcpThreadListItem,
  type OrchestratorMcpThreadListResult,
  type OrchestratorMcpThreadReadInput,
  type OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadRun,
  type OrchestratorMcpThreadSendInput,
  type OrchestratorMcpThreadSendResult,
  type OrchestratorMcpThreadTimelineItem,
  type OrchestratorMcpThreadWaitInput,
  type OrchestratorMcpThreadWaitResult,
  type ProviderInteractionMode,
  ProviderInstanceId,
  type RuntimeMode,
  type ScheduledTask,
  type ScheduledTaskUpsertInput,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { isBuiltInProviderAdapterDriverV2 } from "../orchestration-v2/builtInProviderAdapterDrivers.ts";
import { goalGraphPublisherIssue } from "../orchestration-v2/GoalGraphSemantics.ts";
import {
  describeGoalRoutingCatalog,
  GoalRoutingService,
} from "../orchestration-v2/GoalRoutingService.ts";
import { subagentResultForRun } from "../orchestration-v2/SubagentProjection.ts";
import {
  isActiveRun,
  latestActiveRun,
  latestRun,
  ThreadManagementError,
  ThreadManagementService,
} from "../orchestration-v2/ThreadManagementService.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import { ScheduledTaskService } from "../scheduledTasks/ScheduledTaskService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";
import { goalAuthority } from "./McpInvocationContext.ts";
import { selectGoalNodeForAuthority, validateGoalWorkerBinding } from "./GoalMcpAuthorization.ts";

const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_WAIT_TIMEOUT_MS = 60 * 60 * 1_000;
const TASK_POLL_INTERVAL_MS = 50;
const DEFAULT_THREAD_LIST_LIMIT = 50;
const DEFAULT_THREAD_READ_LIMIT = 50;
const DEFAULT_THREAD_RUN_LIMIT = 10;
const DEFAULT_THREAD_ITEM_MAX_CHARS = 20_000;

interface ResolvedTarget {
  readonly modelSelection: ModelSelection;
}

type TerminalTaskStatus = Extract<
  OrchestratorMcpDelegateTaskResult["status"],
  "completed" | "failed" | "cancelled" | "interrupted"
>;

export interface OrchestratorMcpServiceShape {
  readonly goalRead: (
    scope: McpInvocationScope,
    goalId: GoalId,
  ) => Effect.Effect<GoalDetail, OrchestratorMcpFailure>;
  readonly goalCapabilities: (
    scope: McpInvocationScope,
  ) => Effect.Effect<GoalMcpCapabilitiesResult, OrchestratorMcpFailure>;
  readonly goalReplaceGraph: (
    scope: McpInvocationScope,
    input: GoalMcpReplaceGraphInput,
  ) => Effect.Effect<GoalMcpMutationResult, OrchestratorMcpFailure>;
  readonly goalNodeRead: (
    scope: McpInvocationScope,
    input: GoalMcpNodeReadInput,
  ) => Effect.Effect<GoalMcpNodeReadResult, OrchestratorMcpFailure>;
  readonly goalNodeCancel: (
    scope: McpInvocationScope,
    input: GoalMcpNodeCancelInput,
  ) => Effect.Effect<GoalMcpMutationResult, OrchestratorMcpFailure>;
  readonly goalResultPublish: (
    scope: McpInvocationScope,
    input: GoalMcpResultPublishInput,
  ) => Effect.Effect<GoalMcpMutationResult, OrchestratorMcpFailure>;
  readonly goalEvidenceRead: (
    scope: McpInvocationScope,
    input: GoalMcpEvidenceReadInput,
  ) => Effect.Effect<GoalMcpEvidenceReadResult, OrchestratorMcpFailure>;
  readonly goalEvidenceSubmit: (
    scope: McpInvocationScope,
    input: GoalMcpEvidenceSubmitInput,
  ) => Effect.Effect<GoalMcpMutationResult, OrchestratorMcpFailure>;
  readonly capabilities: (
    scope: McpInvocationScope,
  ) => Effect.Effect<OrchestratorMcpCapabilitiesResult, OrchestratorMcpFailure>;
  readonly delegateTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpDelegateTaskInput,
  ) => Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure>;
  readonly taskStatus: (
    scope: McpInvocationScope,
    taskId: NodeId,
  ) => Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure>;
  readonly cancelTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpTaskCancelInput,
  ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
  readonly createThreads: (
    scope: McpInvocationScope,
    input: OrchestratorMcpCreateThreadsInput,
  ) => Effect.Effect<OrchestratorMcpCreateThreadsResult, OrchestratorMcpFailure>;
  readonly scheduleTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpScheduleTaskInput,
  ) => Effect.Effect<OrchestratorMcpScheduleTaskResult, OrchestratorMcpFailure>;
  readonly listScheduledTasks: (
    scope: McpInvocationScope,
  ) => Effect.Effect<OrchestratorMcpListScheduledTasksResult, OrchestratorMcpFailure>;
  readonly updateScheduledTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpUpdateScheduledTaskInput,
  ) => Effect.Effect<OrchestratorMcpScheduleTaskResult, OrchestratorMcpFailure>;
  readonly deleteScheduledTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpDeleteScheduledTaskInput,
  ) => Effect.Effect<OrchestratorMcpDeleteScheduledTaskResult, OrchestratorMcpFailure>;
  readonly listThreads: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadListInput,
  ) => Effect.Effect<OrchestratorMcpThreadListResult, OrchestratorMcpFailure>;
  readonly readThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadReadInput,
  ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>;
  readonly sendToThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadSendInput,
  ) => Effect.Effect<OrchestratorMcpThreadSendResult, OrchestratorMcpFailure>;
  readonly waitForThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadWaitInput,
  ) => Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>;
  readonly interruptThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadInterruptInput,
  ) => Effect.Effect<OrchestratorMcpThreadInterruptResult, OrchestratorMcpFailure>;
}

export class OrchestratorMcpService extends Context.Service<
  OrchestratorMcpService,
  OrchestratorMcpServiceShape
>()("t3/mcp/OrchestratorMcpService") {}

const isThreadManagementError = Schema.is(ThreadManagementError);

function failure(code: OrchestratorMcpFailure["code"], message: string): OrchestratorMcpFailure {
  return new OrchestratorMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorCauseChain(error: unknown): ReadonlyArray<unknown> {
  const chain: Array<unknown> = [];
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (current !== undefined && current !== null && !seen.has(current)) {
    seen.add(current);
    chain.push(current);
    if (typeof current !== "object" || !("cause" in current)) break;
    current = current.cause;
  }
  return chain;
}

function isStaleActiveRunTarget(error: unknown): boolean {
  return errorCauseChain(error).some(
    (candidate) =>
      typeof candidate === "string" && candidate.startsWith("stale_active_run_target:"),
  );
}

export function goalMutationFailure(error: unknown): OrchestratorMcpFailure {
  for (const candidate of errorCauseChain(error).toReversed()) {
    if (typeof candidate !== "object" || candidate === null) continue;
    const tagged = candidate as {
      readonly _tag?: unknown;
      readonly reason?: unknown;
      readonly detail?: unknown;
    };
    if (
      tagged._tag !== "GoalProjectionValidationError" ||
      typeof tagged.reason !== "string" ||
      typeof tagged.detail !== "string"
    ) {
      continue;
    }
    if (tagged.reason === "goal_not_found") return failure("goal_not_found", tagged.detail);
    if (tagged.reason === "stale_revision") return failure("stale_revision", tagged.detail);
    if (tagged.reason === "stale_active_run_target")
      return failure("stale_active_run_target", tagged.detail);
    if (tagged.reason !== "persistence_error") return failure("invalid_request", tagged.detail);
  }
  if (isStaleActiveRunTarget(error)) {
    const detail = errorCauseChain(error).find(
      (candidate) =>
        typeof candidate === "string" && candidate.startsWith("stale_active_run_target:"),
    );
    return failure("stale_active_run_target", String(detail));
  }
  return failure("orchestration_error", errorMessage(error));
}

/**
 * Workspace strategy for a scheduled task created/updated over MCP: bound runs
 * post into the existing thread (the strategy is unused, keep root); unbound
 * runs launch a fresh worktree per run.
 */
function scheduledTaskWorkspaceStrategy(
  boundToThread: boolean,
): ScheduledTask["workspaceStrategy"] {
  return boundToThread
    ? { type: "root" }
    : { type: "worktree", baseRef: "main", startFromOrigin: true };
}

function scheduledTaskSummary(task: ScheduledTask): OrchestratorMcpScheduledTask {
  return {
    scheduledTaskId: task.id,
    title: task.title,
    prompt: task.prompt,
    enabled: task.enabled,
    projectId: task.projectId,
    boundThreadId: task.threadId,
    schedule: task.schedule,
    nextRunAt: task.nextRunAt,
    lastRunStatus: task.lastRunStatus,
  };
}

function providerConstraints(
  provider: ServerProvider | undefined,
  supportsOrchestrationV2: boolean,
): ReadonlyArray<string> {
  const constraints: Array<string> = [];
  if (!supportsOrchestrationV2) {
    constraints.push("No V2 provider adapter is registered.");
  }
  if (provider === undefined) return constraints;
  if (!provider.enabled) constraints.push("Provider instance is disabled.");
  if (!provider.installed) constraints.push("Provider executable is not installed.");
  if (!isProviderAvailable(provider)) {
    constraints.push(provider.unavailableReason ?? "Provider driver is unavailable.");
  }
  if (provider.status === "error" || provider.status === "disabled") {
    constraints.push(provider.message ?? `Provider status is ${provider.status}.`);
  }
  if (provider.auth.status === "unauthenticated") {
    constraints.push("Provider is not authenticated.");
  }
  return constraints;
}

function taskStatusForRun(
  run: OrchestrationV2Run | undefined,
): OrchestratorMcpDelegateTaskResult["status"] {
  switch (run?.status) {
    case "queued":
      return "queued";
    case "waiting":
      return "waiting";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
    case "rolled_back":
      return "cancelled";
    case "interrupted":
      return "interrupted";
    case "preparing":
    case "starting":
    case "running":
    case undefined:
      return "running";
  }
}

function isTerminalTaskStatus(
  status: OrchestratorMcpDelegateTaskResult["status"],
): status is TerminalTaskStatus {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

function runtimeModeRank(mode: RuntimeMode): number {
  switch (mode) {
    case "approval-required":
      return 0;
    case "auto-accept-edits":
      return 1;
    case "full-access":
      return 2;
  }
}

function interactionModeRank(mode: ProviderInteractionMode): number {
  return mode === "plan" ? 0 : 1;
}

function resolveRuntimeMode(
  parentMode: RuntimeMode,
  requested: OrchestratorMcpRuntimeMode | undefined,
): Effect.Effect<RuntimeMode, OrchestratorMcpFailure> {
  const resolved = requested === undefined || requested === "inherit" ? parentMode : requested;
  return runtimeModeRank(resolved) > runtimeModeRank(parentMode)
    ? Effect.fail(
        failure(
          "runtime_mode_escalation_denied",
          `Child runtime mode ${resolved} is broader than parent mode ${parentMode}.`,
        ),
      )
    : Effect.succeed(resolved);
}

function resolveInteractionMode(
  parentMode: ProviderInteractionMode,
  requested: OrchestratorMcpInteractionMode | undefined,
): Effect.Effect<ProviderInteractionMode, OrchestratorMcpFailure> {
  const resolved = requested === undefined || requested === "inherit" ? parentMode : requested;
  return interactionModeRank(resolved) > interactionModeRank(parentMode)
    ? Effect.fail(
        failure(
          "interaction_mode_escalation_denied",
          `Child interaction mode ${resolved} is broader than parent mode ${parentMode}.`,
        ),
      )
    : Effect.succeed(resolved);
}

function stablePart(value: string): string {
  return encodeURIComponent(value);
}

function stableCommandId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly operation: string;
  readonly index?: number;
}): CommandId {
  return CommandId.make(
    [
      "command",
      "mcp",
      stablePart(input.scope.providerSessionId),
      stablePart(input.operation),
      stablePart(input.requestKey),
      ...(input.index === undefined ? [] : [String(input.index)]),
    ].join(":"),
  );
}

function stableThreadId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly index: number;
}): ThreadId {
  return ThreadId.make(
    [
      "thread",
      "mcp",
      stablePart(input.scope.providerSessionId),
      stablePart(input.requestKey),
      String(input.index),
    ].join(":"),
  );
}

function stableMessageId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly index: number;
}): MessageId {
  return MessageId.make(
    [
      "message",
      "mcp",
      stablePart(input.scope.providerSessionId),
      stablePart(input.requestKey),
      String(input.index),
    ].join(":"),
  );
}

function stableOperationMessageId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly operation: string;
}): MessageId {
  return MessageId.make(
    [
      "message",
      "mcp",
      stablePart(input.scope.providerSessionId),
      stablePart(input.operation),
      stablePart(input.requestKey),
    ].join(":"),
  );
}

function threadTitle(input: {
  readonly parentTitle: string;
  readonly prompt: string | undefined;
  readonly title: string | undefined;
  readonly index: number;
}): string {
  const detail = input.title?.trim() || input.prompt?.trim();
  if (!detail) return `${input.parentTitle} thread ${input.index + 1}`;
  return detail.length > 80 ? `${detail.slice(0, 77)}...` : detail;
}

function taskPrompt(input: OrchestratorMcpDelegateTaskInput): string {
  return input.role === undefined || input.role === "general"
    ? input.task
    : `Act as the ${input.role} sub-agent for this task.\n\n${input.task}`;
}

function listItemFromShell(shell: OrchestrationV2ThreadShell): OrchestratorMcpThreadListItem {
  return {
    threadId: shell.id,
    title: shell.title,
    createdBy: shell.createdBy,
    creationSource: shell.creationSource,
    status: shell.status,
    latestRunId: shell.latestRunId,
    providerInstanceId: shell.modelSelection.instanceId,
    model: shell.modelSelection.model,
    runtimeMode: shell.runtimeMode,
    interactionMode: shell.interactionMode,
    parentThreadId: shell.lineage.parentThreadId,
    relationshipToParent: shell.lineage.relationshipToParent,
    itemCount: shell.visibleItemCount,
    createdAt: DateTime.formatIso(shell.createdAt),
    updatedAt: DateTime.formatIso(shell.updatedAt),
  };
}

function threadDetail(projection: OrchestrationV2ThreadProjection): OrchestratorMcpThreadDetail {
  const latest = latestRun(projection);
  const active = latestActiveRun(projection);
  return {
    threadId: projection.thread.id,
    projectId: projection.thread.projectId,
    title: projection.thread.title,
    createdBy: projection.thread.createdBy,
    creationSource: projection.thread.creationSource,
    status: latest?.status ?? "idle",
    latestRunId: latest?.id ?? null,
    activeRunId: active?.id ?? null,
    providerInstanceId: projection.thread.modelSelection.instanceId,
    model: projection.thread.modelSelection.model,
    runtimeMode: projection.thread.runtimeMode,
    interactionMode: projection.thread.interactionMode,
    branch: projection.thread.branch,
    worktreePath: projection.thread.worktreePath,
    parentThreadId: projection.thread.lineage.parentThreadId,
    relationshipToParent: projection.thread.lineage.relationshipToParent,
    runCount: projection.runs.length,
    itemCount: projection.visibleTurnItems.length,
    pendingRequestCount: projection.runtimeRequests.filter(
      (request) => request.status === "pending",
    ).length,
    archived: projection.thread.archivedAt !== null,
    createdAt: DateTime.formatIso(projection.thread.createdAt),
    updatedAt: DateTime.formatIso(projection.updatedAt),
  };
}

function threadRun(run: OrchestrationV2Run): OrchestratorMcpThreadRun {
  return {
    runId: run.id,
    ordinal: run.ordinal,
    status: run.status,
    providerInstanceId: run.modelSelection.instanceId,
    model: run.modelSelection.model,
    requestedAt: DateTime.formatIso(run.requestedAt),
    startedAt: run.startedAt === null ? null : DateTime.formatIso(run.startedAt),
    completedAt: run.completedAt === null ? null : DateTime.formatIso(run.completedAt),
  };
}

function jsonText(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function turnItemText(item: OrchestrationV2TurnItem): string | null {
  switch (item.type) {
    case "user_message":
    case "assistant_message":
    case "reasoning":
      return item.text;
    case "proposed_plan":
      return item.markdown;
    case "todo_list":
      return [item.explanation, ...item.steps.map((step) => `[${step.status}] ${step.text}`)]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "user_input_request":
      return jsonText(item.questions);
    case "file_change":
      return [
        item.fileName,
        item.additions === undefined && item.deletions === undefined
          ? undefined
          : `+${item.additions ?? 0} -${item.deletions ?? 0}`,
        item.diffStr ?? item.newStr,
      ]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "command_execution":
      return [`$ ${item.input}`, item.output]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "file_search":
      return jsonText({ pattern: item.pattern, results: item.results });
    case "web_search":
      return jsonText({ patterns: item.patterns, results: item.results });
    case "approval_request":
      return item.prompt ?? item.requestKind;
    case "checkpoint":
      return jsonText(item.files);
    case "run_interrupt_request":
    case "run_interrupt_result":
      return item.message;
    case "error":
      return item.failure.message;
    case "compaction":
      return item.summary ?? null;
    case "handoff":
      return item.summary ?? `${item.strategy} handoff to ${item.toProviderInstanceId}`;
    case "fork":
      return `Forked to thread ${item.targetThreadId}.`;
    case "thread_created":
      return `Created thread ${item.targetThreadId} with ${item.targetProviderInstanceId} (${item.targetModel}).`;
    case "subagent":
      return item.result ?? item.progress ?? item.prompt;
    case "dynamic_tool":
      return jsonText({ toolName: item.toolName, input: item.input, output: item.output });
  }
}

function timelineItem(input: {
  readonly row: OrchestrationV2ThreadProjection["visibleTurnItems"][number];
  readonly maxChars: number;
  readonly projection: OrchestrationV2ThreadProjection;
}): OrchestratorMcpThreadTimelineItem {
  const text = turnItemText(input.row.item);
  const textTruncated = text !== null && text.length > input.maxChars;
  const messageId =
    input.row.item.type === "user_message" || input.row.item.type === "assistant_message"
      ? input.row.item.messageId
      : null;
  const message =
    messageId === null
      ? undefined
      : input.projection.messages.find((candidate) => candidate.id === messageId);
  return {
    position: input.row.position,
    visibility: input.row.visibility,
    sourceThreadId: input.row.sourceThreadId,
    itemId: input.row.sourceItemId,
    runId: input.row.item.runId,
    messageId,
    createdBy: message?.createdBy ?? null,
    creationSource: message?.creationSource ?? null,
    type: input.row.item.type,
    status: input.row.item.status,
    title: input.row.item.title,
    text: textTruncated ? `${text.slice(0, input.maxChars)}\n…[truncated]` : text,
    textTruncated,
    updatedAt: DateTime.formatIso(input.row.item.updatedAt),
  };
}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const threadManagement = yield* ThreadManagementService;
  const providerRegistry = yield* ProviderRegistry;
  const goalRouting = yield* GoalRoutingService;
  const scheduledTasks = yield* ScheduledTaskService;

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("orchestration") && goalAuthority(scope).kind === "ordinary"
      ? Effect.void
      : Effect.fail(
          failure(
            "capability_denied",
            goalAuthority(scope).kind === "ordinary"
              ? "This MCP credential does not grant orchestration capabilities."
              : "Goal-bound credentials cannot use generic delegation, thread creation, or scheduled-task tools.",
          ),
        );

  const loadGoal = Effect.fn("OrchestratorMcpService.loadGoal")(function* (
    scope: McpInvocationScope,
    goalId: GoalId,
  ) {
    const authority = goalAuthority(scope);
    if (authority.kind === "ordinary" || authority.goalId !== goalId) {
      return yield* failure("goal_scope_mismatch", `Credential is not scoped to goal ${goalId}.`);
    }
    if (authority.kind === "goal_worker" && authority.executionThreadId !== scope.threadId) {
      return yield* failure(
        "goal_scope_mismatch",
        "Goal worker credential execution thread claim does not match this invocation.",
      );
    }
    const rootThreadId = authority.rootThreadId;
    const projection = yield* loadProjection(rootThreadId);
    const detail = projection.goal;
    if (
      detail === null ||
      detail === undefined ||
      detail.goal.id !== goalId ||
      detail.goal.rootThreadId !== rootThreadId
    ) {
      return yield* failure(
        "goal_scope_mismatch",
        `Goal ${goalId} is not bound to root thread ${rootThreadId}.`,
      );
    }
    if (authority.kind === "goal_lead") {
      if (authority.rootThreadId !== scope.threadId) {
        return yield* failure(
          "goal_scope_mismatch",
          "Goal lead credential root thread claim does not match this invocation.",
        );
      }
      const activeThread = projection.providerThreads.find(
        (candidate) => candidate.id === projection.thread.activeProviderThreadId,
      );
      if (
        activeThread?.providerSessionId !== scope.providerSessionId ||
        activeThread.providerInstanceId !== scope.providerInstanceId
      ) {
        return yield* failure(
          "stale_goal_session",
          "Goal lead credential no longer matches the root active provider session.",
        );
      }
    } else {
      const attempt = detail.attempts.find((candidate) => candidate.id === authority.attemptId);
      if (attempt === undefined) {
        return yield* failure("stale_goal_session", "Goal worker attempt no longer exists.");
      }
      const issue = validateGoalWorkerBinding({ authority, scope, attempt });
      if (issue !== null) return yield* failure("stale_goal_session", issue);
    }
    return { authority, projection, detail };
  });

  const dispatchGoalMutation = Effect.fn("OrchestratorMcpService.dispatchGoalMutation")(function* (
    scope: McpInvocationScope,
    command: Parameters<ThreadManagementService["Service"]["dispatch"]>[0],
  ) {
    yield* threadManagement.dispatch(command).pipe(Effect.mapError(goalMutationFailure));
    return { accepted: true as const };
  });

  const loadProjection = (threadId: ThreadId) =>
    threadManagement
      .getThreadProjection(threadId)
      .pipe(
        Effect.mapError((error) =>
          failure(
            "orchestration_error",
            `Unable to read thread ${threadId}: ${errorMessage(error)}`,
          ),
        ),
      );

  const loadProjectThread = (
    projectId: OrchestrationV2ThreadProjection["thread"]["projectId"],
    threadId: ThreadId,
  ): Effect.Effect<OrchestrationV2ThreadProjection, OrchestratorMcpFailure> =>
    threadManagement
      .getProjectThread({ projectId, threadId })
      .pipe(
        Effect.mapError(() =>
          failure("thread_not_found", `Thread ${threadId} was not found in the calling project.`),
        ),
      );

  const loadScopedThread = (scope: McpInvocationScope, threadId: ThreadId) =>
    Effect.gen(function* () {
      yield* requireCapability(scope);
      const parent = yield* loadProjection(scope.threadId);
      const target =
        threadId === scope.threadId
          ? parent
          : yield* loadProjectThread(parent.thread.projectId, threadId);
      return { parent, target } as const;
    });

  const loadProviders = providerRegistry.getProviders;

  const resolveTarget = (input: {
    readonly parent: OrchestrationV2ThreadProjection;
    readonly target: OrchestratorMcpTarget | undefined;
    readonly providers: ReadonlyArray<ServerProvider>;
  }): Effect.Effect<ResolvedTarget, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const requestedInstanceId = input.target?.providerInstanceId;
      const requestedDriver = input.target?.driverKind;
      let instanceId = requestedInstanceId;

      if (instanceId === undefined && requestedDriver !== undefined) {
        const candidates = input.providers.filter(
          (provider) =>
            provider.driver === requestedDriver &&
            isBuiltInProviderAdapterDriverV2(provider.driver),
        );
        if (candidates.length === 0) {
          return yield* failure(
            "provider_unavailable",
            `No V2 provider adapter is registered for driver ${requestedDriver}.`,
          );
        }
        const inheritedCandidate = candidates.find(
          (candidate) => candidate.instanceId === input.parent.thread.modelSelection.instanceId,
        );
        const availableCandidate = candidates.find((candidate) => {
          return (
            providerConstraints(candidate, isBuiltInProviderAdapterDriverV2(candidate.driver))
              .length === 0
          );
        });
        instanceId = inheritedCandidate?.instanceId ?? availableCandidate?.instanceId;
      }
      instanceId ??= input.parent.thread.modelSelection.instanceId;

      const provider = input.providers.find((candidate) => candidate.instanceId === instanceId);
      if (provider === undefined) {
        return yield* failure(
          "provider_unavailable",
          `Provider instance ${instanceId} is not registered.`,
        );
      }
      if (requestedDriver !== undefined && provider.driver !== requestedDriver) {
        return yield* failure(
          "invalid_request",
          `Provider instance ${instanceId} uses driver ${provider.driver}, not ${requestedDriver}.`,
        );
      }
      const constraints = providerConstraints(
        provider,
        isBuiltInProviderAdapterDriverV2(provider.driver),
      );
      if (constraints.length > 0) {
        return yield* failure(
          "provider_unavailable",
          `Provider ${instanceId} cannot run a child task: ${constraints.join(" ")}`,
        );
      }

      const inheritedSelection = input.parent.thread.modelSelection;
      const requestedModel = input.target?.model;
      const model =
        requestedModel ??
        (instanceId === inheritedSelection.instanceId
          ? inheritedSelection.model
          : provider?.models[0]?.slug);
      if (model === undefined) {
        return yield* failure(
          "model_unavailable",
          `Provider ${instanceId} has no model available for inheritance.`,
        );
      }
      if (
        requestedModel !== undefined &&
        provider !== undefined &&
        provider.models.length > 0 &&
        !provider.models.some((candidate) => candidate.slug === requestedModel)
      ) {
        return yield* failure(
          "model_unavailable",
          `Model ${requestedModel} is not advertised by provider ${instanceId}.`,
        );
      }

      return {
        modelSelection:
          instanceId === inheritedSelection.instanceId && model === inheritedSelection.model
            ? inheritedSelection
            : { instanceId, model },
      };
    });

  const requestKey = (clientRequestId: string | undefined): Effect.Effect<string> =>
    clientRequestId === undefined
      ? crypto.randomUUIDv4.pipe(Effect.orDie)
      : Effect.succeed(clientRequestId);

  const readTask = (
    scope: McpInvocationScope,
    taskId: NodeId,
    waitTimedOut = false,
  ): Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      yield* requireCapability(scope);
      const parentProjection = yield* loadProjection(scope.threadId);
      const task = parentProjection.subagents.find(
        (candidate) =>
          candidate.id === taskId &&
          candidate.origin === "app_owned" &&
          candidate.threadId === scope.threadId,
      );
      if (task === undefined || task.childThreadId === null) {
        return yield* failure(
          "task_not_found",
          `Delegated task ${taskId} does not belong to thread ${scope.threadId}.`,
        );
      }
      const childProjection = yield* loadProjection(task.childThreadId);
      const childRun = childProjection.runs[0];
      const status = taskStatusForRun(childRun);
      const derivedResult =
        task.result !== null
          ? task.result
          : childRun !== undefined && isTerminalTaskStatus(status)
            ? subagentResultForRun(childProjection, childRun).text
            : null;
      const resultTransfer =
        parentProjection.contextTransfers.find(
          (transfer) =>
            transfer.type === "subagent_result" &&
            transfer.sourceThreadId === task.childThreadId &&
            transfer.targetThreadId === scope.threadId,
        ) ?? null;
      return {
        taskId: task.id,
        childThreadId: task.childThreadId,
        childRunId: childRun?.id ?? null,
        childNodeId: task.id,
        status,
        providerInstanceId: ProviderInstanceId.make(task.driver),
        model: task.model,
        summary: derivedResult,
        resultContextTransferId: resultTransfer?.id ?? null,
        waitTimedOut,
      };
    });

  const waitForTask = (scope: McpInvocationScope, taskId: NodeId, timeoutMs: number) =>
    Effect.gen(function* () {
      while (true) {
        const result = yield* readTask(scope, taskId);
        if (isTerminalTaskStatus(result.status)) return result;
        yield* Effect.sleep(Duration.millis(TASK_POLL_INTERVAL_MS));
      }
    }).pipe(Effect.timeoutOption(Duration.millis(timeoutMs)));

  // Load a single scheduled task and enforce that it belongs to the calling
  // thread's project, so agents can only read/mutate tasks in their own scope.
  const loadScopedScheduledTask = (
    projectId: ScheduledTask["projectId"],
    scheduledTaskId: ScheduledTask["id"],
  ): Effect.Effect<ScheduledTask, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const { tasks } = yield* scheduledTasks
        .list()
        .pipe(
          Effect.mapError((error) =>
            failure("orchestration_error", `Could not load scheduled task: ${error.message}`),
          ),
        );
      const task = tasks.find((candidate) => candidate.id === scheduledTaskId);
      if (task === undefined || task.projectId !== projectId) {
        return yield* failure(
          "task_not_found",
          `Scheduled task ${scheduledTaskId} was not found in the calling project.`,
        );
      }
      return task;
    });

  return OrchestratorMcpService.of({
    goalRead: (scope, goalId) =>
      loadGoal(scope, goalId).pipe(
        Effect.flatMap(({ authority, detail }) =>
          authority.kind === "goal_lead"
            ? Effect.succeed(detail)
            : Effect.fail(
                failure("capability_denied", "Workers must use goal_node_read for scoped data."),
              ),
        ),
      ),
    goalCapabilities: (scope) =>
      Effect.gen(function* () {
        const authority = goalAuthority(scope);
        if (authority.kind === "ordinary")
          return yield* failure("goal_scope_mismatch", "Credential is not goal scoped.");
        const { detail } = yield* loadGoal(scope, authority.goalId);
        const routeCandidates = yield* goalRouting.catalog.pipe(
          Effect.map((catalog) =>
            describeGoalRoutingCatalog({
              catalog,
              providerAllowlist: detail.goal.policy.providerAllowlist,
            }),
          ),
          Effect.mapError((cause) =>
            failure(
              "orchestration_error",
              `Could not load goal route capabilities: ${errorMessage(cause)}`,
            ),
          ),
        );
        return {
          goalId: authority.goalId,
          role: authority.kind === "goal_lead" ? "lead" : "worker",
          canReplaceGraph: authority.kind === "goal_lead",
          canCancelAnyNode: authority.kind === "goal_lead",
          scopedNodeId: authority.kind === "goal_worker" ? authority.nodeId : null,
          routeCandidates,
        };
      }),
    goalReplaceGraph: (scope, input) =>
      Effect.gen(function* () {
        const { authority, detail } = yield* loadGoal(scope, input.goalId);
        if (authority.kind !== "goal_lead")
          return yield* failure("capability_denied", "Only the root lead may replace the graph.");
        const publisherIssue = goalGraphPublisherIssue(input.graph, detail.goal.rootThreadId);
        if (publisherIssue !== null) return yield* failure("invalid_request", publisherIssue);
        return yield* dispatchGoalMutation(scope, {
          type: "goal.graph.replace",
          commandId: CommandId.make(`mcp:${scope.providerSessionId}:goal-graph:${input.graph.id}`),
          threadId: scope.threadId,
          goalId: input.goalId,
          expectedRevision: input.expectedRevision,
          graph: input.graph,
        });
      }),
    goalNodeRead: (scope, input) =>
      Effect.gen(function* () {
        const { authority, detail } = yield* loadGoal(scope, input.goalId);
        if (authority.kind === "goal_worker" && authority.nodeId !== input.nodeId)
          return yield* failure(
            "capability_denied",
            "Workers may only read their owning goal node.",
          );
        const node = selectGoalNodeForAuthority(detail, authority, input.nodeId);
        return node ?? (yield* failure("goal_not_found", `Node ${input.nodeId} was not found.`));
      }),
    goalNodeCancel: (scope, input) =>
      Effect.gen(function* () {
        const { authority, detail } = yield* loadGoal(scope, input.goalId);
        if (authority.kind !== "goal_lead")
          return yield* failure(
            "capability_denied",
            "Only the root lead may request node cancellation.",
          );
        const node = detail.nodes.find(
          (candidate) =>
            candidate.graphVersionId === input.graphVersionId && candidate.node.id === input.nodeId,
        );
        if (node === undefined)
          return yield* failure(
            "goal_not_found",
            `Node ${input.nodeId} was not found in graph version ${input.graphVersionId}.`,
          );
        if (
          node.status === "processing" ||
          node.node.workspaceMode === "integration" ||
          ["succeeded", "failed", "cancelled", "superseded"].includes(node.status)
        )
          return yield* failure(
            "stale_active_run_target",
            `Node ${input.nodeId} is ${node.status} and no longer has a cancellable provider attempt.`,
          );
        return yield* dispatchGoalMutation(scope, {
          type: "goal.node.cancel",
          commandId: CommandId.make(
            `mcp:${scope.providerSessionId}:goal-node-cancel:${input.graphVersionId}:${input.nodeId}:${input.disposition ?? "cancelled"}`,
          ),
          threadId: scope.threadId,
          goalId: input.goalId,
          graphVersionId: input.graphVersionId,
          nodeId: input.nodeId,
          disposition: input.disposition ?? "cancelled",
          ...(input.reason === undefined ? {} : { reason: input.reason }),
        });
      }),
    goalResultPublish: (scope, input) =>
      Effect.gen(function* () {
        const { authority, detail } = yield* loadGoal(scope, input.goalId);
        if (authority.kind !== "goal_worker")
          return yield* failure(
            "capability_denied",
            "The root lead cannot publish worker results.",
          );
        const attempt = detail.attempts.find((candidate) => candidate.id === input.attemptId);
        if (
          attempt === undefined ||
          authority.attemptId !== attempt.id ||
          authority.nodeId !== attempt.nodeId
        )
          return yield* failure(
            "capability_denied",
            "Attempt does not belong to the credential's owning node.",
          );
        const node = selectGoalNodeForAuthority(detail, authority, attempt.nodeId);
        if (node?.status === "cancelled" || node?.status === "superseded")
          return yield* failure(
            "stale_active_run_target",
            `Node ${attempt.nodeId} was ${node.status}; late result publication is not accepted.`,
          );
        const ownershipIssue = validateGoalWorkerBinding({
          authority,
          scope,
          attempt,
          artifacts: input.artifacts,
        });
        if (ownershipIssue !== null) return yield* failure("capability_denied", ownershipIssue);
        const publicationKey = input.artifacts
          .map((artifact) => artifact.id)
          .sort()
          .join(":");
        return yield* dispatchGoalMutation(scope, {
          type: "goal.result.publish",
          commandId: CommandId.make(
            `mcp:${scope.providerSessionId}:goal-result:${input.attemptId}:${publicationKey}`,
          ),
          threadId: authority.rootThreadId,
          goalId: input.goalId,
          attemptId: input.attemptId,
          artifacts: input.artifacts,
        });
      }),
    goalEvidenceRead: (scope, input) =>
      loadGoal(scope, input.goalId).pipe(
        Effect.map(({ authority, detail }) =>
          detail.evidence.filter(
            (evidence) =>
              (input.evidenceId === undefined || evidence.id === input.evidenceId) &&
              (authority.kind === "goal_lead" || evidence.attemptId === authority.attemptId),
          ),
        ),
      ),
    goalEvidenceSubmit: (scope, input) =>
      Effect.gen(function* () {
        const { authority, detail } = yield* loadGoal(scope, input.goalId);
        if (authority.kind !== "goal_worker")
          return yield* failure(
            "capability_denied",
            "The root lead cannot submit worker evidence.",
          );
        const attempt = detail.attempts.find(
          (candidate) => candidate.id === input.evidence.attemptId,
        );
        if (
          attempt === undefined ||
          authority.attemptId !== attempt.id ||
          authority.nodeId !== attempt.nodeId
        )
          return yield* failure(
            "capability_denied",
            "Evidence attempt does not belong to the credential's owning node.",
          );
        const node = selectGoalNodeForAuthority(detail, authority, attempt.nodeId);
        if (node?.status === "cancelled" || node?.status === "superseded")
          return yield* failure(
            "stale_active_run_target",
            `Node ${attempt.nodeId} was ${node.status}; late evidence publication is not accepted.`,
          );
        const ownershipIssue = validateGoalWorkerBinding({
          authority,
          scope,
          attempt,
          evidence: input.evidence,
          resolvedArtifacts: detail.artifacts,
        });
        if (ownershipIssue !== null) return yield* failure("capability_denied", ownershipIssue);
        return yield* dispatchGoalMutation(scope, {
          type: "goal.evidence.publish",
          commandId: CommandId.make(
            `mcp:${scope.providerSessionId}:goal-evidence:${input.evidence.id}`,
          ),
          threadId: authority.rootThreadId,
          goalId: input.goalId,
          evidence: input.evidence,
        });
      }),
    scheduleTask: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const bindToCurrentThread = input.bindToCurrentThread ?? true;
        const derivedTitle = input.prompt.split("\n")[0]?.trim() ?? "";
        const title =
          input.title ?? (derivedTitle.length > 0 ? derivedTitle.slice(0, 80) : "Scheduled task");
        const upsertInput: ScheduledTaskUpsertInput = {
          title,
          prompt: input.prompt,
          enabled: input.enabled ?? true,
          schedule: input.schedule,
          projectId: parent.thread.projectId,
          threadId: bindToCurrentThread ? scope.threadId : null,
          workspaceStrategy: scheduledTaskWorkspaceStrategy(bindToCurrentThread),
          modelSelection: parent.thread.modelSelection,
          runtimeMode: parent.thread.runtimeMode,
          interactionMode: parent.thread.interactionMode,
          createdBy: "agent",
          creationSource: "mcp",
          // Scope the idempotency key by provider session so two callers
          // reusing the same clientRequestId cannot collide on one task row.
          ...(input.clientRequestId === undefined
            ? {}
            : {
                commandId: stableCommandId({
                  scope,
                  requestKey: input.clientRequestId,
                  operation: "schedule-task",
                }),
              }),
        };
        const { task } = yield* scheduledTasks
          .upsert(upsertInput)
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Could not schedule task: ${error.message}`),
            ),
          );
        return scheduledTaskSummary(task);
      }),
    listScheduledTasks: (scope) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const { tasks } = yield* scheduledTasks
          .list()
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Could not list scheduled tasks: ${error.message}`),
            ),
          );
        // Only expose tasks belonging to the calling thread's project.
        return {
          tasks: tasks
            .filter((task) => task.projectId === parent.thread.projectId)
            .map(scheduledTaskSummary),
        };
      }),
    updateScheduledTask: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const existing = yield* loadScopedScheduledTask(
          parent.thread.projectId,
          input.scheduledTaskId,
        );
        const threadId =
          input.bindToCurrentThread === undefined
            ? existing.threadId
            : input.bindToCurrentThread
              ? scope.threadId
              : null;
        // Rebinding changes where runs execute, so the workspace strategy must
        // follow: unbinding a root-strategy task would otherwise run loose
        // prompts in the shared project checkout.
        const workspaceStrategy =
          input.bindToCurrentThread === undefined
            ? existing.workspaceStrategy
            : scheduledTaskWorkspaceStrategy(input.bindToCurrentThread);
        const upsertInput: ScheduledTaskUpsertInput = {
          id: existing.id,
          title: input.title ?? existing.title,
          prompt: input.prompt ?? existing.prompt,
          enabled: input.enabled ?? existing.enabled,
          schedule: input.schedule ?? existing.schedule,
          projectId: existing.projectId,
          threadId,
          workspaceStrategy,
          modelSelection: existing.modelSelection,
          runtimeMode: existing.runtimeMode,
          interactionMode: existing.interactionMode,
          createdBy: existing.createdBy,
          creationSource: existing.creationSource,
        };
        const { task } = yield* scheduledTasks
          .upsert(upsertInput)
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Could not update scheduled task: ${error.message}`),
            ),
          );
        return scheduledTaskSummary(task);
      }),
    deleteScheduledTask: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const existing = yield* loadScopedScheduledTask(
          parent.thread.projectId,
          input.scheduledTaskId,
        );
        yield* scheduledTasks
          .delete({ id: existing.id })
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Could not delete scheduled task: ${error.message}`),
            ),
          );
        return { scheduledTaskId: existing.id, deleted: true };
      }),
    capabilities: (scope) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const providers = yield* loadProviders;
        return {
          parentThreadId: scope.threadId,
          inheritedProviderInstanceId: parent.thread.modelSelection.instanceId,
          inheritedModel: parent.thread.modelSelection.model,
          runtimeMode: parent.thread.runtimeMode,
          interactionMode: parent.thread.interactionMode,
          providers: providers.map((provider) => {
            const constraints = providerConstraints(
              provider,
              isBuiltInProviderAdapterDriverV2(provider.driver),
            );
            return {
              providerInstanceId: provider.instanceId,
              driverKind: provider.driver,
              displayName: provider?.displayName ?? null,
              models:
                provider?.models.map((model) => ({
                  id: model.slug,
                  label: model.name ?? null,
                })) ?? [],
              canRunChildTask: constraints.length === 0,
              canRunCrossProviderChildTask: constraints.length === 0,
              constraints: [...constraints],
            };
          }),
          features: {
            appOwnedSubagents: true,
            asyncPolling: true,
            cancellation: true,
            batchThreadCreation: true,
            threadManagement: true,
            incrementalThreadRead: true,
            scheduledTasks: true,
            maxBatchThreads: 20,
          },
        };
      }),
    delegateTask: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const parentRun = parent.runs
          .filter(isActiveRun)
          .toSorted((left, right) => right.ordinal - left.ordinal)[0];
        if (
          parentRun === undefined ||
          parentRun.rootNodeId === null ||
          parentRun.providerInstanceId !== scope.providerInstanceId
        ) {
          return yield* failure(
            "parent_not_active",
            "Delegated tasks require an active run owned by this MCP provider session.",
          );
        }
        const providers = yield* loadProviders;
        const target = yield* resolveTarget({
          parent,
          target: input.target,
          providers,
        });
        const runtimeMode = yield* resolveRuntimeMode(parent.thread.runtimeMode, input.runtimeMode);
        const interactionMode = yield* resolveInteractionMode(
          parent.thread.interactionMode,
          input.interactionMode,
        );
        const key = yield* requestKey(input.clientRequestId);
        const commandId = stableCommandId({
          scope,
          requestKey: key,
          operation: "delegate-task",
        });
        const result = yield* threadManagement
          .dispatch({
            type: "delegated_task.request",
            createdBy: "agent",
            creationSource: "mcp",
            commandId,
            parentThreadId: scope.threadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId,
            task: taskPrompt(input),
            ...(input.title === undefined ? {} : { title: input.title }),
            modelSelection: target.modelSelection,
            runtimeMode,
            interactionMode,
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "orchestration_error",
                `Unable to create delegated task: ${errorMessage(error)}`,
              ),
            ),
          );
        const taskEvent = result.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.origin === "app_owned",
        );
        if (taskEvent?.event.type !== "subagent.updated") {
          return yield* failure(
            "orchestration_error",
            "Delegated task command did not produce a task projection.",
          );
        }

        if (input.mode !== "wait") {
          return yield* readTask(scope, taskEvent.event.payload.id);
        }
        const timeoutMs = Math.min(
          MAX_WAIT_TIMEOUT_MS,
          Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
        );
        const waited = yield* waitForTask(scope, taskEvent.event.payload.id, timeoutMs);
        return Option.isSome(waited)
          ? waited.value
          : yield* readTask(scope, taskEvent.event.payload.id, true);
      }),
    taskStatus: (scope, taskId) => readTask(scope, taskId),
    cancelTask: (scope, input) =>
      Effect.gen(function* () {
        const current = yield* readTask(scope, input.taskId);
        if (isTerminalTaskStatus(current.status)) {
          return {
            taskId: input.taskId,
            status: current.status,
          } satisfies OrchestratorMcpTaskCancelResult;
        }
        const child = yield* loadProjection(current.childThreadId);
        const activeRun = child.runs.find(isActiveRun);
        if (activeRun === undefined) {
          return yield* failure(
            "task_not_cancellable",
            `Delegated task ${input.taskId} has no interruptible child run.`,
          );
        }
        const key = yield* requestKey(input.clientRequestId);
        yield* threadManagement
          .dispatch({
            type: "run.interrupt",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "cancel-task",
            }),
            threadId: current.childThreadId,
            runId: activeRun.id,
            ...(input.reason === undefined ? {} : { reason: input.reason }),
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "task_not_cancellable",
                `Unable to interrupt delegated task ${input.taskId}: ${errorMessage(error)}`,
              ),
            ),
          );
        return {
          taskId: input.taskId,
          status: "cancel_requested",
        };
      }),
    createThreads: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const parentRun = latestActiveRun(parent);
        if (
          parentRun === undefined ||
          parentRun.rootNodeId === null ||
          parentRun.providerInstanceId !== scope.providerInstanceId
        ) {
          return yield* failure(
            "parent_not_active",
            "Thread creation requires an active run owned by this MCP provider session.",
          );
        }
        const parentNodeId = parentRun.rootNodeId;
        const providers = yield* loadProviders;
        const key = yield* requestKey(input.clientRequestId);
        const created = yield* Effect.forEach(
          input.threads,
          (request, index) =>
            Effect.gen(function* () {
              const target = yield* resolveTarget({
                parent,
                target: request.target,
                providers,
              });
              const runtimeMode = yield* resolveRuntimeMode(
                parent.thread.runtimeMode,
                request.runtimeMode,
              );
              const interactionMode = yield* resolveInteractionMode(
                parent.thread.interactionMode,
                request.interactionMode,
              );
              const threadId = stableThreadId({
                scope,
                requestKey: key,
                index,
              });
              const title = threadTitle({
                parentTitle: parent.thread.title,
                prompt: request.prompt,
                title: request.title,
                index,
              });
              yield* threadManagement
                .dispatch({
                  type: "thread.create",
                  createdBy: "agent",
                  creationSource: "mcp",
                  commandId: stableCommandId({
                    scope,
                    requestKey: key,
                    operation: "create-thread",
                    index,
                  }),
                  threadId,
                  projectId: parent.thread.projectId,
                  title,
                  modelSelection: target.modelSelection,
                  runtimeMode,
                  interactionMode,
                  branch: parent.thread.branch,
                  worktreePath: parent.thread.worktreePath,
                })
                .pipe(
                  Effect.mapError((error) =>
                    failure(
                      "orchestration_error",
                      `Unable to create thread ${index + 1}: ${errorMessage(error)}`,
                    ),
                  ),
                );
              if (request.prompt !== undefined) {
                yield* threadManagement
                  .dispatch({
                    type: "message.dispatch",
                    createdBy: "agent",
                    creationSource: "mcp",
                    commandId: stableCommandId({
                      scope,
                      requestKey: key,
                      operation: "dispatch-thread",
                      index,
                    }),
                    threadId,
                    messageId: stableMessageId({
                      scope,
                      requestKey: key,
                      index,
                    }),
                    text: request.prompt,
                    attachments: [],
                    modelSelection: target.modelSelection,
                    dispatchMode: { type: "start_immediately" },
                  })
                  .pipe(
                    Effect.mapError((error) =>
                      failure(
                        "orchestration_error",
                        `Unable to start thread ${index + 1}: ${errorMessage(error)}`,
                      ),
                    ),
                  );
              }
              const projection = yield* loadProjection(threadId);
              const run = projection.runs.at(-1);
              yield* threadManagement
                .dispatch({
                  type: "thread.created.record",
                  commandId: stableCommandId({
                    scope,
                    requestKey: key,
                    operation: "record-created-thread",
                    index,
                  }),
                  parentThreadId: scope.threadId,
                  parentRunId: parentRun.id,
                  parentNodeId,
                  targetThreadId: threadId,
                  targetRunId: run?.id ?? null,
                })
                .pipe(
                  Effect.mapError((error) =>
                    failure(
                      "orchestration_error",
                      `Unable to record thread ${index + 1} in the parent timeline: ${errorMessage(error)}`,
                    ),
                  ),
                );
              return {
                threadId,
                runId: run?.id ?? null,
                status: run?.status ?? "idle",
                title: projection.thread.title,
                createdBy: projection.thread.createdBy,
                creationSource: projection.thread.creationSource,
                providerInstanceId: target.modelSelection.instanceId,
                model: target.modelSelection.model,
              } satisfies OrchestratorMcpCreatedThread;
            }),
          { concurrency: 1 },
        );
        return { threads: created };
      }),
    listThreads: (scope, input) =>
      Effect.gen(function* () {
        yield* requireCapability(scope);
        const parent = yield* loadProjection(scope.threadId);
        const projectThreads = yield* threadManagement
          .listProjectThreads({
            projectId: parent.thread.projectId,
            includeSubagents: input.includeSubagents !== false,
          })
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Unable to list threads: ${errorMessage(error)}`),
            ),
          );
        const statuses = input.statuses === undefined ? null : new Set(input.statuses);
        const titleContains = input.titleContains?.toLocaleLowerCase();
        const filtered = projectThreads
          .filter((thread) => statuses === null || statuses.has(thread.status))
          .filter(
            (thread) =>
              titleContains === undefined ||
              thread.title.toLocaleLowerCase().includes(titleContains),
          );
        const cursor = input.cursor ?? 0;
        const limit = input.limit ?? DEFAULT_THREAD_LIST_LIMIT;
        const page = filtered.slice(cursor, cursor + limit);
        const nextCursor = cursor + page.length < filtered.length ? cursor + page.length : null;
        return {
          projectId: parent.thread.projectId,
          currentThreadId: scope.threadId,
          threads: page.map(listItemFromShell),
          nextCursor,
          total: filtered.length,
        } satisfies OrchestratorMcpThreadListResult;
      }),
    readThread: (scope, input) =>
      Effect.gen(function* () {
        const { target } = yield* loadScopedThread(scope, input.threadId);
        const view = input.view ?? "messages";
        const afterPosition = input.afterPosition ?? -1;
        const limit = input.limit ?? DEFAULT_THREAD_READ_LIMIT;
        const maxChars = input.maxCharsPerItem ?? DEFAULT_THREAD_ITEM_MAX_CHARS;
        const matching = target.visibleTurnItems
          .filter((row) => row.position > afterPosition)
          .filter(
            (row) =>
              view === "activity" ||
              row.item.type === "user_message" ||
              row.item.type === "assistant_message" ||
              row.item.type === "proposed_plan",
          );
        const page = matching.slice(0, limit);
        return {
          thread: threadDetail(target),
          recentRuns: target.runs
            .toSorted((left, right) => right.ordinal - left.ordinal)
            .slice(0, input.runLimit ?? DEFAULT_THREAD_RUN_LIMIT)
            .map(threadRun),
          items: page.map((row) => timelineItem({ row, maxChars, projection: target })),
          nextPosition: page.at(-1)?.position ?? null,
          hasMore: page.length < matching.length,
        } satisfies OrchestratorMcpThreadReadResult;
      }),
    sendToThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent, target } = yield* loadScopedThread(scope, input.threadId);
        yield* resolveRuntimeMode(parent.thread.runtimeMode, target.thread.runtimeMode);
        yield* resolveInteractionMode(parent.thread.interactionMode, target.thread.interactionMode);

        const mode = input.mode ?? "auto";
        const key = yield* requestKey(input.clientRequestId);
        const messageId = stableOperationMessageId({
          scope,
          requestKey: key,
          operation: "thread-send",
        });
        const result = yield* threadManagement
          .sendToThread({
            projectId: parent.thread.projectId,
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "thread-send",
            }),
            threadId: input.threadId,
            messageId,
            text: input.message,
            attachments: [],
            mode,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "thread_not_sendable",
                `Unable to send to thread ${input.threadId}: ${errorMessage(error)}`,
              ),
            ),
          );
        return {
          threadId: input.threadId,
          messageId,
          runId: result.run.id,
          status: result.run.status,
          delivery: result.delivery,
        } satisfies OrchestratorMcpThreadSendResult;
      }),
    waitForThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent } = yield* loadScopedThread(scope, input.threadId);
        const result = yield* threadManagement
          .waitForThread({
            projectId: parent.thread.projectId,
            threadId: input.threadId,
            ...(input.runId === undefined ? {} : { runId: input.runId }),
            timeoutMs: Math.min(
              MAX_WAIT_TIMEOUT_MS,
              Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
            ),
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                error.code === "run_not_found" ? "run_not_found" : "orchestration_error",
                error.message,
              ),
            ),
          );
        return {
          threadId: input.threadId,
          runId: result.run?.id ?? null,
          status: result.run?.status ?? "idle",
          timedOut: result.timedOut,
        } satisfies OrchestratorMcpThreadWaitResult;
      }),
    interruptThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent } = yield* loadScopedThread(scope, input.threadId);
        const key = yield* requestKey(input.clientRequestId);
        const result = yield* threadManagement
          .interruptThread({
            projectId: parent.thread.projectId,
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "thread-interrupt",
            }),
            threadId: input.threadId,
            ...(input.runId === undefined ? {} : { runId: input.runId }),
            ...(input.reason === undefined ? {} : { reason: input.reason }),
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                isThreadManagementError(error) && error.code === "run_not_found"
                  ? "run_not_found"
                  : "thread_not_interruptible",
                isThreadManagementError(error)
                  ? error.message
                  : `Unable to interrupt thread ${input.threadId}: ${errorMessage(error)}`,
              ),
            ),
          );
        if (result.type === "no_active_run") {
          return {
            threadId: input.threadId,
            runId: null,
            status: "no_active_run",
          } satisfies OrchestratorMcpThreadInterruptResult;
        }
        return {
          threadId: input.threadId,
          runId: result.run.id,
          status: result.type === "already_terminal" ? result.run.status : "interrupt_requested",
        } satisfies OrchestratorMcpThreadInterruptResult;
      }),
  });
});

export const layer: Layer.Layer<
  OrchestratorMcpService,
  never,
  | Crypto.Crypto
  | ThreadManagementService
  | ProviderRegistry
  | ScheduledTaskService
  | GoalRoutingService
> = Layer.effect(OrchestratorMcpService, make);
