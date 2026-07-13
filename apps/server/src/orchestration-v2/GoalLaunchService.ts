import {
  type ChatAttachment,
  CommandId,
  type GoalDetail,
  type OrchestrationV2StoredEvent,
  type RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import {
  goalBranchName,
  GoalWorkspaceService,
  type GoalPreparedWorkspace,
} from "./GoalWorkspaceService.ts";
import { ThreadLaunchService } from "./ThreadLaunchService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Option from "effect/Option";

type SettlementEvent = {
  readonly type: "run.updated";
  readonly runId: string | RunId;
  readonly status: string;
};

const TERMINAL = new Set(["completed", "failed", "cancelled", "interrupted", "rolled_back"]);

/**
 * The root lead only plans and steers the durable workflow. Keep its provider
 * session in the narrowest cross-provider runtime mode so it cannot make
 * unattended repository edits outside server-controlled writer attempts.
 */
export const GOAL_ROOT_LEAD_RUNTIME_MODE = "approval-required" as const;

export function goalRootLeadWorkspaceBindingError(input: {
  readonly goalId: GoalDetail["goal"]["id"];
  readonly integrationWorktreePath: string | null;
  readonly integrationBranch: string | null;
  readonly workspace: GoalPreparedWorkspace;
}): string | null {
  if (input.integrationWorktreePath === null || input.integrationBranch === null)
    return "Goal integration workspace must be provisioned before launching the root lead.";
  if (
    input.workspace.path === input.integrationWorktreePath ||
    input.workspace.branch === input.integrationBranch
  )
    return "The root lead must not receive the retained integration workspace.";
  if (!input.workspace.sharedReadOnly)
    return "The root lead must receive a shared read-only workspace.";
  const expectedBranch = goalBranchName("read", `${input.goalId}:${input.workspace.baseSha}`);
  return input.workspace.branch === expectedBranch
    ? null
    : "The root lead must receive the goal's deterministic read-only workspace.";
}

export function shouldFinalizeGoalAfterEvent(
  sourceActiveRunId: RunId | string | null,
  event: SettlementEvent | null,
): boolean {
  return (
    sourceActiveRunId === null ||
    (event !== null && event.runId === sourceActiveRunId && TERMINAL.has(event.status))
  );
}

export function sourceRunIsSettled(
  sourceActiveRunId: string | null,
  runs: ReadonlyArray<{ readonly id: string; readonly status: string }>,
): boolean {
  if (sourceActiveRunId === null) return true;
  const run = runs.find((candidate) => candidate.id === sourceActiveRunId);
  return run !== undefined && TERMINAL.has(run.status);
}

/**
 * A pending-root launch may proceed only while it owns the exact durable
 * provisioning claim. This check is intentionally cheap and repeatable so
 * cancellation can win at every external side-effect boundary.
 */
export function goalPendingLaunchClaimIsCurrent(
  goal: GoalDetail["goal"],
  claimId: string,
): boolean {
  return goal.status === "provisioning" && goal.pendingLaunchClaimId === claimId;
}

export function makeGoalLaunchCoordinator<
  T extends { readonly goalId: string; readonly sourceActiveRunId: string | null },
>(input: {
  readonly listPending: () => Promise<ReadonlyArray<T>>;
  readonly claim: (goal: T) => Promise<boolean>;
  readonly recheckClaim?: (goal: T) => Promise<boolean>;
  readonly finalize: (goal: T) => Promise<void>;
  readonly onError?: (goal: T, cause: unknown) => Promise<void>;
}) {
  return {
    rescan: async (event: SettlementEvent | null) => {
      const pending = await input.listPending();
      await Promise.all(
        pending.map(async (goal) => {
          try {
            if (!shouldFinalizeGoalAfterEvent(goal.sourceActiveRunId, event)) return;
            if (
              (await input.claim(goal)) &&
              (input.recheckClaim === undefined || (await input.recheckClaim(goal)))
            ) {
              await input.finalize(goal);
            }
          } catch (cause) {
            await input.onError?.(goal, cause);
          }
        }),
      );
    },
  };
}

const bounded = (value: string, limit = 20_000) => value.slice(0, limit);

function sourceSynopsis(value: string, limit = 1_100): string {
  const compacted = value.replace(/\s+/gu, " ").trim();
  if (compacted.length <= limit) return compacted;
  const omission = " … [content omitted] … ";
  const tailLength = 360;
  return `${compacted.slice(0, limit - tailLength - omission.length)}${omission}${compacted.slice(-tailLength)}`;
}

export function buildGoalSourceHandoff(input: {
  readonly objective: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly selectedContextText: ReadonlyArray<string>;
  readonly messages: ReadonlyArray<{ readonly role: string; readonly text: string }>;
  readonly projectInstructions: ReadonlyArray<string>;
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly checkpoints: ReadonlyArray<{ readonly ref: string; readonly status: string }>;
}) {
  const messages = input.messages
    .map((message) => ({ ...message, text: message.text.trim() }))
    .filter((message) => message.text.length > 0);
  const userMessages = messages.filter((message) => message.role === "user");
  const assistantMessages = messages.filter((message) => message.role === "assistant");
  const systemMessages = messages.filter(
    (message) => message.role !== "user" && message.role !== "assistant",
  );
  const summaryLines =
    messages.length === 0
      ? []
      : [
          `Source conversation: ${messages.length} non-empty messages (${userMessages.length} user, ${assistantMessages.length} assistant, ${systemMessages.length} system).`,
          ...(userMessages.at(-1) === undefined
            ? []
            : [`Latest user request: ${sourceSynopsis(userMessages.at(-1)!.text)}`]),
          ...(assistantMessages.at(-1) === undefined
            ? []
            : [`Latest assistant outcome: ${sourceSynopsis(assistantMessages.at(-1)!.text)}`]),
          ...(systemMessages.at(-1) === undefined
            ? []
            : [`Latest system context: ${sourceSynopsis(systemMessages.at(-1)!.text)}`]),
        ];
  const sourceSummary = summaryLines.join("\n").slice(0, 4_000);
  const branchParts = [
    input.branch === null ? null : `branch=${input.branch}`,
    input.worktreePath === null ? null : `worktree=${input.worktreePath}`,
  ].filter((part): part is string => part !== null);
  return {
    objective: bounded(input.objective),
    attachments: input.attachments.slice(0, 32),
    selectedContextText: input.selectedContextText.slice(0, 32).map((text) => bounded(text)),
    sourceSummary: sourceSummary === "" ? null : sourceSummary,
    projectInstructions: input.projectInstructions.slice(0, 16).map((text) => bounded(text)),
    branchState: branchParts.length === 0 ? null : bounded(branchParts.join("; ")),
    relevantCheckpoints: input.checkpoints
      .filter((checkpoint) => checkpoint.status === "ready")
      .slice(-32)
      .map((checkpoint) => bounded(checkpoint.ref)),
  };
}

export class GoalLaunchService extends Context.Service<
  GoalLaunchService,
  { readonly rescan: Effect.Effect<void, unknown> }
>()("t3/orchestration-v2/GoalLaunchService") {}

export const layer = Layer.effect(
  GoalLaunchService,
  Effect.gen(function* () {
    const goals = yield* GoalProjectionStore;
    const workspaces = yield* GoalWorkspaceService;
    const events = yield* EventSinkV2;
    const launches = yield* ThreadLaunchService;
    const threads = yield* ThreadManagementService;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const projectSnapshots = yield* Effect.serviceOption(ProjectionSnapshotQuery);

    const finalize = Effect.fn("GoalLaunchService.finalize")(function* (detail: GoalDetail) {
      const current = yield* goals.getDetail(detail.goal.id);
      if (current.goal.status === "cancelled" || current.goal.status === "failed") return;
      const input = current.goal.sourceInput;
      if (input === undefined) return;
      const claimId = `goal-root-launch:${current.goal.id}`;
      if (current.goal.status === "waiting_for_source") {
        yield* threads.dispatch({
          type: "goal.pending-launch.claim",
          commandId: CommandId.make(`goal-pending-launch-claim:${current.goal.id}`),
          threadId: current.goal.rootThreadId,
          goalId: current.goal.id,
          claimId,
        });
      } else if (current.goal.status !== "provisioning") {
        return;
      }
      const claimed = yield* goals.getDetail(current.goal.id);
      if (!goalPendingLaunchClaimIsCurrent(claimed.goal, claimId)) return;
      const claimStillCurrent = Effect.fn("GoalLaunchService.claimStillCurrent")(function* () {
        const fresh = yield* goals.getDetail(current.goal.id);
        return goalPendingLaunchClaimIsCurrent(fresh.goal, claimId);
      });
      const source = yield* threads.getThreadProjection(current.goal.sourceThreadId);
      const project = Option.isSome(projectSnapshots)
        ? yield* projectSnapshots.value
            .getProjectShellById(source.thread.projectId)
            .pipe(Effect.map(Option.getOrUndefined))
        : undefined;
      const workspaceRoot = source.thread.worktreePath ?? project?.workspaceRoot;
      const projectInstructions =
        workspaceRoot === undefined
          ? []
          : yield* fileSystem.readFileString(path.join(workspaceRoot, "AGENTS.md")).pipe(
              Effect.map((contents) => [bounded(contents)] as const),
              Effect.orElseSucceed(() => [] as const),
            );
      const handoff = buildGoalSourceHandoff({
        objective: current.goal.objective,
        attachments: input.attachments,
        selectedContextText: input.selectedContextText,
        messages: source.messages,
        projectInstructions,
        branch: source.thread.branch,
        worktreePath: source.thread.worktreePath,
        checkpoints: source.checkpoints,
      });
      // Re-check after nontrivial reads and immediately before provisioning a
      // retained integration worktree. A cancelled pending launch must not
      // create repository state merely because it was claimed earlier.
      if (!(yield* claimStillCurrent())) return;
      const provisioned = yield* workspaces.provision(current.goal.id);
      // Provisioning can race a user cancellation. Never bind or start the
      // root provider turn unless this precise claim still owns the goal.
      if (!(yield* claimStillCurrent())) return;
      const integrationWorktreePath = provisioned.goal.integrationWorktreePath;
      const integrationBranch = provisioned.goal.integrationBranch;
      if (integrationWorktreePath === null || integrationBranch === null) {
        return yield* Effect.die("Goal integration workspace was not provisioned.");
      }
      const rootLeadWorkspace = yield* workspaces.prepareRootLead(current.goal.id);
      const rootLeadWorkspaceError = goalRootLeadWorkspaceBindingError({
        goalId: current.goal.id,
        integrationWorktreePath,
        integrationBranch,
        workspace: rootLeadWorkspace,
      });
      if (rootLeadWorkspaceError !== null) return yield* Effect.die(rootLeadWorkspaceError);
      // The root thread was created with the source thread's runtime mode so
      // that source provenance is preserved. Narrow it before the first
      // provider run; ThreadLaunchService's reusable-thread path does not
      // update runtime mode itself.
      yield* threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make(`goal-root-runtime-policy:${current.goal.id}`),
        threadId: current.goal.rootThreadId,
        runtimeMode: GOAL_ROOT_LEAD_RUNTIME_MODE,
      });
      // `ThreadLaunchService` carries the same claim into its durable initial
      // message dispatch. This pre-flight check avoids even scheduling that
      // launch if Cancel Goal won while the read-only workspace was prepared.
      if (!(yield* claimStillCurrent())) return;
      yield* launches.launch({
        commandId: CommandId.make(`goal-root-launch:${current.goal.id}`),
        threadId: current.goal.rootThreadId,
        reuseExistingThread: true,
        projectId:
          current.goal.projectId ?? (yield* Effect.die("Goal is missing its project identity.")),
        title: current.goal.objective.slice(0, 512),
        modelSelection:
          current.goal.rootModelSelection ??
          (yield* Effect.die("Goal is missing its root model selection.")),
        runtimeMode: GOAL_ROOT_LEAD_RUNTIME_MODE,
        interactionMode:
          current.goal.rootInteractionMode ??
          (yield* Effect.die("Goal is missing its interaction mode.")),
        workspaceStrategy: {
          type: "existing_worktree",
          worktreePath: rootLeadWorkspace.path,
          branch: rootLeadWorkspace.branch,
        },
        initialMessage: {
          text: `Goal objective:\n${current.goal.objective}\n\nSource summary:\n${handoff.sourceSummary ?? "The source thread has no conversation history."}\n\nProject instructions:\n${handoff.projectInstructions.join("\n\n") || "No project instruction file was found."}\n\nBranch state:\n${handoff.branchState ?? "Unknown"}\n\nRelevant checkpoints:\n${handoff.relevantCheckpoints.join("\n") || "None"}\n\nSelected context:\n${handoff.selectedContextText.join("\n\n") || "None"}`,
          attachments: handoff.attachments,
        },
        goalLaunchClaim: { goalId: current.goal.id, claimId },
        createdBy: "system",
        creationSource: "server",
      });
      if (!(yield* claimStillCurrent())) return;
      yield* threads.dispatch({
        type: "goal.pending-launch.complete",
        commandId: CommandId.make(`goal-pending-launch-complete:${current.goal.id}`),
        threadId: current.goal.rootThreadId,
        goalId: current.goal.id,
        handoff,
        claimId,
      });
    });

    const process = Effect.fn("GoalLaunchService.process")(function* (
      event: SettlementEvent | null,
    ) {
      const pending = yield* goals.listPendingLaunches;
      yield* Effect.forEach(
        pending,
        (detail) =>
          Effect.gen(function* () {
            const capturedRunId = detail.goal.sourceActiveRunId ?? null;
            if (event !== null) {
              if (!shouldFinalizeGoalAfterEvent(capturedRunId, event)) return;
            } else if (capturedRunId !== null) {
              const source = yield* threads.getThreadProjection(detail.goal.sourceThreadId);
              if (!sourceRunIsSettled(capturedRunId, source.runs)) return;
            }
            yield* finalize(detail).pipe(
              Effect.catchCause((cause) =>
                threads
                  .dispatch({
                    type: "goal.pending-launch.fail",
                    commandId: CommandId.make(`goal-pending-launch-fail:${detail.goal.id}`),
                    threadId: detail.goal.rootThreadId,
                    goalId: detail.goal.id,
                    claimId: `goal-root-launch:${detail.goal.id}`,
                    detail: String(cause).slice(0, 4_000),
                  })
                  .pipe(
                    Effect.tap(() =>
                      Effect.logError("Goal pending launch failed", {
                        goalId: detail.goal.id,
                        cause,
                      }),
                    ),
                    Effect.catchCause((persistCause) =>
                      Effect.logError("Goal pending launch failure could not be persisted", {
                        goalId: detail.goal.id,
                        cause,
                        persistCause,
                      }),
                    ),
                  ),
              ),
            );
          }),
        { concurrency: 1 },
      );
    });
    yield* events.stream().pipe(
      Stream.filter(
        (stored: OrchestrationV2StoredEvent) =>
          stored.event.type === "run.updated" && TERMINAL.has(stored.event.payload.status),
      ),
      Stream.map((stored) => {
        const event = stored.event;
        return event.type === "run.updated"
          ? {
              type: "run.updated" as const,
              runId: event.payload.id as RunId,
              status: event.payload.status,
            }
          : null;
      }),
      Stream.runForEach((event) => (event === null ? Effect.void : process(event))),
      Effect.catch((cause) => Effect.logError("Goal launch event consumer failed", { cause })),
      Effect.forkScoped,
    );
    const rescan = process(null);
    yield* rescan.pipe(
      Effect.catch((cause) => Effect.logError("Goal launch startup rescan failed", { cause })),
    );
    return GoalLaunchService.of({ rescan });
  }),
);
