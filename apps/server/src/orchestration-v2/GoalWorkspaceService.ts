import * as NodeCrypto from "node:crypto";

import {
  CommandId,
  type GoalAttemptId,
  type GoalDetail,
  type GoalId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { GitVcsDriver } from "../vcs/GitVcsDriver.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";

export function goalBranchName(kind: "integration" | "worker" | "read", identity: string): string {
  const prefix = kind === "integration" ? "goal" : kind === "worker" ? "goal-worker" : "goal-read";
  const slug = identity
    .replace(/[^a-zA-Z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 32)
    .toLowerCase();
  const digest = NodeCrypto.createHash("sha256").update(identity).digest("hex").slice(0, 12);
  return `${prefix}/${slug || "workspace"}-${digest}`;
}

export function parseGitWorktreeList(
  output: string,
): ReadonlyArray<{ readonly path: string; readonly branch: string | null }> {
  return output
    .trim()
    .split(/\r?\n\r?\n/gu)
    .flatMap((block) => {
      const lines = block.split(/\r?\n/gu);
      const worktree = lines
        .find((line) => line.startsWith("worktree "))
        ?.slice("worktree ".length);
      if (!worktree) return [];
      const branchRef = lines.find((line) => line.startsWith("branch "))?.slice("branch ".length);
      return [
        {
          path: worktree,
          branch: branchRef?.startsWith("refs/heads/")
            ? branchRef.slice("refs/heads/".length)
            : null,
        },
      ];
    });
}

export interface GoalPreparedWorkspace {
  readonly path: string;
  readonly branch: string;
  readonly baseSha: string;
  readonly sharedReadOnly: boolean;
}

export class GoalWorkspaceError extends Schema.TaggedErrorClass<GoalWorkspaceError>()(
  "GoalWorkspaceError",
  {
    operation: Schema.String,
    goalId: Schema.optional(Schema.String),
    attemptId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class GoalWorkspaceService extends Context.Service<
  GoalWorkspaceService,
  {
    readonly provision: (goalId: GoalId) => Effect.Effect<GoalDetail, GoalWorkspaceError>;
    readonly prepareAttempt: (input: {
      readonly goalId: GoalId;
      readonly attemptId: GoalAttemptId;
    }) => Effect.Effect<GoalPreparedWorkspace, GoalWorkspaceError>;
  }
>()("t3/orchestration-v2/GoalWorkspaceService") {}

const workspaceError =
  (operation: string, goalId?: GoalId, attemptId?: GoalAttemptId) =>
  (cause: unknown): GoalWorkspaceError =>
    new GoalWorkspaceError({
      operation,
      ...(goalId === undefined ? {} : { goalId }),
      ...(attemptId === undefined ? {} : { attemptId }),
      cause,
    });

export const layer = Layer.effect(
  GoalWorkspaceService,
  Effect.gen(function* () {
    const goals = yield* GoalProjectionStore;
    const git = yield* GitVcsDriver;
    const events = yield* EventSinkV2;
    const ids = yield* IdAllocatorV2;
    const serial = yield* makeKeyedSerialExecutor<GoalId>();

    const revParse = (cwd: string, ref = "HEAD") =>
      git
        .execute({
          operation: "GoalWorkspaceService.revParse",
          cwd,
          args: ["rev-parse", "--verify", `${ref}^{commit}`],
        })
        .pipe(Effect.map((result) => result.stdout.trim()));

    const ensureWorktree = Effect.fn("GoalWorkspaceService.ensureWorktree")(function* (input: {
      readonly repositoryRoot: string;
      readonly baseSha: string;
      readonly branch: string;
      readonly baseRefName: string;
    }) {
      const listed = yield* git.execute({
        operation: "GoalWorkspaceService.listWorktrees",
        cwd: input.repositoryRoot,
        args: ["worktree", "list", "--porcelain"],
      });
      const existing = parseGitWorktreeList(listed.stdout).find(
        (worktree) => worktree.branch === input.branch,
      );
      if (existing !== undefined) {
        const head = yield* revParse(existing.path);
        return { path: existing.path, branch: input.branch, head };
      }
      const branchResult = yield* git.execute({
        operation: "GoalWorkspaceService.branchExists",
        cwd: input.repositoryRoot,
        args: ["show-ref", "--verify", `refs/heads/${input.branch}`],
        allowNonZeroExit: true,
      });
      const branchExists = Number(branchResult.exitCode) === 0;
      const created = yield* git.createWorktree({
        cwd: input.repositoryRoot,
        refName: branchExists ? input.branch : input.baseSha,
        ...(branchExists ? {} : { newRefName: input.branch }),
        baseRefName: input.baseRefName,
        path: null,
      });
      const head = yield* revParse(created.worktree.path);
      return { path: created.worktree.path, branch: input.branch, head };
    });

    const provision = Effect.fn("GoalWorkspaceService.provision")(function* (goalId: GoalId) {
      return yield* serial.withLock(
        goalId,
        Effect.gen(function* () {
          const detail = yield* goals
            .getDetail(goalId)
            .pipe(Effect.mapError(workspaceError("read-goal", goalId)));
          if (
            detail.goal.integrationBranch !== null &&
            detail.goal.integrationWorktreePath !== null &&
            detail.goal.integrationSha !== null
          )
            return detail;
          const repositoryRoot = detail.goal.repositoryRoot;
          const sourceWorkspacePath = detail.goal.sourceWorkspacePath ?? repositoryRoot;
          if (repositoryRoot === undefined || sourceWorkspacePath === undefined) {
            return yield* workspaceError(
              "resolve-repository",
              goalId,
            )("Goal source repository paths are unavailable.");
          }
          const baseSha = yield* revParse(sourceWorkspacePath).pipe(
            Effect.mapError(workspaceError("resolve-base-sha", goalId)),
          );
          const branch = goalBranchName("integration", goalId);
          const worktree = yield* ensureWorktree({
            repositoryRoot,
            baseSha,
            branch,
            baseRefName: "HEAD",
          }).pipe(Effect.mapError(workspaceError("create-integration-worktree", goalId)));
          if (worktree.head !== baseSha) {
            return yield* workspaceError(
              "reconcile-integration-worktree",
              goalId,
            )(`Existing integration worktree is at ${worktree.head}, expected ${baseSha}.`);
          }
          const now = yield* DateTime.now;
          const timestamp = DateTime.formatIso(now);
          const commandId = CommandId.make(`goal-workspace-provision:${goalId}`);
          const updatedGoal = {
            ...detail.goal,
            integrationBranch: branch,
            integrationWorktreePath: worktree.path,
            integrationSha: baseSha,
            verifiedSha: null,
            updatedAt: timestamp,
          };
          const eventId = yield* ids.allocate
            .event({ threadId: detail.goal.rootThreadId, commandId })
            .pipe(Effect.mapError(workspaceError("allocate-event", goalId)));
          const integrationEvent = {
            id: eventId,
            threadId: detail.goal.rootThreadId,
            type: "goal.integration-updated",
            payload: updatedGoal,
            occurredAt: now,
          } satisfies OrchestrationV2DomainEvent;
          yield* events
            .commitCommand({
              commandId,
              threadId: detail.goal.rootThreadId,
              commandType: "goal.workspace.provision",
              acceptedAt: now,
              events: [integrationEvent],
              effects: [],
            })
            .pipe(Effect.mapError(workspaceError("persist-integration-workspace", goalId)));
          return yield* goals
            .getDetail(goalId)
            .pipe(Effect.mapError(workspaceError("read-provisioned-goal", goalId)));
        }),
      );
    });

    const prepareAttempt = Effect.fn("GoalWorkspaceService.prepareAttempt")(function* (input: {
      readonly goalId: GoalId;
      readonly attemptId: GoalAttemptId;
    }) {
      const detail = yield* provision(input.goalId);
      const attempt = detail.attempts.find((candidate) => candidate.id === input.attemptId);
      if (attempt === undefined)
        return yield* workspaceError(
          "read-attempt",
          input.goalId,
          input.attemptId,
        )("Goal attempt does not exist.");
      const node = detail.nodes.find(
        (candidate) =>
          candidate.graphVersionId === attempt.graphVersionId &&
          candidate.node.id === attempt.nodeId,
      );
      if (node === undefined)
        return yield* workspaceError(
          "read-node",
          input.goalId,
          input.attemptId,
        )("Goal attempt node does not exist.");
      const repositoryRoot = detail.goal.repositoryRoot;
      const integrationPath = detail.goal.integrationWorktreePath;
      const baseSha = attempt.baseIntegrationSha ?? detail.goal.integrationSha;
      if (repositoryRoot === undefined || integrationPath === null || baseSha === null)
        return yield* workspaceError(
          "resolve-attempt-base",
          input.goalId,
          input.attemptId,
        )("Goal integration workspace or attempt base SHA is unavailable.");
      if (node.node.workspaceMode === "integration") {
        return {
          path: integrationPath,
          branch: detail.goal.integrationBranch!,
          baseSha,
          sharedReadOnly: false,
        };
      }
      const branch =
        node.node.workspaceMode === "writer"
          ? goalBranchName("worker", input.attemptId)
          : goalBranchName("read", `${input.goalId}:${baseSha}`);
      const worktree = yield* ensureWorktree({
        repositoryRoot,
        baseSha,
        branch,
        baseRefName: detail.goal.integrationBranch ?? "HEAD",
      }).pipe(
        Effect.mapError(workspaceError("create-attempt-worktree", input.goalId, input.attemptId)),
      );
      if (worktree.head !== baseSha)
        return yield* workspaceError(
          "reconcile-attempt-worktree",
          input.goalId,
          input.attemptId,
        )(`Attempt worktree is at ${worktree.head}, expected ${baseSha}.`);
      return {
        path: worktree.path,
        branch,
        baseSha,
        sharedReadOnly: node.node.workspaceMode === "read_only",
      };
    });

    return GoalWorkspaceService.of({ provision, prepareAttempt });
  }),
);
