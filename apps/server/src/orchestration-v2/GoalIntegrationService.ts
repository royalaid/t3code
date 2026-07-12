import {
  CommandId,
  GoalArtifactId,
  GoalEvidenceId,
  type GoalDetail,
  type GoalId,
  type GoalNodeProjection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { GitVcsDriver } from "../vcs/GitVcsDriver.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";
import { goalBranchName, parseGitWorktreeList } from "./GoalWorkspaceService.ts";

export class GoalIntegrationError extends Schema.TaggedErrorClass<GoalIntegrationError>()(
  "GoalIntegrationError",
  {
    operation: Schema.String,
    goalId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class GoalIntegrationService extends Context.Service<
  GoalIntegrationService,
  { readonly rescan: (goalId: GoalId) => Effect.Effect<void, GoalIntegrationError> }
>()("t3/orchestration-v2/GoalIntegrationService") {}

const integrationError =
  (operation: string, goalId?: GoalId) =>
  (cause: unknown): GoalIntegrationError =>
    new GoalIntegrationError({
      operation,
      ...(goalId === undefined ? {} : { goalId }),
      cause,
    });

export function hasIndependentAcceptedEvidence(detail: GoalDetail): boolean {
  const sha = detail.goal.integrationSha;
  if (sha === null) return false;
  return detail.evidence.some((evidence) => {
    if (evidence.verdict !== "accepted" || evidence.integrationSha !== sha) return false;
    const verifier = detail.attempts.find((attempt) => attempt.id === evidence.attemptId);
    const producer = detail.attempts.find((attempt) => attempt.id === evidence.producerAttemptId);
    return (
      verifier !== undefined &&
      producer !== undefined &&
      verifier.id !== producer.id &&
      verifier.nodeId !== producer.nodeId &&
      verifier.executionThreadId !== null &&
      producer.executionThreadId !== null &&
      verifier.executionThreadId !== producer.executionThreadId
    );
  });
}

export const layer = Layer.effect(
  GoalIntegrationService,
  Effect.gen(function* () {
    const goals = yield* GoalProjectionStore;
    const events = yield* EventSinkV2;
    const ids = yield* IdAllocatorV2;
    const git = yield* GitVcsDriver;
    const serial = yield* makeKeyedSerialExecutor<GoalId>();

    const commit = Effect.fn("GoalIntegrationService.commit")(function* (input: {
      readonly detail: GoalDetail;
      readonly key: string;
      readonly type: string;
      readonly payloads: ReadonlyArray<{
        readonly type: OrchestrationV2DomainEvent["type"];
        readonly payload: unknown;
      }>;
    }) {
      const now = yield* DateTime.now;
      const commandId = CommandId.make(`goal-integration:${input.key}`);
      const domainEvents = yield* Effect.forEach(input.payloads, (item) =>
        ids.allocate.event({ threadId: input.detail.goal.rootThreadId, commandId }).pipe(
          Effect.map(
            (id) =>
              ({
                id,
                threadId: input.detail.goal.rootThreadId,
                type: item.type,
                payload: item.payload,
                occurredAt: now,
              }) as OrchestrationV2DomainEvent,
          ),
        ),
      );
      yield* events.commitCommand({
        commandId,
        threadId: input.detail.goal.rootThreadId,
        commandType: input.type,
        acceptedAt: now,
        events: domainEvents,
        effects: [],
      });
    });

    const markSimpleNodeSucceeded = Effect.fn("GoalIntegrationService.markSimpleNodeSucceeded")(
      function* (detail: GoalDetail, node: GoalNodeProjection) {
        const attempt =
          detail.attempts.find((candidate) => candidate.id === node.activeAttemptId) ??
          [...detail.attempts]
            .toReversed()
            .find(
              (candidate) =>
                candidate.graphVersionId === node.graphVersionId &&
                candidate.nodeId === node.node.id,
            );
        if (attempt === undefined) return;
        const hasResult = detail.artifacts.some(
          (artifact) => artifact.attemptId === attempt.id && artifact.kind === "result",
        );
        const hasAcceptedEvidence = detail.evidence.some(
          (evidence) => evidence.attemptId === attempt.id && evidence.verdict === "accepted",
        );
        if (node.node.evidenceRequirements.length > 0 ? !hasAcceptedEvidence : !hasResult) return;
        const timestamp = DateTime.formatIso(yield* DateTime.now);
        yield* commit({
          detail,
          key: `node-succeeded:${node.graphVersionId}:${node.node.id}:${attempt.id}`,
          type: "goal.node.processed",
          payloads: [
            {
              type: "goal.node-transitioned",
              payload: { ...node, status: "succeeded", blocker: null, updatedAt: timestamp },
            },
          ],
        });
      },
    );

    const cleanupWriter = Effect.fn("GoalIntegrationService.cleanupWriter")(function* (
      detail: GoalDetail,
      attempt: GoalDetail["attempts"][number],
    ) {
      if (
        detail.goal.repositoryRoot === undefined ||
        attempt.workspacePath === null ||
        attempt.workspacePath === detail.goal.integrationWorktreePath
      )
        return;
      const listed = yield* git.execute({
        operation: "GoalIntegrationService.listWorktreesForCleanup",
        cwd: detail.goal.repositoryRoot,
        args: ["worktree", "list", "--porcelain"],
      });
      const target = attempt.workspacePath.replaceAll("\\", "/").toLowerCase();
      if (
        parseGitWorktreeList(listed.stdout).some(
          (worktree) => worktree.path.replaceAll("\\", "/").toLowerCase() === target,
        )
      )
        yield* git.removeWorktree({
          cwd: detail.goal.repositoryRoot,
          path: attempt.workspacePath,
          force: false,
        });
      yield* git.execute({
        operation: "GoalIntegrationService.deleteIntegratedBranch",
        cwd: detail.goal.repositoryRoot,
        args: ["branch", "-D", goalBranchName("worker", attempt.id)],
        allowNonZeroExit: true,
      });
    });

    const processWriter = Effect.fn("GoalIntegrationService.processWriter")(function* (
      detail: GoalDetail,
      node: GoalNodeProjection,
    ) {
      const attempt = [...detail.attempts]
        .toReversed()
        .find(
          (candidate) =>
            candidate.graphVersionId === node.graphVersionId && candidate.nodeId === node.node.id,
        );
      if (
        attempt === undefined ||
        attempt.workspacePath === null ||
        attempt.baseIntegrationSha === null ||
        detail.goal.integrationWorktreePath === null ||
        detail.goal.integrationSha === null
      )
        return;
      const existing = detail.writerCommits.find((record) => record.attemptId === attempt.id);
      if (existing?.state === "integrated") {
        yield* cleanupWriter(detail, attempt);
        return;
      }
      if (existing?.state === "conflicted") return;
      const status = yield* git.execute({
        operation: "GoalIntegrationService.writerStatus",
        cwd: attempt.workspacePath,
        args: ["status", "--porcelain=v1", "--untracked-files=all"],
      });
      const head = (yield* git.execute({
        operation: "GoalIntegrationService.writerHead",
        cwd: attempt.workspacePath,
        args: ["rev-parse", "--verify", "HEAD^{commit}"],
      })).stdout.trim();
      const count = Number(
        (yield* git.execute({
          operation: "GoalIntegrationService.writerCommitCount",
          cwd: attempt.workspacePath,
          args: ["rev-list", "--count", `${attempt.baseIntegrationSha}..${head}`],
        })).stdout.trim(),
      );
      if (status.stdout.trim() !== "" || count !== 1) {
        const timestamp = DateTime.formatIso(yield* DateTime.now);
        yield* commit({
          detail,
          key: `writer-rejected:${attempt.id}:${head}`,
          type: "goal.writer.reject",
          payloads: [
            {
              type: "goal.node-transitioned",
              payload: {
                ...node,
                status: "blocked",
                blocker: "Writer must publish exactly one clean commit.",
                updatedAt: timestamp,
              },
            },
          ],
        });
        return;
      }
      const timestamp = DateTime.formatIso(yield* DateTime.now);
      const record: GoalDetail["writerCommits"][number] = existing ?? {
        id: GoalArtifactId.make(`goal-writer-commit:${attempt.id}`),
        goalId: detail.goal.id,
        graphVersionId: attempt.graphVersionId,
        nodeId: attempt.nodeId,
        attemptId: attempt.id,
        baseSha: attempt.baseIntegrationSha,
        commitSha: head,
        cleanSingleCommit: true,
        integrationBeforeSha: detail.goal.integrationSha,
        integrationAfterSha: null,
        state: "published",
        createdAt: timestamp,
        updatedAt: timestamp,
      };
      if (existing === undefined)
        yield* commit({
          detail,
          key: `writer-published:${attempt.id}:${head}`,
          type: "goal.writer.publish",
          payloads: [{ type: "goal.writer-commit-recorded", payload: record }],
        });

      const integrationPath = detail.goal.integrationWorktreePath;
      const projectedBefore = detail.goal.integrationSha;
      const actualBefore = (yield* git.execute({
        operation: "GoalIntegrationService.integrationHead",
        cwd: integrationPath,
        args: ["rev-parse", "--verify", "HEAD^{commit}"],
      })).stdout.trim();
      let afterSha: string;
      if (actualBefore !== projectedBefore) {
        const body = (yield* git.execute({
          operation: "GoalIntegrationService.reconcileCherryPick",
          cwd: integrationPath,
          args: ["log", "-1", "--format=%B"],
        })).stdout;
        if (!body.includes(`cherry picked from commit ${head}`))
          return yield* integrationError(
            "integration-head-diverged",
            detail.goal.id,
          )(`Integration HEAD is ${actualBefore}; projection expects ${projectedBefore}.`);
        afterSha = actualBefore;
      } else {
        const cherryPick = yield* git.execute({
          operation: "GoalIntegrationService.cherryPick",
          cwd: integrationPath,
          args: ["cherry-pick", "-x", head],
          allowNonZeroExit: true,
        });
        if (Number(cherryPick.exitCode) !== 0) {
          yield* git.execute({
            operation: "GoalIntegrationService.abortCherryPick",
            cwd: integrationPath,
            args: ["cherry-pick", "--abort"],
            allowNonZeroExit: true,
          });
          const conflicted = { ...record, state: "conflicted" as const, updatedAt: timestamp };
          yield* commit({
            detail,
            key: `writer-conflict:${attempt.id}:${head}`,
            type: "goal.writer.conflict",
            payloads: [
              { type: "goal.writer-commit-recorded", payload: conflicted },
              {
                type: "goal.node-transitioned",
                payload: {
                  ...node,
                  status: "blocked",
                  blocker: cherryPick.stderr.trim().slice(0, 4_000) || "Integration conflict",
                  updatedAt: timestamp,
                },
              },
              {
                type: "goal.integration-conflicted",
                payload: { ...detail.goal, status: "blocked", updatedAt: timestamp },
              },
              {
                type: "goal.failure-recorded",
                payload: {
                  id: GoalEvidenceId.make(`goal-integration-conflict:${attempt.id}`),
                  goalId: detail.goal.id,
                  graphVersionId: attempt.graphVersionId,
                  nodeId: attempt.nodeId,
                  attemptId: attempt.id,
                  reason: {
                    type: "integration_conflict",
                    artifactId: record.id,
                    detail: cherryPick.stderr.trim().slice(0, 4_000) || "Integration conflict",
                  },
                  recoveryState: "unresolved",
                  blocker:
                    "Integration conflict requires a root-lead graph revision with a dedicated resolver node.",
                  occurredAt: timestamp,
                },
              },
            ],
          });
          return;
        }
        afterSha = (yield* git.execute({
          operation: "GoalIntegrationService.integratedHead",
          cwd: integrationPath,
          args: ["rev-parse", "--verify", "HEAD^{commit}"],
        })).stdout.trim();
      }
      const integrated = {
        ...record,
        integrationAfterSha: afterSha,
        state: "integrated" as const,
        updatedAt: timestamp,
      };
      yield* commit({
        detail,
        key: `writer-integrated:${attempt.id}:${afterSha}`,
        type: "goal.writer.integrate",
        payloads: [
          { type: "goal.writer-commit-recorded", payload: integrated },
          {
            type: "goal.integration-updated",
            payload: {
              ...detail.goal,
              status: "running",
              integrationSha: afterSha,
              verifiedSha: null,
              updatedAt: timestamp,
            },
          },
          {
            type: "goal.node-transitioned",
            payload: { ...node, status: "succeeded", blocker: null, updatedAt: timestamp },
          },
        ],
      });
      yield* cleanupWriter(detail, attempt);
    });

    const rescanUnlocked = Effect.fn("GoalIntegrationService.rescanUnlocked")(function* (
      goalId: GoalId,
    ) {
      let detail = yield* goals.getDetail(goalId);
      const activeVersion = detail.goal.currentGraphVersionId;
      if (activeVersion === null) return;
      yield* Effect.forEach(
        detail.writerCommits.filter((record) => record.state === "integrated"),
        (record) => {
          const attempt = detail.attempts.find((candidate) => candidate.id === record.attemptId);
          return attempt === undefined ? Effect.void : cleanupWriter(detail, attempt);
        },
        { concurrency: 1, discard: true },
      );
      const processing = detail.nodes
        .filter((node) => node.graphVersionId === activeVersion && node.status === "processing")
        .sort((left, right) => {
          const leftAttempt = detail.attempts.find((attempt) => attempt.nodeId === left.node.id);
          const rightAttempt = detail.attempts.find((attempt) => attempt.nodeId === right.node.id);
          return (leftAttempt?.createdAt ?? "").localeCompare(rightAttempt?.createdAt ?? "");
        });
      const graph = detail.graphVersions.find((version) => version.id === activeVersion);
      for (const node of processing) {
        const dependencies =
          graph?.edges
            .filter((edge) => edge.toNodeId === node.node.id)
            .map((edge) => edge.fromNodeId) ?? [];
        if (
          dependencies.some(
            (dependency) =>
              !detail.nodes.some(
                (candidate) =>
                  candidate.graphVersionId === activeVersion &&
                  candidate.node.id === dependency &&
                  candidate.status === "succeeded",
              ),
          )
        )
          continue;
        if (node.node.workspaceMode === "writer") yield* processWriter(detail, node);
        else yield* markSimpleNodeSucceeded(detail, node);
        detail = yield* goals.getDetail(goalId);
      }
      const currentNodes = detail.nodes.filter((node) => node.graphVersionId === activeVersion);
      if (currentNodes.length > 0 && currentNodes.every((node) => node.status === "succeeded")) {
        if (!hasIndependentAcceptedEvidence(detail)) return;
        const timestamp = DateTime.formatIso(yield* DateTime.now);
        yield* commit({
          detail,
          key: `complete:${goalId}:${detail.goal.integrationSha}`,
          type: "goal.complete",
          payloads: [
            {
              type: "goal.completed",
              payload: {
                ...detail.goal,
                status: "completed",
                verifiedSha: detail.goal.integrationSha,
                updatedAt: timestamp,
              },
            },
          ],
        });
      }
    });

    const rescan = (goalId: GoalId) =>
      serial
        .withLock(goalId, rescanUnlocked(goalId))
        .pipe(Effect.mapError(integrationError("rescan", goalId)));

    yield* events.stream().pipe(
      Stream.filter((stored: OrchestrationV2StoredEvent) =>
        new Set([
          "goal.node-transitioned",
          "goal.artifact-published",
          "goal.evidence-submitted",
          "goal.verdict-recorded",
        ]).has(stored.event.type),
      ),
      Stream.runForEach((stored) => {
        const event = stored.event;
        const payload = event.payload as { readonly goalId?: GoalId };
        return payload.goalId === undefined
          ? Effect.void
          : rescan(payload.goalId).pipe(
              Effect.catch((cause) =>
                Effect.logError("Goal integration event processing failed", {
                  goalId: payload.goalId,
                  cause,
                }),
              ),
            );
      }),
      Effect.catch((cause) => Effect.logError("Goal integration event consumer failed", { cause })),
      Effect.forkScoped,
    );
    const schedulable = yield* goals.listSchedulable;
    yield* Effect.forEach(schedulable, (detail) => rescan(detail.goal.id), {
      concurrency: 1,
      discard: true,
    });
    return GoalIntegrationService.of({ rescan });
  }),
);
