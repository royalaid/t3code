import { useCallback } from "react";

import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import { mapAtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { parseGoalComposerCommand } from "@t3tools/client-runtime/state/thread-workflows";
import {
  CommandId,
  MessageId,
  ThreadId,
  type ModelSelection,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";

import { threadEnvironment } from "../../state/threads";
import type { DraftComposerImageAttachment } from "../../lib/composerImages";
import { makeTurnCommandMetadata, type TurnCommandMetadata } from "../../lib/commandMetadata";
import {
  buildProjectThreadStartTurnInput,
  deriveThreadTitleFromPrompt,
} from "../../lib/projectThreadStartTurn";
import { randomHex } from "../../lib/uuid";
import { useAtomCommand } from "../../state/use-atom-command";
import { setPendingConnectionError } from "../../state/use-remote-environment-registry";
import { validateProjectThreadCreation } from "./projectThreadCreationValidation";

export function useCreateProjectThread() {
  const startTurn = useAtomCommand(threadEnvironment.startTurn, { reportFailure: false });
  const provisionGoalSource = useAtomCommand(threadEnvironment.provisionGoalSource, {
    reportFailure: false,
  });
  const launchGoal = useAtomCommand(threadEnvironment.launchGoal, { reportFailure: false });

  return useCallback(
    async (input: {
      readonly project: EnvironmentProject;
      readonly modelSelection: ModelSelection;
      readonly envMode: "local" | "worktree";
      readonly branch: string | null;
      readonly worktreePath: string | null;
      readonly startFromOrigin?: boolean;
      readonly runtimeMode: RuntimeMode;
      readonly interactionMode: ProviderInteractionMode;
      readonly initialMessageText: string;
      readonly initialAttachments: ReadonlyArray<DraftComposerImageAttachment>;
      /** Reuse identifiers from a queued pending task instead of minting new ones. */
      readonly turnMetadata?: TurnCommandMetadata;
    }) => {
      const metadata = input.turnMetadata ?? makeTurnCommandMetadata();
      const threadId = ThreadId.make(metadata.threadId);
      const initialMessageText = input.initialMessageText.trim();

      const validationError = validateProjectThreadCreation({
        environmentId: input.project.environmentId,
        projectId: input.project.id,
        environmentMode: input.envMode,
        branch: input.branch,
        initialMessageText,
      });
      if (validationError !== null) {
        setPendingConnectionError(validationError.message);
        return AsyncResult.failure(Cause.fail(validationError));
      }

      const goalCommand = parseGoalComposerCommand(initialMessageText);
      if (goalCommand !== null) {
        const rootThreadId = ThreadId.make(`goal-root:${metadata.commandId}`);
        const worktreeBranch = buildTemporaryWorktreeBranchName(() =>
          metadata.commandId
            .replace(/[^a-f0-9]/giu, "")
            .slice(-8)
            .padEnd(8, "0"),
        );
        const provisioned = await provisionGoalSource({
          environmentId: input.project.environmentId,
          input: {
            commandId: CommandId.make(`${metadata.commandId}:source`),
            creationSource: "mobile",
            threadId,
            projectId: input.project.id,
            title: deriveThreadTitleFromPrompt(goalCommand.objective),
            modelSelection: input.modelSelection,
            runtimeMode: input.runtimeMode,
            interactionMode: input.interactionMode,
            branch: input.branch,
            worktreePath: input.worktreePath,
            workspaceStrategy:
              input.envMode === "worktree"
                ? {
                    type: "worktree",
                    baseRef: input.branch!,
                    branch: worktreeBranch,
                    ...(input.startFromOrigin ? { startFromOrigin: true } : {}),
                  }
                : input.worktreePath === null
                  ? { type: "root", ...(input.branch === null ? {} : { branch: input.branch }) }
                  : {
                      type: "existing_worktree",
                      worktreePath: input.worktreePath,
                      ...(input.branch === null ? {} : { branch: input.branch }),
                    },
          },
        });
        if (AsyncResult.isFailure(provisioned)) return provisioned;
        const launched = await launchGoal({
          environmentId: input.project.environmentId,
          input: {
            commandId: CommandId.make(metadata.commandId),
            creationSource: "mobile",
            threadId,
            rootThreadId,
            objective: goalCommand.objective,
            messageId: MessageId.make(metadata.messageId),
            attachments: input.initialAttachments,
            selectedContextText: [],
          },
        });
        if (AsyncResult.isFailure(launched)) return launched;
        setPendingConnectionError(null);
        return mapAtomCommandResult(launched, () =>
          scopeThreadRef(input.project.environmentId, rootThreadId),
        );
      }

      const result = await startTurn({
        environmentId: input.project.environmentId,
        input: buildProjectThreadStartTurnInput({
          projectId: input.project.id,
          projectCwd: input.project.workspaceRoot,
          threadId: metadata.threadId,
          commandId: metadata.commandId,
          messageId: metadata.messageId,
          createdAt: metadata.createdAt,
          text: initialMessageText,
          attachments: input.initialAttachments,
          modelSelection: input.modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: input.interactionMode,
          workspaceMode: input.envMode,
          branch: input.branch,
          worktreePath: input.worktreePath,
          startFromOrigin: input.startFromOrigin ?? false,
          worktreeBranchName: buildTemporaryWorktreeBranchName(randomHex),
        }),
      });
      if (AsyncResult.isFailure(result)) {
        const error = Cause.squash(result.cause);
        setPendingConnectionError(
          error instanceof Error ? error.message : "The task could not be started.",
        );
        return AsyncResult.failure(result.cause);
      }
      setPendingConnectionError(null);

      return mapAtomCommandResult(result, () =>
        scopeThreadRef(input.project.environmentId, threadId),
      );
    },
    [launchGoal, provisionGoalSource, startTurn],
  );
}
