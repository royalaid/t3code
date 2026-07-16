import {
  classifyThreadOutboxFailure,
  deriveOutboxSettingsCommandId,
  groupQueuedThreadTurns,
  isThreadTurnDeliveryEligible,
  sanitizeThreadOutboxFailure,
  threadOutboxRetryDelayMs,
  type QueuedThreadTurnRecord,
} from "@t3tools/client-runtime/outbox";
import { AsyncResult } from "effect/unstable/reactivity";
import * as Cause from "effect/Cause";
import { useEffect, useRef, useState } from "react";
import { useEnvironments } from "./state/environments";
import { useThreadShells } from "./state/entities";
import { threadEnvironment } from "./state/threads";
import { useAtomCommand } from "./state/use-atom-command";
import {
  getThreadOutboxDiagnostics,
  setThreadOutboxSending,
  useThreadOutboxRecords,
  webThreadOutbox,
} from "./threadOutbox";

const inFlight = new Set<string>();

export function useThreadOutboxDrain(): void {
  const records = useThreadOutboxRecords();
  const threads = useThreadShells();
  const { environments } = useEnvironments();
  const startTurn = useAtomCommand(threadEnvironment.startTurn, { reportFailure: false });
  const updateMetadata = useAtomCommand(threadEnvironment.updateMetadata, { reportFailure: false });
  const setRuntimeMode = useAtomCommand(threadEnvironment.setRuntimeMode, { reportFailure: false });
  const setInteractionMode = useAtomCommand(threadEnvironment.setInteractionMode, {
    reportFailure: false,
  });
  const retries = useRef(new Map<string, number>());
  const retryAfter = useRef(new Map<string, number>());
  const [, wake] = useState(0);

  useEffect(() => {
    const dispatch = async (record: QueuedThreadTurnRecord) => {
      const messageId = record.command.message.messageId;
      inFlight.add(messageId);
      setThreadOutboxSending(messageId, true);
      let accepted = false;
      try {
        const thread = threads.find(
          (candidate) =>
            candidate.environmentId === record.environmentId &&
            candidate.id === record.command.threadId,
        );
        if (thread && record.command.bootstrap?.createThread === undefined) {
          const model = record.command.modelSelection;
          if (
            model &&
            (model.instanceId !== thread.modelSelection.instanceId ||
              model.model !== thread.modelSelection.model ||
              JSON.stringify(model.options ?? null) !==
                JSON.stringify(thread.modelSelection.options ?? null))
          ) {
            const result = await updateMetadata({
              environmentId: record.environmentId,
              input: {
                commandId: deriveOutboxSettingsCommandId(
                  record.command.commandId,
                  "model-selection",
                ),
                threadId: record.command.threadId,
                modelSelection: model,
              },
            });
            if (AsyncResult.isFailure(result)) throw Cause.squash(result.cause);
          }
          if (record.command.runtimeMode !== thread.runtimeMode) {
            const result = await setRuntimeMode({
              environmentId: record.environmentId,
              input: {
                commandId: deriveOutboxSettingsCommandId(record.command.commandId, "runtime-mode"),
                threadId: record.command.threadId,
                runtimeMode: record.command.runtimeMode,
                createdAt: record.command.createdAt,
              },
            });
            if (AsyncResult.isFailure(result)) throw Cause.squash(result.cause);
          }
          if (record.command.interactionMode !== thread.interactionMode) {
            const result = await setInteractionMode({
              environmentId: record.environmentId,
              input: {
                commandId: deriveOutboxSettingsCommandId(
                  record.command.commandId,
                  "interaction-mode",
                ),
                threadId: record.command.threadId,
                interactionMode: record.command.interactionMode,
                createdAt: record.command.createdAt,
              },
            });
            if (AsyncResult.isFailure(result)) throw Cause.squash(result.cause);
          }
        }
        const { type: _, ...input } = record.command;
        const result = await startTurn({ environmentId: record.environmentId, input });
        if (AsyncResult.isFailure(result)) {
          const error = Cause.squash(result.cause);
          if (
            classifyThreadOutboxFailure({
              error,
              interrupted: Cause.hasInterruptsOnly(result.cause),
            }) === "permanent"
          ) {
            await webThreadOutbox.update({
              ...record,
              deliveryState: "failed",
              failureMessage: sanitizeThreadOutboxFailure(error),
            });
            return;
          }
          throw error;
        }
        accepted = true;
        retries.current.delete(messageId);
        retryAfter.current.delete(messageId);
        await webThreadOutbox.remove(messageId);
        console.info("[thread-outbox] message accepted", {
          environmentId: record.environmentId,
          threadId: record.command.threadId,
          messageId,
          queueAgeMs: Math.max(0, Date.now() - Date.parse(record.command.createdAt)),
          ...getThreadOutboxDiagnostics(),
        });
      } catch (error) {
        if (!accepted && classifyThreadOutboxFailure({ error }) === "permanent") {
          try {
            await webThreadOutbox.update({
              ...record,
              deliveryState: "failed",
              failureMessage: sanitizeThreadOutboxFailure(error),
            });
          } catch (storageError) {
            console.warn("[thread-outbox] could not persist permanent failure", {
              environmentId: record.environmentId,
              threadId: record.command.threadId,
              messageId,
              storageError,
            });
          }
          return;
        }
        const attempt = (retries.current.get(messageId) ?? 0) + 1;
        retries.current.set(messageId, attempt);
        const delay = threadOutboxRetryDelayMs(attempt);
        retryAfter.current.set(messageId, Date.now() + delay);
        console.warn("[thread-outbox] delivery retry scheduled", {
          environmentId: record.environmentId,
          threadId: record.command.threadId,
          messageId,
          attempt,
          delayMs: delay,
          queueAgeMs: Math.max(0, Date.now() - Date.parse(record.command.createdAt)),
          ...getThreadOutboxDiagnostics(),
        });
        setTimeout(() => wake((value) => value + 1), delay);
      } finally {
        inFlight.delete(messageId);
        setThreadOutboxSending(messageId, false);
      }
    };

    const presentMessageIds = new Set<string>(
      records.map((record) => record.command.message.messageId),
    );
    for (const messageId of retries.current.keys()) {
      if (!presentMessageIds.has(messageId)) retries.current.delete(messageId);
    }
    for (const messageId of retryAfter.current.keys()) {
      if (!presentMessageIds.has(messageId)) retryAfter.current.delete(messageId);
    }

    for (const queue of groupQueuedThreadTurns(records).values()) {
      const record = queue[0];
      if (!record) continue;
      const messageId = record.command.message.messageId;
      if (record.deliveryState === "failed") {
        retries.current.delete(messageId);
        retryAfter.current.delete(messageId);
        continue;
      }
      if (inFlight.has(messageId) || (retryAfter.current.get(messageId) ?? 0) > Date.now())
        continue;
      const connected = environments.some(
        (environment) =>
          environment.environmentId === record.environmentId &&
          environment.connection.phase === "connected",
      );
      const thread = threads.find(
        (candidate) =>
          candidate.environmentId === record.environmentId &&
          candidate.id === record.command.threadId,
      );
      const threadStatus =
        thread?.session?.status === "running" || thread?.session?.status === "starting"
          ? thread.session.status
          : thread
            ? "idle"
            : "missing";
      if (
        !isThreadTurnDeliveryEligible({
          record,
          isFirstForThread: true,
          connected,
          threadStatus,
        }) ||
        (!thread && record.command.bootstrap?.createThread === undefined)
      )
        continue;
      void dispatch(record);
    }
  }, [
    environments,
    records,
    setInteractionMode,
    setRuntimeMode,
    startTurn,
    threads,
    updateMetadata,
  ]);
}
