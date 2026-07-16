import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  QueuedThreadTurnRecord,
  classifyThreadOutboxFailure,
  createThreadOutboxManager,
  groupQueuedThreadTurns,
  isThreadTurnDeliveryEligible,
  threadOutboxRetryDelayMs,
  type ThreadOutboxStore,
} from "./index.ts";

const record = (messageId: string, threadId = "thread-1", createdAt = "2026-07-14T00:00:00.000Z") =>
  ({
    schemaVersion: 1,
    environmentId: EnvironmentId.make("environment-1"),
    deliveryState: "queued",
    command: {
      type: "thread.turn.start",
      commandId: CommandId.make(`command-${messageId}`),
      threadId: ThreadId.make(threadId),
      message: {
        messageId: MessageId.make(messageId),
        role: "user",
        text: "hello",
        attachments: [],
      },
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt,
    },
  }) satisfies QueuedThreadTurnRecord;

describe("thread outbox core", () => {
  it("round-trips complete turn commands and rejects corrupt records", () => {
    const decode = Schema.decodeUnknownSync(QueuedThreadTurnRecord);
    const complete = {
      ...record("message-1"),
      command: {
        ...record("message-1").command,
        message: {
          ...record("message-1").command.message,
          attachments: [
            {
              type: "image" as const,
              name: "diagram.png",
              mimeType: "image/png",
              sizeBytes: 4,
              dataUrl: "data:image/png;base64,dGVzdA==",
            },
          ],
        },
        bootstrap: {
          createThread: {
            projectId: ProjectId.make("project-1"),
            title: "Queued thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5",
            },
            runtimeMode: "full-access" as const,
            interactionMode: "default" as const,
            branch: "main",
            worktreePath: null,
            createdAt: "2026-07-14T00:00:00.000Z",
          },
          prepareWorktree: {
            projectCwd: "C:/repo",
            baseBranch: "main",
            branch: "queued-worktree",
          },
          runSetupScript: true,
        },
        sourceProposedPlan: {
          threadId: ThreadId.make("source-thread"),
          planId: "plan-1",
        },
      },
    } satisfies QueuedThreadTurnRecord;
    expect(decode(complete)).toEqual(complete);
    expect(() => decode({ schemaVersion: 1 })).toThrow();
  });

  it("preserves FIFO ordering independently per thread", () => {
    const later = record("message-2", "thread-1", "2026-07-14T00:00:02.000Z");
    const earlier = record("message-1", "thread-1", "2026-07-14T00:00:01.000Z");
    const other = record("message-3", "thread-2", "2026-07-14T00:00:00.000Z");
    expect(groupQueuedThreadTurns([later, other, earlier]).get(ThreadId.make("thread-1"))).toEqual([
      earlier,
      later,
    ]);
  });

  it("delivers only the FIFO head of an idle connected thread", () => {
    const queued = record("message-1");
    expect(
      isThreadTurnDeliveryEligible({
        record: queued,
        isFirstForThread: true,
        connected: true,
        threadStatus: "idle",
      }),
    ).toBe(true);
    for (const blocked of [
      { isFirstForThread: false, connected: true, threadStatus: "idle" as const },
      { isFirstForThread: true, connected: false, threadStatus: "idle" as const },
      { isFirstForThread: true, connected: true, threadStatus: "starting" as const },
      { isFirstForThread: true, connected: true, threadStatus: "running" as const },
    ]) {
      expect(isThreadTurnDeliveryEligible({ record: queued, ...blocked })).toBe(false);
    }
  });

  it("classifies transport and interruption failures as transient with capped backoff", () => {
    expect(classifyThreadOutboxFailure({ error: new Error("Socket is not connected") })).toBe(
      "transient",
    );
    expect(classifyThreadOutboxFailure({ error: new Error("not authorized") })).toBe("permanent");
    expect(classifyThreadOutboxFailure({ error: null, interrupted: true })).toBe("transient");
    expect([1, 2, 3, 4, 5, 6].map(threadOutboxRetryDelayMs)).toEqual([
      1_000, 2_000, 4_000, 8_000, 16_000, 16_000,
    ]);
  });

  it("persists before publishing manager state and removes idempotently", async () => {
    const stored = new Map<string, QueuedThreadTurnRecord>();
    const store: ThreadOutboxStore = {
      load: async () => [...stored.values()],
      write: async (value) => void stored.set(value.command.message.messageId, value),
      update: async (value) => void stored.set(value.command.message.messageId, value),
      remove: async (messageId) => void stored.delete(messageId),
      clearEnvironment: async (environmentId) => {
        for (const [id, value] of stored)
          if (value.environmentId === environmentId) stored.delete(id);
      },
    };
    const manager = createThreadOutboxManager(store);
    await manager.load();
    await manager.enqueue(record("message-1"));
    expect(manager.getSnapshot()).toEqual([record("message-1")]);
    await manager.remove(MessageId.make("message-1"));
    await manager.remove(MessageId.make("message-1"));
    expect(manager.getSnapshot()).toEqual([]);
  });

  it("rejects enqueue beyond the configured capacity without dropping existing records", async () => {
    const stored = new Map<string, QueuedThreadTurnRecord>();
    const store: ThreadOutboxStore = {
      load: async () => [...stored.values()],
      write: async (value) => void stored.set(value.command.message.messageId, value),
      update: async (value) => void stored.set(value.command.message.messageId, value),
      remove: async (messageId) => void stored.delete(messageId),
      clearEnvironment: async () => undefined,
    };
    const manager = createThreadOutboxManager(store, { maxMessages: 1 });
    await manager.load();
    await manager.enqueue(record("message-1"));
    await expect(manager.enqueue(record("message-2"))).rejects.toThrow("queue is full");
    expect(manager.getSnapshot()).toEqual([record("message-1")]);
    expect([...stored.values()]).toEqual([record("message-1")]);
  });
});
