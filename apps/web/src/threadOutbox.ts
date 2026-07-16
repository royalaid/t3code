import {
  createThreadOutboxManager,
  type QueuedThreadTurnRecord,
  type ThreadOutboxStore,
} from "@t3tools/client-runtime/outbox";
import { useSyncExternalStore } from "react";
import { openWebThreadOutboxStore } from "./connection/storage";

const listeners = new Set<() => void>();
let subscribed = false;
const resolveStore = async () => {
  const store = await openWebThreadOutboxStore();
  if (!subscribed) {
    subscribed = true;
    store.subscribe?.(() => listeners.forEach((listener) => listener()));
  }
  return store;
};
const lazyStore: ThreadOutboxStore = {
  load: async () => (await resolveStore()).load(),
  write: async (record) => (await resolveStore()).write(record),
  update: async (record) => (await resolveStore()).update(record),
  remove: async (messageId) => (await resolveStore()).remove(messageId),
  clearEnvironment: async (environmentId) => (await resolveStore()).clearEnvironment(environmentId),
  subscribe: (listener) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};
const manager = createThreadOutboxManager(lazyStore);

function diagnostics(now = Date.now()) {
  const records = manager.getSnapshot();
  const oldestCreatedAt = records.reduce<number | null>((oldest, record) => {
    const createdAt = Date.parse(record.command.createdAt);
    return oldest === null || createdAt < oldest ? createdAt : oldest;
  }, null);
  return {
    queueDepth: records.length,
    oldestItemAgeMs: oldestCreatedAt === null ? 0 : Math.max(0, now - oldestCreatedAt),
  };
}

export const webThreadOutbox = {
  ...manager,
  enqueue: async (record: QueuedThreadTurnRecord) => {
    await manager.enqueue(record);
    console.info("[thread-outbox] message enqueued", {
      environmentId: record.environmentId,
      threadId: record.command.threadId,
      messageId: record.command.message.messageId,
      ...diagnostics(),
    });
  },
  update: async (record: QueuedThreadTurnRecord) => {
    await manager.update(record);
    if (record.deliveryState === "failed") {
      console.warn("[thread-outbox] permanent delivery failure", {
        environmentId: record.environmentId,
        threadId: record.command.threadId,
        messageId: record.command.message.messageId,
        ...diagnostics(),
      });
    }
  },
};
void webThreadOutbox.load().catch((error) => {
  console.warn("[thread-outbox] initial load failed", { error });
});

export function getThreadOutboxDiagnostics(): {
  readonly queueDepth: number;
  readonly oldestItemAgeMs: number;
} {
  return diagnostics();
}

let sendingMessageIds: ReadonlySet<string> = new Set();
const sendingListeners = new Set<() => void>();
export function setThreadOutboxSending(messageId: string, sending: boolean): void {
  const next = new Set(sendingMessageIds);
  if (sending) next.add(messageId);
  else next.delete(messageId);
  sendingMessageIds = next;
  sendingListeners.forEach((listener) => listener());
}

export function useThreadOutboxSendingIds(): ReadonlySet<string> {
  return useSyncExternalStore(
    (listener) => {
      sendingListeners.add(listener);
      return () => sendingListeners.delete(listener);
    },
    () => sendingMessageIds,
    () => sendingMessageIds,
  );
}

export function useThreadOutboxRecords(): ReadonlyArray<QueuedThreadTurnRecord> {
  return useSyncExternalStore(
    webThreadOutbox.subscribe,
    webThreadOutbox.getSnapshot,
    webThreadOutbox.getSnapshot,
  );
}
