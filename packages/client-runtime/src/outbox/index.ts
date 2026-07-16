import { isTransportConnectionErrorMessage } from "../errors/transport.ts";
import {
  ClientThreadTurnStartCommand,
  CommandId,
  EnvironmentId,
  MessageId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const THREAD_OUTBOX_SCHEMA_VERSION = 1;
export const THREAD_OUTBOX_MAX_MESSAGES = 100;

export const QueuedThreadTurnRecord = Schema.Struct({
  schemaVersion: Schema.Literal(THREAD_OUTBOX_SCHEMA_VERSION),
  environmentId: EnvironmentId,
  command: ClientThreadTurnStartCommand,
  deliveryState: Schema.Literals(["queued", "failed"]),
  failureMessage: Schema.optional(Schema.String),
});
export type QueuedThreadTurnRecord = typeof QueuedThreadTurnRecord.Type;

export interface ThreadOutboxStore {
  readonly load: () => Promise<ReadonlyArray<QueuedThreadTurnRecord>>;
  readonly write: (record: QueuedThreadTurnRecord) => Promise<void>;
  readonly update: (record: QueuedThreadTurnRecord) => Promise<void>;
  readonly remove: (messageId: MessageId) => Promise<void>;
  readonly clearEnvironment: (environmentId: EnvironmentId) => Promise<void>;
  readonly subscribe?: (listener: () => void) => () => void;
}

export type ThreadOutboxFailureKind = "transient" | "permanent";

function errorMessage(error: unknown): string | null {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  if (typeof error === "object" && error !== null && "message" in error) {
    return typeof error.message === "string" ? error.message : null;
  }
  return null;
}

export function classifyThreadOutboxFailure(input: {
  readonly error: unknown;
  readonly interrupted?: boolean;
}): ThreadOutboxFailureKind {
  if (input.interrupted) return "transient";
  if (typeof input.error === "object" && input.error !== null && "_tag" in input.error) {
    const tag = input.error._tag;
    if (tag === "ConnectionTransientError" || tag === "RpcClientError" || tag === "SocketError") {
      return "transient";
    }
  }
  return isTransportConnectionErrorMessage(errorMessage(input.error)) ? "transient" : "permanent";
}

export function sanitizeThreadOutboxFailure(error: unknown): string {
  const message = errorMessage(error)?.replaceAll(/\s+/g, " ").trim();
  return (message || "The server rejected this message.").slice(0, 500);
}

export function threadOutboxRetryDelayMs(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.max(0, attempt - 1), 16_000);
}

export function compareQueuedThreadTurns(
  left: QueuedThreadTurnRecord,
  right: QueuedThreadTurnRecord,
): number {
  return (
    left.command.createdAt.localeCompare(right.command.createdAt) ||
    left.command.message.messageId.localeCompare(right.command.message.messageId)
  );
}

export function groupQueuedThreadTurns(
  records: ReadonlyArray<QueuedThreadTurnRecord>,
): ReadonlyMap<ThreadId, ReadonlyArray<QueuedThreadTurnRecord>> {
  const grouped = new Map<ThreadId, QueuedThreadTurnRecord[]>();
  for (const record of records) {
    const queue = grouped.get(record.command.threadId) ?? [];
    queue.push(record);
    grouped.set(record.command.threadId, queue);
  }
  for (const queue of grouped.values()) queue.sort(compareQueuedThreadTurns);
  return grouped;
}

export function isThreadTurnDeliveryEligible(input: {
  readonly record: QueuedThreadTurnRecord;
  readonly isFirstForThread: boolean;
  readonly connected: boolean;
  readonly threadStatus: "idle" | "starting" | "running" | "missing";
}): boolean {
  return (
    input.record.deliveryState === "queued" &&
    input.isFirstForThread &&
    input.connected &&
    input.threadStatus !== "starting" &&
    input.threadStatus !== "running"
  );
}

export function deriveOutboxSettingsCommandId(
  commandId: CommandId,
  setting: "model-selection" | "runtime-mode" | "interaction-mode",
): CommandId {
  return CommandId.make(`${commandId}:${setting}`);
}

export class ThreadOutboxCapacityError extends Error {
  override readonly name = "ThreadOutboxCapacityError";
  readonly limit: number;
  constructor(limit = THREAD_OUTBOX_MAX_MESSAGES) {
    super(`The message queue is full (${limit} messages).`);
    this.limit = limit;
  }
}

export function createThreadOutboxManager(
  store: ThreadOutboxStore,
  options?: { readonly maxMessages?: number },
) {
  let records: ReadonlyArray<QueuedThreadTurnRecord> = [];
  let mutation = Promise.resolve();
  const listeners = new Set<() => void>();
  const maxMessages = options?.maxMessages ?? THREAD_OUTBOX_MAX_MESSAGES;
  const notify = () => listeners.forEach((listener) => listener());
  const serialize = <A>(operation: () => Promise<A>): Promise<A> => {
    const result = mutation.then(operation, operation);
    mutation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };
  const replace = (next: ReadonlyArray<QueuedThreadTurnRecord>) => {
    const byMessageId = new Map<MessageId, QueuedThreadTurnRecord>();
    for (const record of next) byMessageId.set(record.command.message.messageId, record);
    records = [...byMessageId.values()].sort(compareQueuedThreadTurns);
    notify();
  };
  const load = () => serialize(async () => replace(await store.load()));
  const enqueue = (record: QueuedThreadTurnRecord) =>
    serialize(async () => {
      if (
        records.length >= maxMessages &&
        !records.some(
          (candidate) => candidate.command.message.messageId === record.command.message.messageId,
        )
      ) {
        throw new ThreadOutboxCapacityError(maxMessages);
      }
      await store.write(record);
      replace([...records, record]);
    });
  const update = (record: QueuedThreadTurnRecord) =>
    serialize(async () => {
      await store.update(record);
      replace([
        ...records.filter(
          (candidate) => candidate.command.message.messageId !== record.command.message.messageId,
        ),
        record,
      ]);
    });
  const remove = (messageId: MessageId) =>
    serialize(async () => {
      await store.remove(messageId);
      replace(records.filter((record) => record.command.message.messageId !== messageId));
    });
  const clearEnvironment = (environmentId: EnvironmentId) =>
    serialize(async () => {
      await store.clearEnvironment(environmentId);
      replace(records.filter((record) => record.environmentId !== environmentId));
    });
  const unsubscribeStore = store.subscribe?.(() => {
    void load().catch(() => undefined);
  });
  return {
    getSnapshot: () => records,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    load,
    enqueue,
    update,
    remove,
    clearEnvironment,
    dispose: () => unsubscribeStore?.(),
  };
}
