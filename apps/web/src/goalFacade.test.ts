import {
  GoalId,
  MessageId,
  NodeId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type GoalEpisodeSummary,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  composeSourceGoalTimeline,
  deriveGoalFacadePendingRequests,
  goalObjectiveEntry,
  goalRootHandoffMessageId,
  resolveGoalFacadeRouting,
  resolveSourceGoalFacade,
  selectGoalFacadeRootItems,
} from "./goalFacade";
import type { TimelineEntry } from "./session-logic";

const goalId = GoalId.make("goal:facade-test");
const rootThreadId = ThreadId.make("thread:facade-root");
const sourceThreadId = ThreadId.make("thread:facade-source");

function episode(
  status: GoalEpisodeSummary["status"],
  overrides: Partial<GoalEpisodeSummary> = {},
): GoalEpisodeSummary {
  return {
    goalId,
    rootThreadId,
    objective: "Ship the source-owned facade",
    status,
    currentRevision: 1,
    readyCount: 0,
    runningCount: status === "running" ? 1 : 0,
    blockedCount: 0,
    attentionRequired: false,
    verified: status === "completed",
    sourceResult: null,
    createdAt: "2026-07-15T00:01:00.000Z",
    updatedAt: "2026-07-15T00:02:00.000Z",
    ...overrides,
  };
}

function sourceMessage(id: string, text: string, createdAt: string): TimelineEntry {
  return {
    id: MessageId.make(id),
    kind: "message",
    createdAt,
    message: {
      id: MessageId.make(id),
      role: "user",
      text,
      runId: null,
      streaming: false,
      createdAt,
      updatedAt: createdAt,
    },
  };
}

function projectedItem(item: OrchestrationV2TurnItem): OrchestrationV2ProjectedTurnItem {
  return {
    position: item.ordinal,
    visibility: "local",
    sourceThreadId: item.threadId,
    sourceItemId: item.id,
    item,
  };
}

function itemBase(id: string, ordinal: number) {
  const now = DateTime.makeUnsafe("2026-07-15T00:01:30.000Z");
  return {
    id: TurnItemId.make(id),
    threadId: rootThreadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
  };
}

describe("source-owned goal facade", () => {
  it("routes every active conversational control to the root and returns terminal chat to source", () => {
    const active = episode("running");
    expect(resolveGoalFacadeRouting({ sourceThreadId, activeEpisode: active })).toEqual({
      targetThreadId: rootThreadId,
      isGoalActive: true,
    });

    const terminalFacade = resolveSourceGoalFacade({
      activeGoalId: goalId,
      episodes: [episode("completed")],
    });
    expect(terminalFacade.activeEpisode).toBeNull();
    expect(
      resolveGoalFacadeRouting({
        sourceThreadId,
        activeEpisode: terminalFacade.activeEpisode,
      }),
    ).toEqual({ targetThreadId: sourceThreadId, isGoalActive: false });
  });

  it("hides the deterministic handoff and internal plumbing from root output", () => {
    const handoff = {
      ...itemBase("item-handoff", 0),
      type: "user_message" as const,
      messageId: goalRootHandoffMessageId(goalId),
      inputIntent: "turn_start" as const,
      text: "BEGIN UNTRUSTED GOAL TASK DATA",
      attachments: [],
      createdBy: "system" as const,
      creationSource: "server" as const,
    } satisfies OrchestrationV2TurnItem;
    const coordinator = {
      ...itemBase("item-coordinator", 1),
      type: "assistant_message" as const,
      messageId: MessageId.make("message:coordinator"),
      text: "Both writers are running.",
      streaming: false,
    } satisfies OrchestrationV2TurnItem;
    const plumbing = {
      ...itemBase("item-tool", 2),
      type: "command_execution" as const,
      input: "goal_read",
      output: "revision 1",
      exitCode: 0,
    } satisfies OrchestrationV2TurnItem;

    const selected = selectGoalFacadeRootItems({
      goalId,
      items: [projectedItem(handoff), projectedItem(coordinator), projectedItem(plumbing)],
    });
    expect(selected.map((row) => row.item.id)).toEqual([coordinator.id]);
  });

  it("surfaces worker approvals through the root-owned facade without duplicating requests", () => {
    const requestId = RuntimeRequestId.make("request:worker-approval");
    const now = DateTime.makeUnsafe("2026-07-15T00:01:30.000Z");
    const workerProjection = {
      runtimeRequests: [
        {
          id: requestId,
          nodeId: NodeId.make("node:worker-approval"),
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "command",
          status: "pending",
          responseCapability: { type: "not_resumable", reason: "provider disconnected" },
          createdAt: now,
          resolvedAt: null,
        },
      ],
      turnItems: [
        {
          ...itemBase("item-worker-approval", 0),
          threadId: ThreadId.make("thread:worker"),
          nodeId: NodeId.make("node:worker-approval"),
          type: "approval_request",
          requestId,
          requestKind: "command",
          prompt: "Allow the worker command?",
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;

    expect(deriveGoalFacadePendingRequests([workerProjection, workerProjection]).approvals).toEqual(
      [
        {
          requestId,
          requestKind: "command",
          createdAt: "2026-07-15T00:01:30.000Z",
          detail: "Allow the worker command?",
          responseCapability: "not_resumable",
        },
      ],
    );
  });

  it("renders a normal /goal entry and keeps sequential episodes chronological", () => {
    const first = episode("completed", {
      goalId: GoalId.make("goal:first"),
      rootThreadId: ThreadId.make("thread:first-root"),
      objective: "First objective",
      createdAt: "2026-07-15T00:01:00.000Z",
      updatedAt: "2026-07-15T00:02:00.000Z",
    });
    const second = episode("running", {
      goalId: GoalId.make("goal:second"),
      rootThreadId: ThreadId.make("thread:second-root"),
      objective: "Second objective",
      createdAt: "2026-07-15T00:04:00.000Z",
      updatedAt: "2026-07-15T00:05:00.000Z",
    });
    const facade = resolveSourceGoalFacade({
      activeGoalId: second.goalId,
      episodes: [second, first],
    });
    const rootReply = sourceMessage(
      "message:root-reply",
      "Working on the second objective.",
      "2026-07-15T00:05:00.000Z",
    );
    const composed = composeSourceGoalTimeline({
      sourceEntries: [
        sourceMessage("message:before", "Before", "2026-07-15T00:00:00.000Z"),
        sourceMessage("message:between", "Between", "2026-07-15T00:03:00.000Z"),
      ],
      facade,
      activeRootEntries: [rootReply],
    });

    expect(composed.map((entry) => entry.kind)).toEqual([
      "message",
      "message",
      "goal-episode",
      "message",
      "message",
      "message",
    ]);
    expect(composed.map((entry) => entry.id)).toEqual([
      MessageId.make("message:before"),
      MessageId.make("goal-objective:goal:first"),
      "goal-episode:goal:first",
      MessageId.make("message:between"),
      MessageId.make("goal-objective:goal:second"),
      MessageId.make("message:root-reply"),
    ]);
    expect(goalObjectiveEntry(second)).toMatchObject({
      message: { text: "/goal Second objective" },
    });
  });
});
