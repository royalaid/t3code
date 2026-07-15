import {
  type GoalEpisodeSummary,
  type GoalId,
  type GoalLifecycleStatus,
  type GoalSurface,
  MessageId,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2TurnItem,
  type ThreadId,
} from "@t3tools/contracts";

import type { TimelineEntry } from "./session-logic";

/**
 * Source-owned goal facade.
 *
 * A `/goal` launch stays on the source thread. The root lead coordinator runs in
 * its own durable thread, and the source view presents that thread's live output
 * as an ordinary conversation. Everything here is pure so the composition,
 * hiding, routing, and terminal-return rules stay testable without a server and
 * behave identically across restarts, reconnects, and partial streams.
 *
 * Root items are always passed through by reference — this module never copies a
 * durable event into the source thread.
 */

const TERMINAL_GOAL_STATUSES = new Set<GoalLifecycleStatus>(["completed", "failed", "cancelled"]);

export function isTerminalGoalStatus(status: GoalLifecycleStatus): boolean {
  return TERMINAL_GOAL_STATUSES.has(status);
}

/**
 * Deterministic id of the root lead's initial handoff message.
 *
 * `initialRootMessageId` in `apps/server/src/orchestration-v2/GoalLaunchService.ts`
 * is the source of truth for this format. The id is durable and content-free, so
 * hiding the handoff by id stays correct across restarts and partial streams —
 * unlike matching on message text or on "the first user message".
 */
export function goalRootHandoffMessageId(goalId: GoalId): MessageId {
  return MessageId.make(`goal-root-message:${goalId}`);
}

/** Stable id for the synthetic objective message rendered on the source thread. */
export function goalObjectiveMessageId(goalId: GoalId): MessageId {
  return MessageId.make(`goal-objective:${goalId}`);
}

/** Stable id for an episode's collapsed summary entry. */
export function goalEpisodeEntryId(goalId: GoalId): string {
  return `goal-episode:${goalId}`;
}

/**
 * The source timeline shows the coordinator as a normal conversation, so only
 * conversational output crosses the facade. This is an allowlist on purpose:
 * goal, tool, and workspace plumbing is the overwhelming majority of a root
 * lead's transcript, and an allowlist keeps *new* plumbing item types hidden by
 * default instead of leaking into the source thread the day they are added.
 */
const FACADE_CONVERSATIONAL_ITEM_TYPES = new Set<OrchestrationV2TurnItem["type"]>([
  "user_message",
  "assistant_message",
  "error",
  "approval_request",
  "user_input_request",
]);

export function isGoalFacadeVisibleRootItem(input: {
  readonly item: OrchestrationV2TurnItem;
  readonly goalId: GoalId;
}): boolean {
  const { item } = input;
  // The handoff prompt is trusted plumbing addressed to the coordinator, not
  // something the source author wrote or should read back.
  if (item.type === "user_message" && item.messageId === goalRootHandoffMessageId(input.goalId)) {
    return false;
  }
  return FACADE_CONVERSATIONAL_ITEM_TYPES.has(item.type);
}

/** Filters root items down to the facade's conversational surface, by reference. */
export function selectGoalFacadeRootItems(input: {
  readonly items: ReadonlyArray<OrchestrationV2ProjectedTurnItem>;
  readonly goalId: GoalId;
}): ReadonlyArray<OrchestrationV2ProjectedTurnItem> {
  return input.items.filter((row) =>
    isGoalFacadeVisibleRootItem({ item: row.item, goalId: input.goalId }),
  );
}

export interface SourceGoalFacade {
  /** The episode currently driving the source view, or null when none is live. */
  readonly activeEpisode: GoalEpisodeSummary | null;
  /** Terminal episodes, oldest first — rendered as collapsed summaries. */
  readonly completedEpisodes: ReadonlyArray<GoalEpisodeSummary>;
  /** Every episode owned by this source thread, oldest first. */
  readonly episodes: ReadonlyArray<GoalEpisodeSummary>;
  /** Thread whose live output composes into the source timeline. */
  readonly activeRootThreadId: ThreadId | null;
}

const byCreatedAt = (a: GoalEpisodeSummary, b: GoalEpisodeSummary): number =>
  a.createdAt === b.createdAt ? a.goalId.localeCompare(b.goalId) : a.createdAt < b.createdAt ? -1 : 1;

/**
 * Resolves the facade state from the source thread's server-owned goal surface.
 *
 * `activeGoalId` is only honored when it still points at a non-terminal episode.
 * A goal that reaches a terminal status therefore returns the source view to
 * ordinary chat immediately, even if a surface update is momentarily stale.
 */
export function resolveSourceGoalFacade(
  goalSurface: GoalSurface | null | undefined,
): SourceGoalFacade {
  const episodes = [...(goalSurface?.episodes ?? [])].sort(byCreatedAt);
  const candidate =
    goalSurface?.activeGoalId == null
      ? null
      : (episodes.find((episode) => episode.goalId === goalSurface.activeGoalId) ?? null);
  const activeEpisode = candidate !== null && !isTerminalGoalStatus(candidate.status) ? candidate : null;
  return {
    activeEpisode,
    completedEpisodes: episodes.filter((episode) => isTerminalGoalStatus(episode.status)),
    episodes,
    activeRootThreadId: activeEpisode?.rootThreadId ?? null,
  };
}

export interface GoalFacadeRouting {
  /**
   * Thread that owns turn control from the source view: follow-ups, Queue,
   * Steer, Stop, approvals, pending user input, and queued runs.
   */
  readonly targetThreadId: ThreadId;
  /** True while a goal episode owns the composer. */
  readonly isGoalActive: boolean;
}

/**
 * Single source of truth for "where do this view's controls go?".
 *
 * While an episode is live every control targets the root run; once the episode
 * reaches a terminal status `activeEpisode` is null and the composer falls back
 * to ordinary source chat. Both behaviors follow from one branch, so they cannot
 * drift apart.
 */
export function resolveGoalFacadeRouting(input: {
  readonly sourceThreadId: ThreadId;
  readonly activeEpisode: GoalEpisodeSummary | null;
}): GoalFacadeRouting {
  return input.activeEpisode === null
    ? { targetThreadId: input.sourceThreadId, isGoalActive: false }
    : { targetThreadId: input.activeEpisode.rootThreadId, isGoalActive: true };
}

/**
 * The launch rendered the way the author wrote it: a plain user message carrying
 * the objective. It has no `projectedItem`, so the timeline renders it without
 * provenance badges — the facade's whole point is that it looks normal.
 */
export function goalObjectiveEntry(episode: GoalEpisodeSummary): TimelineEntry {
  return {
    id: goalObjectiveMessageId(episode.goalId),
    kind: "message",
    createdAt: episode.createdAt,
    message: {
      id: goalObjectiveMessageId(episode.goalId),
      role: "user",
      text: episode.objective,
      runId: null,
      streaming: false,
      createdAt: episode.createdAt,
      updatedAt: episode.updatedAt,
    },
  };
}

export function goalEpisodeEntry(episode: GoalEpisodeSummary): TimelineEntry {
  return {
    id: goalEpisodeEntryId(episode.goalId),
    kind: "goal-episode",
    createdAt: episode.createdAt,
    episode,
  };
}

/**
 * Builds the entries contributed by one episode: the synthetic objective, then
 * either the live root conversation (active) or a collapsed summary (terminal).
 * A terminal episode's transcript is deliberately not included — it loads only
 * when the reader expands the summary.
 */
function goalEpisodeEntries(input: {
  readonly episode: GoalEpisodeSummary;
  readonly activeEpisode: GoalEpisodeSummary | null;
  readonly activeRootEntries: ReadonlyArray<TimelineEntry>;
}): ReadonlyArray<TimelineEntry> {
  const isActive = input.activeEpisode?.goalId === input.episode.goalId;
  return [
    goalObjectiveEntry(input.episode),
    ...(isActive ? input.activeRootEntries : [goalEpisodeEntry(input.episode)]),
  ];
}

/**
 * Merges episode blocks into the source thread's own entries.
 *
 * Source order is never disturbed — entries keep the exact order the server
 * committed them in. Each episode block is spliced in at its launch time, which
 * keeps multi-episode threads (chat → goal → chat → goal) chronological.
 */
export function composeSourceGoalTimeline(input: {
  readonly sourceEntries: ReadonlyArray<TimelineEntry>;
  readonly facade: SourceGoalFacade;
  readonly activeRootEntries: ReadonlyArray<TimelineEntry>;
}): TimelineEntry[] {
  if (input.facade.episodes.length === 0) return [...input.sourceEntries];

  const blocks = input.facade.episodes.map((episode) => ({
    createdAt: episode.createdAt,
    entries: goalEpisodeEntries({
      episode,
      activeEpisode: input.facade.activeEpisode,
      activeRootEntries: input.activeRootEntries,
    }),
  }));

  const composed: TimelineEntry[] = [];
  let next = 0;
  for (const entry of input.sourceEntries) {
    while (next < blocks.length && blocks[next]!.createdAt <= entry.createdAt) {
      composed.push(...blocks[next]!.entries);
      next += 1;
    }
    composed.push(entry);
  }
  for (; next < blocks.length; next += 1) {
    composed.push(...blocks[next]!.entries);
  }
  return composed;
}

/** Bounded, human-readable progress for a live episode. */
export function goalEpisodeProgressLabel(episode: GoalEpisodeSummary): string {
  const status = episode.status.replaceAll("_", " ");
  const parts = [
    episode.runningCount > 0 ? `${episode.runningCount} running` : null,
    episode.readyCount > 0 ? `${episode.readyCount} ready` : null,
    episode.blockedCount > 0 ? `${episode.blockedCount} blocked` : null,
  ].filter((part): part is string => part !== null);
  return parts.length === 0 ? status : `${status} · ${parts.join(" · ")}`;
}
