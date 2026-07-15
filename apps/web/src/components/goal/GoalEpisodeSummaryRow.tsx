import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, GoalEpisodeSummary } from "@t3tools/contracts";
import {
  CheckCircle2,
  ChevronDownIcon,
  ChevronRightIcon,
  CircleSlash,
  XCircle,
} from "lucide-react";

import { selectGoalFacadeRootItems } from "../../goalFacade";
import { useThreadVisibleTurnItems } from "../../state/entities";

function episodeIcon(status: GoalEpisodeSummary["status"]) {
  if (status === "completed") return CheckCircle2;
  if (status === "failed") return XCircle;
  return CircleSlash;
}

function episodeTone(status: GoalEpisodeSummary["status"]): string {
  if (status === "completed") return "text-emerald-600";
  if (status === "failed") return "text-red-600";
  return "text-muted-foreground";
}

/**
 * Renders the root lead's conversation for a finished episode.
 *
 * This component is mounted only while the summary is expanded, so the root
 * thread's transcript is fetched lazily — a source thread with many finished
 * episodes costs nothing until the reader opens one.
 */
function GoalEpisodeTranscript(props: {
  readonly environmentId: EnvironmentId;
  readonly episode: GoalEpisodeSummary;
}) {
  const items = useThreadVisibleTurnItems(
    scopeThreadRef(props.environmentId, props.episode.rootThreadId),
  );
  const conversation = selectGoalFacadeRootItems({ items, goalId: props.episode.goalId });

  if (conversation.length === 0) {
    return (
      <p className="px-2.5 py-2 text-[11px] text-muted-foreground">Loading goal transcript…</p>
    );
  }

  return (
    <div
      className="flex flex-col gap-2 px-2.5 py-2"
      data-goal-episode-transcript={props.episode.goalId}
    >
      {conversation.map((row) => {
        const { item } = row;
        const text =
          item.type === "user_message" || item.type === "assistant_message"
            ? item.text
            : item.type === "error"
              ? item.failure.message
              : (item.title ?? item.type.replaceAll("_", " "));
        return (
          <div key={item.id} className="flex flex-col gap-0.5">
            <span className="text-[10px] uppercase tracking-wide text-muted-foreground/70">
              {item.type === "user_message" ? "You" : item.type === "error" ? "Error" : "Goal"}
            </span>
            <p className="whitespace-pre-wrap break-words text-xs text-foreground/85">{text}</p>
          </div>
        );
      })}
    </div>
  );
}

/**
 * A finished goal episode, collapsed to a single summary block in the source
 * timeline. The objective itself is rendered separately as the author's own
 * message, so this block carries only the outcome.
 */
export function GoalEpisodeSummaryRow(props: {
  readonly episode: GoalEpisodeSummary;
  readonly expanded: boolean;
  readonly environmentId: EnvironmentId;
  readonly onToggle: () => void;
}) {
  const Icon = episodeIcon(props.episode.status);
  const Chevron = props.expanded ? ChevronDownIcon : ChevronRightIcon;

  return (
    <div
      className="rounded-xl border border-border/60 bg-muted/20"
      data-goal-episode-id={props.episode.goalId}
      data-goal-episode-status={props.episode.status}
      data-goal-episode-expanded={props.expanded}
    >
      <button
        type="button"
        aria-expanded={props.expanded}
        data-scroll-anchor-ignore
        onClick={props.onToggle}
        className="flex w-full cursor-pointer select-none items-center gap-2 rounded-xl px-2.5 py-2 text-left transition-colors hover:bg-muted/35 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70"
      >
        <Chevron className="size-3.5 shrink-0 text-muted-foreground" />
        <Icon className={`size-3.5 shrink-0 ${episodeTone(props.episode.status)}`} />
        <span className="min-w-0 flex-1 truncate text-xs font-medium text-foreground/80">
          Goal {props.episode.status}
        </span>
        <span className="shrink-0 text-[10px] text-muted-foreground">
          {props.episode.verified ? "verified · " : ""}revision {props.episode.currentRevision}
        </span>
      </button>
      {props.episode.sourceResult?.summary ? (
        <p className="px-8 pb-2 text-[11px] leading-relaxed text-muted-foreground">
          {props.episode.sourceResult.summary}
        </p>
      ) : null}
      {props.expanded ? (
        <div className="border-t border-border/60">
          <GoalEpisodeTranscript environmentId={props.environmentId} episode={props.episode} />
        </div>
      ) : null}
    </div>
  );
}
