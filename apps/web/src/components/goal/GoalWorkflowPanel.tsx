import type { GoalDetail } from "@t3tools/contracts";
import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  GitCommit,
  Route,
  ShieldCheck,
} from "lucide-react";

const statusTone: Record<string, string> = {
  blocked: "text-amber-700 dark:text-amber-300",
  failed: "text-red-700 dark:text-red-300",
  succeeded: "text-emerald-700 dark:text-emerald-300",
  completed: "text-emerald-700 dark:text-emerald-300",
  running: "text-sky-700 dark:text-sky-300",
  processing: "text-violet-700 dark:text-violet-300",
};

export function GoalWorkflowPanel({ detail }: { readonly detail: GoalDetail }) {
  const activeVersion = detail.goal.currentGraphVersionId;
  const nodes = detail.nodes.filter((node) => node.graphVersionId === activeVersion);
  const accepted = detail.evidence.filter(
    (evidence) =>
      evidence.verdict === "accepted" && evidence.integrationSha === detail.goal.integrationSha,
  );
  const usage = detail.attempts.reduce(
    (total, attempt) => ({
      input: total.input + (attempt.usage.inputTokens ?? 0),
      output: total.output + (attempt.usage.outputTokens ?? 0),
      descendants: total.descendants + attempt.usage.nativeDescendantCount,
    }),
    { input: 0, output: 0, descendants: 0 },
  );

  return (
    <div className="h-full overflow-y-auto bg-background p-4" aria-label="Goal workflow">
      <header className="border-b border-border pb-4">
        <div className="flex items-start gap-3">
          <CircleDashed className="mt-0.5 size-5 shrink-0 text-sky-600" />
          <div className="min-w-0">
            <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              Goal · graph revision {detail.goal.currentRevision}
            </p>
            <h2 className="mt-1 text-sm font-semibold leading-snug">{detail.goal.objective}</h2>
            <p className={`mt-2 text-xs font-medium ${statusTone[detail.goal.status] ?? ""}`}>
              {detail.goal.status.replaceAll("_", " ")}
            </p>
          </div>
        </div>
        <dl className="mt-4 grid grid-cols-2 gap-2 text-xs">
          <div className="rounded-lg border border-border p-2">
            <dt className="text-muted-foreground">Integration</dt>
            <dd className="mt-1 truncate font-mono" title={detail.goal.integrationSha ?? undefined}>
              {detail.goal.integrationSha?.slice(0, 12) ?? "provisioning"}
            </dd>
          </div>
          <div className="rounded-lg border border-border p-2">
            <dt className="text-muted-foreground">Verification</dt>
            <dd className="mt-1 flex items-center gap-1 font-medium">
              {detail.goal.verifiedSha === detail.goal.integrationSha && detail.goal.verifiedSha ? (
                <>
                  <ShieldCheck className="size-3.5 text-emerald-600" /> accepted
                </>
              ) : (
                <>
                  <CircleDashed className="size-3.5" /> pending
                </>
              )}
            </dd>
          </div>
        </dl>
      </header>

      <section className="py-4" aria-labelledby="goal-nodes-heading">
        <div className="mb-2 flex items-center justify-between">
          <h3 id="goal-nodes-heading" className="text-xs font-semibold uppercase tracking-wide">
            Nodes
          </h3>
          <span className="text-[11px] text-muted-foreground">{nodes.length} total</span>
        </div>
        <ol className="space-y-2">
          {nodes.map((node) => {
            const attempt = [...detail.attempts]
              .toReversed()
              .find(
                (candidate) =>
                  candidate.graphVersionId === node.graphVersionId &&
                  candidate.nodeId === node.node.id,
              );
            return (
              <li key={node.node.id} className="rounded-lg border border-border bg-card/40 p-3">
                <div className="flex items-start gap-2">
                  {node.status === "succeeded" ? (
                    <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-emerald-600" />
                  ) : node.status === "blocked" || node.status === "failed" ? (
                    <AlertTriangle className="mt-0.5 size-4 shrink-0 text-amber-600" />
                  ) : (
                    <CircleDashed className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
                  )}
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate text-xs font-semibold">{node.node.role}</span>
                      <span
                        className={`text-[10px] ${statusTone[node.status] ?? "text-muted-foreground"}`}
                      >
                        {node.status}
                      </span>
                    </div>
                    <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                      {node.node.objective}
                    </p>
                    <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-muted-foreground">
                      <span>{node.node.workspaceMode.replace("_", " ")}</span>
                      {attempt?.resolvedRoute ? (
                        <span className="flex items-center gap-1">
                          <Route className="size-3" />
                          {attempt.resolvedRoute.providerInstanceId}/{attempt.resolvedRoute.model}
                        </span>
                      ) : null}
                      {attempt?.usage.nativeDescendantCount ? (
                        <span>{attempt.usage.nativeDescendantCount} descendants</span>
                      ) : null}
                    </div>
                    {node.blocker ? (
                      <p className="mt-2 rounded-md bg-amber-500/10 px-2 py-1.5 text-[11px] text-amber-800 dark:text-amber-200">
                        {node.blocker}
                      </p>
                    ) : null}
                  </div>
                </div>
              </li>
            );
          })}
        </ol>
      </section>

      <section className="border-t border-border py-4" aria-labelledby="goal-evidence-heading">
        <h3 id="goal-evidence-heading" className="text-xs font-semibold uppercase tracking-wide">
          Evidence and delivery
        </h3>
        <div className="mt-2 space-y-2 text-xs">
          <p className="flex items-center gap-2 text-muted-foreground">
            <GitCommit className="size-3.5" />{" "}
            {detail.writerCommits.filter((commit) => commit.state === "integrated").length}{" "}
            integrated commits
          </p>
          <p className="flex items-center gap-2 text-muted-foreground">
            <ShieldCheck className="size-3.5" /> {accepted.length} accepted current-SHA verdicts
          </p>
          {detail.evidence.slice(-5).map((evidence) => (
            <article key={evidence.id} className="rounded-lg border border-border p-2">
              <div className="flex justify-between gap-2">
                <span className="font-medium">{evidence.verdict}</span>
                <span className="font-mono text-[10px] text-muted-foreground">
                  {evidence.integrationSha.slice(0, 10)}
                </span>
              </div>
              <p className="mt-1 text-muted-foreground">{evidence.summary}</p>
            </article>
          ))}
        </div>
      </section>

      <footer className="border-t border-border pt-3 text-[11px] text-muted-foreground">
        Tokens: {usage.input.toLocaleString()} in · {usage.output.toLocaleString()} out ·{" "}
        {usage.descendants} native descendants
      </footer>
    </div>
  );
}
