# Dynamic Goal Orchestrator — Validation & Fix Handoff (2026-07-13)

Branch: `feat/dynamic-goal-orchestrator-v2` (fork `royalaid/t3code`, upstream
`pingdotgg/t3code`). This document records the first end-to-end validation of the
durable `/goal` feature on macOS, the bugs it surfaced, the fixes committed, and
the design gaps that remain open for a maintainer decision.

## TL;DR

The branch is coherent and well-tested at the unit/integration level, honestly
documented, and structurally faithful to its design docs. But the live `/goal`
pipeline had **never been run end-to-end** — the first real UI drive found a
chain of bugs that made goal launch impossible on any machine. Five are fixed
and pushed (all typecheck-clean, full server suite green: 1284 passing). The
feature now reaches a **live root lead**, but does not yet complete a goal because
the lead is never instructed to act as an orchestrator (finding #9 below).

## Environment / how to run it on this machine

- Node **24.16.0** via nvm (repo pins `^24.13.1`; system node 26 is too new).
  `export PATH="$HOME/.nvm/versions/node/v24.16.0/bin:$PATH"`.
- pnpm 11.10.0; global `vp` (vite-plus) required; provider CLIs installed
  (codex, claude, cursor-agent, opencode).
- Install: `pnpm install`. Typecheck: `pnpm --filter t3 typecheck`.
  Tests: `pnpm --filter t3 test` (run through `vp`, not bare vitest — the
  `@effect/vitest` patch redirects to `vite-plus/test`).
- Dev: `pnpm dev` → server `:13773`, web `:5733`. Set `T3CODE_NO_BROWSER=true`
  to stop the server auto-opening a new browser tab (and pairing URL) on every
  boot/restart.
- Pairing: each boot prints a one-time `pairingUrl` in the server log; paste the
  token into the pair page. Do **not** run two dev stacks at once — they share
  `~/.t3/dev/state.sqlite` and produce "database is locked" startup failures.
- State DB for inspection: `~/.t3/dev/state.sqlite` (tables `goals`,
  `goal_graph_versions`, `goal_nodes`, `goal_attempts`, `goal_writer_commits`,
  `orchestration_v2_projection_runtime_requests`, ...).

## Fixes committed on this branch

All are `apps/server/src/orchestration-v2/` unless noted. Server suite green after
each; no production behavior beyond goal launch is touched.

1. **`fix(goals): provide ProjectionSnapshotQuery to orchestrator and goal-launch layers`**
   `runtimeLayer.ts`. `Orchestrator.ts:504` and `GoalLaunchService.ts:202`
   resolve the source project's workspace root via
   `Effect.serviceOption(ProjectionSnapshotQuery)`, but the layer was never
   provided, so it silently resolved to `Option.none` and every `/goal` failed
   with "Source project `<id>` is unavailable." Added the live layer to both
   provide-lists; closed the resulting `ProjectEnrichmentService` dependency in
   four isolated test layers.

2. **`fix(goals): persist pending-launch claim and source handoff in lifecycle projection`**
   `GoalProjectionStore.ts`. `persistLifecycle` rebuilt the goal row from the
   prior projection and dropped `pendingLaunchClaimId`, `sourceActiveRunId`, and
   `sourceHandoff` from `goal.updated` payloads. The launch claim was therefore
   never persisted, so `GoalLaunchService.finalize`'s claim-current check always
   failed and **no goal could advance past `provisioning` on any machine.**

3. **`fix(goals): serialize shared read-only worktree creation per goal`**
   `GoalWorkspaceService.ts`. The startup rescan and the settlement event stream
   can drive launch concurrently, racing the same `git worktree add -b
goal-read/<...>` and failing with "cannot lock ref ... reference already
   exists." Wrapped the shared read-only worktree creation in the existing
   per-goal serial executor (same guard `provision()` already uses).

4. **`fix(goals): launch root lead on goal.created without awaiting a restart`**
   `GoalLaunchService.ts`. The launch consumer only reacted to terminal
   `run.updated` events, so a `/goal` from an idle source thread had no trigger
   and sat in `waiting_for_source` until an unrelated run settled or the server
   restarted. Now also reacts to `goal.created`. Also logs the failure cause on
   the pending-launch error path (the durable `goal.failed` state carries no
   reason field).

5. **`fix(goals): correct stale read-only fixture in FoundationPersistence test`**
   Fixture declared `read_only` workspace mode with a workspace-write policy;
   `validateGoalNodeWorkspace` (added in a later hardening commit) now rejects it.
   Made it a coherent read-only node. The stray
   `packages/effect-codex-app-server/.codex-probe-write-action.txt` recording
   artifact was also removed (folded into commit 1 by the pre-commit hook).

### Verified end-to-end after the fixes

`/goal` → `waiting_for_source` → (`goal.created` trigger, no restart) →
`provisioning` (claim persists) → `planning`; integration + read worktrees
created; **root lead launched as a real provider session** (Codex app-server),
running under the approval-required runtime with the goal MCP toolkit. The
transport-boundary client-authority enforcement (`d086daae6`) rejects client
commands on goal threads exactly as documented. File-change approvals surface in
the UI and resolve.

## Open findings (NOT fixed — need a maintainer decision)

- **★ #9 — the root lead is never told to orchestrate (why goals don't complete).**
  The lead is launched (`GoalLaunchService.ts` ~L305) with an `initialMessage`
  that is only the objective + handoff context. It gets **no** instruction that
  it is a coordinator or that it must publish a worker-node graph via the
  `goal_replace_graph` MCP tool (that tool exists and is authorized for the lead
  — `OrchestratorMcpService.ts:945-958`, `mcp/toolkits/orchestrator/tools.ts`).
  Codex `developer_instructions` are only injected for `interactionMode ===
"plan"` (`CodexAdapterV2.ts:538`); the goal lead runs in default build mode.
  Result: the lead behaves as a plain coding agent and edits files **directly in
  its read-only `goal-read/` checkout** (approval-gated), producing `nodes=0,
rev=0` indefinitely. Those direct edits are an architectural dead end — they
  never become a writer commit and never reach the server-controlled integration
  worktree, so the goal can never reach integration/verification/completion.
  Fix requires authoring an orchestrator system/developer prompt for the lead —
  a deliberate product/prompt decision, intentionally left to the maintainer.

- **#7 — exec-approval routing (the "it's taking forever" hang).** A lead's
  native `command` (exec) approval request is projected under the **raw provider
  thread id** (`thread:provider:codex:native-thread:...`) instead of the goal-root
  orchestration thread, so the UI (subscribed to the orchestration thread) never
  renders it and the lead blocks forever at 0% CPU. `file-change` approvals bind
  correctly and are resolvable. This is the same provider-turn-mapping bug family
  the upstream author filed against PR #2829. Start at
  `ProviderEventIngestor.ts:214` (`runtime_request.updated`) and the native-thread
  id derivation in `IdAllocator.ts:365`.

- **#8 — cancellation doesn't terminalize.** Cancel Goal killed the lead's
  provider session but the `goals` row stayed `planning` (updatedAt unchanged) —
  cancellation projection/fencing gap, consistent with the committed
  `TODO(interrupt-hardening)` at `Orchestrator.ts:4678`. The web Cancel Goal
  control also uses a blocking `window.confirm()`.

- **Recovery orphaning.** A goal that dies in `planning` (e.g. server restart) is
  not re-driven: startup recovery only rescans `waiting_for_source` /
  `provisioning`. Goals stuck in `planning` remain stuck across boots.

## Upstream context

- PR **#2829** ("feat(orchestrator): introduce new orchestrator") is the upstream
  maintainer's own **still-open draft** — no human reviews (bots only), not merged
  or superseded. This branch reconciles #2829 and adds an original **durable-goal
  layer with no upstream counterpart** (closest is #2877, a different `/goal`).
- The **Windows Android build problems have no upstream fix** — upstream builds
  Android on macOS/Linux (PR #3579). The ninja "build.ninja still dirty" loop is
  the known NDK-27 / ninja-<1.12 MAX_PATH bug (`ninja-build/ninja#1900`, fixed in
  ninja 1.12 but not bundled; `react-native-screens#3471`). This branch's
  CMake-path-shortening patches are the ecosystem-recommended workaround and are
  inert for macOS web/server use.
- Fork `main` is only 3 commits behind upstream `main`; the just-merged Android
  PR #3579 overlaps this branch's mobile surface and will need reconciling.

## Suggested next steps

1. Decide the lead-orchestration model (#9): give the goal lead a developer/system
   prompt that instructs decompose-and-publish-graph, OR formally support a
   "lead does the work directly" mode where the lead's edits are captured as a
   writer commit onto the integration branch. Until one of these exists, `/goal`
   cannot complete.
2. Fix the exec-approval thread binding (#7) so lead command approvals are
   visible; this is the most user-visible remaining bug.
3. Harden cancellation/recovery (#8 + planning-orphan) per the existing
   interrupt-hardening TODO.
