# Dynamic Goal Orchestrator — Validation & Fix Handoff (2026-07-13)

Branch: `feat/dynamic-goal-orchestrator-v2` (fork `royalaid/t3code`, upstream
`pingdotgg/t3code`). This document records the first end-to-end validation of the
durable `/goal` feature on macOS, the bugs it surfaced, the fixes committed, and
the design gaps that remain open for a maintainer decision.

## TL;DR

The first real UI drive found a chain of bugs that made goal launch impossible
on any machine. The initial five launch fixes were pushed with a typecheck-clean,
1284-test server run. A second implementation pass resolved the root contract,
approval routing, worker authority binding, graph-revision lookup, and root-run
pause races. A Codex-led live goal now reaches `completed` through one isolated
writer, server integration, and one independent verifier with accepted evidence
for the exact final SHA. The source checkout remains unchanged by design; the
retained integration branch is the explicit local deliverable.

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
   can drive launch concurrently, racing the same
   `git worktree add -b goal-read/<...>` and failing with "cannot lock ref ... reference already
   exists." Wrapped the shared read-only worktree creation in the existing
   per-goal serial executor (the same guard `provision()` already uses).

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

### Follow-up execution-contract fixes

1. The server now delivers a deterministic root-lead orchestration contract
   through Claude and Codex trusted instruction channels. Objective and handoff
   text stay in the user role. The contract requires `goal_read`,
   `goal_capabilities`, and a completion-valid whole-graph publication instead
   of direct root edits.
2. Graph activation derives publisher identity from the authenticated lead and
   rejects graphs without a producer-backed verifier that transitively follows
   every writer. Terminal goals remain fenced; running replacements,
   revision-zero blocked recovery, and revisioned root-controlled paused
   replacement are explicit lifecycle paths.
3. Worker prompts are fresh execution capsules containing only the active node,
   attempt, workspace SHA, usable ancestors, and relevant artifacts. Worker MCP
   node lookup is pinned to the attempt's graph revision so reused node IDs
   cannot resolve a stale immutable revision.
4. Runtime approvals are projected onto the bound application thread while
   responses retain the original provider turn/request identity. Codex's
   injected `t3-code` MCP approves exactly the eight goal tools through per-tool
   configuration so the required control plane cannot silently self-reject;
   unrelated MCP, shell, and file operations retain their normal approval path.
5. Worker startup persists the exact provider-session binding before issuing
   the goal MCP credential. A stale or different session fails the active
   attempt fence rather than receiving authority.
6. A root-control run pauses scheduling once. Publishing a replacement graph
   resumes the goal, and subsequent `waiting` events from the same run no longer
   re-pause the new graph. Explicit interruption retains Stop semantics.
7. Goal attempt threads now persist the root lead as server-owned parent
   lineage. The project sidebar renders writer and verifier rows beneath that
   root, counts only roots against its preview limit, and exposes per-parent
   collapse without reparenting ordinary threads or forks.
8. `goal_capabilities` now reads the scheduler's provider catalog instead of
   returning empty capability arrays. The root sees the same sorted capability
   snapshots and goal-policy provider constraints that will evaluate its graph.

### Final live acceptance

The live fixture goal `goal:2264ac6f-2121-445e-b074-c11c5389a9d3` completed on
graph revision 5. Revision 5 was the stable retry after earlier revisions were
intentionally interrupted by server hot reloads while live-found fixes were
installed.

- Writer attempt: `writer_square_minimal`, one clean commit, integrated as
  `b09ed0f44ca5cffca88c327e896c077986f8573d`.
- Verifier attempt: `verifier_square_minimal`, Codex, launched only after writer
  integration and bound to `provider-session:provider-instance:codex:shared`.
- Verification: exact HEAD, `git diff --check`, changed-file scope, README
  documentation, and direct Node ESM assertions all passed.
- Evidence: durable log/report artifacts and an accepted verdict target the
  same integration SHA; the goal's `integrationSha` and `verifiedSha` match.
- UI: the goal panel shows revision 5 `completed`, two succeeded nodes, one
  integrated commit, and one accepted current-SHA verdict.
- Delivery boundary: the original fixture checkout remains on its initial SHA;
  the clean retained integration branch contains the verified commit, exactly
  as specified by `goal-workflows-context.md`.

## Finding status

- **Resolved #9 — root orchestration contract.** The root receives trusted,
  provider-native coordination instructions. The live Codex root published
  valid writer/verifier graphs without editing its read-only checkout, while
  deterministic adapter tests cover the Claude trusted-channel mapping.
- **Resolved #7 — exec-approval routing.** Runtime requests bind to the
  application thread visible in the UI while response delivery retains the raw
  provider identifiers needed to resume the original turn. Focused routing
  tests are green and live worker approval requests rendered and resolved.

- **Open #8 — cancellation doesn't terminalize.** Cancel Goal killed the lead's
  provider session but the `goals` row stayed `planning` (updatedAt unchanged) —
  cancellation projection/fencing gap, consistent with the committed
  `TODO(interrupt-hardening)` at `Orchestrator.ts:4678`. The web Cancel Goal
  control also uses a blocking `window.confirm()`.

- **Recovery orphaning.** A goal that dies in `planning` (e.g. server restart) is
  not re-driven: startup recovery only rescans `waiting_for_source` /
  `provisioning`. Goals stuck in `planning` remain stuck across boots.

- **Root diagnostic parity.** Humans can open the new nested worker rows and read
  their full timelines. A goal-bound lead sees durable attempt, artifact,
  evidence, and failure state through `goal_read`, but has no narrowly scoped
  read-only tool for the underlying worker timeline. Adding that primitive needs
  a deliberate goal-authority and MCP-surface decision.

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

1. Harden cancellation/recovery (#8 + planning-orphan) per the existing
   interrupt-hardening TODO.
2. Decide whether root leads should receive a same-goal, read-only attempt
   timeline primitive for diagnosing worker provider failures.
3. Run the repository-wide typecheck, check, and full server suite after the
   follow-up execution-contract commits settle.
4. Repeat the live acceptance from a clean revision-zero Codex goal before
   release-candidate sign-off. This iteration intentionally relies on
   deterministic adapter tests, rather than a second live provider run, for
   Claude trusted-channel parity.
