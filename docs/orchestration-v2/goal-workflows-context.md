# Dynamic Goal Workflows: Context And Handoff

This document is the working context for the durable `/goal` implementation on
`feat/dynamic-goal-orchestrator-v2`. It complements
[Dynamic Goal Workflows](./goal-workflows.md): that document describes the
product behavior, while this one records the implementation boundaries and the
invariants future changes must preserve.

## Product boundary

`/goal <objective>` creates a dedicated goal-root thread. It never converts the
source thread, pushes code, opens a pull request, deploys, merges a protected
branch, or modifies the user's starting branch. The retained integration branch
and worktree are a local deliverable that the user explicitly applies or
discards.

The source handoff is durable and bounded. An active source turn settles before
the root lead is launched. The selected composer provider/model becomes that
lead, but the server—not the lead—owns graph persistence, scheduling, routing,
worktree creation, integration, recovery, completion, and client authority.

## Durable model

Goals are modelled beside V2 threads/runs, using immutable graph revisions:

```text
source thread
  -> goal root / root lead
       -> immutable graph revision N
            -> dependency-ready node attempts
                 -> isolated worker threads / provider turns
                      -> artifacts, one writer commit, evidence
                           -> integration SHA + independent verdict
```

- The root lead alone publishes whole-DAG replacements with an expected
  revision. Workers may recommend work but cannot mutate topology.
- Nodes declare role/persona, objective, contracts, dependencies, routing,
  policy, workspace mode, and evidence requirements.
- Goal/node/attempt/artifact/evidence projections are written in the same
  transactional V2 event/projection boundary as the command receipt.
- Attempt routes, leases, provider capability snapshots, usage, descendant
  counts, and integration bases are durable. Recovery never silently reroutes
  an existing attempt.

## Authority and policy

Authority narrows in this order:

```text
user instruction -> root thread policy -> durable goal policy
  -> graph revision -> node policy -> provider runtime
```

Important enforcement points:

- Browser/mobile clients may operate only the goal root's human controls.
  Child threads are inspectable; they cannot send, Queue, Steer, Stop, reorder,
  promote, alter model/runtime, or forge server/MCP workflow commands. A child
  can still receive a human response to its own runtime approval/input request.
- Root-lead and worker MCP credentials are scoped to the goal, thread, and
  active provider session. Generic orchestration commands cannot bypass the
  graph or resource accounting.
- The root lead uses a deterministic shared read-only checkout and
  `approval-required` runtime. It cannot use the retained integration worktree.
- Read-only nodes use the shared read checkout; writers use isolated branches
  from their recorded integration SHA. The retained integration worktree is
  server-controlled assembly only.
- A policy can only narrow. Provider launch must receive the resolved runtime
  policy; a provider that cannot enforce a non-wildcard tool allowlist must
  reject the attempt rather than silently broaden it.

## Scheduling, cancellation, and recovery

The scheduler is environment-global and fair. Overall worker capacity is
`min(16, max(2, logical CPU count - 2))` (14 on the development host); writers
have an independent cap of four. Occupancy includes running/leased work from
paused, blocked, and superseded revisions, because Queue/Steer/Stop can pause
new launches while existing workers continue.

- The lifetime agent backstop is applied during fair selection, so a goal at
  999 created agents receives at most one additional launch.
- Native-descendant overage is persisted as a structured failure, pauses new
  launches, and durably interrupts the owning run.
- Node cancellation targets an explicit graph revision and has a
  `cancelled` or `superseded` disposition. It is CAS-protected, works for
  historical revisions, fences a launch before provider work begins, and
  preserves the cancellation disposition through late provider completion.
- Cancel Goal is terminal. It must cancel unbound launch effects and fence
  pending goal provisioning as well as interrupt bound work; cancellation
  preserves artifacts and diagnostic worktrees.

## Integration and verification

Every successful writer publishes exactly one clean commit. Assembly is serial,
replay-safe, and records the before/after integration SHA. Conflicts preserve
diagnostic worktrees and block the workflow rather than attempting an implicit
resolution.

Completion is a server invariant, not a prompt convention:

- accepted machine evidence belongs to the exact current integration SHA;
- each command includes durable log-artifact evidence;
- the verifier uses a distinct node, thread, and attempt from the producer;
- integration SHA changes and reopening invalidate accepted verification;
- reopening retains historical evidence but requires a fresh graph revision and
  fresh verification.

## Validation record

Focused contract, projection, scheduler, workspace, integration/evidence,
client-policy, recovery, and cancellation suites accompany the implementation.
The native Windows validation uses a disposable worktree at `C:\t3goal-android`
and a real Android emulator. Windows-specific CMake staging patches shorten the
package build directories for React Native Worklets, Reanimated, Expo Modules
Core, Nitro Modules, and Nitro Markdown; each patch is generated through
`pnpm patch`/`patch-commit` and is validated from a clean install. Their
individual CMake targets pass. At this checkpoint, the clean full
`android:dev` build proceeds through those targets but still fails in the
unpatched `react-native-screens` package with Ninja's Windows
`build.ninja ... still dirty` loop. Do not represent Android runtime validation
as complete until that final package-level issue is resolved and the app is
installed/launched on the emulator.

Before declaring a release candidate, run:

```text
vp check
vp run typecheck
vp run lint:mobile
vp test
vp run test
```

Then run the focused orchestration/replay suites and a real mobile/browser
validation. Do not claim browser validation if the collaborative preview is not
authenticated or Android validation if the built application was not installed
and launched on an emulator.

## Current follow-up checklist

1. Complete the required repository-wide gates after all concurrent hardening
   changes settle.
2. Re-run the clean full Android development build, then install and launch the
   development package on the emulator.
3. Reconcile the feature branch with the latest `origin/main` before a merge
   decision, resolving behavior in favor of current V2 semantics.
4. Perform browser/mobile inspection through an authenticated collaborative
   preview; generated remote links must use the advertised MagicDNS endpoint,
   never localhost, LAN, or a raw Tailscale IP.
5. Keep the branch local/pushed for review only. A user must explicitly request
   any PR, merge, deployment, or application/discard of a goal integration
   branch.
