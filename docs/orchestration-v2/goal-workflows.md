# Dynamic goal workflows

`/goal <objective>` creates a dedicated goal root thread linked to the source thread. The selected provider and model become the root lead. If the source has an active run, launch remains durably pending until that run settles; the handoff is a bounded source capsule rather than a copied transcript.

The lead publishes immutable whole-graph revisions through goal-scoped MCP tools. Nodes have free-form roles, explicit success criteria, context and output contracts, capability routing requests, policy, workspace mode, dependencies, and evidence requirements. Only the root lead can replace the graph. Expected-revision checks, acyclicity, referential integrity, and policy narrowing are projection invariants.

T3 schedules ready nodes from durable projections. Routing filters installed and authenticated provider instances by the inherited policy and requested capabilities. Exact routes are persisted; ambiguous capability routes are returned as typed candidates instead of silently ranked. Attempts retain their requested route, resolved provider and model, capability snapshot, lease, usage, and native descendants.

Every goal owns a retained integration branch and worktree. Writers start from the recorded integration SHA in isolated branches and must finish with exactly one clean commit. Assembly is serialized in dependency-ready and launch order. Successful integrations invalidate older verification and prune the writer worktree. Conflicts abort the cherry-pick, preserve the writer worktree, block the goal, and require the lead to publish a resolver node.

Completion requires accepted evidence for the exact current integration SHA from a distinct verifier node, attempt, and thread. A projection transaction rejects stale or self-verification. Reopening retains evidence history while clearing verified state and requiring a new graph revision and verification pass.

Queue, Steer, and Stop operate only on the goal root. Goal child threads are inspect-only to web, mobile, and MCP clients. Queue entries are durable V2 runs; steering or queue dispatch pauses new launches until the lead publishes or reaffirms a graph. Stop pauses future launches but allows workers to finish. Cancel Goal is terminal and requests cancellation of the root and active worker runs while preserving state and artifacts.

No goal workflow pushes, opens a pull request, deploys, modifies a protected branch, or applies its integration branch to the user's starting branch. Applying or discarding the retained local branch is always explicit.
