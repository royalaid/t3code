import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SchemaTransformation from "effect/SchemaTransformation";

import {
  CommandId,
  GoalArtifactId,
  GoalAttemptId,
  GoalEdgeId,
  GoalEvidenceId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  NodeId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ProviderSessionId,
  RunId,
  MessageId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ChatAttachment } from "./chatAttachment.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

export const GoalLifecycleStatus = Schema.Literals([
  "waiting_for_source",
  "provisioning",
  "planning",
  "running",
  "paused",
  "blocked",
  "verifying",
  "completed",
  "failed",
  "cancelled",
]);
export type GoalLifecycleStatus = typeof GoalLifecycleStatus.Type;
export const GoalNodeStatus = Schema.Literals([
  "pending",
  "ready",
  "queued",
  "running",
  "processing",
  "blocked",
  "succeeded",
  "failed",
  "cancelled",
  "superseded",
]);
export type GoalNodeStatus = typeof GoalNodeStatus.Type;
export const GoalAttemptStatus = Schema.Literals([
  "leased",
  "launching",
  "running",
  "stalled",
  "succeeded",
  "failed",
  "cancelled",
  "expired",
]);
export type GoalAttemptStatus = typeof GoalAttemptStatus.Type;
export const GoalWorkspaceMode = Schema.Literals(["read_only", "writer", "integration"]);
const normalizeGoalTimestamp = (value: string) =>
  Option.match(DateTime.make(value), {
    onNone: () =>
      Effect.fail(
        new SchemaIssue.InvalidValue(Option.some(value), {
          message: `Invalid UTC DateTime string: ${value}`,
        }),
      ),
    onSome: (dateTime) => {
      const normalized = DateTime.formatIso(DateTime.toUtc(dateTime));
      return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value) && normalized === value
        ? Effect.succeed(normalized)
        : Effect.fail(
            new SchemaIssue.InvalidValue(Option.some(value), {
              message: `UTC DateTime must be canonical and calendar-valid: ${value}`,
            }),
          );
    },
  });
export const GoalTimestamp = Schema.String.pipe(
  Schema.decodeTo(
    Schema.String,
    SchemaTransformation.transformOrFail({
      decode: normalizeGoalTimestamp,
      encode: normalizeGoalTimestamp,
    }),
  ),
);

export const GoalWorkflowPolicy = Schema.Struct({
  sandboxMode: Schema.Literals(["read-only", "workspace-write", "danger-full-access"]),
  approvalPolicy: Schema.Literals(["never", "on-request", "untrusted"]),
  writableRoots: Schema.Array(TrimmedNonEmptyString),
  providerAllowlist: Schema.Array(TrimmedNonEmptyString),
  toolAllowlist: Schema.Array(TrimmedNonEmptyString),
});
export type GoalWorkflowPolicy = typeof GoalWorkflowPolicy.Type;

export const GoalContextPacket = Schema.Struct({
  schemaVersion: PositiveInt,
  digest: Schema.NullOr(TrimmedNonEmptyString),
  objective: TrimmedNonEmptyString,
  artifacts: Schema.Array(GoalArtifactId),
  dependencyOutputs: Schema.Array(GoalNodeId),
  notes: Schema.Array(Schema.String),
});
export const GoalOutputContract = Schema.Struct({
  kind: Schema.Literals(["structured_result", "commit", "verification", "resolution"]),
  description: TrimmedNonEmptyString,
  requiredFields: Schema.Array(TrimmedNonEmptyString),
});
export const GoalEvidenceRequirement = Schema.Struct({
  kind: Schema.Literals(["test", "command", "artifact", "review", "verdict"]),
  description: TrimmedNonEmptyString,
  required: Schema.Boolean,
});
export const GoalRoutingRequest = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("exact"),
    providerInstanceId: ProviderInstanceId,
    model: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("requirements"),
    capabilities: Schema.Array(TrimmedNonEmptyString),
    latencyClass: Schema.Literals(["interactive", "standard", "batch"]),
    costClass: Schema.Literals(["economy", "standard", "premium"]),
  }),
]);
export type GoalRoutingRequest = typeof GoalRoutingRequest.Type;
export const GoalRouteCandidate = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
  capabilities: Schema.Array(TrimmedNonEmptyString),
  unmetConstraints: Schema.Array(TrimmedNonEmptyString),
});
export type GoalRouteCandidate = typeof GoalRouteCandidate.Type;
export const GoalResolvedRoute = Schema.Struct({
  requested: GoalRoutingRequest,
  providerInstanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
  capabilitySnapshot: Schema.Array(TrimmedNonEmptyString),
  rationale: TrimmedNonEmptyString,
});
export type GoalResolvedRoute = typeof GoalResolvedRoute.Type;
export const GoalRoutingDecision = Schema.Union([
  Schema.Struct({ type: Schema.Literal("resolved"), route: GoalResolvedRoute }),
  Schema.Struct({
    type: Schema.Literal("ambiguous"),
    candidates: Schema.Array(GoalRouteCandidate),
    unmetConstraints: Schema.Array(TrimmedNonEmptyString),
  }),
]);
export type GoalRoutingDecision = typeof GoalRoutingDecision.Type;

export const GoalGraphNode = Schema.Struct({
  id: GoalNodeId,
  role: TrimmedNonEmptyString,
  persona: TrimmedNonEmptyString,
  objective: TrimmedNonEmptyString,
  successCriteria: Schema.Array(TrimmedNonEmptyString),
  contextPacket: GoalContextPacket,
  outputContract: GoalOutputContract,
  requiredCapabilities: Schema.Array(TrimmedNonEmptyString),
  workspaceMode: GoalWorkspaceMode,
  routingRequest: GoalRoutingRequest,
  evidenceRequirements: Schema.Array(GoalEvidenceRequirement),
  policy: GoalWorkflowPolicy,
});
export type GoalGraphNode = typeof GoalGraphNode.Type;
export const GoalGraphEdge = Schema.Struct({
  id: GoalEdgeId,
  fromNodeId: GoalNodeId,
  toNodeId: GoalNodeId,
});
export type GoalGraphEdge = typeof GoalGraphEdge.Type;
export const GoalGraphVersion = Schema.Struct({
  id: GoalGraphVersionId,
  goalId: GoalId,
  revision: PositiveInt,
  publishedByNodeId: GoalNodeId,
  nodes: Schema.Array(GoalGraphNode),
  edges: Schema.Array(GoalGraphEdge),
  createdAt: GoalTimestamp,
});
export type GoalGraphVersion = typeof GoalGraphVersion.Type;

export const GoalResourceUsage = Schema.Struct({
  inputTokens: Schema.NullOr(NonNegativeInt),
  outputTokens: Schema.NullOr(NonNegativeInt),
  cachedTokens: Schema.NullOr(NonNegativeInt),
  costMicros: Schema.NullOr(NonNegativeInt),
  nativeDescendantCount: NonNegativeInt,
});
export const GoalAttempt = Schema.Struct({
  id: GoalAttemptId,
  goalId: GoalId,
  graphVersionId: GoalGraphVersionId,
  nodeId: GoalNodeId,
  ordinal: PositiveInt,
  status: GoalAttemptStatus,
  requestedRoute: GoalRoutingRequest,
  resolvedRoute: Schema.NullOr(GoalResolvedRoute),
  providerSessionId: Schema.NullOr(ProviderSessionId),
  executionThreadId: Schema.NullOr(ThreadId),
  runId: Schema.NullOr(RunId),
  rootExecutionNodeId: Schema.NullOr(NodeId),
  baseIntegrationSha: Schema.NullOr(TrimmedNonEmptyString),
  workspacePath: Schema.NullOr(TrimmedNonEmptyString),
  leaseOwner: Schema.NullOr(TrimmedNonEmptyString),
  leaseExpiresAt: Schema.NullOr(GoalTimestamp),
  usage: GoalResourceUsage,
  failureReason: Schema.NullOr(Schema.String),
  createdAt: GoalTimestamp,
  updatedAt: GoalTimestamp,
});
export type GoalAttempt = typeof GoalAttempt.Type;
export const GoalArtifact = Schema.Struct({
  id: GoalArtifactId,
  goalId: GoalId,
  nodeId: GoalNodeId,
  attemptId: GoalAttemptId,
  kind: Schema.Literals(["result", "file", "diff", "commit", "log", "report"]),
  uri: TrimmedNonEmptyString,
  digest: Schema.NullOr(TrimmedNonEmptyString),
  metadata: Schema.Record(Schema.String, Schema.Unknown),
  createdAt: GoalTimestamp,
});
export const GoalEvidenceCommand = Schema.Struct({
  command: TrimmedNonEmptyString,
  exitCode: Schema.Int,
  logArtifactId: Schema.NullOr(GoalArtifactId),
});
export type GoalEvidenceCommand = typeof GoalEvidenceCommand.Type;

const GoalEvidenceBaseFields = {
  id: GoalEvidenceId,
  goalId: GoalId,
  nodeId: GoalNodeId,
  attemptId: GoalAttemptId,
  integrationSha: TrimmedNonEmptyString,
  producerAttemptId: GoalAttemptId,
  artifacts: Schema.Array(GoalArtifactId),
  summary: Schema.String,
  createdAt: GoalTimestamp,
} as const;
export const GoalAcceptedEvidence = Schema.Struct({
  ...GoalEvidenceBaseFields,
  commands: Schema.Array(GoalEvidenceCommand).check(Schema.isMinLength(1)),
  verdict: Schema.Literal("accepted"),
});
export type GoalAcceptedEvidence = typeof GoalAcceptedEvidence.Type;
export const GoalUnacceptedEvidence = Schema.Struct({
  ...GoalEvidenceBaseFields,
  commands: Schema.Array(GoalEvidenceCommand),
  verdict: Schema.Literals(["rejected", "inconclusive"]),
});
export type GoalUnacceptedEvidence = typeof GoalUnacceptedEvidence.Type;
export const GoalEvidence = Schema.Union([GoalAcceptedEvidence, GoalUnacceptedEvidence]);
export type GoalEvidence = typeof GoalEvidence.Type;
export const GoalWriterCommit = Schema.Struct({
  id: GoalArtifactId,
  goalId: GoalId,
  graphVersionId: GoalGraphVersionId,
  nodeId: GoalNodeId,
  attemptId: GoalAttemptId,
  baseSha: TrimmedNonEmptyString,
  commitSha: TrimmedNonEmptyString,
  cleanSingleCommit: Schema.Boolean,
  integrationBeforeSha: TrimmedNonEmptyString,
  integrationAfterSha: Schema.NullOr(TrimmedNonEmptyString),
  state: Schema.Literals(["published", "integrating", "integrated", "conflicted", "rejected"]),
  createdAt: GoalTimestamp,
  updatedAt: GoalTimestamp,
});

const GoalHandoffText = Schema.String.check(Schema.isMaxLength(20_000));
const GoalSourceSummary = Schema.String.check(Schema.isMaxLength(40_000));
const GoalHandoffTextList = (maximum: number) =>
  Schema.Array(GoalHandoffText).check(Schema.isMaxLength(maximum));

export const GoalSourceHandoff = Schema.Struct({
  objective: TrimmedNonEmptyString.check(Schema.isMaxLength(20_000)),
  attachments: Schema.Array(ChatAttachment).check(Schema.isMaxLength(32)),
  selectedContextText: GoalHandoffTextList(32),
  sourceSummary: Schema.NullOr(GoalSourceSummary),
  projectInstructions: GoalHandoffTextList(16),
  branchState: Schema.NullOr(GoalHandoffText),
  relevantCheckpoints: GoalHandoffTextList(32),
});

export const Goal = Schema.Struct({
  id: GoalId,
  projectId: Schema.optional(ProjectId),
  repositoryRoot: Schema.optional(GoalHandoffText),
  sourceWorkspacePath: Schema.optional(GoalHandoffText),
  objective: TrimmedNonEmptyString,
  status: GoalLifecycleStatus,
  sourceThreadId: ThreadId,
  rootThreadId: ThreadId,
  /** Run that must settle before the source capsule may be finalized. */
  sourceActiveRunId: Schema.optional(Schema.NullOr(RunId)),
  /** Exact first provider run launched for the root lead. Optional for legacy projections. */
  initialRootRunId: Schema.optional(RunId),
  pendingLaunchClaimId: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
  /** Immutable, client-supplied inputs captured before any provider launch. */
  sourceInput: Schema.optional(
    Schema.Struct({
      messageId: MessageId,
      attachments: GoalSourceHandoff.fields.attachments,
      selectedContextText: GoalSourceHandoff.fields.selectedContextText,
    }),
  ),
  /** Bounded portable handoff finalized only after sourceActiveRunId settles. */
  sourceHandoff: Schema.optional(Schema.NullOr(GoalSourceHandoff)),
  rootModelSelection: Schema.optional(ModelSelection),
  rootRuntimeMode: Schema.optional(RuntimeMode),
  rootInteractionMode: Schema.optional(ProviderInteractionMode),
  policy: GoalWorkflowPolicy,
  currentGraphVersionId: Schema.NullOr(GoalGraphVersionId),
  currentRevision: NonNegativeInt,
  integrationBranch: Schema.NullOr(TrimmedNonEmptyString),
  integrationWorktreePath: Schema.NullOr(TrimmedNonEmptyString),
  integrationSha: Schema.NullOr(TrimmedNonEmptyString),
  verifiedSha: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: GoalTimestamp,
  updatedAt: GoalTimestamp,
});
export type Goal = typeof Goal.Type;
export const GoalNodeProjection = Schema.Struct({
  goalId: GoalId,
  graphVersionId: GoalGraphVersionId,
  node: GoalGraphNode,
  status: GoalNodeStatus,
  activeAttemptId: Schema.NullOr(GoalAttemptId),
  blocker: Schema.NullOr(Schema.String),
  updatedAt: GoalTimestamp,
});
export type GoalNodeProjection = typeof GoalNodeProjection.Type;
export const GoalSummary = Schema.Struct({
  id: GoalId,
  status: GoalLifecycleStatus,
  currentRevision: NonNegativeInt,
  readyCount: NonNegativeInt,
  runningCount: NonNegativeInt,
  blockedCount: NonNegativeInt,
  attentionRequired: Schema.Boolean,
  verified: Schema.Boolean,
});
export const GoalEpisodeSummary = Schema.Struct({
  goalId: GoalId,
  rootThreadId: ThreadId,
  objective: Schema.String,
  status: GoalLifecycleStatus,
  currentRevision: NonNegativeInt,
  readyCount: NonNegativeInt,
  runningCount: NonNegativeInt,
  blockedCount: NonNegativeInt,
  attentionRequired: Schema.Boolean,
  verified: Schema.Boolean,
  createdAt: GoalTimestamp,
  updatedAt: GoalTimestamp,
});
export type GoalEpisodeSummary = typeof GoalEpisodeSummary.Type;
export const GoalSurface = Schema.Struct({
  activeGoalId: Schema.NullOr(GoalId),
  episodes: Schema.Array(GoalEpisodeSummary),
});
export type GoalSurface = typeof GoalSurface.Type;
export const GoalDetail = Schema.Struct({
  goal: Goal,
  graphVersions: Schema.Array(GoalGraphVersion),
  nodes: Schema.Array(GoalNodeProjection),
  attempts: Schema.Array(GoalAttempt),
  artifacts: Schema.Array(GoalArtifact),
  evidence: Schema.Array(GoalEvidence),
  writerCommits: Schema.Array(GoalWriterCommit),
  failures: Schema.Array(Schema.suspend(() => GoalFailureRecord)),
});
export type GoalDetail = typeof GoalDetail.Type;

export const GoalFailureReason = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("stale_revision"),
    expected: NonNegativeInt,
    actual: NonNegativeInt,
  }),
  Schema.Struct({ type: Schema.Literal("policy_rejection"), detail: TrimmedNonEmptyString }),
  Schema.Struct({
    type: Schema.Literal("ambiguous_routing"),
    candidates: Schema.Array(GoalRouteCandidate),
    unmetConstraints: Schema.Array(TrimmedNonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("dependency_failure"), dependencyNodeId: GoalNodeId }),
  Schema.Struct({
    type: Schema.Literal("native_descendant_overage"),
    limit: PositiveInt,
    observed: PositiveInt,
  }),
  Schema.Struct({
    type: Schema.Literal("integration_conflict"),
    artifactId: GoalArtifactId,
    detail: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("stale_evidence"),
    evidenceSha: TrimmedNonEmptyString,
    integrationSha: TrimmedNonEmptyString,
  }),
  Schema.Struct({ type: Schema.Literal("unsupported_queue_steer"), detail: TrimmedNonEmptyString }),
  Schema.Struct({ type: Schema.Literal("stale_active_run_target"), detail: TrimmedNonEmptyString }),
  Schema.Struct({
    type: Schema.Literal("root_lead_no_graph"),
    runId: RunId,
    terminalStatus: Schema.Literals(["completed", "failed", "interrupted", "rolled_back"]),
    detail: TrimmedNonEmptyString.check(Schema.isMaxLength(4_000)),
  }),
  Schema.Struct({
    type: Schema.Literal("resource_backstop"),
    limit: PositiveInt,
    observed: NonNegativeInt,
    warning: Schema.Boolean,
  }),
]);
export type GoalFailureReason = typeof GoalFailureReason.Type;

export const GoalFailureRecord = Schema.Struct({
  id: GoalEvidenceId,
  goalId: GoalId,
  graphVersionId: Schema.NullOr(GoalGraphVersionId),
  nodeId: Schema.NullOr(GoalNodeId),
  attemptId: Schema.NullOr(GoalAttemptId),
  reason: GoalFailureReason,
  recoveryState: Schema.Literals(["unresolved", "retryable", "resolved", "terminal"]),
  blocker: Schema.NullOr(Schema.String),
  occurredAt: GoalTimestamp,
});

const commandBase = { commandId: CommandId, threadId: ThreadId, goalId: GoalId };
export const GoalWorkflowCommand = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("goal.launch"),
    commandId: CommandId,
    threadId: ThreadId,
    rootThreadId: ThreadId,
    objective: TrimmedNonEmptyString.check(Schema.isMaxLength(20_000)),
    messageId: MessageId,
    attachments: GoalSourceHandoff.fields.attachments,
    selectedContextText: GoalSourceHandoff.fields.selectedContextText,
    createdBy: Schema.Literals(["user", "agent", "system"]),
    creationSource: Schema.Literals(["web", "mobile", "mcp", "provider", "server"]),
  }),
  Schema.Struct({ type: Schema.Literal("goal.create"), ...commandBase, goal: Goal }),
  Schema.Struct({ type: Schema.Literal("goal.reopen"), ...commandBase }),
  Schema.Struct({
    type: Schema.Literal("goal.cancel"),
    ...commandBase,
    reason: Schema.optional(Schema.String),
  }),
  Schema.Struct({ type: Schema.Literal("goal.pending-launch.cancel"), ...commandBase }),
  Schema.Struct({
    type: Schema.Literal("goal.pending-launch.fail"),
    ...commandBase,
    claimId: TrimmedNonEmptyString,
    detail: Schema.String.check(Schema.isMaxLength(4_000)),
  }),
  Schema.Struct({
    type: Schema.Literal("goal.pending-launch.claim"),
    ...commandBase,
    claimId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("goal.pending-launch.complete"),
    ...commandBase,
    handoff: GoalSourceHandoff,
    claimId: TrimmedNonEmptyString,
    initialRootRunId: RunId,
  }),
  Schema.Struct({
    type: Schema.Literal("goal.graph.replace"),
    ...commandBase,
    expectedRevision: NonNegativeInt,
    graph: GoalGraphVersion,
  }),
  Schema.Struct({
    type: Schema.Literal("goal.node.cancel"),
    ...commandBase,
    /**
     * Cancellation targets a concrete immutable graph version. A lead may need
     * to stop a still-running node from a prior revision after publishing a
     * replacement graph, so this must never be inferred from the current
     * revision.
     */
    graphVersionId: GoalGraphVersionId,
    nodeId: GoalNodeId,
    disposition: Schema.Literals(["cancelled", "superseded"]),
    reason: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("goal.result.publish"),
    ...commandBase,
    attemptId: GoalAttemptId,
    artifacts: Schema.Array(GoalArtifact),
  }),
  Schema.Struct({
    type: Schema.Literal("goal.evidence.publish"),
    ...commandBase,
    evidence: GoalEvidence,
  }),
]);
export type GoalWorkflowCommand = typeof GoalWorkflowCommand.Type;

export const GoalGraphActivatedPayload = Schema.Struct({
  goalId: GoalId,
  expectedRevision: NonNegativeInt,
  /** Present on live events; optional only so projections can replay legacy stored events. */
  expectedStatus: Schema.optional(GoalLifecycleStatus),
  graph: GoalGraphVersion,
  activatedAt: GoalTimestamp,
});
export const GoalFailureRecordedPayload = Schema.Struct({
  id: GoalEvidenceId,
  goalId: GoalId,
  graphVersionId: Schema.NullOr(GoalGraphVersionId),
  nodeId: Schema.NullOr(GoalNodeId),
  attemptId: Schema.NullOr(GoalAttemptId),
  reason: GoalFailureReason,
  recoveryState: Schema.Literals(["unresolved", "retryable", "resolved", "terminal"]),
  blocker: Schema.NullOr(Schema.String),
  occurredAt: GoalTimestamp,
});
export const GoalWorkflowEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("goal.created"), payload: Goal }),
  Schema.Struct({
    type: Schema.Literals(["goal.updated", "goal.reopened", "goal.cancelled", "goal.completed"]),
    payload: Goal,
  }),
  Schema.Struct({
    type: Schema.Literal("goal.graph-version-activated"),
    payload: GoalGraphActivatedPayload,
  }),
  Schema.Struct({
    type: Schema.Literals(["goal.node-transitioned", "goal.node-cancellation-requested"]),
    payload: GoalNodeProjection,
  }),
  Schema.Struct({
    type: Schema.Literals([
      "goal.attempt-created",
      "goal.attempt-transitioned",
      "goal.route-resolved",
    ]),
    payload: GoalAttempt,
  }),
  Schema.Struct({ type: Schema.Literal("goal.artifact-published"), payload: GoalArtifact }),
  Schema.Struct({ type: Schema.Literal("goal.writer-commit-recorded"), payload: GoalWriterCommit }),
  Schema.Struct({
    type: Schema.Literals(["goal.integration-updated", "goal.integration-conflicted"]),
    payload: Goal,
  }),
  Schema.Struct({
    type: Schema.Literals(["goal.evidence-submitted", "goal.verdict-recorded"]),
    payload: GoalEvidence,
  }),
  Schema.Struct({
    type: Schema.Literal("goal.failure-recorded"),
    payload: GoalFailureRecordedPayload,
  }),
]);
export type GoalWorkflowEvent = typeof GoalWorkflowEvent.Type;
