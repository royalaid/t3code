import {
  type GoalWorkflowPolicy,
  type GoalGraphNode,
  ModelSelection,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProjectionProjects from "../persistence/Services/ProjectionProjects.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2RuntimePolicy as ProviderAdapterV2RuntimePolicyType,
} from "./ProviderAdapter.ts";

/**
 * ERRORS
 */
export class RuntimePolicyResolveError extends Schema.TaggedErrorClass<RuntimePolicyResolveError>()(
  "RuntimePolicyResolveError",
  {
    projectId: ProjectId,
    providerInstanceId: ProviderInstanceId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to resolve runtime policy for provider instance ${this.providerInstanceId} in project ${this.projectId}.`;
  }
}

export const RuntimePolicyV2Error = Schema.Union([RuntimePolicyResolveError]);
export type RuntimePolicyV2Error = typeof RuntimePolicyV2Error.Type;

/**
 * Goal graph validation rejects policy expansion before a graph becomes
 * active. This second, launch-time check is deliberately independent: a
 * provider run must never receive broader authority if a stale/malformed
 * projection reaches the effect worker.
 */
export class GoalRuntimePolicyResolveError extends Schema.TaggedErrorClass<GoalRuntimePolicyResolveError>()(
  "GoalRuntimePolicyResolveError",
  {
    reason: Schema.Literals([
      "policy_expansion",
      "workspace_policy_mismatch",
      "provider_not_allowed",
      "tool_allowlist_unsupported",
    ]),
    detail: Schema.String,
    toolAllowlist: Schema.optional(Schema.Array(Schema.String)),
  },
) {}
export const isGoalRuntimePolicyResolveError = Schema.is(GoalRuntimePolicyResolveError);

const sandboxRank = { "read-only": 0, "workspace-write": 1, "danger-full-access": 2 } as const;
const approvalRank = { untrusted: 0, "on-request": 1, never: 2 } as const;

type GoalSandboxMode = keyof typeof sandboxRank;
type GoalApprovalPolicy = keyof typeof approvalRank;

const isGoalApprovalPolicy = (value: unknown): value is GoalApprovalPolicy =>
  value === "untrusted" || value === "on-request" || value === "never";

const isWindowsRoot = (root: string) => /^[a-zA-Z]:[\\/]/.test(root) || root.startsWith("\\\\");
const resolveSegments = (segments: ReadonlyArray<string>) => {
  const resolved: Array<string> = [];
  for (const segment of segments) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") resolved.pop();
    else resolved.push(segment);
  }
  return resolved;
};
const rootContains = (allowed: string, child: string) => {
  if (isWindowsRoot(allowed)) {
    if (!isWindowsRoot(child)) return false;
    const allowedParts = resolveSegments(allowed.replaceAll("/", "\\").split("\\")).map((part) =>
      part.toLocaleLowerCase("en-US"),
    );
    const childParts = resolveSegments(child.replaceAll("/", "\\").split("\\")).map((part) =>
      part.toLocaleLowerCase("en-US"),
    );
    return allowedParts.every((part, index) => childParts[index] === part);
  }
  if (isWindowsRoot(child)) return false;
  const allowedParts = resolveSegments(allowed.split("/"));
  const childParts = resolveSegments(child.split("/"));
  return (
    allowed.startsWith("/") === child.startsWith("/") &&
    allowedParts.every((part, index) => childParts[index] === part)
  );
};
const subsetOf = (child: ReadonlyArray<string>, parent: ReadonlyArray<string>) =>
  parent.includes("*") || child.every((value) => parent.includes(value));
const writableRootsNarrow = (child: ReadonlyArray<string>, parent: ReadonlyArray<string>) =>
  child.every((root) => parent.some((allowed) => rootContains(allowed, root)));
const intersectToolAllowlist = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) => {
  if (left.includes("*")) return [...right];
  if (right.includes("*")) return [...left];
  return left.filter((value) => right.includes(value));
};
const intersectWritableRoots = (child: ReadonlyArray<string>, parent: ReadonlyArray<string>) =>
  child.filter((root) => parent.some((allowed) => rootContains(allowed, root)));
const runtimeModeSandbox = (
  runtimeMode: ProviderAdapterV2RuntimePolicyType["runtimeMode"],
): GoalSandboxMode =>
  runtimeMode === "approval-required"
    ? "read-only"
    : runtimeMode === "auto-accept-edits"
      ? "workspace-write"
      : "danger-full-access";
const runtimeModeApproval = (
  runtimeMode: ProviderAdapterV2RuntimePolicyType["runtimeMode"],
): GoalApprovalPolicy =>
  runtimeMode === "approval-required"
    ? "untrusted"
    : runtimeMode === "auto-accept-edits"
      ? "on-request"
      : "never";

const record = (value: unknown): Readonly<Record<string, unknown>> | undefined =>
  typeof value === "object" && value !== null
    ? (value as Readonly<Record<string, unknown>>)
    : undefined;

function inheritedSandboxMode(policy: ProviderAdapterV2RuntimePolicyType): GoalSandboxMode {
  const type = record(policy.sandboxPolicy)?.type;
  switch (type) {
    case "readOnly":
      return "read-only";
    case "workspaceWrite":
    case "externalSandbox":
      return "workspace-write";
    case "dangerFullAccess":
      return "danger-full-access";
    default:
      return runtimeModeSandbox(policy.runtimeMode);
  }
}

function inheritedApprovalPolicy(policy: ProviderAdapterV2RuntimePolicyType): GoalApprovalPolicy {
  if (policy.approvalPolicy === undefined) return runtimeModeApproval(policy.runtimeMode);
  return isGoalApprovalPolicy(policy.approvalPolicy) ? policy.approvalPolicy : "untrusted";
}

function inheritedWritableRoots(
  policy: ProviderAdapterV2RuntimePolicyType,
): ReadonlyArray<string> | undefined {
  const value = record(policy.sandboxPolicy)?.writableRoots;
  return Array.isArray(value) && value.every((root) => typeof root === "string")
    ? (value as ReadonlyArray<string>)
    : undefined;
}

function providerAllowed(
  policy: GoalWorkflowPolicy,
  providerInstanceId: ProviderInstanceId,
): boolean {
  return (
    policy.providerAllowlist.includes("*") || policy.providerAllowlist.includes(providerInstanceId)
  );
}

function providerSandboxPolicy(
  sandboxMode: GoalSandboxMode,
  writableRoots: ReadonlyArray<string>,
): unknown {
  switch (sandboxMode) {
    case "read-only":
      return { type: "readOnly" };
    case "workspace-write":
      return { type: "workspaceWrite", writableRoots: [...writableRoots] };
    case "danger-full-access":
      return { type: "dangerFullAccess" };
  }
}

function runtimeModeForGoalPolicy(
  sandboxMode: GoalSandboxMode,
  approvalPolicy: GoalApprovalPolicy,
): ProviderAdapterV2RuntimePolicyType["runtimeMode"] {
  if (sandboxMode === "read-only" || approvalPolicy === "untrusted") return "approval-required";
  if (sandboxMode === "workspace-write") return "auto-accept-edits";
  return approvalPolicy === "never" ? "full-access" : "auto-accept-edits";
}

function goalRuntimePolicyFailure(
  reason: GoalRuntimePolicyResolveError["reason"],
  detail: string,
  toolAllowlist?: ReadonlyArray<string>,
): never {
  throw new GoalRuntimePolicyResolveError({
    reason,
    detail,
    ...(toolAllowlist === undefined ? {} : { toolAllowlist: [...toolAllowlist] }),
  });
}

export interface GoalWorkerRuntimePolicyInput {
  readonly inherited: ProviderAdapterV2RuntimePolicyType;
  readonly rootPolicy: GoalWorkflowPolicy;
  readonly nodePolicy: GoalWorkflowPolicy;
  readonly workspaceMode: GoalGraphNode["workspaceMode"];
  readonly providerInstanceId: ProviderInstanceId;
}

/**
 * Resolve the policy for one durable goal worker attempt. The result is the
 * intersection of the selected node, the durable goal root, and the
 * thread's already-resolved provider policy. It deliberately produces
 * explicit provider fields so an auto-accept writer cannot accidentally
 * discard an `untrusted` node approval posture.
 */
export function resolveGoalWorkerRuntimePolicy(
  input: GoalWorkerRuntimePolicyInput,
): Effect.Effect<ProviderAdapterV2RuntimePolicyType, GoalRuntimePolicyResolveError> {
  return Effect.try({
    try: () => {
      const { inherited, nodePolicy, rootPolicy } = input;
      if (
        sandboxRank[nodePolicy.sandboxMode] > sandboxRank[rootPolicy.sandboxMode] ||
        approvalRank[nodePolicy.approvalPolicy] > approvalRank[rootPolicy.approvalPolicy] ||
        !writableRootsNarrow(nodePolicy.writableRoots, rootPolicy.writableRoots) ||
        !subsetOf(nodePolicy.providerAllowlist, rootPolicy.providerAllowlist) ||
        !subsetOf(nodePolicy.toolAllowlist, rootPolicy.toolAllowlist)
      ) {
        return goalRuntimePolicyFailure(
          "policy_expansion",
          "Goal node policy expands its durable root policy.",
        );
      }
      if (
        !providerAllowed(rootPolicy, input.providerInstanceId) ||
        !providerAllowed(nodePolicy, input.providerInstanceId)
      ) {
        return goalRuntimePolicyFailure(
          "provider_not_allowed",
          `Provider ${input.providerInstanceId} is not permitted by the active goal node policy.`,
        );
      }
      if (
        input.workspaceMode === "integration" ||
        (input.workspaceMode === "read_only" &&
          (nodePolicy.sandboxMode !== "read-only" || nodePolicy.writableRoots.length > 0)) ||
        (input.workspaceMode === "writer" && nodePolicy.sandboxMode !== "workspace-write")
      ) {
        return goalRuntimePolicyFailure(
          "workspace_policy_mismatch",
          "Goal node workspace mode does not match its sandbox policy.",
        );
      }

      // There is no provider-neutral named-tool protocol. Never treat a goal
      // allowlist such as ["shell"] as advisory: doing so would let an adapter
      // with no native mapping run arbitrary tools. A future adapter-specific
      // resolver may replace this guarded rejection with a verified mapping.
      if (!nodePolicy.toolAllowlist.includes("*")) {
        return goalRuntimePolicyFailure(
          "tool_allowlist_unsupported",
          "Restricted goal tool allowlists require an adapter-native enforcement mapping.",
          nodePolicy.toolAllowlist,
        );
      }

      const sandboxMode =
        sandboxRank[inheritedSandboxMode(inherited)] < sandboxRank[nodePolicy.sandboxMode]
          ? inheritedSandboxMode(inherited)
          : nodePolicy.sandboxMode;
      const approvalPolicy =
        approvalRank[inheritedApprovalPolicy(inherited)] < approvalRank[nodePolicy.approvalPolicy]
          ? inheritedApprovalPolicy(inherited)
          : nodePolicy.approvalPolicy;
      const inheritedRoots = inheritedWritableRoots(inherited);
      const writableRoots =
        sandboxMode === "workspace-write"
          ? inheritedRoots === undefined
            ? nodePolicy.writableRoots
            : intersectWritableRoots(nodePolicy.writableRoots, inheritedRoots)
          : [];
      const toolAllowlist =
        inherited.toolAllowlist === undefined
          ? nodePolicy.toolAllowlist
          : intersectToolAllowlist(nodePolicy.toolAllowlist, inherited.toolAllowlist);

      return ProviderAdapterV2RuntimePolicy.make({
        ...inherited,
        runtimeMode: runtimeModeForGoalPolicy(sandboxMode, approvalPolicy),
        approvalPolicy,
        sandboxPolicy: providerSandboxPolicy(sandboxMode, writableRoots),
        toolAllowlist: [...toolAllowlist],
      });
    },
    catch: (cause) =>
      isGoalRuntimePolicyResolveError(cause)
        ? cause
        : new GoalRuntimePolicyResolveError({
            reason: "policy_expansion",
            detail: `Could not resolve goal worker runtime policy: ${String(cause)}`,
          }),
  });
}

export const RuntimePolicyV2Override = Schema.Struct({
  cwd: Schema.optional(Schema.String),
  approvalPolicy: Schema.optional(Schema.Unknown),
  sandboxPolicy: Schema.optional(Schema.Unknown),
  reasoningEffort: Schema.optional(Schema.String),
});
export type RuntimePolicyV2Override = typeof RuntimePolicyV2Override.Type;

/**
 * SERVICE DEFINITION
 */
export interface RuntimePolicyV2Shape {
  readonly resolve: (input: {
    readonly thread: OrchestrationV2AppThread;
    readonly modelSelection: ModelSelection;
  }) => Effect.Effect<ProviderAdapterV2RuntimePolicyType, RuntimePolicyV2Error>;
}

export class RuntimePolicyV2 extends Context.Service<RuntimePolicyV2, RuntimePolicyV2Shape>()(
  "t3/orchestration-v2/RuntimePolicy/RuntimePolicyV2",
) {}

/**
 * IMPLEMENTATIONS
 */
export const layer: Layer.Layer<RuntimePolicyV2> = Layer.succeed(RuntimePolicyV2, {
  resolve: (input) =>
    Effect.succeed({
      runtimeMode: input.thread.runtimeMode,
      interactionMode: input.thread.interactionMode,
      cwd: input.thread.worktreePath,
    }),
});

export const layerFromProjectRepository: Layer.Layer<
  RuntimePolicyV2,
  never,
  ProjectionProjects.ProjectionProjectRepository
> = Layer.effect(
  RuntimePolicyV2,
  Effect.gen(function* () {
    const projects = yield* ProjectionProjects.ProjectionProjectRepository;
    return RuntimePolicyV2.of({
      resolve: Effect.fn("RuntimePolicyV2.resolve")(function* (input) {
        const cwd =
          input.thread.worktreePath ??
          (yield* projects.getById({ projectId: input.thread.projectId }).pipe(
            Effect.mapError(
              (cause) =>
                new RuntimePolicyResolveError({
                  projectId: input.thread.projectId,
                  providerInstanceId: input.modelSelection.instanceId,
                  cause,
                }),
            ),
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  Effect.fail(
                    new RuntimePolicyResolveError({
                      projectId: input.thread.projectId,
                      providerInstanceId: input.modelSelection.instanceId,
                      cause: "Project not found.",
                    }),
                  ),
                onSome: (project) => Effect.succeed(project.workspaceRoot),
              }),
            ),
          ));
        return ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: input.thread.runtimeMode,
          interactionMode: input.thread.interactionMode,
          cwd,
        });
      }),
    });
  }),
);

export function layerWithOverride(
  override: RuntimePolicyV2Override,
): Layer.Layer<RuntimePolicyV2, never, RuntimePolicyV2> {
  return Layer.effect(
    RuntimePolicyV2,
    Effect.gen(function* () {
      const base = yield* RuntimePolicyV2;
      return {
        resolve: (input) =>
          base.resolve(input).pipe(
            Effect.map((policy) =>
              ProviderAdapterV2RuntimePolicy.make({
                ...policy,
                ...(override.cwd === undefined ? {} : { cwd: override.cwd }),
                ...(override.approvalPolicy === undefined
                  ? {}
                  : { approvalPolicy: override.approvalPolicy }),
                ...(override.sandboxPolicy === undefined
                  ? {}
                  : { sandboxPolicy: override.sandboxPolicy }),
                ...(override.reasoningEffort === undefined
                  ? {}
                  : { reasoningEffort: override.reasoningEffort }),
              }),
            ),
          ),
      } satisfies RuntimePolicyV2Shape;
    }),
  );
}
