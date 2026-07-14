import {
  type GoalResolvedRoute,
  type GoalRouteCandidate,
  type GoalRoutingDecision,
  type GoalRoutingRequest,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";

export interface GoalRoutingCatalogEntry {
  readonly providerInstanceId: ProviderInstanceId;
  readonly model: string;
  readonly capabilities: ReadonlyArray<string>;
  readonly latencyClasses: ReadonlyArray<"interactive" | "standard" | "batch">;
  readonly costClasses: ReadonlyArray<"economy" | "standard" | "premium">;
  readonly enabled: boolean;
  readonly installed: boolean;
  readonly authenticated: boolean;
}

export interface GoalRouteInput {
  readonly requested: GoalRoutingRequest;
  readonly requiredCapabilities: ReadonlyArray<string>;
  readonly providerAllowlist: ReadonlyArray<string>;
  readonly catalog: ReadonlyArray<GoalRoutingCatalogEntry>;
}

const sortedUnique = (values: Iterable<string>): ReadonlyArray<string> =>
  [...new Set(values)].toSorted((left, right) => left.localeCompare(right));

function baseUnmetConstraints(
  providerAllowlist: ReadonlyArray<string>,
  entry: GoalRoutingCatalogEntry,
): ReadonlyArray<string> {
  const allowed =
    providerAllowlist.includes("*") || providerAllowlist.includes(entry.providerInstanceId);
  return [
    ...(allowed ? [] : [`provider_allowlist:${entry.providerInstanceId}`]),
    ...(entry.enabled ? [] : [`provider_disabled:${entry.providerInstanceId}`]),
    ...(entry.installed ? [] : [`provider_not_installed:${entry.providerInstanceId}`]),
    ...(entry.authenticated ? [] : [`provider_not_authenticated:${entry.providerInstanceId}`]),
  ];
}

export function describeGoalRoutingCatalog(input: {
  readonly catalog: ReadonlyArray<GoalRoutingCatalogEntry>;
  readonly providerAllowlist: ReadonlyArray<string>;
}): ReadonlyArray<GoalRouteCandidate> {
  return input.catalog
    .map((entry) => ({
      providerInstanceId: entry.providerInstanceId,
      model: entry.model,
      capabilities: sortedUnique(entry.capabilities),
      unmetConstraints: sortedUnique(baseUnmetConstraints(input.providerAllowlist, entry)),
    }))
    .toSorted((left, right) => {
      const providerOrder = left.providerInstanceId.localeCompare(right.providerInstanceId);
      return providerOrder === 0 ? left.model.localeCompare(right.model) : providerOrder;
    });
}

function candidateFor(input: GoalRouteInput, entry: GoalRoutingCatalogEntry): GoalRouteCandidate {
  const required =
    input.requested.type === "requirements"
      ? [...input.requiredCapabilities, ...input.requested.capabilities]
      : input.requiredCapabilities;
  const capabilities = sortedUnique(entry.capabilities);
  const unmet = [
    ...baseUnmetConstraints(input.providerAllowlist, entry),
    ...sortedUnique(required)
      .filter((capability) => !capabilities.includes(capability))
      .map((capability) => `capability:${capability}`),
    ...(input.requested.type === "requirements" &&
    !entry.latencyClasses.includes(input.requested.latencyClass)
      ? [`latency_class:${input.requested.latencyClass}`]
      : []),
    ...(input.requested.type === "requirements" &&
    !entry.costClasses.includes(input.requested.costClass)
      ? [`cost_class:${input.requested.costClass}`]
      : []),
  ];
  return {
    providerInstanceId: entry.providerInstanceId,
    model: entry.model,
    capabilities,
    unmetConstraints: sortedUnique(unmet),
  };
}

function resolvedRoute(
  requested: GoalRoutingRequest,
  candidate: GoalRouteCandidate,
  rationale: string,
): GoalResolvedRoute {
  return {
    requested,
    providerInstanceId: candidate.providerInstanceId,
    model: candidate.model,
    capabilitySnapshot: candidate.capabilities,
    rationale,
  };
}

export function resolveGoalRoute(input: GoalRouteInput): GoalRoutingDecision {
  const orderedCatalog = input.catalog.toSorted((left, right) => {
    const providerOrder = left.providerInstanceId.localeCompare(right.providerInstanceId);
    return providerOrder === 0 ? left.model.localeCompare(right.model) : providerOrder;
  });
  if (input.requested.type === "exact") {
    const requested = input.requested;
    const exact = orderedCatalog.find(
      (entry) =>
        entry.providerInstanceId === requested.providerInstanceId &&
        entry.model === requested.model,
    );
    if (exact === undefined) {
      return {
        type: "ambiguous",
        candidates: [],
        unmetConstraints: [
          `exact_route_unavailable:${requested.providerInstanceId}/${requested.model}`,
        ],
      };
    }
    const candidate = candidateFor(input, exact);
    if (candidate.unmetConstraints.length > 0) {
      return {
        type: "ambiguous",
        candidates: [candidate],
        unmetConstraints: candidate.unmetConstraints,
      };
    }
    return {
      type: "resolved",
      route: resolvedRoute(
        input.requested,
        candidate,
        "Exact provider/model satisfied policy, authentication, and capability constraints.",
      ),
    };
  }

  const candidates = orderedCatalog.map((entry) => candidateFor(input, entry));
  const matches = candidates.filter((candidate) => candidate.unmetConstraints.length === 0);
  if (matches.length === 1) {
    return {
      type: "resolved",
      route: resolvedRoute(
        input.requested,
        matches[0]!,
        "The sole policy-compliant route satisfied every requested capability, latency, and cost constraint.",
      ),
    };
  }
  return {
    type: "ambiguous",
    candidates,
    unmetConstraints:
      matches.length > 1
        ? ["multiple_matching_routes"]
        : sortedUnique([
            "no_matching_route",
            ...candidates.flatMap((candidate) => candidate.unmetConstraints),
          ]),
  };
}

export class GoalRoutingError extends Schema.TaggedErrorClass<GoalRoutingError>()(
  "GoalRoutingError",
  { operation: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

export class GoalRoutingService extends Context.Service<
  GoalRoutingService,
  {
    readonly route: (
      input: Omit<GoalRouteInput, "catalog">,
    ) => Effect.Effect<GoalRoutingDecision, GoalRoutingError>;
    readonly catalog: Effect.Effect<ReadonlyArray<GoalRoutingCatalogEntry>, GoalRoutingError>;
  }
>()("t3/orchestration-v2/GoalRoutingService") {}

function flattenCapabilities(value: unknown, prefix = ""): ReadonlyArray<string> {
  if (value === true) return prefix === "" ? [] : [prefix];
  if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
  return Object.entries(value).flatMap(([key, child]) =>
    flattenCapabilities(child, prefix === "" ? key : `${prefix}.${key}`),
  );
}

export const layer = Layer.effect(
  GoalRoutingService,
  Effect.gen(function* () {
    const instances = yield* ProviderInstanceRegistry;
    const catalog = Effect.gen(function* () {
      const available = yield* instances.listInstances;
      return yield* Effect.forEach(available, (instance) =>
        Effect.gen(function* () {
          const [snapshot, capabilities] = yield* Effect.all([
            instance.snapshot.getSnapshot,
            instance.orchestrationAdapter.getCapabilities(),
          ]);
          const capabilitySnapshot = sortedUnique(flattenCapabilities(capabilities));
          return snapshot.models.map(
            (model): GoalRoutingCatalogEntry => ({
              providerInstanceId: instance.instanceId,
              model: model.slug,
              capabilities: capabilitySnapshot,
              // Providers do not expose neutral latency/cost metadata yet. Requirement
              // routes remain typed-ambiguous until explicit metadata is available.
              latencyClasses: [],
              costClasses: [],
              enabled: snapshot.enabled,
              installed: snapshot.installed,
              authenticated: snapshot.auth.status === "authenticated",
            }),
          );
        }),
      ).pipe(
        Effect.map((entries) => entries.flat()),
        Effect.mapError((cause) => new GoalRoutingError({ operation: "catalog", cause })),
      );
    });
    return GoalRoutingService.of({
      catalog,
      route: (input) =>
        catalog.pipe(Effect.map((entries) => resolveGoalRoute({ ...input, catalog: entries }))),
    });
  }),
);
