import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";

import {
  describeGoalRoutingCatalog,
  resolveGoalRoute,
  type GoalRoutingCatalogEntry,
} from "./GoalRoutingService.ts";

const entry = (
  instanceId: string,
  model: string,
  capabilities: ReadonlyArray<string>,
  overrides: Partial<GoalRoutingCatalogEntry> = {},
): GoalRoutingCatalogEntry => ({
  providerInstanceId: ProviderInstanceId.make(instanceId),
  model,
  capabilities,
  latencyClasses: ["standard"],
  costClasses: ["standard"],
  enabled: true,
  installed: true,
  authenticated: true,
  ...overrides,
});

describe("resolveGoalRoute", () => {
  it("describes the scheduler catalog with capabilities and goal policy constraints", () => {
    expect(
      describeGoalRoutingCatalog({
        catalog: [
          entry("codex", "gpt-5.4", ["workspace.patch", "analysis", "tools.shell"]),
          entry("claudeAgent", "claude-sonnet-4-6", ["tools.shell", "analysis"]),
        ],
        providerAllowlist: ["codex"],
      }),
    ).toEqual([
      {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
        capabilities: ["analysis", "tools.shell"],
        unmetConstraints: ["provider_allowlist:claudeAgent"],
      },
      {
        providerInstanceId: "codex",
        model: "gpt-5.4",
        capabilities: ["analysis", "tools.shell", "workspace.patch"],
        unmetConstraints: [],
      },
    ]);
  });

  it("resolves an exact authenticated policy-compliant route", () => {
    const requested = {
      type: "exact" as const,
      providerInstanceId: ProviderInstanceId.make("codex-personal"),
      model: "gpt-5.4",
    };
    expect(
      resolveGoalRoute({
        requested,
        requiredCapabilities: ["tools.shell"],
        providerAllowlist: ["codex-personal"],
        catalog: [entry("codex-personal", "gpt-5.4", ["tools.shell", "subagents.native"])],
      }),
    ).toEqual({
      type: "resolved",
      route: {
        requested,
        providerInstanceId: "codex-personal",
        model: "gpt-5.4",
        capabilitySnapshot: ["subagents.native", "tools.shell"],
        rationale:
          "Exact provider/model satisfied policy, authentication, and capability constraints.",
      },
    });
  });

  it("returns typed unmet constraints instead of inheriting an unavailable exact route", () => {
    const decision = resolveGoalRoute({
      requested: {
        type: "exact",
        providerInstanceId: ProviderInstanceId.make("claude-work"),
        model: "claude-sonnet-5",
      },
      requiredCapabilities: ["tools.shell"],
      providerAllowlist: ["codex-personal"],
      catalog: [entry("claude-work", "claude-sonnet-5", ["tools.shell"])],
    });
    expect(decision.type).toBe("ambiguous");
    if (decision.type === "ambiguous") {
      expect(decision.unmetConstraints).toContain("provider_allowlist:claude-work");
      expect(decision.candidates[0]?.unmetConstraints).toContain("provider_allowlist:claude-work");
    }
  });

  it("does not silently rank multiple requirement matches", () => {
    const decision = resolveGoalRoute({
      requested: {
        type: "requirements",
        capabilities: ["tools.shell"],
        latencyClass: "standard",
        costClass: "standard",
      },
      requiredCapabilities: ["checkpointing.app"],
      providerAllowlist: ["codex", "claude"],
      catalog: [
        entry("codex", "gpt-5.4", ["tools.shell", "checkpointing.app"]),
        entry("claude", "claude-sonnet-5", ["tools.shell", "checkpointing.app"]),
      ],
    });
    expect(decision.type).toBe("ambiguous");
    if (decision.type === "ambiguous") {
      expect(decision.candidates).toHaveLength(2);
      expect(decision.unmetConstraints).toEqual(["multiple_matching_routes"]);
    }
  });

  it("resolves a sole requirements match and exposes why other candidates failed", () => {
    const requested = {
      type: "requirements" as const,
      capabilities: ["tools.shell"],
      latencyClass: "batch" as const,
      costClass: "economy" as const,
    };
    const decision = resolveGoalRoute({
      requested,
      requiredCapabilities: [],
      providerAllowlist: ["fast", "slow"],
      catalog: [
        entry("fast", "economy-model", ["tools.shell"], {
          latencyClasses: ["batch"],
          costClasses: ["economy"],
        }),
        entry("slow", "premium-model", ["tools.shell"], {
          latencyClasses: ["standard"],
          costClasses: ["premium"],
        }),
      ],
    });
    expect(decision.type).toBe("resolved");
    if (decision.type === "resolved") {
      expect(decision.route.providerInstanceId).toBe("fast");
      expect(decision.route.rationale).toContain("sole");
    }
  });
});
