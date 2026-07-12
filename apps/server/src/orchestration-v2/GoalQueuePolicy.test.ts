import { describe, expect, it } from "@effect/vitest";

import { goalThreadAcceptsHumanOperation } from "./Orchestrator.ts";

describe("goal root-only human controls", () => {
  it("rejects web, mobile, and MCP operations on worker threads", () => {
    expect(goalThreadAcceptsHumanOperation("worker", "web")).toBe(false);
    expect(goalThreadAcceptsHumanOperation("worker", "mobile")).toBe(false);
    expect(goalThreadAcceptsHumanOperation("worker", "mcp")).toBe(false);
  });

  it("allows root operations and server-owned worker lifecycle commands", () => {
    expect(goalThreadAcceptsHumanOperation("lead", "mobile")).toBe(true);
    expect(goalThreadAcceptsHumanOperation(null, "web")).toBe(true);
    expect(goalThreadAcceptsHumanOperation("worker", "server")).toBe(true);
  });
});
