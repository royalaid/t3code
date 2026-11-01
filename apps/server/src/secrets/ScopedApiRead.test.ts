// @effect-diagnostics nodeBuiltinImport:off - synthetic transport fixtures only.
import { describe, expect, it, vi, beforeEach } from "vite-plus/test";
import { EventEmitter } from "node:events";
import type * as Https from "node:https";
import { apiReadUrl, apiReadJson, scopedApiRead } from "./ScopedApiRead.ts";

const fixtures = vi.hoisted(() => ({
  addresses: [{ address: "93.184.216.34", family: 4 }],
  status: 200,
  headers: { "content-type": "application/json" } as Record<string, string>,
  body: '{"count":3}',
  error: false,
  calls: [] as Array<{ url: URL; options: Https.RequestOptions }>,
}));
vi.mock("../htmlRender/publicProxy.ts", () => ({
  publicAddresses: async () => fixtures.addresses,
}));
vi.mock("node:https", () => ({
  request: (
    url: URL,
    options: Https.RequestOptions,
    respond: (
      response: EventEmitter & {
        statusCode: number;
        headers: Record<string, string>;
        destroy: () => void;
      },
    ) => void,
  ) => {
    fixtures.calls.push({ url, options });
    const request = new EventEmitter() as EventEmitter & { end: () => void };
    request.end = () =>
      queueMicrotask(() => {
        if (fixtures.error) {
          request.emit("error", new Error("sensitive transport diagnostic"));
          return;
        }
        const response = Object.assign(new EventEmitter(), {
          statusCode: fixtures.status,
          headers: fixtures.headers,
          destroy: vi.fn(),
        });
        respond(response);
        response.emit("data", Buffer.from(fixtures.body));
        response.emit("end");
      });
    return request;
  },
}));

const TOKEN = "synthetic-token-only-abcdefgh123456";
beforeEach(() => {
  fixtures.addresses = [{ address: "93.184.216.34", family: 4 }];
  fixtures.status = 200;
  fixtures.headers = { "content-type": "application/json" };
  fixtures.body = '{"count":3}';
  fixtures.error = false;
  fixtures.calls = [];
});

describe("scoped API read", () => {
  it.each([
    "http://example.com/a",
    "https://name:password@example.com/a",
    "https://example.com/a?q=x",
    "https://example.com/a#x",
    "https://example.com:8443/a",
    "https://127.0.0.1/a",
    "https://[::1]/a",
    "https://localhost/a",
    "https://example.com/\npath",
    "https://example.com\\@other.example/a",
  ])("rejects an unapprovable destination: %s", (url) => {
    expect(() => apiReadUrl(url)).toThrow("destination");
  });
  it("makes one pinned TLS GET and returns only JSON and numeric status", async () => {
    const result = await scopedApiRead(apiReadUrl("https://example.com/read"), TOKEN);
    expect(result).toEqual({ status: 200, data: { count: 3 } });
    expect(JSON.stringify(result)).not.toContain(TOKEN);
    expect(fixtures.calls).toHaveLength(1);
    const { url, options } = fixtures.calls[0]!;
    expect(url.href).toBe("https://example.com/read");
    expect(options).toMatchObject({
      method: "GET",
      agent: false,
      rejectUnauthorized: true,
      servername: "example.com",
      headers: { Authorization: `Bearer ${TOKEN}` },
    });
    const lookup = vi.fn();
    options.lookup!("ignored.example", {}, lookup);
    expect(lookup).toHaveBeenCalledWith(null, "93.184.216.34", 4);
  });
  it("refuses a resolver rejection without starting a request", async () => {
    fixtures.addresses = [];
    await expect(scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN)).rejects.toThrow(
      "destination",
    );
    expect(fixtures.calls).toHaveLength(0);
  });
  it.each(["", "token\r\nInjected: yes", "token\u0000", "x".repeat(8193)])(
    "rejects invalid bearer values",
    async (value) => {
      await expect(scopedApiRead(apiReadUrl("https://example.com/a"), value)).rejects.toThrow(
        "credential",
      );
      expect(fixtures.calls).toHaveLength(0);
    },
  );
  it.each([301, 302, 307, 308, 401, 500])(
    "does not follow or expose response %s",
    async (status) => {
      fixtures.status = status;
      fixtures.headers.location = "https://another.example/";
      fixtures.body = TOKEN;
      await expect(scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN)).rejects.toThrow(
        "response",
      );
      expect(fixtures.calls).toHaveLength(1);
    },
  );
  it.each([
    { "content-type": "text/plain" },
    { "content-type": "application/json", "content-encoding": "gzip" },
  ])("refuses unsafe response framing", async (headers) => {
    fixtures.headers = headers;
    await expect(scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN)).rejects.toThrow(
      "response",
    );
  });
  it("caps the streamed body", async () => {
    fixtures.body = '"' + "a".repeat(1024 * 1024) + '"';
    await expect(scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN)).rejects.toThrow(
      "response",
    );
  });
  it("suppresses raw, escaped and common encoded credential reflection", () => {
    const bodies = [
      JSON.stringify({ echo: TOKEN }),
      JSON.stringify({ echo: Buffer.from(TOKEN).toString("base64") }),
      JSON.stringify({ echo: Buffer.from(TOKEN).toString("base64url") }),
      '{"echo":"' +
        [...TOKEN].map((c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0")).join("") +
        '"}',
    ];
    for (const body of bodies) {
      expect(() => apiReadJson(Buffer.from(body), TOKEN)).toThrow("response");
    }
    expect(() => apiReadJson(Buffer.from('{"echo":"a%2Fb"}'), "a/b")).toThrow("response");
  });
  it("never returns invalid JSON or raw transport errors", async () => {
    expect(() => apiReadJson(Buffer.from("invalid body"), TOKEN)).toThrow("response");
    fixtures.error = true;
    await expect(scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN)).rejects.toThrow(
      "API read refused: network.",
    );
  });
  it("cancels before transport when the invocation is interrupted", async () => {
    await expect(
      scopedApiRead(apiReadUrl("https://example.com/a"), TOKEN, AbortSignal.abort()),
    ).rejects.toThrow("network");
    expect(fixtures.calls).toHaveLength(0);
  });
});
