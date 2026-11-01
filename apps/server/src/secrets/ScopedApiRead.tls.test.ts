// @effect-diagnostics nodeBuiltinImport:off - isolated synthetic TLS server and client.
import * as Https from "node:https";
import * as Tls from "node:tls";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";
import { apiReadUrl, scopedApiRead } from "./ScopedApiRead.ts";

const state = vi.hoisted(() => ({ port: 0 }));
vi.mock("../htmlRender/publicProxy.ts", () => ({
  publicAddresses: async () => [{ address: "127.0.0.1", family: 4 }],
}));
vi.mock("node:https", async (importOriginal) => {
  const actual = await importOriginal<typeof Https>();
  return {
    ...actual,
    request: (
      url: URL,
      options: Https.RequestOptions,
      handler: Parameters<typeof actual.request>[2],
    ) =>
      actual.request(
        new URL(`https://${url.hostname}:${state.port}${url.pathname}`),
        options,
        handler,
      ),
  };
});

// Public synthetic fixture material, never an account credential.
const key =
  "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgyfozWfQr3pirjIiV\ncE6+ghV14ssar4fnxtVT+CcKZ2mhRANCAASNYECXJvUiTBXYz80GnX7I4REItVuE\n+xPYGtl9cYzfOr+1G3In+MdRwXrVYe9iXRlfff2zN2i9jTxncHPa2ERq\n-----END PRIVATE KEY-----\n";
const cert =
  "-----BEGIN CERTIFICATE-----\nMIIBmTCCAT+gAwIBAgIUC3VJ98Jyz/PM0jwAQh9GfGF4FNcwCgYIKoZIzj0EAwIw\nFjEUMBIGA1UEAwwLZXhhbXBsZS5jb20wHhcNMjYxMTAxMDUzMTAwWhcNMzYxMDI5\nMDUzMTAwWjAWMRQwEgYDVQQDDAtleGFtcGxlLmNvbTBZMBMGByqGSM49AgEGCCqG\nSM49AwEHA0IABI1gQJcm9SJMFdjPzQadfsjhEQi1W4T7E9ga2X1xjN86v7Ubcif4\nx1HBetVh72JdGV99/bM3aL2NPGdwc9rYRGqjazBpMB0GA1UdDgQWBBS1itoOf1Fd\nGMCguKDDuB865SW/BzAfBgNVHSMEGDAWgBS1itoOf1FdGMCguKDDuB865SW/BzAP\nBgNVHRMBAf8EBTADAQH/MBYGA1UdEQQPMA2CC2V4YW1wbGUuY29tMAoGCCqGSM49\nBAMCA0gAMEUCIQC7+2HJ0e6eLehj/iSKansMX0Y5c5Ce1TGRhfPyo0lFaQIgCHae\ntD3mTy1tvzehRxCX2ZNfQ0Vlfrc9ZcO6iUcVFRU=\n-----END CERTIFICATE-----\n";
const token = "synthetic-tls-bearer-0123456789";
let received = "";
let requests = 0;
const server = Https.createServer({ key, cert }, (request, response) => {
  requests += 1;
  received = request.headers.authorization ?? "";
  response.setHeader("Content-Type", "application/json");
  if (request.url !== "/stall") response.end('{"ok":true}');
});
const previousCertificates = Tls.getCACertificates("default");
beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  state.port = (server.address() as AddressInfo).port;
});
afterAll(async () => {
  Tls.setDefaultCACertificates(previousCertificates);
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe("synthetic TLS integration", () => {
  it("fails closed on an untrusted certificate before sending Authorization", async () => {
    await expect(scopedApiRead(apiReadUrl("https://example.com/read"), token)).rejects.toThrow(
      "network",
    );
    expect(requests).toBe(0);
    expect(received).toBe("");
  });
  it("uses the pinned socket with certificate and hostname verification", async () => {
    Tls.setDefaultCACertificates([...previousCertificates, cert]);
    const result = await scopedApiRead(apiReadUrl("https://example.com/read"), token);
    expect(result).toEqual({ status: 200, data: { ok: true } });
    expect(received).toBe(`Bearer ${token}`);
    expect(JSON.stringify(result)).not.toContain(token);
    expect(requests).toBe(1);
  });
  it("aborts a stalled response at the overall deadline", async () => {
    Tls.setDefaultCACertificates([...previousCertificates, cert]);
    const start = performance.now();
    await expect(scopedApiRead(apiReadUrl("https://example.com/stall"), token)).rejects.toThrow(
      "network",
    );
    expect(performance.now() - start).toBeGreaterThanOrEqual(14_000);
    expect(performance.now() - start).toBeLessThan(25_000);
    expect(requests).toBe(2);
  }, 30_000);
  it("refuses a mismatched hostname even when the certificate is trusted", async () => {
    Tls.setDefaultCACertificates([...previousCertificates, cert]);
    await expect(
      scopedApiRead(apiReadUrl("https://different.example/read"), token),
    ).rejects.toThrow("network");
    expect(requests).toBe(2);
  });
});
