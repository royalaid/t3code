// @effect-diagnostics nodeBuiltinImport:off - TLS requests need a pinned DNS lookup.
import * as Https from "node:https";
import { publicAddresses } from "../htmlRender/publicProxy.ts";

const MAX_BYTES = 1024 * 1024;
const DEADLINE_MS = 15_000;

export class ApiReadError extends Error {
  constructor(category: "destination" | "credential" | "network" | "response") {
    super(`API read refused: ${category}.`);
  }
}

/** A URL is shown verbatim on the private card before any credential is accepted. */
export function apiReadUrl(value: string): URL {
  try {
    if (value.length > 4096 || /[\x00-\x20\x7f\\]/.test(value)) throw new Error();
    const url = new URL(value);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.hash ||
      url.search ||
      (url.port && url.port !== "443") ||
      !/^[a-z0-9.-]+$/i.test(url.hostname) ||
      !url.hostname.includes(".") ||
      /^[\d.]+$/.test(url.hostname)
    )
      throw new Error();
    return url;
  } catch {
    throw new ApiReadError("destination");
  }
}

export function apiReadJson(bytes: Buffer, token: string): unknown {
  try {
    if (bytes.length > MAX_BYTES) throw new Error();
    const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    const json: unknown = JSON.parse(text);
    const decoded = JSON.stringify(json);
    const representations = [
      token,
      encodeURIComponent(token),
      Buffer.from(token).toString("base64"),
      Buffer.from(token).toString("base64url"),
    ];
    if (representations.some((value) => text.includes(value) || decoded.includes(value))) {
      throw new Error();
    }
    return json;
  } catch {
    throw new ApiReadError("response");
  }
}

/** Never throws a transport error containing request headers, options or response data. */
export async function scopedApiRead(
  url: URL,
  token: string,
  signal?: AbortSignal,
): Promise<{
  status: number;
  data: unknown;
}> {
  if (!token || Buffer.byteLength(token) > 8192 || /[\x00-\x20\x7f-\uffff]/.test(token)) {
    throw new ApiReadError("credential");
  }
  // Re-validate even for internal callers. No API can supply transport options.
  url = apiReadUrl(url.href);
  const deadline = AbortSignal.timeout(DEADLINE_MS);
  const abort = signal ? AbortSignal.any([deadline, signal]) : deadline;
  try {
    const addresses = await Promise.race([
      publicAddresses(url.hostname),
      new Promise<never>((_, reject) => {
        abort.addEventListener("abort", () => reject(new ApiReadError("network")), { once: true });
        if (abort.aborted) reject(new ApiReadError("network"));
      }),
    ]);
    if (!addresses?.length) throw new ApiReadError("destination");
    const address = addresses[0]!;
    return await new Promise((resolve, reject) => {
      if (abort.aborted) {
        reject(new ApiReadError("network"));
        return;
      }
      const request = Https.request(
        url,
        {
          method: "GET",
          agent: false,
          signal: abort,
          rejectUnauthorized: true,
          family: address.family,
          servername: url.hostname,
          lookup: (_hostname, _options, callback) =>
            callback(null, address.address, address.family),
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/json",
            "Accept-Encoding": "identity",
          },
        },
        (response) => {
          const status = response.statusCode ?? 0;
          const mime =
            response.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase() ?? "";
          if (
            status < 200 ||
            status >= 300 ||
            !(mime === "application/json" || /^application\/[a-z0-9.+-]+\+json$/.test(mime)) ||
            (response.headers["content-encoding"] &&
              response.headers["content-encoding"] !== "identity")
          ) {
            response.destroy();
            reject(new ApiReadError("response"));
            return;
          }
          const chunks: Buffer[] = [];
          let size = 0;
          response.on("data", (chunk: Buffer) => {
            size += chunk.length;
            if (size > MAX_BYTES) {
              response.destroy();
              reject(new ApiReadError("response"));
            } else chunks.push(chunk);
          });
          response.on("error", () => reject(new ApiReadError("network")));
          response.on("end", () => {
            try {
              resolve({ status, data: apiReadJson(Buffer.concat(chunks), token) });
            } catch {
              reject(new ApiReadError("response"));
            }
          });
        },
      );
      request.on("error", () => reject(new ApiReadError("network")));
      request.end();
    });
  } catch (error) {
    throw error instanceof ApiReadError ? error : new ApiReadError("network");
  }
}
