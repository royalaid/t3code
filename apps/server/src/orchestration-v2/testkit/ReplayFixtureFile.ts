import * as NodeURL from "node:url";

/** Converts a fixture URL into the native path expected by the platform FileSystem service. */
export function replayFixturePath(file: URL): string {
  return NodeURL.fileURLToPath(file);
}
