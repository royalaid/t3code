// @effect-diagnostics-next-line nodeBuiltinImport:off -- exercises the local filesystem migration boundary.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- constructs temporary fixture paths for filesystem migration coverage.
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

import {
  migrateInstallation,
  preflightInstallationMigration,
  resolveInstallationProfile,
} from "./InstallationMigration.ts";

const temporaryRoots: string[] = [];

async function createHomeDirectory(): Promise<string> {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "scriptorium-migration-"));
  temporaryRoots.push(root);
  return root;
}

afterEach(async () => {
  await Promise.all(
    temporaryRoots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});

describe("InstallationMigration", () => {
  it("copies a T3 Code profile into Scriptorium and retains a restore point", async () => {
    const homeDirectory = await createHomeDirectory();
    const source = resolveInstallationProfile("t3code", homeDirectory);
    const destination = resolveInstallationProfile("scriptorium", homeDirectory);
    await NodeFSP.mkdir(NodePath.join(source.baseDir, "userdata", "attachments"), {
      recursive: true,
    });
    await NodeFSP.writeFile(NodePath.join(source.baseDir, "userdata", "state.sqlite"), "history");
    await NodeFSP.writeFile(
      NodePath.join(source.baseDir, "userdata", "attachments", "note.txt"),
      "attachment",
    );
    await NodeFSP.mkdir(destination.baseDir, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(destination.baseDir, "old-state"), "restore me");

    const result = await migrateInstallation({
      direction: "t3code-to-scriptorium",
      homeDirectory,
      timestamp: "2026-07-16T19:00:00.000Z",
    });

    await expect(
      NodeFSP.readFile(NodePath.join(destination.baseDir, "userdata", "state.sqlite"), "utf8"),
    ).resolves.toBe("history");
    await expect(
      NodeFSP.readFile(
        NodePath.join(destination.baseDir, "userdata", "attachments", "note.txt"),
        "utf8",
      ),
    ).resolves.toBe("attachment");
    await expect(
      NodeFSP.readFile(
        NodePath.join(
          destination.baseDir + ".restore-points",
          "2026-07-16T19-00-00.000Z",
          ".royal-scriptorium",
          "old-state",
        ),
        "utf8",
      ),
    ).resolves.toBe("restore me");
    expect(result.restorePoint).toBe(
      `${destination.baseDir}.restore-points/2026-07-16T19-00-00.000Z`,
    );
    expect(result.copiedPaths).toContain(
      "profile state, SQLite history, settings, attachments, and logs",
    );
  });

  it("resolves reverse migration profiles without sharing T3 Code paths", () => {
    const homeDirectory = "/Users/royal";

    expect(resolveInstallationProfile("scriptorium", homeDirectory)).toEqual({
      kind: "scriptorium",
      baseDir: "/Users/royal/.royal-scriptorium",
      userDataDir: "/Users/royal/Library/Application Support/royals-scriptorium",
    });
    expect(resolveInstallationProfile("t3code", homeDirectory)).toEqual({
      kind: "t3code",
      baseDir: "/Users/royal/.t3",
      userDataDir: "/Users/royal/Library/Application Support/t3code",
    });
  });

  it("reports missing source data before attempting a migration", async () => {
    const homeDirectory = await createHomeDirectory();

    await expect(
      preflightInstallationMigration({
        direction: "scriptorium-to-t3code",
        homeDirectory,
      }),
    ).resolves.toMatchObject({
      formatVersion: 1,
      sourceAvailable: false,
      destinationExists: false,
    });
  });
});
