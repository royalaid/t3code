// @effect-diagnostics-next-line nodeBuiltinImport:off -- filesystem migration runs before an Effect runtime exists.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- filesystem migration runs before an Effect runtime exists.
import * as NodePath from "node:path";

export const INSTALLATION_MIGRATION_FORMAT_VERSION = 1;

export type InstallationKind = "t3code" | "scriptorium";
export type InstallationMigrationDirection = "t3code-to-scriptorium" | "scriptorium-to-t3code";

export interface InstallationProfile {
  readonly kind: InstallationKind;
  readonly baseDir: string;
  readonly userDataDir: string;
}

export interface InstallationMigrationResult {
  readonly formatVersion: number;
  readonly direction: InstallationMigrationDirection;
  readonly source: InstallationProfile;
  readonly destination: InstallationProfile;
  readonly restorePoint: string | null;
  readonly copiedPaths: readonly string[];
  readonly credentialNotice: string;
}

export class InstallationMigrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InstallationMigrationError";
  }
}

export function resolveInstallationProfile(
  kind: InstallationKind,
  homeDirectory: string,
): InstallationProfile {
  const appSupport = NodePath.join(homeDirectory, "Library", "Application Support");
  return kind === "scriptorium"
    ? {
        kind,
        baseDir: NodePath.join(homeDirectory, ".royal-scriptorium"),
        userDataDir: NodePath.join(appSupport, "royals-scriptorium"),
      }
    : {
        kind,
        baseDir: NodePath.join(homeDirectory, ".t3"),
        userDataDir: NodePath.join(appSupport, "t3code"),
      };
}

function profilesForDirection(
  direction: InstallationMigrationDirection,
): readonly [InstallationKind, InstallationKind] {
  return direction === "t3code-to-scriptorium"
    ? ["t3code", "scriptorium"]
    : ["scriptorium", "t3code"];
}

async function exists(path: string): Promise<boolean> {
  try {
    await NodeFSP.access(path);
    return true;
  } catch {
    return false;
  }
}

async function copyDirectory(source: string, destination: string): Promise<void> {
  await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
  await NodeFSP.cp(source, destination, { recursive: true, force: true, errorOnExist: false });
}

async function moveToRestorePoint(path: string, restoreRoot: string): Promise<string | null> {
  if (!(await exists(path))) return null;
  const target = NodePath.join(restoreRoot, NodePath.basename(path));
  await NodeFSP.mkdir(restoreRoot, { recursive: true });
  await NodeFSP.rename(path, target);
  return target;
}

export async function migrateInstallation(input: {
  readonly direction: InstallationMigrationDirection;
  readonly homeDirectory: string;
  readonly timestamp: string;
}): Promise<InstallationMigrationResult> {
  const [sourceKind, destinationKind] = profilesForDirection(input.direction);
  const source = resolveInstallationProfile(sourceKind, input.homeDirectory);
  const destination = resolveInstallationProfile(destinationKind, input.homeDirectory);
  if (!(await exists(source.baseDir)) && !(await exists(source.userDataDir))) {
    throw new InstallationMigrationError(`No ${source.kind} profile was found to migrate.`);
  }

  const timestamp = input.timestamp.replaceAll(":", "-");
  const restoreRoot = `${destination.baseDir}.restore-points/${timestamp}`;
  const stageRoot = `${destination.baseDir}.migration-stage-${timestamp}`;
  const stagedBaseDir = NodePath.join(stageRoot, "base");
  const stagedUserDataDir = NodePath.join(stageRoot, "user-data");
  const copiedPaths: string[] = [];
  try {
    if (await exists(source.baseDir)) {
      await copyDirectory(source.baseDir, stagedBaseDir);
      copiedPaths.push("profile state, SQLite history, settings, attachments, and logs");
    }
    if (await exists(source.userDataDir)) {
      await copyDirectory(source.userDataDir, stagedUserDataDir);
      copiedPaths.push("Electron user-data and protected connection material");
    }
    if (copiedPaths.length === 0) {
      throw new InstallationMigrationError(
        `The ${source.kind} profile contained no transferable data.`,
      );
    }

    const restoredBase = await moveToRestorePoint(destination.baseDir, restoreRoot);
    const restoredUserData = await moveToRestorePoint(destination.userDataDir, restoreRoot);
    if (await exists(stagedBaseDir)) await NodeFSP.rename(stagedBaseDir, destination.baseDir);
    if (await exists(stagedUserDataDir))
      await NodeFSP.rename(stagedUserDataDir, destination.userDataDir);
    await NodeFSP.rm(stageRoot, { recursive: true, force: true });

    return {
      formatVersion: INSTALLATION_MIGRATION_FORMAT_VERSION,
      direction: input.direction,
      source,
      destination,
      restorePoint: (restoredBase ?? restoredUserData) ? restoreRoot : null,
      copiedPaths,
      credentialNotice:
        "Application-managed encrypted data was copied locally. Reauthenticate any provider or OS-keychain credential that is unavailable after restart.",
    };
  } catch (error) {
    await NodeFSP.rm(stageRoot, { recursive: true, force: true });
    throw error;
  }
}
