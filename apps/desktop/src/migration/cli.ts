// oxlint-disable-next-line t3code/no-global-process-runtime -- standalone migration sidecar reads its explicit CLI arguments.
import * as NodeProcess from "node:process";

import {
  InstallationMigrationError,
  migrateInstallation,
  preflightInstallationMigration,
  type InstallationMigrationDirection,
} from "./InstallationMigration.ts";

const DIRECTIONS = new Set<InstallationMigrationDirection>([
  "t3code-to-scriptorium",
  "scriptorium-to-t3code",
]);

function usage(): string {
  return [
    "Usage: bun src/migration/cli.ts <direction> --apply",
    "Directions: t3code-to-scriptorium | scriptorium-to-t3code",
    "Close both applications before running this command.",
  ].join("\n");
}

async function main(): Promise<void> {
  const [directionArg, confirmation] = NodeProcess.argv.slice(2);
  if (
    directionArg === undefined ||
    !DIRECTIONS.has(directionArg as InstallationMigrationDirection)
  ) {
    throw new InstallationMigrationError(usage());
  }
  if (confirmation !== "--apply") {
    throw new InstallationMigrationError(
      `${usage()}\n\nPass --apply after reviewing the direction.`,
    );
  }

  const homeDirectory = NodeProcess.env.HOME;
  if (!homeDirectory) {
    throw new InstallationMigrationError(
      "HOME is required to locate the local installation profiles.",
    );
  }
  const direction = directionArg as InstallationMigrationDirection;
  const preflight = await preflightInstallationMigration({ direction, homeDirectory });
  if (!preflight.sourceAvailable) {
    throw new InstallationMigrationError(
      `No ${preflight.source.kind} profile is available to migrate.`,
    );
  }

  // @effect-diagnostics-next-line globalDate:off -- restore-point names must be unique for a standalone CLI invocation.
  const timestamp = new Date().toISOString();
  const result = await migrateInstallation({ direction, homeDirectory, timestamp });
  // @effect-diagnostics-next-line globalConsole:off -- standalone CLI emits its machine-readable result to stdout.
  console.log(JSON.stringify(result, null, 2));
}

void main().catch((error: unknown) => {
  // @effect-diagnostics-next-line globalConsole:off -- standalone CLI writes its terminal error to stderr.
  console.error(error instanceof Error ? error.message : "Migration failed.");
  NodeProcess.exit(1);
});
