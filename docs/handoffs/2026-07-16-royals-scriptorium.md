# Royal's Scriptorium handoff

Branch: `royal-scriptorium`

## Goal

Deliver Royal's Scriptorium as a co-installable T3 Code fork. It must expose its
build provenance and migrate a local profile in either direction without sharing
live state with T3 Code.

## Implemented foundation

- `customizations/manifest.json` is the source-controlled ledger for the
  Scriptorium identity and migration changes.
- `apps/desktop/src/app/Scriptorium.ts` drives the distinct app name, identifier,
  protocol, state-directory names, artifact prefix, and provenance model through
  the desktop environment, renderer, package metadata, and desktop build.
- `apps/desktop/src/migration/InstallationMigration.ts` has the first migration
  engine. It stages a source profile, creates a destination restore point, then
  swaps the staged profile into place. It covers the app state directory and
  Electron user-data directory in both directions.
- `apps/desktop/src/migration/cli.ts` is the explicit local sidecar. Run it
  only after closing both apps: `vp run --filter @t3tools/desktop migrate-profile
t3code-to-scriptorium --apply` (reverse the direction to return to T3 Code).

## Remaining implementation

1. Package the migration CLI as a standalone desktop artifact resource if a
   double-clickable sidecar is required; it currently runs through the workspace
   package script so it cannot copy a live Electron profile.
2. Add a copy action, progress UI, and post-migration restart flow if migration
   needs to be initiated entirely from the app. Keep the actual transfer in an
   external process.
3. Validate the customization manifest during staging and add build timestamp
   metadata to the packaged manifest.
4. Add reverse-copy and injected-failure rollback coverage for the migration
   engine, then run a desktop artifact smoke test.

## Important constraints

- This is same-machine migration only.
- Both installations must be closed before migration; do not copy a live SQLite
  database or a live Electron profile.
- A full clone means application-owned state. Provider CLIs and OS-managed
  credentials may need reauthentication after migration.
- The working tree also contains an unrelated ignored `.pnpm-store/`; leave it
  untouched.
