# Royal's Scriptorium handoff

Branch: `royal-scriptorium`

## Goal

Deliver Royal's Scriptorium as a co-installable T3 Code fork. It must expose its
build provenance and migrate a local profile in either direction without sharing
live state with T3 Code.

## Work already started

- `customizations/manifest.json` is the source-controlled ledger for the
  Scriptorium identity and migration changes.
- `apps/desktop/src/app/Scriptorium.ts` defines the intended distinct app name,
  identifier, protocol, state-directory names, artifact prefix, and provenance
  model.
- `apps/desktop/src/migration/InstallationMigration.ts` has the first migration
  engine. It stages a source profile, creates a destination restore point, then
  swaps the staged profile into place. It covers the app state directory and
  Electron user-data directory in both directions.

## Remaining implementation

1. Wire `Scriptorium.ts` into desktop environment, renderer branding, package
   identity, protocol registration, artifact naming, documentation, and release
   wording. Preserve upstream compatibility only where a dependency requires it.
2. Replace the migration engine's direct filesystem-only surface with a typed
   desktop IPC/API contract. Add the General/About UI: provenance, included
   customizations, migration direction chooser, preflight warnings, progress,
   result, and restore-point path.
3. Validate and embed the customization manifest during desktop packaging; the
   About panel and staged package metadata must agree on revision and version.
4. Add focused tests for identity isolation, manifest validation, both migration
   directions, restore points, invalid/missing profiles, interrupted staging,
   and credential reauthentication notices. Do not promise that external CLI or
   OS-keychain credentials can be moved.
5. Run `vp check` and `vp run typecheck`; then run a desktop build smoke test.

## Important constraints

- This is same-machine migration only.
- Both installations must be closed before migration; do not copy a live SQLite
  database or a live Electron profile.
- A full clone means application-owned state. Provider CLIs and OS-managed
  credentials may need reauthentication after migration.
- The working tree also contains an unrelated ignored `.pnpm-store/`; leave it
  untouched.
