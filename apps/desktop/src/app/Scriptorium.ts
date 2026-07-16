import manifest from "../../../../customizations/manifest.json" with { type: "json" };

export const SCRIPTORIUM = {
  baseName: "Royal's Scriptorium",
  appId: "com.royalaid.scriptorium",
  protocol: "royal-scriptorium",
  baseDirName: ".royal-scriptorium",
  userDataDirName: "royals-scriptorium",
  legacyUserDataDirName: "Royal's Scriptorium",
  artifactPrefix: "Royals-Scriptorium",
} as const;

export interface ScriptoriumChange {
  readonly id: string;
  readonly title: string;
  readonly status: "included";
}

export interface ScriptoriumProvenance {
  readonly schemaVersion: number;
  readonly upstreamRevision: string;
  readonly scriptoriumRevision: string;
  readonly changes: readonly ScriptoriumChange[];
}

export const SCRIPTORIUM_PROVENANCE: ScriptoriumProvenance = {
  schemaVersion: manifest.schemaVersion,
  upstreamRevision: manifest.upstreamRevision,
  scriptoriumRevision: manifest.scriptoriumRevision,
  changes: manifest.changes.map((change) => ({
    id: change.id,
    title: change.title,
    status: "included" as const,
  })),
};
