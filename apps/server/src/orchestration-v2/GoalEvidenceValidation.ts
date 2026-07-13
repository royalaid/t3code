import type { GoalEvidence } from "@t3tools/contracts";

/**
 * Accepted verification must retain a command whose output can be retrieved
 * from a durable goal artifact. Ownership and existence are checked by the
 * projection store before the evidence is persisted.
 */
export const hasDurableCommandEvidence = (evidence: Pick<GoalEvidence, "commands">): boolean =>
  evidence.commands.some((command) => command.logArtifactId !== null);
