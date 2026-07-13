import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { SymbolView } from "expo-symbols";
import { useMemo, useState } from "react";
import { Alert, Pressable, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { useThemeColor } from "../../lib/useThemeColor";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useThreadProjection } from "../../state/use-thread-detail";

export function ThreadGoalControl(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const scoped = useThreadProjection(props);
  const detail = scoped?.projection.goal;
  const cancelGoal = useAtomCommand(threadEnvironment.cancelGoal, "cancel goal");
  const reopenGoal = useAtomCommand(threadEnvironment.reopenGoal, "reopen goal");
  const [expanded, setExpanded] = useState(false);
  const iconColor = useThemeColor("--color-icon-subtle");
  const nodes = useMemo(
    () =>
      detail?.nodes.filter((node) => node.graphVersionId === detail.goal.currentGraphVersionId) ??
      [],
    [detail],
  );
  if (!detail) return null;
  const terminal = ["completed", "failed", "cancelled"].includes(detail.goal.status);
  const cancel = () =>
    Alert.alert(
      "Cancel Goal?",
      "Running workers will be asked to stop. State and artifacts remain available.",
      [
        { text: "Keep running", style: "cancel" },
        {
          text: "Cancel Goal",
          style: "destructive",
          onPress: () => {
            void cancelGoal({
              environmentId: props.environmentId,
              input: {
                threadId: props.threadId,
                goalId: detail.goal.id,
                reason: "Cancelled from mobile goal controls.",
                creationSource: "mobile",
              },
            });
          },
        },
      ],
    );
  const reopen = () =>
    Alert.alert(
      "Reopen Goal?",
      "The prior completion evidence stays in history. The lead must publish a new workflow revision and obtain a new verification verdict.",
      [
        { text: "Keep completed", style: "cancel" },
        {
          text: "Reopen Goal",
          onPress: () => {
            void reopenGoal({
              environmentId: props.environmentId,
              input: {
                threadId: props.threadId,
                goalId: detail.goal.id,
                creationSource: "mobile",
              },
            });
          },
        },
      ],
    );

  return (
    <View className="mx-4 mb-3 overflow-hidden rounded-2xl border border-neutral-300/60 bg-card dark:border-white/[0.1]">
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Inspect goal workflow"
        onPress={() => setExpanded((current) => !current)}
        className="min-h-12 flex-row items-center gap-2 px-3 py-2"
      >
        <SymbolView
          name="point.3.connected.trianglepath.dotted"
          size={15}
          tintColor={iconColor}
          type="monochrome"
        />
        <View className="min-w-0 flex-1">
          <Text className="font-t3-medium text-xs text-foreground">Goal workflow</Text>
          <Text className="text-2xs text-foreground-muted">
            {detail.goal.status.replaceAll("_", " ")} · revision {detail.goal.currentRevision}
          </Text>
        </View>
        <Text className="text-2xs text-foreground-muted">
          {nodes.filter((node) => node.status === "succeeded").length}/{nodes.length}
        </Text>
        <SymbolView
          name={expanded ? "chevron.up" : "chevron.down"}
          size={12}
          tintColor={iconColor}
          type="monochrome"
        />
      </Pressable>
      {expanded ? (
        <View className="gap-2 border-t border-neutral-300/50 px-3 py-3 dark:border-white/[0.08]">
          {nodes.slice(0, 12).map((node) => {
            const attempt = [...detail.attempts]
              .toReversed()
              .find((item) => item.nodeId === node.node.id);
            return (
              <View key={node.node.id} className="rounded-xl bg-subtle px-3 py-2">
                <View className="flex-row justify-between gap-2">
                  <Text
                    className="min-w-0 flex-1 font-t3-medium text-xs text-foreground"
                    numberOfLines={1}
                  >
                    {node.node.role}
                  </Text>
                  <Text className="text-3xs text-foreground-muted">{node.status}</Text>
                </View>
                <Text className="mt-1 text-2xs text-foreground-muted" numberOfLines={2}>
                  {node.node.objective}
                </Text>
                {attempt?.resolvedRoute ? (
                  <Text className="mt-1 text-3xs text-foreground-muted" numberOfLines={1}>
                    {attempt.resolvedRoute.providerInstanceId}/{attempt.resolvedRoute.model}
                  </Text>
                ) : null}
                {node.blocker ? (
                  <Text
                    accessibilityLabel={`Blocker: ${node.blocker}`}
                    className="mt-1 text-2xs text-danger-foreground"
                  >
                    {node.blocker}
                  </Text>
                ) : null}
              </View>
            );
          })}
          <Text
            accessibilityLabel="Goal evidence summary"
            className="text-2xs text-foreground-muted"
          >
            {detail.writerCommits.filter((commit) => commit.state === "integrated").length} commits
            ·{" "}
            {
              detail.evidence.filter(
                (evidence) =>
                  evidence.verdict === "accepted" &&
                  evidence.integrationSha === detail.goal.integrationSha,
              ).length
            }{" "}
            accepted current-SHA verdicts
          </Text>
          {detail.goal.status === "completed" ? (
            <Pressable
              accessibilityRole="button"
              accessibilityLabel="Reopen Goal"
              onPress={reopen}
              className="min-h-10 items-center justify-center rounded-xl border border-neutral-300/70 bg-subtle dark:border-white/[0.12]"
            >
              <Text className="font-t3-medium text-xs text-foreground">Reopen Goal</Text>
            </Pressable>
          ) : !terminal ? (
            <Pressable
              accessibilityRole="button"
              accessibilityLabel="Cancel Goal"
              onPress={cancel}
              className="min-h-10 items-center justify-center rounded-xl border border-danger-border bg-danger"
            >
              <Text className="font-t3-medium text-xs text-danger-foreground">Cancel Goal</Text>
            </Pressable>
          ) : null}
        </View>
      ) : null}
    </View>
  );
}
