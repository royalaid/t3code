import {
  GoalAttemptId,
  GoalGraphVersionId,
  GoalId,
  GoalNodeId,
  RunId,
  ThreadId,
  type GoalDetail,
  type OrchestrationV2Command,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { EffectOutboxV2 } from "./EffectOutbox.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { GoalProjectionStore } from "./GoalProjectionStore.ts";
import { GoalScheduler } from "./GoalScheduler.ts";
import { layer as idAllocatorLayer } from "./IdAllocator.ts";
import { GoalWorkflowService, layer, rootNoGraphTerminalStatus } from "./GoalWorkflowService.ts";
import { planUnboundGoalAttemptCancellation } from "./Orchestrator.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";

it("terminalizes only unbound leased or launching attempts when Cancel Goal commits", () => {
  const goalId = GoalId.make("goal:terminalize-on-goal-cancel");
  const graphVersionId = GoalGraphVersionId.make("graph:terminalize-on-goal-cancel");
  const unboundAttemptId = GoalAttemptId.make("attempt:unbound-on-goal-cancel");
  const boundAttemptId = GoalAttemptId.make("attempt:bound-on-goal-cancel");
  const unboundNodeId = GoalNodeId.make("node:unbound-on-goal-cancel");
  const boundNodeId = GoalNodeId.make("node:bound-on-goal-cancel");
  const detail = {
    goal: { id: goalId },
    attempts: [
      {
        id: unboundAttemptId,
        goalId,
        graphVersionId,
        nodeId: unboundNodeId,
        status: "leased",
        runId: null,
        executionThreadId: null,
        leaseOwner: "scheduler:test",
        leaseExpiresAt: "2026-07-12T00:02:00.000Z",
      },
      {
        id: boundAttemptId,
        goalId,
        graphVersionId,
        nodeId: boundNodeId,
        status: "running",
        runId: RunId.make("run:bound-on-goal-cancel"),
        executionThreadId: ThreadId.make("thread:bound-on-goal-cancel"),
      },
    ],
    nodes: [
      {
        goalId,
        graphVersionId,
        node: { id: unboundNodeId },
        status: "running",
        activeAttemptId: unboundAttemptId,
        blocker: null,
      },
      {
        goalId,
        graphVersionId,
        node: { id: boundNodeId },
        status: "running",
        activeAttemptId: boundAttemptId,
        blocker: null,
      },
    ],
  } as unknown as GoalDetail;

  const transitions = planUnboundGoalAttemptCancellation({
    detail,
    updatedAt: "2026-07-12T00:01:00.000Z",
  });

  expect(transitions).toHaveLength(1);
  expect(transitions[0]?.attempt).toMatchObject({
    id: unboundAttemptId,
    status: "cancelled",
    leaseOwner: null,
    leaseExpiresAt: null,
    failureReason: "goal_cancelled_before_provider_start",
  });
  expect(transitions[0]?.node).toMatchObject({
    activeAttemptId: null,
    status: "cancelled",
    blocker: "Goal cancelled by the user.",
  });
  expect(transitions[0]?.node?.node.id).toBe(unboundNodeId);
});

it("classifies only diagnostic no-graph terminal statuses", () => {
  expect(
    (["completed", "failed", "interrupted", "rolled_back"] as const).map((status) =>
      rootNoGraphTerminalStatus(status),
    ),
  ).toEqual(["completed", "failed", "interrupted", "rolled_back"]);
  expect(rootNoGraphTerminalStatus("cancelled")).toBeNull();
  expect(rootNoGraphTerminalStatus("running")).toBeNull();
});

it.effect("blocks the exact initial root run from the live boundary after it terminalizes", () =>
  Effect.gen(function* () {
    const goalId = GoalId.make("goal:initial-no-graph-live");
    const rootThreadId = ThreadId.make("thread:initial-no-graph-live");
    const runId = RunId.make("run:initial-no-graph-live");
    const now = "2026-07-12T00:00:00.000Z";
    const detail = {
      goal: {
        id: goalId,
        rootThreadId,
        status: "planning",
        currentRevision: 0,
        currentGraphVersionId: null,
        initialRootRunId: runId,
        pendingLaunchClaimId: `goal-root-launch:${goalId}`,
        updatedAt: now,
      },
      nodes: [],
      attempts: [],
    } as unknown as GoalDetail;
    const terminal = {
      id: "event:initial-no-graph-live",
      threadId: rootThreadId,
      type: "run.updated",
      payload: { id: runId, status: "completed" },
      occurredAt: now,
    } as unknown as OrchestrationV2DomainEvent;
    const unrelatedTerminal = {
      ...terminal,
      id: "event:unrelated-no-graph-live",
      payload: { id: RunId.make("run:unrelated-no-graph-live"), status: "failed" },
    } as unknown as OrchestrationV2DomainEvent;
    const committedInput = yield* Ref.make<unknown>(null);
    const observed = yield* Deferred.make<void>();
    let subscribedAfterSequence: number | undefined;
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        listSchedulable: Effect.succeed([]),
        getDetail: () => Effect.succeed(detail),
        resolveMcpBinding: () => Effect.succeed({ kind: "lead" as const, goalId, rootThreadId }),
      }),
      Layer.mock(EventSinkV2)({
        latestSequence: () => Effect.succeed(41),
        stream: (input) => {
          subscribedAfterSequence = input?.afterSequence;
          return Stream.fromIterable([
            { sequence: 42, commandId: "command:unrelated", event: unrelatedTerminal },
            { sequence: 43, commandId: "command:terminal", event: terminal },
          ] as never);
        },
        commitGoalNoGraphCommand: (input) =>
          Ref.set(committedInput, input).pipe(
            Effect.andThen(Deferred.succeed(observed, undefined)),
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
      }),
      Layer.mock(EffectOutboxV2)({}),
      Layer.succeed(
        GoalScheduler,
        GoalScheduler.of({
          tick: Effect.succeed({
            plan: { launches: [], transitions: [], warnings: [] },
            leasedAttempts: [],
          }),
        }),
      ),
      Layer.mock(ThreadManagementService)({
        getThreadProjection: () => Effect.succeed({ runs: [], subagents: [] } as never),
      }),
      idAllocatorLayer,
    );

    yield* Effect.gen(function* () {
      yield* GoalWorkflowService;
      expect(
        Option.isSome(yield* Deferred.await(observed).pipe(Effect.timeoutOption("1 second"))),
      ).toBe(true);
      expect(subscribedAfterSequence).toBe(41);
      expect(yield* Ref.get(committedInput)).toMatchObject({
        goalId,
        runId,
        expectedStatus: "planning",
        requireInitialRootRun: true,
      });
      const input = (yield* Ref.get(committedInput)) as {
        readonly events: ReadonlyArray<{ readonly type: string; readonly payload: unknown }>;
      };
      expect(input.events.map((event) => event.type)).toEqual([
        "goal.failure-recorded",
        "goal.updated",
      ]);
      expect(input.events[0]?.payload).toMatchObject({
        reason: { type: "root_lead_no_graph", runId, terminalStatus: "completed" },
      });
      expect(input.events[1]?.payload).toMatchObject({ status: "blocked" });
    }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));
  }),
);

it.effect("retries terminal-before-planning and tracks a blocked root retry until terminal", () =>
  Effect.gen(function* () {
    const goalId = GoalId.make("goal:no-graph-orderings");
    const rootThreadId = ThreadId.make("thread:no-graph-orderings");
    const initialRunId = RunId.make("run:no-graph-initial");
    const retryRunId = RunId.make("run:no-graph-retry");
    const now = "2026-07-12T00:00:00.000Z";
    const baseGoal = {
      id: goalId,
      rootThreadId,
      currentRevision: 0,
      currentGraphVersionId: null,
      initialRootRunId: initialRunId,
      pendingLaunchClaimId: `goal-root-launch:${goalId}`,
      updatedAt: now,
    };
    const detailsByEvent = [
      { goal: { ...baseGoal, status: "provisioning" }, nodes: [], attempts: [] },
      { goal: { ...baseGoal, status: "planning" }, nodes: [], attempts: [] },
      { goal: { ...baseGoal, status: "blocked" }, nodes: [], attempts: [] },
      { goal: { ...baseGoal, status: "blocked" }, nodes: [], attempts: [] },
      { goal: { ...baseGoal, status: "blocked" }, nodes: [], attempts: [] },
    ] as unknown as ReadonlyArray<GoalDetail>;
    const currentDetail = yield* Ref.make(detailsByEvent[0]!);
    const events = [
      {
        id: "event:initial-terminal-before-planning",
        threadId: rootThreadId,
        type: "run.updated",
        payload: { id: initialRunId, status: "failed" },
        occurredAt: now,
      },
      {
        id: "event:planning-after-terminal",
        threadId: rootThreadId,
        type: "goal.updated",
        payload: { ...baseGoal, status: "planning" },
        occurredAt: now,
      },
      {
        id: "event:blocked-retry-created",
        threadId: rootThreadId,
        type: "run.created",
        payload: { id: retryRunId, status: "starting" },
        occurredAt: now,
      },
      {
        id: "event:blocked-retry-running",
        threadId: rootThreadId,
        type: "run.updated",
        payload: { id: retryRunId, status: "running" },
        occurredAt: now,
      },
      {
        id: "event:blocked-retry-terminal",
        threadId: rootThreadId,
        type: "run.updated",
        payload: { id: retryRunId, status: "rolled_back" },
        occurredAt: now,
      },
    ] as unknown as ReadonlyArray<OrchestrationV2DomainEvent>;
    const commits = yield* Ref.make<ReadonlyArray<unknown>>([]);
    const observed = yield* Deferred.make<void>();
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        listSchedulable: Effect.succeed([]),
        getDetail: () => Ref.get(currentDetail),
        resolveMcpBinding: () => Effect.succeed({ kind: "lead" as const, goalId, rootThreadId }),
      }),
      Layer.mock(EventSinkV2)({
        latestSequence: () => Effect.succeed(100),
        stream: () =>
          Stream.fromIterable(
            events.map((event, index) => ({
              detail: detailsByEvent[index]!,
              stored: {
                sequence: 101 + index,
                commandId: `command:no-graph:${index}`,
                event,
              },
            })),
          ).pipe(
            Stream.mapEffect(({ detail, stored }) =>
              Ref.set(currentDetail, detail).pipe(Effect.as(stored)),
            ),
          ) as never,
        commitGoalNoGraphCommand: (input) =>
          Ref.update(commits, (current) => [...current, input]).pipe(
            Effect.andThen(
              input.runId === retryRunId ? Deferred.succeed(observed, undefined) : Effect.void,
            ),
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
      }),
      Layer.mock(EffectOutboxV2)({}),
      Layer.succeed(
        GoalScheduler,
        GoalScheduler.of({
          tick: Effect.succeed({
            plan: { launches: [], transitions: [], warnings: [] },
            leasedAttempts: [],
          }),
        }),
      ),
      Layer.mock(ThreadManagementService)({
        getThreadProjection: () =>
          Effect.succeed({
            runs: [{ id: initialRunId, status: "failed" }],
            subagents: [],
          } as never),
      }),
      idAllocatorLayer,
    );

    yield* Effect.gen(function* () {
      yield* GoalWorkflowService;
      expect(
        Option.isSome(yield* Deferred.await(observed).pipe(Effect.timeoutOption("1 second"))),
      ).toBe(true);
      const captured = yield* Ref.get(commits);
      expect(captured).toHaveLength(2);
      expect(captured[0]).toMatchObject({
        runId: initialRunId,
        expectedStatus: "planning",
        requireInitialRootRun: true,
      });
      expect(captured[1]).toMatchObject({
        runId: retryRunId,
        expectedStatus: "blocked",
        requireInitialRootRun: false,
      });
      expect(
        (captured[1] as { readonly events: ReadonlyArray<{ readonly payload: unknown }> }).events[1]
          ?.payload,
      ).toMatchObject({ status: "blocked" });
    }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));
  }),
);

it.effect(
  "requests interruption for a bound running worker instead of terminalizing it on Cancel Goal",
  () =>
    Effect.gen(function* () {
      const goalId = GoalId.make("goal:interrupt-on-goal-cancel");
      const graphVersionId = GoalGraphVersionId.make("graph:interrupt-on-goal-cancel");
      const attemptId = GoalAttemptId.make("attempt:interrupt-on-goal-cancel");
      const nodeId = GoalNodeId.make("node:interrupt-on-goal-cancel");
      const rootThreadId = ThreadId.make("thread:root:interrupt-on-goal-cancel");
      const workerThreadId = ThreadId.make("thread:worker:interrupt-on-goal-cancel");
      const runId = RunId.make("run:interrupt-on-goal-cancel");
      const detail = {
        goal: { id: goalId, rootThreadId, status: "cancelled" },
        nodes: [
          {
            goalId,
            graphVersionId,
            node: { id: nodeId },
            status: "running",
            activeAttemptId: attemptId,
            blocker: null,
          },
        ],
        attempts: [
          {
            id: attemptId,
            goalId,
            graphVersionId,
            nodeId,
            status: "running",
            executionThreadId: workerThreadId,
            runId,
            usage: { nativeDescendantCount: 0 },
          },
        ],
      } as unknown as GoalDetail;
      const cancelledEvent = {
        id: "event:goal-cancel-running-worker",
        threadId: rootThreadId,
        type: "goal.cancelled",
        payload: detail.goal,
        occurredAt: "2026-07-12T00:00:00.000Z",
      } as unknown as OrchestrationV2DomainEvent;
      const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
      const interrupted = yield* Deferred.make<void>();
      const dependencies = Layer.mergeAll(
        Layer.mock(GoalProjectionStore)({
          listSchedulable: Effect.succeed([]),
          getDetail: () => Effect.succeed(detail),
        }),
        Layer.mock(EventSinkV2)({
          latestSequence: () => Effect.succeed(0),
          stream: () =>
            Stream.fromIterable([
              { sequence: 1, commandId: "command:goal-cancel", event: cancelledEvent },
            ] as never),
        }),
        Layer.mock(EffectOutboxV2)({}),
        Layer.succeed(
          GoalScheduler,
          GoalScheduler.of({
            tick: Effect.succeed({
              plan: { launches: [], transitions: [], warnings: [] },
              leasedAttempts: [],
            }),
          }),
        ),
        Layer.mock(ThreadManagementService)({
          getThreadProjection: () => Effect.succeed({ runs: [] } as never),
          dispatch: (command) =>
            Ref.update(commands, (current) => [...current, command]).pipe(
              Effect.andThen(
                command.type === "run.interrupt"
                  ? Deferred.succeed(interrupted, undefined)
                  : Effect.void,
              ),
              Effect.as({ sequence: 1, storedEvents: [] }),
            ),
        }),
        idAllocatorLayer,
      );

      yield* Effect.gen(function* () {
        yield* GoalWorkflowService;
        expect(
          Option.isSome(yield* Deferred.await(interrupted).pipe(Effect.timeoutOption("1 second"))),
        ).toBe(true);
        const interrupts = (yield* Ref.get(commands)).filter(
          (
            command,
          ): command is Extract<OrchestrationV2Command, { readonly type: "run.interrupt" }> =>
            command.type === "run.interrupt",
        );
        expect(interrupts).toHaveLength(1);
        expect(interrupts[0]).toMatchObject({
          threadId: workerThreadId,
          runId,
          reason: "Goal cancelled by the user.",
        });
      }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));
    }),
);

it.effect("persists and interrupts a recovered native-descendant budget overage", () =>
  Effect.gen(function* () {
    const goalId = GoalId.make("goal:recovery-overage");
    const graphVersionId = GoalGraphVersionId.make("graph:recovery-overage");
    const attemptId = GoalAttemptId.make("attempt:recovery-overage");
    const nodeId = GoalNodeId.make("node:recovery-overage");
    const rootThreadId = ThreadId.make("thread:root:recovery-overage");
    const workerThreadId = ThreadId.make("thread:worker:recovery-overage");
    const runId = RunId.make("run:recovery-overage");
    const detail = {
      goal: {
        id: goalId,
        rootThreadId,
        status: "running",
        updatedAt: "2026-07-12T00:00:00.000Z",
      },
      nodes: [
        {
          graphVersionId,
          node: { id: nodeId },
          status: "running",
          activeAttemptId: attemptId,
          blocker: null,
          updatedAt: "2026-07-12T00:00:00.000Z",
        },
      ],
      attempts: [
        {
          id: attemptId,
          goalId,
          graphVersionId,
          nodeId,
          status: "running",
          executionThreadId: workerThreadId,
          runId,
          usage: { nativeDescendantCount: 1_000 },
          createdAt: "2026-07-12T00:00:00.000Z",
          updatedAt: "2026-07-12T00:00:00.000Z",
        },
      ],
    } as unknown as GoalDetail;
    const commits = yield* Ref.make<ReadonlyArray<unknown>>([]);
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
    const recoveryOrder = yield* Ref.make<ReadonlyArray<string>>([]);
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        listSchedulable: Effect.succeed([detail]),
      }),
      Layer.mock(EventSinkV2)({
        latestSequence: () => Effect.succeed(0),
        commitGoalAttemptCommand: (input) =>
          Ref.update(commits, (current) => [...current, input]).pipe(
            Effect.andThen(Ref.update(recoveryOrder, (current) => [...current, "persist"])),
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
      }),
      Layer.mock(EffectOutboxV2)({}),
      Layer.succeed(
        GoalScheduler,
        GoalScheduler.of({
          tick: Ref.update(recoveryOrder, (current) => [...current, "schedule"]).pipe(
            Effect.as({
              plan: { launches: [], transitions: [], warnings: [] },
              leasedAttempts: [],
            }),
          ),
        }),
      ),
      Layer.mock(ThreadManagementService)({
        dispatch: (command) =>
          Ref.update(commands, (current) => [...current, command]).pipe(
            Effect.andThen(Ref.update(recoveryOrder, (current) => [...current, "interrupt"])),
            Effect.as({ sequence: 1, storedEvents: [], effects: [], cancelledEffectCount: 0 }),
          ),
      }),
      idAllocatorLayer,
    );

    yield* GoalWorkflowService.pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));

    const persisted = (yield* Ref.get(commits)) as ReadonlyArray<{
      readonly commandType: string;
      readonly events: ReadonlyArray<{ readonly type: string; readonly payload: unknown }>;
    }>;
    expect(persisted).toHaveLength(1);
    expect(persisted[0]?.commandType).toBe("goal.recovery.native_descendant_overage");
    expect(persisted[0]?.events.map((event) => event.type)).toEqual([
      "goal.failure-recorded",
      "goal.updated",
    ]);
    expect(persisted[0]?.events[0]?.payload).toMatchObject({
      reason: { type: "native_descendant_overage", limit: 1_000, observed: 1_001 },
      recoveryState: "unresolved",
    });
    expect(persisted[0]?.events[1]?.payload).toMatchObject({ status: "paused" });

    const dispatched = yield* Ref.get(commands);
    expect(dispatched).toHaveLength(1);
    expect(dispatched[0]).toMatchObject({
      type: "run.interrupt",
      threadId: workerThreadId,
      runId,
      reason: "Native descendant lifetime backstop exceeded.",
    });
    expect(yield* Ref.get(recoveryOrder)).toEqual(["persist", "interrupt", "schedule"]);
  }),
);

it.effect(
  "interrupts a superseded bound worker and preserves supersession on a late completion",
  () =>
    Effect.gen(function* () {
      const goalId = GoalId.make("goal:node-cancel");
      const graphVersionId = GoalGraphVersionId.make("graph:node-cancel");
      const attemptId = GoalAttemptId.make("attempt:node-cancel");
      const nodeId = GoalNodeId.make("node:node-cancel");
      const rootThreadId = ThreadId.make("thread:root:node-cancel");
      const workerThreadId = ThreadId.make("thread:worker:node-cancel");
      const runId = RunId.make("run:node-cancel");
      const now = "2026-07-12T00:00:00.000Z";
      const detail = {
        goal: { id: goalId, rootThreadId, status: "running", updatedAt: now },
        nodes: [
          {
            goalId,
            graphVersionId,
            node: { id: nodeId },
            status: "superseded",
            activeAttemptId: attemptId,
            blocker: "replacement graph accepted",
            updatedAt: now,
          },
        ],
        attempts: [
          {
            id: attemptId,
            goalId,
            graphVersionId,
            nodeId,
            status: "running",
            executionThreadId: workerThreadId,
            runId,
            usage: { nativeDescendantCount: 0 },
            createdAt: now,
            updatedAt: now,
          },
        ],
      } as unknown as GoalDetail;
      const cancellation = {
        id: "event:node-cancel",
        threadId: rootThreadId,
        type: "goal.node-cancellation-requested",
        payload: detail.nodes[0],
        occurredAt: now,
      } as unknown as OrchestrationV2DomainEvent;
      const completedRun = {
        id: "event:node-cancel-late-completion",
        threadId: workerThreadId,
        type: "run.updated",
        payload: { id: runId, status: "completed", rootNodeId: null },
        occurredAt: now,
      } as unknown as OrchestrationV2DomainEvent;
      const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
      const commits = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const observedLateCompletion = yield* Deferred.make<void>();
      const dependencies = Layer.mergeAll(
        Layer.mock(GoalProjectionStore)({
          listSchedulable: Effect.succeed([]),
          getDetail: () => Effect.succeed(detail),
          resolveMcpBinding: (threadId) =>
            Effect.succeed(
              threadId === workerThreadId
                ? {
                    kind: "worker" as const,
                    goalId,
                    graphVersionId,
                    nodeId,
                    attemptId,
                    rootThreadId,
                    executionThreadId: workerThreadId,
                  }
                : null,
            ),
        }),
        Layer.mock(EventSinkV2)({
          latestSequence: () => Effect.succeed(0),
          stream: () =>
            Stream.fromIterable([
              { sequence: 1, commandId: "command:node-cancel", event: cancellation },
              { sequence: 2, commandId: "command:late-completion", event: completedRun },
            ] as never),
          commitGoalAttemptCommand: (input) =>
            Ref.update(commits, (current) => [...current, input]).pipe(
              Effect.andThen(
                input.commandType === "goal.attempt.observe"
                  ? Deferred.succeed(observedLateCompletion, undefined)
                  : Effect.void,
              ),
              Effect.as({ committed: true, stale: false, storedEvents: [] }),
            ),
        }),
        Layer.mock(EffectOutboxV2)({}),
        Layer.succeed(
          GoalScheduler,
          GoalScheduler.of({
            tick: Effect.succeed({
              plan: { launches: [], transitions: [], warnings: [] },
              leasedAttempts: [],
            }),
          }),
        ),
        Layer.mock(ThreadManagementService)({
          dispatch: (command) =>
            Ref.update(commands, (current) => [...current, command]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            ),
          getThreadProjection: () => Effect.succeed({ subagents: [] } as never),
        }),
        idAllocatorLayer,
      );

      yield* Effect.gen(function* () {
        yield* GoalWorkflowService;
        const observed = yield* Deferred.await(observedLateCompletion).pipe(
          Effect.timeoutOption("1 second"),
        );
        expect(Option.isSome(observed)).toBe(true);

        const interrupts = (yield* Ref.get(commands)).filter(
          (
            command,
          ): command is Extract<OrchestrationV2Command, { readonly type: "run.interrupt" }> =>
            command.type === "run.interrupt",
        );
        expect(interrupts).toHaveLength(1);
        expect(interrupts[0]).toMatchObject({
          commandId: `goal-node-cancel-interrupt:${attemptId}:superseded`,
          threadId: workerThreadId,
          runId,
          reason: "replacement graph accepted",
        });
        const lateCommit = (yield* Ref.get(commits)).find(
          (entry) =>
            (entry as { readonly commandType?: string }).commandType === "goal.attempt.observe",
        ) as
          | {
              readonly events: ReadonlyArray<{
                readonly type: string;
                readonly payload: {
                  readonly status?: string;
                  readonly activeAttemptId?: string | null;
                };
              }>;
            }
          | undefined;
        expect(lateCommit).toBeDefined();
        expect(lateCommit?.events[0]?.payload.status).toBe("cancelled");
        expect(lateCommit?.events[1]?.payload).toMatchObject({
          status: "superseded",
          activeAttemptId: null,
        });
      }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));
    }),
);

it.effect("terminalizes an unbound leased attempt without creating or interrupting a worker", () =>
  Effect.gen(function* () {
    const goalId = GoalId.make("goal:unbound-cancel");
    const graphVersionId = GoalGraphVersionId.make("graph:unbound-cancel");
    const attemptId = GoalAttemptId.make("attempt:unbound-cancel");
    const nodeId = GoalNodeId.make("node:unbound-cancel");
    const rootThreadId = ThreadId.make("thread:root:unbound-cancel");
    const now = "2026-07-12T00:00:00.000Z";
    const detail = {
      goal: { id: goalId, rootThreadId, status: "running", updatedAt: now },
      nodes: [
        {
          goalId,
          graphVersionId,
          node: { id: nodeId },
          status: "cancelled",
          activeAttemptId: attemptId,
          blocker: "lead cancelled this lease",
          updatedAt: now,
        },
      ],
      attempts: [
        {
          id: attemptId,
          goalId,
          graphVersionId,
          nodeId,
          status: "leased",
          executionThreadId: null,
          runId: null,
          usage: { nativeDescendantCount: 0 },
          createdAt: now,
          updatedAt: now,
        },
      ],
    } as unknown as GoalDetail;
    const cancellation = {
      id: "event:unbound-cancel",
      threadId: rootThreadId,
      type: "goal.node-cancellation-requested",
      payload: detail.nodes[0],
      occurredAt: now,
    } as unknown as OrchestrationV2DomainEvent;
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2Command>>([]);
    const commits = yield* Ref.make<ReadonlyArray<unknown>>([]);
    const terminalized = yield* Deferred.make<void>();
    const dependencies = Layer.mergeAll(
      Layer.mock(GoalProjectionStore)({
        listSchedulable: Effect.succeed([]),
        getDetail: () => Effect.succeed(detail),
      }),
      Layer.mock(EventSinkV2)({
        latestSequence: () => Effect.succeed(0),
        stream: () =>
          Stream.fromIterable([
            { sequence: 1, commandId: "command:unbound-cancel", event: cancellation },
          ] as never),
        commitGoalAttemptCommand: (input) =>
          Ref.update(commits, (current) => [...current, input]).pipe(
            Effect.andThen(
              input.commandType === "goal.node-cancel.terminalize-unbound"
                ? Deferred.succeed(terminalized, undefined)
                : Effect.void,
            ),
            Effect.as({ committed: true, stale: false, storedEvents: [] }),
          ),
      }),
      Layer.mock(EffectOutboxV2)({}),
      Layer.succeed(
        GoalScheduler,
        GoalScheduler.of({
          tick: Effect.succeed({
            plan: { launches: [], transitions: [], warnings: [] },
            leasedAttempts: [],
          }),
        }),
      ),
      Layer.mock(ThreadManagementService)({
        dispatch: (command) =>
          Ref.update(commands, (current) => [...current, command]).pipe(
            Effect.as({ sequence: 1, storedEvents: [] }),
          ),
      }),
      idAllocatorLayer,
    );

    yield* Effect.gen(function* () {
      yield* GoalWorkflowService;
      const observed = yield* Deferred.await(terminalized).pipe(Effect.timeoutOption("1 second"));
      expect(Option.isSome(observed)).toBe(true);
      expect(yield* Ref.get(commands)).toEqual([]);
      const terminalCommit = (yield* Ref.get(commits)).find(
        (entry) =>
          (entry as { readonly commandType?: string }).commandType ===
          "goal.node-cancel.terminalize-unbound",
      ) as
        | {
            readonly events: ReadonlyArray<{
              readonly payload: {
                readonly status?: string;
                readonly activeAttemptId?: string | null;
              };
            }>;
          }
        | undefined;
      expect(terminalCommit?.events[0]?.payload.status).toBe("cancelled");
      expect(terminalCommit?.events[1]?.payload.activeAttemptId).toBeNull();
    }).pipe(Effect.provide(layer.pipe(Layer.provide(dependencies))));
  }),
);
