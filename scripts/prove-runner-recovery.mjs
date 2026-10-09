import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";

const argumentsList = process.argv.slice(2);
const baseUrl =
	argumentsList.find((argument) => !argument.startsWith("--")) ?? "http://localhost:8790";

const requestJson = async (path, init) => {
	const response = await fetch(new URL(path, baseUrl), {
		...init,
		signal: init?.signal ?? AbortSignal.timeout(60_000),
	});
	const body = await response.text();
	assert.equal(response.ok, true, `${init?.method ?? "GET"} ${path}: ${response.status} ${body}`);
	return JSON.parse(body);
};

const preparation = await requestJson("/prepare", { method: "POST" });
assert.equal(preparation.ready, true);
const initialAssignments = preparation.assignments;
assert.equal(initialAssignments.length, 2);
assert.equal(
	initialAssignments[0].ownedShardIds.length + initialAssignments[1].ownedShardIds.length,
	32,
);

const initialInstances = await Promise.all(
	["runner-0", "runner-1"].map((runnerId) => requestJson(`/debug/instance?runner=${runnerId}`)),
);
const instanceByRunner = new Map(initialInstances.map((instance) => [instance.runnerId, instance]));
const runId = Date.now().toString();
const taskId = `workers-repro-runner-recovery-${runId}`;
const entityId = `${taskId}-counter`;
const dispatched = await requestJson(
	`/task/dispatch-interrupted?taskId=${encodeURIComponent(taskId)}&id=${encodeURIComponent(entityId)}`,
	{ method: "POST" },
);
assert.equal(dispatched.status, "running");

let interruptedActivity;
const activityDeadline = Date.now() + 20_000;
while (Date.now() < activityDeadline) {
	const [attempts, counter] = await Promise.all([
		requestJson(`/debug/workflow-attempts?operationId=${encodeURIComponent(taskId)}&via=runner-0`),
		requestJson(`/debug/workflow-counter?id=${encodeURIComponent(entityId)}&via=runner-0`),
	]);
	if (
		attempts.attempt?.phase === "effect-applied" &&
		attempts.effects.length === 1 &&
		counter.value === 1
	) {
		interruptedActivity = attempts;
		break;
	}
	await sleep(250);
}
assert.ok(
	interruptedActivity,
	"activity must reach its committed side effect before runner interruption",
);
assert.equal(interruptedActivity.attempt.attempts, 1);

const taskBeforeInterruption = await requestJson(
	`/task/status?taskId=${encodeURIComponent(taskId)}&via=runner-0`,
);
assert.equal(taskBeforeInterruption.status, "running");
const executionId = taskBeforeInterruption.execution_id;
const runnerToInterrupt = interruptedActivity.attempt.last_runner_id;
assert.ok(instanceByRunner.has(runnerToInterrupt));
const survivingRunner = runnerToInterrupt === "runner-0" ? "runner-1" : "runner-0";
const originalInterruptedInstance = instanceByRunner.get(runnerToInterrupt);
const originalSurvivingInstance = instanceByRunner.get(survivingRunner);

let abortOutcome;
let survivingAssignments;
let retryObservation;
let survivingInstanceAfterFailover;
let taskWhileInterruptedRunnerIsOffline;
let recoveredInstance;
try {
	abortOutcome = await fetch(new URL(`/debug/abort?runner=${runnerToInterrupt}`, baseUrl), {
		method: "POST",
		signal: AbortSignal.timeout(5_000),
	})
		.then((response) => ({ status: response.status }))
		.catch((error) => ({ error: error.message }));
	const failoverDeadline = Date.now() + 45_000;
	while (Date.now() < failoverDeadline) {
		const [assignments, attempts, poll] = await Promise.all([
			requestJson(`/debug/assignments?via=${survivingRunner}`),
			requestJson(
				`/debug/workflow-attempts?operationId=${encodeURIComponent(taskId)}&via=${survivingRunner}`,
			),
			requestJson(`/workflow/poll?id=${encodeURIComponent(executionId)}&via=${survivingRunner}`),
		]);
		if (assignments.ownedShardIds.length === 32 && attempts.attempt?.attempts >= 2) {
			survivingAssignments = assignments;
			retryObservation = { attempts, poll };
			break;
		}
		await sleep(1_000);
	}
	survivingInstanceAfterFailover = await requestJson(`/debug/instance?runner=${survivingRunner}`);
	taskWhileInterruptedRunnerIsOffline = await requestJson(
		`/task/status?taskId=${encodeURIComponent(taskId)}&via=${survivingRunner}`,
	);
} finally {
	recoveredInstance = await requestJson(`/debug/instance?runner=${runnerToInterrupt}`);
}

assert.equal(abortOutcome.status, 500, "ctx.abort should reset the targeted Durable Object");
assert.ok(
	survivingAssignments,
	"the surviving runner must acquire the interrupted runner's expired shards and replay its activity",
);
assert.equal(new Set(survivingAssignments.ownedShardIds).size, 32);
assert.equal(
	retryObservation.attempts.effects.length,
	1,
	"the idempotency ledger must contain one effect",
);
assert.equal(survivingInstanceAfterFailover.instanceId, originalSurvivingInstance.instanceId);
assert.equal(taskWhileInterruptedRunnerIsOffline.status, "running");
assert.equal((await requestJson(`/health?via=${survivingRunner}`)).ok, true);
assert.notEqual(recoveredInstance.instanceId, originalInterruptedInstance.instanceId);
const [recoveredTask, recoveredAttempts, recoveredCounter, recoveredPoll] = await Promise.all([
	requestJson(`/task/status?taskId=${encodeURIComponent(taskId)}&via=${survivingRunner}`),
	requestJson(
		`/debug/workflow-attempts?operationId=${encodeURIComponent(taskId)}&via=${survivingRunner}`,
	),
	requestJson(`/debug/workflow-counter?id=${encodeURIComponent(entityId)}&via=${survivingRunner}`),
	requestJson(`/workflow/poll?id=${encodeURIComponent(executionId)}&via=${survivingRunner}`),
]);
assert.equal(recoveredTask.status, "complete");
assert.equal(recoveredTask.result.entityId, entityId);
assert.equal(recoveredTask.result.value, 1);
assert.ok(recoveredAttempts.attempt.attempts >= 2);
assert.equal(recoveredAttempts.effects.length, 1);
assert.equal(
	recoveredCounter.value,
	1,
	"the interrupted activity must not duplicate its committed side effect",
);
assert.equal(recoveredPoll.state, "Complete");

const finalPreparation = await requestJson("/prepare", { method: "POST" });
assert.equal(finalPreparation.ready, true);
assert.equal(
	new Set(finalPreparation.assignments.flatMap((assignment) => assignment.ownedShardIds)).size,
	32,
);

console.log(
	JSON.stringify(
		{
			runnerInterrupted: runnerToInterrupt,
			survivingRunner,
			abortOutcome,
			leaseFailoverShardCount: survivingAssignments.ownedShardIds.length,
			instanceRecreated: recoveredInstance.instanceId !== originalInterruptedInstance.instanceId,
			instancePreserved:
				survivingInstanceAfterFailover.instanceId === originalSurvivingInstance.instanceId,
			workflowExecutionId: executionId,
			activityAttempts: recoveredAttempts.attempt.attempts,
			activityEffects: recoveredAttempts.effects.length,
			counterValue: recoveredCounter.value,
			taskStatus: recoveredTask.status,
			workflowState: recoveredPoll.state,
			shardsAfterRejoin: finalPreparation.assignments.map((assignment) => ({
				runnerId: assignment.runnerId,
				shards: assignment.ownedShardIds.length,
			})),
		},
		null,
		2,
	),
);
