import assert from "node:assert/strict";

const argumentsList = process.argv.slice(2);
const baseUrl =
	argumentsList.find((argument) => !argument.startsWith("--")) ?? "http://localhost:8787";
const requireExisting = argumentsList.includes("--require-existing");

const requestJson = async (path, init) => {
	const response = await fetch(new URL(path, baseUrl), init);
	const body = await response.text();
	assert.equal(response.ok, true, `${init?.method ?? "GET"} ${path}: ${response.status} ${body}`);
	return JSON.parse(body);
};

const preparation = await requestJson("/prepare", { method: "POST" });
assert.equal(preparation.ready, true, "all shard locks must match their assigned runner");

const recoveryTaskId = "workers-repro-startup-recovery-proof-v6";
const recoveryEntityId = "workers-repro-startup-recovery-counter-v6";
const recoveryTaskBefore = await requestJson(
	`/task/status?taskId=${encodeURIComponent(recoveryTaskId)}`,
);
const recoveryCounterBefore = await requestJson(
	`/debug/workflow-counter?id=${encodeURIComponent(recoveryEntityId)}`,
);
if (requireExisting) {
	assert.equal(
		recoveryTaskBefore.status,
		"complete",
		"startup recovery must redispatch the orphan task",
	);
	assert.equal(recoveryTaskBefore.result?.entityId, recoveryEntityId);
	assert.equal(recoveryTaskBefore.result?.value, 1, "recovered workflow side effect must run once");
	assert.equal(
		recoveryCounterBefore.value,
		1,
		"the recovered activity result must be stored in PostgreSQL",
	);
}

const runner0 = preparation.assignments.find((assignment) => assignment.runnerId === "runner-0");
const runner1 = preparation.assignments.find((assignment) => assignment.runnerId === "runner-1");
assert.ok(runner0);
assert.ok(runner1);
assert.equal(runner0.ownedShardIds.length + runner1.ownedShardIds.length, 32);
assert.equal(new Set([...runner0.ownedShardIds, ...runner1.ownedShardIds]).size, 32);

const routedEntity = await (async () => {
	for (let index = 0; index < 256; index += 1) {
		const entityId = `multi-runner-proof-${index}`;
		const shard = await requestJson(`/debug/shard?id=${encodeURIComponent(entityId)}&via=runner-0`);
		const owner = preparation.assignments.find((assignment) =>
			assignment.ownedShardIds.includes(shard.shardId),
		);
		assert.ok(owner, `shard ${shard.shardId} must have one runner owner`);
		if (owner.runnerId !== runner0.runnerId) {
			return { entityId, shardId: shard.shardId, owner: owner.runnerId };
		}
	}
	return undefined;
})();

assert.ok(routedEntity, "find an entity owned by runner 1 for a request entering runner 0");

const [mailboxBefore, counterBefore] = await Promise.all([
	requestJson("/debug/mailbox"),
	requestJson(
		`/debug/storage?runner=${runner1.runnerId}&id=${encodeURIComponent(routedEntity.entityId)}`,
	),
]);

const previousValue = counterBefore.counterRows.find(
	(row) => row.entity_id === routedEntity.entityId,
)?.value;

if (requireExisting) {
	assert.ok(mailboxBefore.messages > 0, "mailbox messages must survive the process restart");
	assert.ok(mailboxBefore.replies > 0, "mailbox replies must survive the process restart");
	assert.notEqual(previousValue, undefined, "runner counter must survive the process restart");
}

const result = await requestJson(
	`/increment?id=${encodeURIComponent(routedEntity.entityId)}&via=${runner0.runnerId}`,
	{ method: "POST" },
);
assert.equal(result.runnerId, runner1.runnerId, "sharded RPC must execute on runner 1");
if (previousValue !== undefined) assert.equal(result.value, previousValue + 1);

const [mailboxAfter, counterAfter] = await Promise.all([
	requestJson("/debug/mailbox"),
	requestJson(
		`/debug/storage?runner=${runner1.runnerId}&id=${encodeURIComponent(routedEntity.entityId)}`,
	),
]);

assert.equal(mailboxAfter.messages, mailboxBefore.messages + 1);
assert.equal(mailboxAfter.replies, mailboxBefore.replies + 1);
assert.equal(mailboxAfter.pendingMessages, 0);

const previousRequestIds = new Set(mailboxBefore.messageRows.map((row) => String(row.request_id)));
const newMessage = mailboxAfter.messageRows.find(
	(row) => !previousRequestIds.has(String(row.request_id)),
);
assert.ok(newMessage, "new mailbox request must be visible");
assert.ok(
	mailboxAfter.replyRows.some((row) => String(row.request_id) === String(newMessage.request_id)),
	"mailbox reply must reference the exact request ID",
);

const persistedCounter = counterAfter.counterRows.find(
	(row) => row.entity_id === routedEntity.entityId,
);
assert.equal(persistedCounter?.value, result.value);

const workflowExecutionPath =
	"/workflow/execute?operationId=workers-repro-postgres-workflow-proof-v8&id=workflow-postgres-counter-proof-v2";
const workflowExecution = await requestJson(workflowExecutionPath, { method: "POST" });
assert.ok(workflowExecution.executionId);
assert.equal(workflowExecution.result.entityId, "workflow-postgres-counter-proof-v2");
assert.equal(workflowExecution.result.value, 1, "idempotent workflow must increment only once");
const workflowReplay = await requestJson(workflowExecutionPath, { method: "POST" });
assert.equal(workflowReplay.executionId, workflowExecution.executionId);
assert.deepEqual(workflowReplay.result, workflowExecution.result);
const workflowPoll = await requestJson(
	`/workflow/poll?id=${encodeURIComponent(workflowExecution.executionId)}`,
);
assert.equal(workflowPoll.state, "Complete", "workflow polling must recover its persisted result");
const persistedWorkflowCounter = await requestJson(
	`/debug/workflow-counter?id=${encodeURIComponent(workflowExecution.result.entityId)}`,
);
assert.equal(persistedWorkflowCounter.value, workflowExecution.result.value);
const workflowAttempts = await requestJson(
	`/debug/workflow-attempts?operationId=${encodeURIComponent("workers-repro-postgres-workflow-proof-v8")}`,
);
assert.equal(workflowAttempts.attempt.attempts, 1);
assert.equal(workflowAttempts.effects.length, 1);

if (!requireExisting) {
	const pendingRecoveryTask = await requestJson(
		`/task/prepare-recovery?taskId=${encodeURIComponent(recoveryTaskId)}&id=${encodeURIComponent(recoveryEntityId)}`,
		{ method: "POST" },
	);
	assert.equal(pendingRecoveryTask.status, "running");
}

console.log(
	JSON.stringify(
		{
			ready: preparation.ready,
			shardCount: runner0.ownedShardIds.length + runner1.ownedShardIds.length,
			runner0Owns: runner0.ownedShardIds.length,
			runner1Owns: runner1.ownedShardIds.length,
			ingressRunner: runner0.runnerId,
			entityId: routedEntity.entityId,
			shardId: routedEntity.shardId,
			executionRunner: result.runnerId,
			counterValue: result.value,
			mailboxRequestId: String(newMessage.request_id),
			mailboxReplyCount: mailboxAfter.replies,
			workflowExecutionId: workflowExecution.executionId,
			workflowResult: workflowExecution.result,
			workflowReplayValue: workflowReplay.result.value,
			workflowPollState: workflowPoll.state,
			persistedWorkflowCounter: persistedWorkflowCounter.value,
			workflowActivityAttempts: workflowAttempts.attempt.attempts,
			startupRecoveryTaskStatus: recoveryTaskBefore.status,
			startupRecoveryTaskResult: recoveryTaskBefore.result ?? null,
			startupRecoveryCounterBefore: recoveryCounterBefore.value,
			recoveryRequired: requireExisting,
		},
		null,
		2,
	),
);
