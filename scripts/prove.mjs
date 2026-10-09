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

const runner0 = preparation.assignments.find((assignment) => assignment.runnerId === "runner-0");
const runner1 = preparation.assignments.find((assignment) => assignment.runnerId === "runner-1");
assert.ok(runner0);
assert.ok(runner1);
assert.equal(runner0.ownedShardIds.length + runner1.ownedShardIds.length, 32);
assert.equal(new Set([...runner0.ownedShardIds, ...runner1.ownedShardIds]).size, 32);

let routedEntity;
for (let index = 0; index < 256; index += 1) {
  const entityId = `multi-runner-proof-${index}`;
  const shard = await requestJson(`/debug/shard?id=${encodeURIComponent(entityId)}&via=runner-0`);
  const owner = preparation.assignments.find((assignment) =>
    assignment.ownedShardIds.includes(shard.shardId),
  );
  assert.ok(owner, `shard ${shard.shardId} must have one runner owner`);
  if (owner.runnerId !== runner0.runnerId) {
    routedEntity = { entityId, shardId: shard.shardId, owner: owner.runnerId };
    break;
  }
}

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
      recoveryRequired: requireExisting,
    },
    null,
    2,
  ),
);
