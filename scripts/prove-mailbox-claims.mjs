import assert from "node:assert/strict";
import { PgClient } from "@effect/sql-pg";
import { Effect, Fiber, Layer, Redacted } from "effect";
import { ShardId, ShardingConfig, MessageStorage } from "effect/cluster";
import { SqlClient } from "effect/sql";
import {
	BoundedMessageStorageLayer,
	clusterMessageStorageReadBatchSize,
} from "../src/bounded-message-storage.js";
import { cryptoLayer } from "../src/cluster-crypto.js";

const connectionString = process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_CLUSTER_DB;
assert.ok(connectionString, "local PostgreSQL connection string must be set");

const baseId = 8_000_000_000_000_000_000n;
const availableMessageCount = 130;
assert.equal(
	clusterMessageStorageReadBatchSize,
	64,
	"the bounded mailbox claim size matches Emisoup",
);
const shardId = ShardId.make("mailbox-proof", 1);
const shardIdList = [shardId];
const database = PgClient.layer({
	url: Redacted.make(connectionString),
	maxConnections: 4,
	minConnections: 0,
});
const shardingConfig = ShardingConfig.layer({
	shardsPerGroup: 32,
	shardLockDisableAdvisory: true,
});
const proofLayer = BoundedMessageStorageLayer.pipe(
	Layer.provideMerge(Layer.mergeAll(database, shardingConfig, cryptoLayer)),
);

const proof = Effect.gen(function* () {
	const sql = yield* SqlClient.SqlClient;
	const messageStorage = yield* MessageStorage.MessageStorage;
	const messagesTable = sql("cluster_messages");
	const repliesTable = sql("cluster_replies");
	let signalLocksHeld = () => {};
	const rowLocksHeld = new Promise((resolve) => {
		signalLocksHeld = resolve;
	});
	let releaseRowLocks = () => {};
	const rowLocksReleased = new Promise((resolve) => {
		releaseRowLocks = resolve;
	});
	let blockerFiber;
	console.log("mailbox proof connected");

	const removeProofRows = Effect.gen(function* () {
		yield* sql`DELETE FROM ${repliesTable} WHERE request_id >= ${baseId} AND request_id < ${baseId + 200n}`;
		yield* sql`DELETE FROM ${messagesTable} WHERE id >= ${baseId} AND id < ${baseId + 200n}`;
	});

	const run = Effect.gen(function* () {
		yield* removeProofRows;

		const messages = Array.from({ length: availableMessageCount + 3 }, (_, index) => {
			const id = baseId + BigInt(index);
			return {
				id,
				message_id: null,
				shard_id: shardId.toString(),
				entity_type: "MailboxClaimProof",
				entity_id: `message-${index}`,
				kind: 0,
				tag: "Claim",
				payload: JSON.stringify({ value: index }),
				headers: "{}",
				trace_id: null,
				span_id: null,
				sampled: null,
				processed: false,
				request_id: id,
				reply_id: null,
				last_reply_id: null,
				last_read: null,
				deliver_at: index === availableMessageCount ? Date.now() + 60_000 : null,
			};
		});
		yield* sql`INSERT INTO ${messagesTable} ${sql.insert(messages)}`;
		console.log("mailbox proof seeded messages");

		const replies = [
			{
				id: baseId + 1_000n,
				kind: 0,
				request_id: baseId + BigInt(availableMessageCount + 1),
				payload: "{}",
				sequence: null,
				acked: false,
			},
			{
				id: baseId + 1_001n,
				kind: 1,
				request_id: baseId + BigInt(availableMessageCount + 2),
				payload: "[]",
				sequence: 1,
				acked: false,
			},
		];
		yield* sql`INSERT INTO ${repliesTable} ${sql.insert(replies)}`;
		console.log("mailbox proof seeded replies");

		blockerFiber = yield* Effect.forkScoped(
			sql.withTransaction(
				Effect.gen(function* () {
					const lockedRows = yield* sql`
            SELECT id
            FROM ${messagesTable}
            WHERE id >= ${baseId} AND id < ${baseId + BigInt(availableMessageCount)}
            ORDER BY rowid ASC
            LIMIT ${clusterMessageStorageReadBatchSize}
            FOR UPDATE
          `;
					assert.equal(
						lockedRows.length,
						clusterMessageStorageReadBatchSize,
						"the competing transaction locks the first batch",
					);
					signalLocksHeld();
					yield* Effect.promise(() => rowLocksReleased);
				}),
			),
		);
		yield* Effect.promise(() => rowLocksHeld);

		const concurrentClaims = yield* Effect.all(
			[
				messageStorage.unprocessedMessages(shardIdList),
				messageStorage.unprocessedMessages(shardIdList),
			],
			{ concurrency: 2 },
		);
		const concurrentIds = concurrentClaims.flatMap((claim) =>
			claim.map((message) => String(message.envelope.requestId)),
		);
		const concurrentCounts = concurrentClaims
			.map((claim) => claim.length)
			.toSorted((left, right) => left - right);
		assert.equal(
			concurrentCounts.reduce((total, count) => total + count, 0),
			availableMessageCount - clusterMessageStorageReadBatchSize,
			"competing claims return every unlocked eligible message",
		);
		assert.equal(
			concurrentCounts.every((count) => count > 0 && count <= clusterMessageStorageReadBatchSize),
			true,
			"each competing claim stays within the configured batch size",
		);
		console.log("mailbox proof concurrent batch claims", concurrentCounts);
		assert.equal(
			new Set(concurrentIds).size,
			availableMessageCount - clusterMessageStorageReadBatchSize,
			"concurrent claims contain no duplicate messages",
		);
		assert.equal(
			concurrentIds.some(
				(id) =>
					BigInt(id) >= baseId && BigInt(id) < baseId + BigInt(clusterMessageStorageReadBatchSize),
			),
			false,
			"SKIP LOCKED leaves the externally locked first batch untouched",
		);

		const noUnlockedMessages = yield* messageStorage.unprocessedMessages(shardIdList);
		assert.equal(noUnlockedMessages.length, 0, "a fresh claim window does not reread claimed rows");

		releaseRowLocks();
		yield* Fiber.join(blockerFiber);
		const recoveredBatch = yield* messageStorage.unprocessedMessages(shardIdList);
		assert.equal(
			recoveredBatch.length,
			clusterMessageStorageReadBatchSize,
			"the locked first batch becomes claimable after release",
		);
		console.log("mailbox proof recovered locked batch");

		yield* sql`
      UPDATE ${messagesTable}
      SET last_read = NOW() - INTERVAL '11 minutes'
      WHERE id = ${baseId}
    `;
		const staleClaim = yield* messageStorage.unprocessedMessages(shardIdList);
		assert.deepEqual(
			staleClaim.map((message) => String(message.envelope.requestId)),
			[String(baseId)],
		);
		assert.equal((yield* messageStorage.unprocessedMessages(shardIdList)).length, 0);

		yield* sql`UPDATE ${messagesTable} SET deliver_at = 0 WHERE id = ${baseId + BigInt(availableMessageCount)}`;
		const delayedMessage = yield* messageStorage.unprocessedMessages(shardIdList);
		assert.deepEqual(
			delayedMessage.map((message) => String(message.envelope.requestId)),
			[String(baseId + BigInt(availableMessageCount))],
		);

		yield* sql`DELETE FROM ${repliesTable} WHERE request_id >= ${baseId + BigInt(availableMessageCount + 1)} AND request_id < ${baseId + BigInt(availableMessageCount + 3)}`;
		const messagesWithoutReplies = yield* messageStorage.unprocessedMessages(shardIdList);
		assert.equal(
			messagesWithoutReplies.length,
			2,
			"completed and unacked replies block message claims",
		);
		console.log("mailbox proof checked stale claims and reply gating");

		return {
			batchSize: clusterMessageStorageReadBatchSize,
			competingClaimCounts: concurrentCounts,
			uniqueConcurrentClaims: new Set(concurrentIds).size,
			recoveredAfterLockRelease: recoveredBatch.length,
			recoveredAfterStaleClaim: staleClaim.length,
			delayedAfterDueTime: delayedMessage.length,
			releasedAfterReplyCleanup: messagesWithoutReplies.length,
		};
	});

	const cleanup = Effect.gen(function* () {
		releaseRowLocks();
		if (blockerFiber) yield* Fiber.join(blockerFiber);
		yield* removeProofRows;
	});
	const result = yield* run.pipe(Effect.ensuring(cleanup));
	console.log("mailbox proof effect returned");
	return result;
});

const result = await Effect.runPromise(Effect.scoped(proof.pipe(Effect.provide(proofLayer))));
console.log("mailbox proof layer closed");
console.log(JSON.stringify(result, null, 2));
