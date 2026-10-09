import { ClusterError, MessageStorage, Snowflake, SqlMessageStorage } from "effect/cluster";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";

export const clusterMessageStorageReadBatchSize = 64;

const BoundedMessageStorage = Layer.effect(
	MessageStorage.MessageStorage,
	Effect.gen(function* () {
		const storage = yield* MessageStorage.MessageStorage;
		const clusterSql = (yield* SqlClient.SqlClient).withoutTransforms();
		const messagesTable = clusterSql("cluster_messages");
		const repliesTable = clusterSql("cluster_replies");

		const claimUnprocessedMessageIds = (shardIdList) => {
			const escapedShardIdList = Array.from(shardIdList, (shardId) => {
				const value = shardId.toString().replaceAll("'", "''");
				return `'${value}'`;
			}).join(",");
			if (escapedShardIdList.length === 0) return Effect.succeed([]);

			return clusterSql`
        WITH candidates AS (
          SELECT m.id
          FROM ${messagesTable} m
          WHERE m.shard_id IN (${clusterSql.literal(escapedShardIdList)})
            AND NOT EXISTS (
              SELECT 1
              FROM ${repliesTable} r
              WHERE r.request_id = m.request_id
                AND (r.kind = 0 OR r.acked = FALSE)
            )
            AND m.processed = FALSE
            AND (m.last_read IS NULL OR m.last_read < NOW() - INTERVAL '10 minutes')
            AND (m.deliver_at IS NULL OR m.deliver_at <= ${String(Date.now())})
          ORDER BY m.rowid ASC
          LIMIT ${clusterMessageStorageReadBatchSize}
          FOR UPDATE SKIP LOCKED
        ), claimed AS (
          UPDATE ${messagesTable} m
          SET last_read = NOW()
          FROM candidates
          WHERE m.id = candidates.id
          RETURNING m.id
        )
        SELECT id FROM claimed ORDER BY id ASC
      `.pipe(
				clusterSql.withTransaction,
				Effect.provideService(SqlClient.SafeIntegers, true),
				ClusterError.PersistenceError.refail,
				Effect.map((rows) => rows.map((row) => Snowflake.Snowflake(row.id))),
			);
		};

		return {
			...storage,
			unprocessedMessages: (shardIdList) =>
				Effect.flatMap(claimUnprocessedMessageIds(shardIdList), (messageIds) =>
					messageIds.length === 0
						? Effect.succeed([])
						: storage.unprocessedMessagesById(messageIds),
				),
		};
	}),
).pipe(Layer.provide(SqlMessageStorage.layer));

export const BoundedMessageStorageLayer = BoundedMessageStorage;
