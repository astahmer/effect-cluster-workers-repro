import { Effect, Option } from "effect";
import { MessageStorage, ShardId, Snowflake } from "effect/cluster";

const messageTable = "cluster_messages";
const replyTable = "cluster_replies";

const placeholders = (values) => values.map(() => "?").join(", ");

const replyFromRow = (row) =>
  Number(row.kind) === 0
    ? {
        _tag: "WithExit",
        id: String(row.id),
        requestId: String(row.request_id),
        exit: JSON.parse(row.payload),
      }
    : {
        _tag: "Chunk",
        id: String(row.id),
        requestId: String(row.request_id),
        values: JSON.parse(row.payload),
        sequence: Number(row.sequence),
      };

const messageFromRow = (row) => {
  let envelope;
  switch (Number(row.kind)) {
    case 0:
      envelope = {
        _tag: "Request",
        requestId: String(row.id),
        address: {
          shardId: ShardId.fromStringEncoded(row.shard_id),
          entityType: row.entity_type,
          entityId: row.entity_id,
        },
        tag: row.tag,
        payload: JSON.parse(row.payload),
        headers: JSON.parse(row.headers),
        ...(row.trace_id
          ? { traceId: row.trace_id, spanId: row.span_id, sampled: Boolean(row.sampled) }
          : {}),
      };
      break;
    case 1:
      envelope = {
        _tag: "AckChunk",
        id: String(row.id),
        requestId: String(row.request_id),
        replyId: String(row.reply_id),
        address: {
          shardId: ShardId.fromStringEncoded(row.shard_id),
          entityType: row.entity_type,
          entityId: row.entity_id,
        },
      };
      break;
    case 2:
      envelope = {
        _tag: "Interrupt",
        id: String(row.id),
        requestId: String(row.request_id),
        address: {
          shardId: ShardId.fromStringEncoded(row.shard_id),
          entityType: row.entity_type,
          entityId: row.entity_id,
        },
      };
      break;
  }

  const lastSentReply =
    row.reply_reply_id === null || row.reply_reply_id === undefined
      ? Option.none()
      : Option.some(
          replyFromRow({
            id: row.reply_reply_id,
            request_id: row.request_id,
            kind: row.reply_kind,
            payload: row.reply_payload,
            sequence: row.reply_sequence,
          }),
        );

  return { envelope, lastSentReply };
};

export const makeMailboxDriver = (storage) => {
  const sql = storage.sql;

  sql.exec(`CREATE TABLE IF NOT EXISTS ${messageTable} (
    id TEXT PRIMARY KEY NOT NULL,
    message_id TEXT UNIQUE,
    shard_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    kind INTEGER NOT NULL,
    tag TEXT,
    payload TEXT,
    headers TEXT,
    trace_id TEXT,
    span_id TEXT,
    sampled INTEGER,
    request_id TEXT NOT NULL,
    reply_id TEXT,
    deliver_at INTEGER,
    processed INTEGER NOT NULL DEFAULT 0,
    last_read TEXT,
    last_reply_id TEXT
  )`);
  sql.exec(`CREATE TABLE IF NOT EXISTS ${replyTable} (
    id TEXT PRIMARY KEY NOT NULL,
    kind INTEGER,
    request_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    sequence INTEGER,
    acked INTEGER NOT NULL DEFAULT 0
  )`);
  sql.exec(
    `CREATE UNIQUE INDEX IF NOT EXISTS cluster_replies_with_exit ON ${replyTable}(request_id) WHERE kind = 0`,
  );
  sql.exec(
    `CREATE UNIQUE INDEX IF NOT EXISTS cluster_replies_chunk_sequence ON ${replyTable}(request_id, sequence) WHERE kind IS NULL`,
  );
  sql.exec(
    `CREATE INDEX IF NOT EXISTS cluster_messages_shard_unprocessed ON ${messageTable}(shard_id, processed, last_read, deliver_at)`,
  );
  sql.exec(
    `CREATE INDEX IF NOT EXISTS cluster_messages_entity ON ${messageTable}(entity_type, entity_id)`,
  );

  const selectReplyById = (id) =>
    sql
      .exec(
        `SELECT id, kind, request_id, payload, sequence FROM ${replyTable} WHERE id = ?`,
        String(id),
      )
      .toArray()[0];

  const selectReplyForRequest = (requestId) =>
    sql
      .exec(
        `SELECT id, kind, request_id, payload, sequence FROM ${replyTable} WHERE request_id = ? AND kind = 0`,
        String(requestId),
      )
      .toArray()[0];

  const encodeMessage = (envelope, primaryKey, deliverAt) => {
    const address = envelope.address;
    switch (envelope._tag) {
      case "Request":
        return {
          id: String(envelope.requestId),
          messageId: primaryKey,
          shardId: ShardId.toString(address.shardId),
          entityType: String(address.entityType),
          entityId: String(address.entityId),
          kind: 0,
          tag: envelope.tag,
          payload: JSON.stringify(envelope.payload),
          headers: JSON.stringify(envelope.headers),
          traceId: envelope.traceId ?? null,
          spanId: envelope.spanId ?? null,
          sampled: envelope.sampled === undefined ? null : Number(envelope.sampled),
          requestId: String(envelope.requestId),
          replyId: null,
          deliverAt,
        };
      case "AckChunk":
        return {
          id: String(envelope.id),
          messageId: primaryKey,
          shardId: ShardId.toString(address.shardId),
          entityType: String(address.entityType),
          entityId: String(address.entityId),
          kind: 1,
          tag: null,
          payload: null,
          headers: null,
          traceId: null,
          spanId: null,
          sampled: null,
          requestId: String(envelope.requestId),
          replyId: String(envelope.replyId),
          deliverAt,
        };
      case "Interrupt":
        return {
          id: String(envelope.id),
          messageId: primaryKey,
          shardId: ShardId.toString(address.shardId),
          entityType: String(address.entityType),
          entityId: String(address.entityId),
          kind: 2,
          tag: null,
          payload: null,
          headers: null,
          traceId: null,
          spanId: null,
          sampled: null,
          requestId: String(envelope.requestId),
          replyId: null,
          deliverAt,
        };
    }
  };

  const insertMessage = (row) =>
    sql.exec(
      `INSERT INTO ${messageTable} (
      id, message_id, shard_id, entity_type, entity_id, kind, tag, payload, headers,
      trace_id, span_id, sampled, request_id, reply_id, deliver_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      row.id,
      row.messageId,
      row.shardId,
      row.entityType,
      row.entityId,
      row.kind,
      row.tag,
      row.payload,
      row.headers,
      row.traceId,
      row.spanId,
      row.sampled,
      row.requestId,
      row.replyId,
      row.deliverAt,
    );

  const selectReplyJoin = `
    SELECT m.*, r.id AS reply_reply_id, r.kind AS reply_kind,
      r.payload AS reply_payload, r.sequence AS reply_sequence
    FROM ${messageTable} m
    LEFT JOIN ${replyTable} r ON r.id = m.last_reply_id
  `;

  const unprocessedFilter = `
    AND NOT EXISTS (
      SELECT 1 FROM ${replyTable} r
      WHERE r.request_id = m.request_id AND (r.kind = 0 OR r.acked = 0)
    )
    AND m.processed = 0
    AND (m.deliver_at IS NULL OR m.deliver_at <= ?)
  `;

  const readMessages = (rows) => rows.map(messageFromRow);

  return {
    saveEnvelope: ({ envelope, primaryKey, deliverAt }) =>
      Effect.sync(() => {
        const row = encodeMessage(envelope, primaryKey, deliverAt);
        return storage.transactionSync(() => {
          if (envelope._tag === "AckChunk") {
            sql.exec(`UPDATE ${replyTable} SET acked = 1 WHERE id = ?`, row.replyId);
            sql.exec(
              `UPDATE ${messageTable} SET processed = 1 WHERE processed = 0 AND request_id = ? AND kind = 1`,
              row.requestId,
            );
          }
          if (envelope._tag === "Request" && primaryKey !== null) {
            const existing = sql
              .exec(
                `SELECT id, last_reply_id FROM ${messageTable} WHERE message_id = ?`,
                primaryKey,
              )
              .toArray()[0];
            if (existing) {
              const reply =
                existing.last_reply_id === null
                  ? undefined
                  : selectReplyById(existing.last_reply_id);
              return MessageStorage.SaveResultEncoded.Duplicate({
                originalId: Snowflake.Snowflake(existing.id),
                lastReceivedReply: Option.fromNullishOr(reply ? replyFromRow(reply) : undefined),
              });
            }
          }
          insertMessage(row);
          return MessageStorage.SaveResultEncoded.Success();
        });
      }),

    saveReply: (reply) =>
      Effect.sync(() =>
        storage.transactionSync(() => {
          const requestId = String(reply.requestId);
          if (reply._tag === "WithExit") {
            const existing = selectReplyForRequest(requestId);
            const replyId = existing?.id ?? String(reply.id);
            if (!existing) {
              sql.exec(
                `INSERT OR IGNORE INTO ${replyTable} (id, kind, request_id, payload, sequence) VALUES (?, 0, ?, ?, NULL)`,
                String(reply.id),
                requestId,
                JSON.stringify(reply.exit),
              );
            }
            const canonical = selectReplyForRequest(requestId);
            sql.exec(
              `UPDATE ${messageTable} SET processed = 1, last_reply_id = ? WHERE request_id = ?`,
              String(canonical?.id ?? replyId),
              requestId,
            );
            return;
          }

          sql.exec(
            `INSERT OR IGNORE INTO ${replyTable} (id, kind, request_id, payload, sequence) VALUES (?, NULL, ?, ?, ?)`,
            String(reply.id),
            requestId,
            JSON.stringify(reply.values),
            reply.sequence,
          );
          const canonical = sql
            .exec(
              `SELECT id FROM ${replyTable} WHERE request_id = ? AND kind IS NULL AND sequence = ?`,
              requestId,
              reply.sequence,
            )
            .toArray()[0];
          sql.exec(
            `UPDATE ${messageTable} SET last_reply_id = ? WHERE id = ?`,
            String(canonical.id),
            requestId,
          );
        }),
      ),

    clearReplies: (requestId, options) =>
      Effect.sync(() =>
        storage.transactionSync(() => {
          const id = String(requestId);
          const expected =
            options?.expectedReplyId === undefined ? undefined : String(options.expectedReplyId);
          if (expected === undefined) {
            sql.exec(`DELETE FROM ${replyTable} WHERE request_id = ? AND kind = 0`, id);
            sql.exec(`DELETE FROM ${messageTable} WHERE request_id = ? AND kind = 2`, id);
            sql.exec(
              `UPDATE ${messageTable} SET processed = 0, last_reply_id = NULL, last_read = NULL WHERE request_id = ?`,
              id,
            );
            return;
          }
          sql.exec(
            `DELETE FROM ${replyTable} WHERE request_id = ? AND kind = 0 AND EXISTS (
          SELECT 1 FROM ${messageTable} WHERE id = ? AND last_reply_id = ?
        )`,
            id,
            id,
            expected,
          );
          sql.exec(
            `UPDATE ${messageTable} SET processed = 0, last_reply_id = NULL, last_read = NULL
          WHERE request_id = ? AND last_reply_id = ?`,
            id,
            expected,
          );
        }),
      ),

    requestIdForPrimaryKey: (primaryKey) =>
      Effect.sync(() => {
        const row = sql
          .exec(`SELECT id FROM ${messageTable} WHERE message_id = ?`, primaryKey)
          .toArray()[0];
        return Option.fromNullishOr(row?.id).pipe(Option.map(Snowflake.Snowflake));
      }),

    repliesFor: (requestIds) =>
      Effect.sync(() => {
        if (requestIds.length === 0) return [];
        const rows = sql
          .exec(
            `SELECT id, kind, request_id, payload, sequence FROM ${replyTable}
          WHERE request_id IN (${placeholders(requestIds)})
          AND (kind = 0 OR (kind IS NULL AND acked = 0)) ORDER BY rowid ASC`,
            ...requestIds.map(String),
          )
          .toArray();
        return rows.map(replyFromRow);
      }),

    repliesForUnfiltered: (requestIds) =>
      Effect.sync(() => {
        if (requestIds.length === 0) return [];
        const rows = sql
          .exec(
            `SELECT id, kind, request_id, payload, sequence FROM ${replyTable}
          WHERE request_id IN (${placeholders(requestIds)}) ORDER BY rowid ASC`,
            ...requestIds.map(String),
          )
          .toArray();
        return rows.map(replyFromRow);
      }),

    unprocessedMessages: (shardIds, now, options) =>
      Effect.sync(() => {
        if (shardIds.length === 0 || options?.addresses?.length === 0) return [];
        const clauses = [];
        const values = shardIds.map(String);
        let query = `${selectReplyJoin} WHERE m.shard_id IN (${placeholders(shardIds)})`;
        if (options?.addresses !== undefined) {
          for (const address of options.addresses) {
            clauses.push("(m.shard_id = ? AND m.entity_type = ? AND m.entity_id = ?)");
            values.push(
              ShardId.toString(address.shardId),
              String(address.entityType),
              String(address.entityId),
            );
          }
          query += ` AND (${clauses.join(" OR ")})`;
        }
        query += `${unprocessedFilter} ORDER BY m.rowid ASC`;
        values.push(now);
        if (options?.limit !== undefined) {
          query += " LIMIT ?";
          values.push(Math.floor(options.limit));
        }

        const rows = storage.transactionSync(() => {
          const messages = sql.exec(query, ...values).toArray();
          if (messages.length > 0) {
            sql.exec(
              `UPDATE ${messageTable} SET last_read = CURRENT_TIMESTAMP WHERE id IN (${placeholders(messages)})`,
              ...messages.map((message) => String(message.id)),
            );
          }
          return messages;
        });
        return readMessages(rows);
      }),

    unprocessedMessagesById: (messageIds, now) =>
      Effect.sync(() => {
        if (messageIds.length === 0) return [];
        const rows = sql
          .exec(
            `${selectReplyJoin} WHERE m.id IN (${placeholders(messageIds)}) ${unprocessedFilter} ORDER BY m.rowid ASC`,
            ...messageIds.map(String),
            now,
          )
          .toArray();
        return readMessages(rows);
      }),

    resetRequests: (requestIds) =>
      Effect.sync(() => {
        if (requestIds.length === 0) return;
        sql.exec(
          `UPDATE ${messageTable} SET last_read = NULL WHERE processed = 0 AND id IN (${placeholders(requestIds)})`,
          ...requestIds.map(String),
        );
      }),

    resetAddresses: (addresses) =>
      Effect.sync(() => {
        if (addresses.length === 0) return;
        const clauses = addresses.map(() => "(shard_id = ? AND entity_type = ? AND entity_id = ?)");
        const values = addresses.flatMap((address) => [
          String(address.shardId),
          String(address.entityType),
          String(address.entityId),
        ]);
        sql.exec(
          `UPDATE ${messageTable} SET last_read = NULL WHERE processed = 0 AND (${clauses.join(" OR ")})`,
          ...values,
        );
      }),

    clearAddress: (address) =>
      Effect.sync(() =>
        storage.transactionSync(() => {
          const entityType = String(address.entityType);
          const entityId = String(address.entityId);
          sql.exec(
            `DELETE FROM ${replyTable} WHERE request_id IN (
          SELECT id FROM ${messageTable} WHERE entity_type = ? AND entity_id = ?
        )`,
            entityType,
            entityId,
          );
          sql.exec(
            `DELETE FROM ${messageTable} WHERE entity_type = ? AND entity_id = ?`,
            entityType,
            entityId,
          );
        }),
      ),

    resetShards: (shardIds) =>
      Effect.sync(() => {
        if (shardIds.length === 0) return;
        sql.exec(
          `UPDATE ${messageTable} SET last_read = NULL WHERE processed = 0 AND shard_id IN (${placeholders(shardIds)})`,
          ...shardIds.map(String),
        );
      }),

    withTransaction: (effect) => effect,
  };
};
