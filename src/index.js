import { DurableObject } from "cloudflare:workers";
import { Context, Duration, Effect, Layer, Option, Schema } from "effect";
import {
  ClusterSchema,
  Entity,
  EntityAddress,
  EntityId,
  EntityType,
  HttpRunner,
  MessageStorage,
  RunnerAddress,
  RunnerHealth,
  RunnerStorage,
  ShardId,
  Sharding,
  ShardingConfig,
  Snowflake,
} from "effect/cluster";
import { FetchHttpClient, HttpRouter, HttpServerResponse } from "effect/http";
import { Rpc, RpcSerialization } from "effect/rpc";
import { makeMailboxDriver } from "./mailbox-storage.js";

const runnerIds = ["runner-0", "runner-1"];
const runnerHostSuffix = ".cluster.local";
const registryName = "cluster-registry";
const mailboxName = "cluster-mailbox";
const shardLockExpirationMs = 30_000;
const shardCount = 32;
const MailboxDriver = Context.Service()("effect-cluster-workers-repro/MailboxDriver");

const makeRegistryRequest = (namespace, operation, payload = {}) =>
  Effect.promise(async () => {
    const response = await namespace.getByName(registryName).fetch(
      new Request(`http://cluster-registry/${operation}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    if (!response.ok) throw new Error(`Registry operation failed: ${operation}`);
    return response.json();
  });

const makeMailboxRequest = (namespace, operation, payload = {}) =>
  Effect.promise(async () => {
    const response = await namespace.getByName(mailboxName).fetch(
      new Request(`http://cluster-mailbox/${operation}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    if (!response.ok) throw new Error(`Mailbox operation failed: ${operation}`);
    return response.json();
  });

const addressToWire = (address) => ({
  shardId: ShardId.toString(address.shardId),
  entityType: String(address.entityType),
  entityId: String(address.entityId),
});

const addressFromWire = (address) =>
  EntityAddress.make({
    shardId: ShardId.fromString(address.shardId),
    entityType: EntityType.make(address.entityType),
    entityId: EntityId.make(address.entityId),
  });

const makeRemoteMailboxDriver = (namespace) => ({
  saveEnvelope: (options) =>
    Effect.map(makeMailboxRequest(namespace, "save-envelope", options), (result) => {
      if (result._tag === "Success") return MessageStorage.SaveResultEncoded.Success();
      return MessageStorage.SaveResultEncoded.Duplicate({
        originalId: Snowflake.Snowflake(result.originalId),
        lastReceivedReply: Option.fromNullishOr(result.lastReceivedReply),
      });
    }),
  saveReply: (reply) => Effect.asVoid(makeMailboxRequest(namespace, "save-reply", { reply })),
  clearReplies: (requestId, options) =>
    Effect.asVoid(
      makeMailboxRequest(namespace, "clear-replies", {
        requestId: String(requestId),
        expectedReplyId:
          options?.expectedReplyId === undefined ? undefined : String(options.expectedReplyId),
      }),
    ),
  requestIdForPrimaryKey: (primaryKey) =>
    Effect.map(
      makeMailboxRequest(namespace, "request-id-for-primary-key", { primaryKey }),
      (requestId) => Option.map(Option.fromNullishOr(requestId), Snowflake.Snowflake),
    ),
  repliesFor: (requestIds) => makeMailboxRequest(namespace, "replies-for", { requestIds }),
  repliesForUnfiltered: (requestIds) =>
    makeMailboxRequest(namespace, "replies-for-unfiltered", { requestIds }),
  unprocessedMessages: (shardIds, now, options) =>
    Effect.map(
      makeMailboxRequest(namespace, "unprocessed-messages", {
        shardIds,
        now,
        options: options && {
          ...options,
          addresses: options.addresses?.map(addressToWire),
        },
      }),
      (messages) =>
        messages.map((message) => ({
          envelope: message.envelope,
          lastSentReply: Option.fromNullishOr(message.lastSentReply),
        })),
    ),
  unprocessedMessagesById: (messageIds, now) =>
    Effect.map(
      makeMailboxRequest(namespace, "unprocessed-messages-by-id", {
        messageIds: messageIds.map(String),
        now,
      }),
      (messages) =>
        messages.map((message) => ({
          envelope: message.envelope,
          lastSentReply: Option.fromNullishOr(message.lastSentReply),
        })),
    ),
  resetRequests: (requestIds) =>
    Effect.asVoid(
      makeMailboxRequest(namespace, "reset-requests", { requestIds: requestIds.map(String) }),
    ),
  resetAddresses: (addresses) =>
    Effect.asVoid(
      makeMailboxRequest(namespace, "reset-addresses", { addresses: addresses.map(addressToWire) }),
    ),
  clearAddress: (address) =>
    Effect.asVoid(
      makeMailboxRequest(namespace, "clear-address", { address: addressToWire(address) }),
    ),
  resetShards: (shardIds) =>
    Effect.asVoid(makeMailboxRequest(namespace, "reset-shards", { shardIds })),
  withTransaction: (effect) => effect,
});

const makeRemoteMessageStorageLayer = (namespace) =>
  Layer.effect(
    MessageStorage.MessageStorage,
    MessageStorage.makeEncoded(makeRemoteMailboxDriver(namespace)),
  ).pipe(Layer.provide(Snowflake.layerGenerator));

const makeRunnerStorageLayer = (namespace) =>
  Layer.succeed(RunnerStorage.RunnerStorage)(
    RunnerStorage.makeEncoded({
      getRunners: Effect.map(
        makeRegistryRequest(namespace, "get-runners"),
        (result) => result.runners,
      ),
      register: (address, runner, healthy) =>
        Effect.map(
          makeRegistryRequest(namespace, "register", { address, runner, healthy }),
          (result) => result.machineId,
        ),
      unregister: (address) =>
        Effect.asVoid(makeRegistryRequest(namespace, "unregister", { address })),
      setRunnerHealth: (address, healthy) =>
        Effect.asVoid(makeRegistryRequest(namespace, "set-runner-health", { address, healthy })),
      acquire: (address, shardIds) =>
        Effect.map(
          makeRegistryRequest(namespace, "acquire", { address, shardIds }),
          (result) => result.shardIds,
        ),
      refresh: (address, shardIds) =>
        Effect.map(
          makeRegistryRequest(namespace, "refresh", { address, shardIds }),
          (result) => result.shardIds,
        ),
      release: (address, shardId) =>
        Effect.asVoid(makeRegistryRequest(namespace, "release", { address, shardId })),
      releaseAll: (address) =>
        Effect.asVoid(makeRegistryRequest(namespace, "release-all", { address })),
    }),
  );

const makeRunnerFetchLayer = (namespace) => {
  const fetchRunner = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (!url.hostname.endsWith(runnerHostSuffix)) return globalThis.fetch(request);

    const runnerId = url.hostname.slice(0, -runnerHostSuffix.length);
    if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 502 });
    const headers = new Headers(request.headers);
    headers.set("x-cluster-runner-id", runnerId);
    return namespace.getByName(runnerId).fetch(new Request(request, { headers }));
  };

  return FetchHttpClient.layer.pipe(
    Layer.provideMerge(Layer.succeed(FetchHttpClient.Fetch)(fetchRunner)),
  );
};

const Counter = Entity.make("Counter", [
  Rpc.make("Increment", {
    payload: Schema.Struct({ entityId: Schema.String }),
    success: Schema.Struct({ value: Schema.Number, runnerId: Schema.String }),
  }),
]).annotateRpcs(ClusterSchema.Persisted, true);

const makeRunnerLayer = ({ storage, env, runnerId }) => {
  const runnerAddress = RunnerAddress.make(`${runnerId}${runnerHostSuffix}`, 8787);
  const shardingConfig = ShardingConfig.layer({
    runnerAddress: Option.some(runnerAddress),
    shardsPerGroup: shardCount,
    shardLockExpiration: Duration.millis(shardLockExpirationMs),
    shardLockRefreshInterval: Duration.seconds(5),
    refreshAssignmentsInterval: Duration.millis(500),
    entityMessagePollInterval: Duration.millis(250),
    entityReplyPollInterval: Duration.millis(250),
  });
  const clusterInfrastructure = Layer.mergeAll(
    shardingConfig,
    makeRunnerStorageLayer(env.CLUSTER_REGISTRY),
    makeRemoteMessageStorageLayer(env.CLUSTER_MAILBOX),
    RunnerHealth.layerNoop,
    RpcSerialization.layerJson,
    makeRunnerFetchLayer(env.COUNTER_RUNNER),
  );
  const runnerProtocol = HttpRunner.layerClientProtocolHttp({ path: "/cluster" }).pipe(
    Layer.provideMerge(clusterInfrastructure),
  );
  const clusterRunner = HttpRunner.layerHttpOptions({ path: "/cluster" }).pipe(
    Layer.provideMerge(runnerProtocol),
  );
  const counter = Counter.toLayer({
    Increment: (request) =>
      Effect.sync(() => {
        const entityId = request.payload.entityId;
        const rows = storage.sql
          .exec(
            "INSERT INTO counter (entity_id, value) VALUES (?, 1) ON CONFLICT(entity_id) DO UPDATE SET value = value + 1 RETURNING value",
            entityId,
          )
          .toArray();
        return { value: rows[0].value, runnerId };
      }),
  }).pipe(Layer.provideMerge(clusterRunner));
  const routes = Layer.mergeAll(
    HttpRouter.add("GET", "/health", HttpServerResponse.json({ ok: true })),
    HttpRouter.add("POST", "/increment", (request) =>
      Effect.gen(function* () {
        const entityId =
          new URL(request.url, "http://runner.local").searchParams.get("id") ?? "default";
        const makeClient = yield* Counter.client;
        const value = yield* makeClient(entityId).Increment({ entityId });
        return yield* HttpServerResponse.json(value);
      }),
    ),
    HttpRouter.add("GET", "/debug/shard", (request) =>
      Effect.gen(function* () {
        const entityId =
          new URL(request.url, "http://runner.local").searchParams.get("id") ?? "default";
        const shardId = yield* Counter.getShardId(entityId);
        const sharding = yield* Sharding.Sharding;
        return yield* HttpServerResponse.json({
          entityId,
          shardId: shardId.toString(),
          runnerId,
          ownsShard: sharding.hasShardId(shardId),
        });
      }),
    ),
    HttpRouter.add(
      "GET",
      "/debug/assignments",
      Effect.gen(function* () {
        const sharding = yield* Sharding.Sharding;
        const ownedShardIds = Array.from({ length: shardCount }, (_, index) =>
          ShardId.make("default", index + 1),
        )
          .filter((shardId) => sharding.hasShardId(shardId))
          .map((shardId) => shardId.toString());
        return yield* HttpServerResponse.json({ runnerId, ownedShardIds });
      }),
    ),
    HttpRouter.add("GET", "/debug/storage", (request) =>
      Effect.gen(function* () {
        const runnerStorage = yield* RunnerStorage.RunnerStorage;
        const runners = yield* runnerStorage.getRunners;
        const entityId = new URL(request.url, "http://runner.local").searchParams.get("id");
        const counterRows =
          entityId === null
            ? storage.sql
                .exec("SELECT entity_id, value FROM counter ORDER BY entity_id LIMIT 50")
                .toArray()
            : storage.sql
                .exec("SELECT entity_id, value FROM counter WHERE entity_id = ?", entityId)
                .toArray();
        return yield* HttpServerResponse.json({
          runnerId,
          runners: runners.map(([runner, healthy]) => ({
            address: runner.address.toString(),
            healthy,
          })),
          counterRows,
          mailbox: "cluster-mailbox",
        });
      }),
    ),
  );

  return routes.pipe(Layer.provideMerge(counter));
};

const runnerRequest = (request, runnerId) => {
  const url = new URL(request.url);
  const target = new URL(
    `${url.pathname}${url.search}`,
    `http://${runnerId}${runnerHostSuffix}:8787`,
  );
  const headers = new Headers(request.headers);
  headers.set("x-cluster-runner-id", runnerId);
  const init = { method: request.method, headers };
  if (request.body && request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
    init.duplex = "half";
  }
  return new Request(target, init);
};

const fetchRunner = (env, runnerId, request) =>
  env.COUNTER_RUNNER.getByName(runnerId).fetch(runnerRequest(request, runnerId));

const makeMailboxDriverLayer = (storage) =>
  Layer.succeed(MailboxDriver)(makeMailboxDriver(storage));

const mailboxOperation = (path, run) =>
  HttpRouter.add("POST", path, (request) =>
    Effect.gen(function* () {
      const payload = yield* request.json;
      const driver = yield* MailboxDriver;
      const result = yield* run(driver, payload);
      return yield* HttpServerResponse.json(result);
    }),
  );

const makeMailboxLayer = (storage) => {
  const count = (sql) => Number(storage.sql.exec(sql).one().count);
  const routes = Layer.mergeAll(
    mailboxOperation("/save-envelope", (driver, payload) =>
      Effect.map(driver.saveEnvelope(payload), (result) =>
        result._tag === "Success"
          ? result
          : {
              _tag: "Duplicate",
              originalId: String(result.originalId),
              lastReceivedReply: Option.isSome(result.lastReceivedReply)
                ? result.lastReceivedReply.value
                : null,
            },
      ),
    ),
    mailboxOperation("/save-reply", (driver, payload) =>
      Effect.as(driver.saveReply(payload.reply), { ok: true }),
    ),
    mailboxOperation("/clear-replies", (driver, payload) =>
      Effect.as(
        driver.clearReplies(Snowflake.Snowflake(payload.requestId), {
          expectedReplyId:
            payload.expectedReplyId === undefined
              ? undefined
              : Snowflake.Snowflake(payload.expectedReplyId),
        }),
        { ok: true },
      ),
    ),
    mailboxOperation("/request-id-for-primary-key", (driver, payload) =>
      Effect.map(driver.requestIdForPrimaryKey(payload.primaryKey), (result) =>
        Option.isSome(result) ? result.value.toString() : null,
      ),
    ),
    mailboxOperation("/replies-for", (driver, payload) => driver.repliesFor(payload.requestIds)),
    mailboxOperation("/replies-for-unfiltered", (driver, payload) =>
      driver.repliesForUnfiltered(payload.requestIds),
    ),
    mailboxOperation("/unprocessed-messages", (driver, payload) =>
      Effect.map(
        driver.unprocessedMessages(
          payload.shardIds,
          payload.now,
          payload.options && {
            ...payload.options,
            addresses: payload.options.addresses?.map(addressFromWire),
          },
        ),
        (messages) =>
          messages.map((message) => ({
            envelope: message.envelope,
            lastSentReply: Option.isSome(message.lastSentReply)
              ? message.lastSentReply.value
              : null,
          })),
      ),
    ),
    mailboxOperation("/unprocessed-messages-by-id", (driver, payload) =>
      Effect.map(
        driver.unprocessedMessagesById(payload.messageIds.map(Snowflake.Snowflake), payload.now),
        (messages) =>
          messages.map((message) => ({
            envelope: message.envelope,
            lastSentReply: Option.isSome(message.lastSentReply)
              ? message.lastSentReply.value
              : null,
          })),
      ),
    ),
    mailboxOperation("/reset-requests", (driver, payload) =>
      Effect.as(driver.resetRequests(payload.requestIds.map(Snowflake.Snowflake)), { ok: true }),
    ),
    mailboxOperation("/reset-addresses", (driver, payload) =>
      Effect.as(driver.resetAddresses(payload.addresses.map(addressFromWire)), { ok: true }),
    ),
    mailboxOperation("/clear-address", (driver, payload) =>
      Effect.as(driver.clearAddress(addressFromWire(payload.address)), { ok: true }),
    ),
    mailboxOperation("/reset-shards", (driver, payload) =>
      Effect.as(driver.resetShards(payload.shardIds), { ok: true }),
    ),
    HttpRouter.add(
      "GET",
      "/debug/status",
      Effect.gen(function* () {
        const messages = storage.sql
          .exec(
            "SELECT id, shard_id, entity_type, entity_id, kind, processed, request_id, last_read, last_reply_id FROM cluster_messages ORDER BY rowid DESC LIMIT 20",
          )
          .toArray()
          .map((row) => ({ ...row, id: String(row.id), request_id: String(row.request_id) }));
        const replies = storage.sql
          .exec(
            "SELECT id, request_id, kind, sequence FROM cluster_replies ORDER BY rowid DESC LIMIT 20",
          )
          .toArray()
          .map((row) => ({ ...row, id: String(row.id), request_id: String(row.request_id) }));
        return yield* HttpServerResponse.json({
          messages: count("SELECT count(*) AS count FROM cluster_messages"),
          pendingMessages: count(
            "SELECT count(*) AS count FROM cluster_messages WHERE processed = 0",
          ),
          replies: count("SELECT count(*) AS count FROM cluster_replies"),
          messageRows: messages,
          replyRows: replies,
        });
      }),
    ),
  );
  return routes.pipe(Layer.provideMerge(makeMailboxDriverLayer(storage)));
};

export class CounterRunner extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    this.runnerId = undefined;
    this.handler = undefined;
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS counter (entity_id TEXT PRIMARY KEY, value INTEGER NOT NULL)",
    );
  }

  fetch(request) {
    const runnerId = request.headers.get("x-cluster-runner-id");
    if (!runnerIds.includes(runnerId))
      return new Response("Runner identity required", { status: 400 });
    if (this.runnerId && this.runnerId !== runnerId)
      return new Response("Runner identity mismatch", { status: 409 });
    if (!this.handler) {
      this.runnerId = runnerId;
      this.handler = HttpRouter.toWebHandler(
        makeRunnerLayer({ storage: this.ctx.storage, env: this.env, runnerId }),
        { disableLogger: true },
      );
    }

    return this.handler.handler(request);
  }
}

export class ClusterRegistry extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cluster_runners (machine_id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT NOT NULL UNIQUE, runner TEXT NOT NULL, healthy INTEGER NOT NULL, registered_at INTEGER NOT NULL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cluster_shard_locks (shard_id TEXT PRIMARY KEY, address TEXT NOT NULL, acquired_at INTEGER NOT NULL)",
    );
  }

  fetch(request) {
    return this.handleRequest(request);
  }

  async handleRequest(request) {
    const url = new URL(request.url);
    const sql = this.ctx.storage.sql;
    if (request.method === "GET" && url.pathname === "/status") {
      const runners = sql
        .exec("SELECT address, healthy, machine_id FROM cluster_runners ORDER BY machine_id")
        .toArray();
      const locks = sql
        .exec("SELECT shard_id, address, acquired_at FROM cluster_shard_locks ORDER BY shard_id")
        .toArray();
      return Response.json({ runners, locks });
    }
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });

    const payload = await request.json();
    const result = this.ctx.storage.transactionSync(() => {
      if (url.pathname === "/get-runners") {
        return {
          runners: sql
            .exec("SELECT runner, healthy FROM cluster_runners ORDER BY machine_id")
            .toArray()
            .map((row) => [row.runner, Boolean(row.healthy)]),
        };
      }
      if (url.pathname === "/register") {
        const row = sql
          .exec(
            "INSERT INTO cluster_runners (address, runner, healthy, registered_at) VALUES (?, ?, ?, ?) ON CONFLICT(address) DO UPDATE SET runner = excluded.runner, healthy = excluded.healthy, registered_at = excluded.registered_at RETURNING machine_id",
            payload.address,
            payload.runner,
            Number(payload.healthy),
            Date.now(),
          )
          .one();
        return { machineId: row.machine_id };
      }
      if (url.pathname === "/unregister") {
        sql.exec("DELETE FROM cluster_shard_locks WHERE address = ?", payload.address);
        sql.exec("DELETE FROM cluster_runners WHERE address = ?", payload.address);
        return {};
      }
      if (url.pathname === "/set-runner-health") {
        sql.exec(
          "UPDATE cluster_runners SET healthy = ?, registered_at = ? WHERE address = ?",
          Number(payload.healthy),
          Date.now(),
          payload.address,
        );
        return {};
      }
      if (url.pathname === "/acquire") {
        const shardIds = [];
        const now = Date.now();
        for (const shardId of payload.shardIds) {
          sql.exec(
            "INSERT INTO cluster_shard_locks (shard_id, address, acquired_at) VALUES (?, ?, ?) ON CONFLICT(shard_id) DO UPDATE SET address = excluded.address, acquired_at = excluded.acquired_at WHERE cluster_shard_locks.address = excluded.address OR cluster_shard_locks.acquired_at <= ?",
            shardId,
            payload.address,
            now,
            now - shardLockExpirationMs,
          );
          const row = sql
            .exec("SELECT address FROM cluster_shard_locks WHERE shard_id = ?", shardId)
            .toArray()[0];
          if (row?.address === payload.address) shardIds.push(shardId);
        }
        return { shardIds };
      }
      if (url.pathname === "/refresh") {
        const shardIds = [];
        const now = Date.now();
        for (const shardId of payload.shardIds) {
          const rows = sql
            .exec(
              "UPDATE cluster_shard_locks SET acquired_at = ? WHERE shard_id = ? AND address = ? RETURNING shard_id",
              now,
              shardId,
              payload.address,
            )
            .toArray();
          if (rows.length > 0) shardIds.push(shardId);
        }
        return { shardIds };
      }
      if (url.pathname === "/release") {
        sql.exec(
          "DELETE FROM cluster_shard_locks WHERE address = ? AND shard_id = ?",
          payload.address,
          payload.shardId,
        );
        return {};
      }
      if (url.pathname === "/release-all") {
        sql.exec("DELETE FROM cluster_shard_locks WHERE address = ?", payload.address);
        return {};
      }
      return undefined;
    });

    if (result === undefined) return new Response("Not found", { status: 404 });
    return Response.json(result);
  }
}

export class ClusterMailbox extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.handler = undefined;
  }

  fetch(request) {
    if (!this.handler) {
      this.handler = HttpRouter.toWebHandler(makeMailboxLayer(this.ctx.storage), {
        disableLogger: true,
      });
    }
    return this.handler.handler(request);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/prepare" && request.method === "POST") {
      await Promise.all(
        runnerIds.map((runnerId) =>
          fetchRunner(env, runnerId, new Request("http://internal/health")),
        ),
      );
      const deadline = Date.now() + 15_000;
      let status;
      let assignments;
      let consistent = false;
      do {
        const [statusResponse, ...assignmentResponses] = await Promise.all([
          env.CLUSTER_REGISTRY.getByName(registryName).fetch("http://cluster-registry/status"),
          ...runnerIds.map((runnerId) =>
            fetchRunner(env, runnerId, new Request("http://internal/debug/assignments")),
          ),
        ]);
        status = await statusResponse.json();
        assignments = await Promise.all(assignmentResponses.map((response) => response.json()));
        const assignmentOwners = new Map();
        for (const assignment of assignments) {
          for (const shardId of assignment.ownedShardIds)
            assignmentOwners.set(shardId, assignment.runnerId);
        }
        const registeredRunners = new Set(
          status.runners.map((runner) => runner.address.split(".")[0]),
        );
        const lockOwners = new Map(
          status.locks.map((lock) => [lock.shard_id, lock.address.split(".")[0]]),
        );
        consistent =
          runnerIds.every((runnerId) => registeredRunners.has(runnerId)) &&
          assignments.every((assignment) => assignment.ownedShardIds.length > 0) &&
          assignmentOwners.size === shardCount &&
          Array.from(assignmentOwners).every(
            ([shardId, runnerId]) => lockOwners.get(shardId) === runnerId,
          );
        if (consistent) break;
        await new Promise((resolve) => setTimeout(resolve, 250));
      } while (Date.now() < deadline);
      return Response.json(
        { ready: consistent, ...status, assignments },
        { status: consistent ? 200 : 503 },
      );
    }
    if (url.pathname === "/debug/registry" && request.method === "GET") {
      return env.CLUSTER_REGISTRY.getByName(registryName).fetch("http://cluster-registry/status");
    }
    if (url.pathname === "/debug/mailbox" && request.method === "GET") {
      return env.CLUSTER_MAILBOX.getByName(mailboxName).fetch(
        "http://cluster-mailbox/debug/status",
      );
    }
    if (url.pathname === "/debug/storage" && request.method === "GET") {
      const runnerId = url.searchParams.get("runner") ?? runnerIds[0];
      if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
      const storageUrl = new URL("http://internal/debug/storage");
      const entityId = url.searchParams.get("id");
      if (entityId !== null) storageUrl.searchParams.set("id", entityId);
      return fetchRunner(env, runnerId, new Request(storageUrl));
    }
    if (["/health", "/increment", "/debug/shard", "/debug/assignments"].includes(url.pathname)) {
      const runnerId = url.searchParams.get("via") ?? runnerIds[0];
      if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
      return fetchRunner(env, runnerId, request);
    }
    return new Response("Not found", { status: 404 });
  },
};
