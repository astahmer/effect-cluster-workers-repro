import { DurableObject } from "cloudflare:workers";
import { PgClient } from "@effect/sql-pg";
import {
	ClusterWorkflowEngine,
	ClusterSchema,
	Entity,
	HttpRunner,
	RunnerAddress,
	RunnerHealth,
	Runners,
	RunnerStorage,
	ShardId,
	Sharding,
	ShardingConfig,
	SqlRunnerStorage,
} from "effect/cluster";
import { Duration, Effect, Layer, Option, Redacted, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { FetchHttpClient, HttpRouter, HttpServerResponse } from "effect/http";
import { Rpc, RpcSerialization } from "effect/rpc";
import { Activity, Workflow } from "effect/workflow";
import { BoundedMessageStorageLayer } from "./bounded-message-storage.js";
import { cryptoLayer } from "./cluster-crypto.js";

const runnerIds = ["runner-0", "runner-1"];
const runnerHostSuffix = ".cluster.local";
const shardCount = 32;
const shardLockExpirationMs = 30_000;

const Counter = Entity.make("Counter", [
	Rpc.make("Increment", {
		payload: Schema.Struct({ entityId: Schema.String }),
		success: Schema.Struct({ value: Schema.Number, runnerId: Schema.String }),
	}),
]).annotateRpcs(ClusterSchema.Persisted, true);

const CounterWorkflowResult = Schema.Struct({
	entityId: Schema.String,
	value: Schema.Number,
	runnerId: Schema.String,
});

const CounterWorkflow = Workflow.make("CounterWorkflow", {
	payload: Schema.Struct({
		operationId: Schema.String,
		entityId: Schema.String,
		holdAfterEffectMs: Schema.optional(Schema.Number),
	}),
	success: CounterWorkflowResult,
	idempotencyKey: ({ operationId }) => operationId,
});

const ensureTaskTable = (sql) =>
	sql`
		CREATE TABLE IF NOT EXISTS workers_repro_tasks (
			task_id TEXT PRIMARY KEY,
			entity_id TEXT NOT NULL,
			execution_id TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('running', 'complete')),
			result JSONB,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`;

const ensureWorkflowCounterTable = (sql) =>
	sql`
		CREATE TABLE IF NOT EXISTS workers_repro_workflow_counter (
			entity_id TEXT PRIMARY KEY,
			value INTEGER NOT NULL,
			runner_id TEXT NOT NULL
		)
	`;

const ensureWorkflowReliabilityTables = (sql) =>
	Effect.all([
		sql`
			CREATE TABLE IF NOT EXISTS workers_repro_workflow_effects (
				operation_id TEXT PRIMARY KEY,
				entity_id TEXT NOT NULL,
				runner_id TEXT NOT NULL
			)
		`,
		sql`
			CREATE TABLE IF NOT EXISTS workers_repro_workflow_attempts (
				operation_id TEXT PRIMARY KEY,
				attempts INTEGER NOT NULL,
				last_runner_id TEXT NOT NULL,
				phase TEXT NOT NULL CHECK (phase IN ('started', 'effect-applied'))
			)
		`,
	]);

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
		shardLockDisableAdvisory: true,
	});
	const database = PgClient.layer({
		url: Redacted.make(env.CLUSTER_DB.connectionString),
		maxConnections: 4,
		minConnections: 0,
	});
	const sqlClusterStorage = Layer.mergeAll(SqlRunnerStorage.layer, BoundedMessageStorageLayer).pipe(
		Layer.provideMerge(Layer.mergeAll(shardingConfig, database, cryptoLayer)),
	);
	const runnerFetchClient = makeRunnerFetchLayer(env.COUNTER_RUNNER);
	const runnerProtocol = HttpRunner.layerClientProtocolHttp({ path: "/cluster" }).pipe(
		Layer.provideMerge(Layer.mergeAll(runnerFetchClient, RpcSerialization.layerSchemaBinary())),
	);
	const runnerHealth = RunnerHealth.layerPing.pipe(
		Layer.provide(Runners.layerRpc),
		Layer.provide(runnerProtocol),
	);
	const clusterRunner = HttpRunner.layerHttpOptions({ path: "/cluster" }).pipe(
		Layer.provide(runnerHealth),
		Layer.provideMerge(sqlClusterStorage),
		Layer.provide(shardingConfig),
		Layer.provide(RpcSerialization.layerSchemaBinary()),
		Layer.provideMerge(runnerProtocol),
	);
	const counter = Counter.toLayer({
		Increment: (request) =>
			Effect.sync(() => {
				const rows = storage.sql
					.exec(
						"INSERT INTO counter (entity_id, value) VALUES (?, 1) ON CONFLICT(entity_id) DO UPDATE SET value = value + 1 RETURNING value",
						request.payload.entityId,
					)
					.toArray();
				return { value: rows[0].value, runnerId };
			}),
	}).pipe(Layer.provideMerge(clusterRunner));
	const workflowEngine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(clusterRunner));
	const workflow = CounterWorkflow.toLayer((payload) =>
		Effect.gen(function* () {
			const incrementCounter = Activity.make({
				name: "IncrementCounter",
				success: CounterWorkflowResult,
				execute: Effect.gen(function* () {
					const sql = yield* SqlClient.SqlClient;
					yield* ensureWorkflowCounterTable(sql);
					yield* ensureWorkflowReliabilityTables(sql);
					const attempts = yield* sql`
						INSERT INTO workers_repro_workflow_attempts (
							operation_id, attempts, last_runner_id, phase
						)
						VALUES (${payload.operationId}, 1, ${runnerId}, 'started')
						ON CONFLICT (operation_id) DO UPDATE
						SET attempts = workers_repro_workflow_attempts.attempts + 1,
							last_runner_id = EXCLUDED.last_runner_id,
							phase = 'started'
						RETURNING attempts
					`;
					const result = yield* sql.withTransaction(
						Effect.gen(function* () {
							const insertedEffect = yield* sql`
								INSERT INTO workers_repro_workflow_effects (operation_id, entity_id, runner_id)
								VALUES (${payload.operationId}, ${payload.entityId}, ${runnerId})
								ON CONFLICT (operation_id) DO NOTHING
								RETURNING operation_id
							`;
							if (insertedEffect.length > 0) {
								yield* sql`
									INSERT INTO workers_repro_workflow_counter (entity_id, value, runner_id)
									VALUES (${payload.entityId}, 1, ${runnerId})
									ON CONFLICT (entity_id) DO UPDATE
									SET value = workers_repro_workflow_counter.value + 1,
										runner_id = EXCLUDED.runner_id
								`;
							}
							const rows = yield* sql`
								SELECT value, runner_id
								FROM workers_repro_workflow_counter
								WHERE entity_id = ${payload.entityId}
							`;
							yield* sql`
								UPDATE workers_repro_workflow_attempts
								SET phase = 'effect-applied'
								WHERE operation_id = ${payload.operationId}
							`;
							return {
								entityId: payload.entityId,
								value: Number(rows[0].value),
								runnerId: rows[0].runner_id,
							};
						}),
					);
					const holdAfterEffectMs = payload.holdAfterEffectMs ?? 0;
					if (Number(attempts[0].attempts) === 1 && holdAfterEffectMs > 0) {
						yield* Effect.sleep(Duration.millis(holdAfterEffectMs));
					}
					return result;
				}),
			});
			return yield* incrementCounter;
		}),
	).pipe(Layer.provideMerge(workflowEngine));
	const routes = Layer.mergeAll(
		HttpRouter.add("GET", "/health", HttpServerResponse.json({ ok: true })),
		HttpRouter.add("POST", "/internal/startup-recovery", () =>
			Effect.gen(function* () {
				const sql = yield* SqlClient.SqlClient;
				yield* ensureTaskTable(sql);
				const activeTasks = yield* sql`
					SELECT task_id, entity_id, execution_id
					FROM workers_repro_tasks
				WHERE status = 'running'
				ORDER BY updated_at ASC
				LIMIT 20
			`;
				const outcomes = yield* Effect.forEach(
					activeTasks,
					(task) =>
						Effect.gen(function* () {
							const payload = {
								operationId: task.task_id,
								entityId: task.entity_id,
								holdAfterEffectMs: 0,
							};
							const existing = yield* CounterWorkflow.poll(task.execution_id);
							const result = yield* CounterWorkflow.execute(payload);
							yield* sql`
							UPDATE workers_repro_tasks
							SET status = 'complete', result = ${JSON.stringify(result)}::jsonb, updated_at = NOW()
							WHERE task_id = ${task.task_id}
						`;
							return Option.isSome(existing) ? "existing" : "dispatched";
						}),
					{ concurrency: 4 },
				);
				return yield* HttpServerResponse.json({
					activeTaskCount: activeTasks.length,
					dispatchedCount: outcomes.filter((outcome) => outcome === "dispatched").length,
					existingCount: outcomes.filter((outcome) => outcome === "existing").length,
				});
			}),
		),
		HttpRouter.add("POST", "/task/dispatch-interrupted", (request) =>
			Effect.gen(function* () {
				if (env.REPRO_TEST_CONTROLS !== "local-only") {
					return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
				}
				const url = new URL(request.url, "http://runner.local");
				const taskId = url.searchParams.get("taskId");
				const entityId = url.searchParams.get("id");
				if (!taskId || !entityId) {
					return yield* HttpServerResponse.json(
						{ error: "taskId and id are required" },
						{ status: 400 },
					);
				}
				const payload = { operationId: taskId, entityId, holdAfterEffectMs: 30_000 };
				const executionId = yield* CounterWorkflow.executionId(payload);
				const sql = yield* SqlClient.SqlClient;
				yield* ensureTaskTable(sql);
				yield* sql`
					INSERT INTO workers_repro_tasks (task_id, entity_id, execution_id, status, result, updated_at)
					VALUES (${taskId}, ${entityId}, ${executionId}, 'running', NULL, NOW())
					ON CONFLICT (task_id) DO UPDATE
					SET entity_id = EXCLUDED.entity_id,
						execution_id = EXCLUDED.execution_id,
						status = 'running',
						result = NULL,
						updated_at = NOW()
				`;
				yield* CounterWorkflow.execute(payload, { discard: true });
				return yield* HttpServerResponse.json({ taskId, executionId, status: "running" });
			}),
		),
		HttpRouter.add("POST", "/task/prepare-recovery", (request) =>
			Effect.gen(function* () {
				const url = new URL(request.url, "http://runner.local");
				const taskId = url.searchParams.get("taskId");
				const entityId = url.searchParams.get("id");
				if (!taskId || !entityId) {
					return yield* HttpServerResponse.json(
						{ error: "taskId and id are required" },
						{ status: 400 },
					);
				}
				const sql = yield* SqlClient.SqlClient;
				yield* ensureTaskTable(sql);
				const payload = { operationId: taskId, entityId, holdAfterEffectMs: 0 };
				const executionId = yield* CounterWorkflow.executionId(payload);
				yield* sql`
					INSERT INTO workers_repro_tasks (task_id, entity_id, execution_id, status, result, updated_at)
					VALUES (${taskId}, ${entityId}, ${executionId}, 'running', NULL, NOW())
					ON CONFLICT (task_id) DO UPDATE
					SET entity_id = EXCLUDED.entity_id,
						execution_id = EXCLUDED.execution_id,
						status = 'running',
						result = NULL,
						updated_at = NOW()
				`;
				return yield* HttpServerResponse.json({ taskId, executionId, status: "running" });
			}),
		),
		HttpRouter.add("GET", "/task/status", (request) =>
			Effect.gen(function* () {
				const taskId = new URL(request.url, "http://runner.local").searchParams.get("taskId");
				if (!taskId) {
					return yield* HttpServerResponse.json({ error: "taskId is required" }, { status: 400 });
				}
				const sql = yield* SqlClient.SqlClient;
				yield* ensureTaskTable(sql);
				const rows = yield* sql`
					SELECT task_id, entity_id, execution_id, status, result
					FROM workers_repro_tasks
					WHERE task_id = ${taskId}
				`;
				return yield* HttpServerResponse.json(rows[0] ?? { task_id: taskId, status: "missing" });
			}),
		),
		HttpRouter.add("GET", "/debug/workflow-counter", (request) =>
			Effect.gen(function* () {
				const entityId = new URL(request.url, "http://runner.local").searchParams.get("id");
				if (!entityId) {
					return yield* HttpServerResponse.json({ error: "id is required" }, { status: 400 });
				}
				const sql = yield* SqlClient.SqlClient;
				yield* ensureWorkflowCounterTable(sql);
				const rows = yield* sql`
					SELECT entity_id, value, runner_id
					FROM workers_repro_workflow_counter
					WHERE entity_id = ${entityId}
				`;
				return yield* HttpServerResponse.json(
					rows[0] ?? { entity_id: entityId, value: null, runner_id: null },
				);
			}),
		),
		HttpRouter.add("GET", "/debug/workflow-attempts", (request) =>
			Effect.gen(function* () {
				const operationId = new URL(request.url, "http://runner.local").searchParams.get(
					"operationId",
				);
				if (!operationId) {
					return yield* HttpServerResponse.json(
						{ error: "operationId is required" },
						{ status: 400 },
					);
				}
				const sql = yield* SqlClient.SqlClient;
				yield* ensureWorkflowReliabilityTables(sql);
				const attempts = yield* sql`
					SELECT attempts, last_runner_id, phase
					FROM workers_repro_workflow_attempts
					WHERE operation_id = ${operationId}
				`;
				const effects = yield* sql`
					SELECT operation_id, entity_id, runner_id
					FROM workers_repro_workflow_effects
					WHERE operation_id = ${operationId}
				`;
				return yield* HttpServerResponse.json({ attempt: attempts[0] ?? null, effects });
			}),
		),
		HttpRouter.add("GET", "/workflow/poll", (request) =>
			Effect.gen(function* () {
				const executionId = new URL(request.url, "http://runner.local").searchParams.get("id");
				if (executionId === null) {
					return yield* HttpServerResponse.json({ error: "id is required" }, { status: 400 });
				}
				const result = yield* CounterWorkflow.poll(executionId);
				return yield* HttpServerResponse.json({
					executionId,
					state: Option.isSome(result) ? result.value._tag : "missing",
				});
			}),
		),
		HttpRouter.add("POST", "/increment", (request) =>
			Effect.gen(function* () {
				const entityId =
					new URL(request.url, "http://runner.local").searchParams.get("id") ?? "default";
				const makeClient = yield* Counter.client;
				return yield* HttpServerResponse.json(yield* makeClient(entityId).Increment({ entityId }));
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
		HttpRouter.add("GET", "/debug/assignments", () =>
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
				});
			}),
		),
		HttpRouter.add("GET", "/debug/registry", () =>
			Effect.gen(function* () {
				const sql = yield* SqlClient.SqlClient;
				const runners = yield* sql`
          SELECT machine_id, address, runner, healthy
          FROM cluster_runners
          ORDER BY machine_id
        `;
				const locks = yield* sql`
          SELECT shard_id, address, acquired_at
          FROM cluster_locks
          ORDER BY shard_id
        `;
				return yield* HttpServerResponse.json({ runners, locks });
			}),
		),
		HttpRouter.add("GET", "/debug/mailbox", () =>
			Effect.gen(function* () {
				const sql = yield* SqlClient.SqlClient;
				const [messageCount, pendingCount, replyCount, messageRows, replyRows] = yield* Effect.all(
					[
						sql`SELECT COUNT(*) AS count FROM cluster_messages`,
						sql`SELECT COUNT(*) AS count FROM cluster_messages WHERE processed = FALSE`,
						sql`SELECT COUNT(*) AS count FROM cluster_replies`,
						sql`SELECT id, request_id FROM cluster_messages ORDER BY id DESC LIMIT 20`,
						sql`SELECT id, request_id, kind, sequence FROM cluster_replies ORDER BY id DESC LIMIT 20`,
					],
					{ concurrency: 5 },
				);
				return yield* HttpServerResponse.json({
					messages: Number(messageCount[0]?.count ?? 0),
					pendingMessages: Number(pendingCount[0]?.count ?? 0),
					replies: Number(replyCount[0]?.count ?? 0),
					messageRows: messageRows.map((row) => ({
						id: String(row.id),
						request_id: String(row.request_id),
					})),
					replyRows: replyRows.map((row) => ({
						id: String(row.id),
						request_id: String(row.request_id),
						kind: row.kind,
						sequence: row.sequence,
					})),
				});
			}),
		),
	);

	return routes.pipe(Layer.provideMerge(counter), Layer.provideMerge(workflow));
};

const makeClientLayer = (env) => {
	const shardingConfig = ShardingConfig.layer({
		shardsPerGroup: shardCount,
		refreshAssignmentsInterval: Duration.millis(500),
		shardLockDisableAdvisory: true,
	});
	const database = PgClient.layer({
		url: Redacted.make(env.CLUSTER_DB.connectionString),
		maxConnections: 4,
		minConnections: 0,
	});
	const clusterStorage = Layer.mergeAll(SqlRunnerStorage.layer, BoundedMessageStorageLayer).pipe(
		Layer.provideMerge(Layer.mergeAll(shardingConfig, database, cryptoLayer)),
	);
	const clientFetch = makeRunnerFetchLayer(env.COUNTER_RUNNER);
	const clientCluster = HttpRunner.layerHttpClientOnly.pipe(
		Layer.provideMerge(
			Layer.mergeAll(clusterStorage, RpcSerialization.layerSchemaBinary(), clientFetch),
		),
	);
	const workflowEngine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(clientCluster));
	const routes = Layer.mergeAll(
		HttpRouter.add("POST", "/workflow/execute", (request) =>
			Effect.gen(function* () {
				const url = new URL(request.url, "http://client.local");
				const operationId = url.searchParams.get("operationId");
				const entityId = url.searchParams.get("id");
				if (!operationId || !entityId) {
					return yield* HttpServerResponse.json(
						{ error: "operationId and id are required" },
						{ status: 400 },
					);
				}
				const payload = { operationId, entityId, holdAfterEffectMs: 0 };
				const executionId = yield* CounterWorkflow.executionId(payload);
				const result = yield* CounterWorkflow.execute(payload);
				return yield* HttpServerResponse.json({ executionId, result });
			}),
		),
	);

	return routes.pipe(Layer.provideMerge(workflowEngine));
};

const clientHandlerFor = (env) => HttpRouter.toWebHandler(makeClientLayer(env));

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

export class CounterRunner extends DurableObject {
	constructor(ctx, env) {
		super(ctx, env);
		this.env = env;
		this.runnerId = undefined;
		this.handler = undefined;
		this.startup = undefined;
		this.instanceId = crypto.randomUUID();
		this.ctx.storage.sql.exec(
			"CREATE TABLE IF NOT EXISTS counter (entity_id TEXT PRIMARY KEY, value INTEGER NOT NULL)",
		);
	}

	async fetch(request) {
		const runnerId = request.headers.get("x-cluster-runner-id");
		if (!runnerIds.includes(runnerId))
			return new Response("Runner identity required", { status: 400 });
		if (this.runnerId && this.runnerId !== runnerId)
			return new Response("Runner identity mismatch", { status: 409 });
		const url = new URL(request.url);
		if (url.pathname === "/debug/abort" && request.method === "POST") {
			if (this.env.REPRO_TEST_CONTROLS !== "local-only") {
				return new Response("Not found", { status: 404 });
			}
			this.ctx.abort("Intentional local runner recovery proof");
		}
		if (!this.handler) {
			this.runnerId = runnerId;
			this.handler = HttpRouter.toWebHandler(
				makeRunnerLayer({ storage: this.ctx.storage, env: this.env, runnerId }),
				{ disableLogger: true },
			);
			this.startup = this.handler.handler(
				new Request("http://internal/internal/startup-recovery", { method: "POST" }),
			);
		}

		const startupResponse = await this.startup;
		if (!startupResponse.ok) return startupResponse;
		if (url.pathname === "/debug/instance" && this.env.REPRO_TEST_CONTROLS === "local-only") {
			return Response.json({ runnerId, instanceId: this.instanceId });
		}
		return this.handler.handler(request);
	}
}

export default {
	async fetch(request, env) {
		const url = new URL(request.url);
		if (url.pathname === "/workflow/execute" && request.method === "POST") {
			return clientHandlerFor(env).handler(request);
		}
		if (url.pathname === "/workflow/poll" && request.method === "GET") {
			const runnerId = url.searchParams.get("via") ?? runnerIds[0];
			if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
			return fetchRunner(env, runnerId, request);
		}
		if (url.pathname === "/task/prepare-recovery" && request.method === "POST") {
			return fetchRunner(env, runnerIds[0], request);
		}
		if (url.pathname === "/task/dispatch-interrupted" && request.method === "POST") {
			return fetchRunner(env, runnerIds[0], request);
		}
		if (url.pathname === "/task/status" && request.method === "GET") {
			const runnerId = url.searchParams.get("via") ?? runnerIds[0];
			if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
			return fetchRunner(env, runnerId, request);
		}
		if (url.pathname === "/debug/workflow-counter" && request.method === "GET") {
			const runnerId = url.searchParams.get("via") ?? runnerIds[0];
			if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
			return fetchRunner(env, runnerId, request);
		}
		if (url.pathname === "/debug/workflow-attempts" && request.method === "GET") {
			const runnerId = url.searchParams.get("via") ?? runnerIds[0];
			if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
			return fetchRunner(env, runnerId, request);
		}
		if (["/debug/instance", "/debug/abort"].includes(url.pathname)) {
			const runnerId = url.searchParams.get("runner") ?? runnerIds[0];
			if (!runnerIds.includes(runnerId)) return new Response("Unknown runner", { status: 400 });
			const runnerUrl = new URL(url.pathname, request.url);
			return fetchRunner(env, runnerId, new Request(runnerUrl, { method: request.method }));
		}
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
					fetchRunner(env, runnerIds[0], new Request("http://internal/debug/registry")),
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
			return fetchRunner(env, runnerIds[0], new Request("http://internal/debug/registry"));
		}
		if (url.pathname === "/debug/mailbox" && request.method === "GET") {
			return fetchRunner(env, runnerIds[0], new Request("http://internal/debug/mailbox"));
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
