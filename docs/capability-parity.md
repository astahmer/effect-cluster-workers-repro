# Effect Cluster Worker proof: capability parity with Emisoup

Checked 2026-10-09 against the Emisoup checkout and the local PostgreSQL proof. The tracked proof pins `effect@4.0.2` and `@effect/sql-pg@4.0.2`; Emisoup currently pins `4.0.0-beta.107` for Effect, SQL-Pg, and platform-node.

## Capability comparison

| Capability                 | Emisoup                                                                                                                                                        | Worker proof                                                                                                                                                             | Result and boundary                                                                                                                                                                                                                       |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Two runners and sharding   | `NodeClusterSocket` runner processes register in PostgreSQL and own shard leases.                                                                              | Two named Durable Objects act as runners; 32 shards settle at 16 per runner.                                                                                             | Verified locally. Requests enter runner 0 and the counter RPC executes on runner 1.                                                                                                                                                       |
| Runner health              | Emisoup uses the cluster runner health ping.                                                                                                                   | `RunnerHealth.layerPing` is wired through the HTTP RPC client.                                                                                                           | Active pings passed; no no-op health shim remains in the tracked PostgreSQL proof.                                                                                                                                                        |
| Cluster metadata           | SQL-backed runner registry and shard locks.                                                                                                                    | `SqlRunnerStorage` stores registrations and locks in PostgreSQL.                                                                                                         | `/prepare` checks all shard owners against persisted lock owners after each process start.                                                                                                                                                |
| Mailbox                    | PostgreSQL message and reply tables with Emisoup's bounded custom claim layer.                                                                                 | The same bounded claim behavior wraps `SqlMessageStorage`. Claims use batches of 64, `FOR UPDATE SKIP LOCKED`, stale-claim recovery, delayed delivery, and reply gating. | Verified on both package versions. Concurrent claim sizes vary with scheduling; the assertion checks the invariant total of 66 eligible rows, per-call bound, uniqueness, and lock exclusion instead of a fixed split.                    |
| Workflow execution         | Client-only `NodeClusterSocket` plus `ClusterWorkflowEngine`; workflow and activity state is durable in SQL.                                                   | Client-only `HttpRunner` plus `ClusterWorkflowEngine`; workflow journal is in PostgreSQL. An activity increments a PostgreSQL row.                                       | Verified for new execution, same-process replay, poll, and restart replay. Polling is routed to a runner because the client-only Worker poll route hung in the first experiment; runner-side polling matches Emisoup's recovery location. |
| Task recovery              | `TaskRecovery` resolves real task registry entries, cancels stale rows, polls stable workflow IDs, dispatches missing workflows, and retries startup recovery. | A small `workers_repro_tasks` table stages one active row. On first runner request after restart, recovery polls the saved execution ID and dispatches it if absent.     | Verified: a staged task becomes complete and its PostgreSQL activity counter is `1` after restart. This is a focused reproduction, not Emisoup's full task lifecycle.                                                                     |
| Durable runner-local state | Business facts generally live in Emisoup PostgreSQL repositories.                                                                                              | The ordinary RPC counter lives in each runner DO's SQLite. The workflow activity and minimal task ledger live in PostgreSQL.                                             | Both persistence boundaries are exercised; only the PostgreSQL counter represents application business data.                                                                                                                              |

## Differences that matter

### Transport and Worker runtime

Emisoup's `NodeClusterSocket` opens a Node TCP listener for runner RPC. Cloudflare's documented Worker socket API exposes outbound `connect()` calls only; from that documented API boundary, we infer that a Worker cannot host the inbound TCP listener `NodeClusterSocket` needs. The Worker proof therefore uses `HttpRunner.layerHttpOptions` and `HttpRunner.layerHttpClientOnly`, with a fetch shim that routes runner hostnames to named Durable Objects. This proves Effect Cluster's HTTP runner protocol, sharding, and inter-runner RPC through DO bindings. It does not prove the Node TCP transport or that a Node runner process can run inside Workers. See Cloudflare's [TCP sockets API](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/) and Effect's [Node cluster HTTP API](https://effect.website/docs/v4/api/platform-node/NodeClusterHttp/).

The two runners are named Durable Objects in one Worker script, not two independently deployed Node processes. Wrangler restart verifies local Durable Object storage and PostgreSQL state reconstruction. It does not force a particular DO eviction, exercise cross-region placement, or interrupt one runner while the other remains live.

### PostgreSQL and Hyperdrive

`wrangler.pg.toml` has a placeholder Hyperdrive ID. Local `wrangler dev` uses `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_CLUSTER_DB` to connect directly to the disposable PostgreSQL database. Cloudflare documents that this local mode bypasses Hyperdrive connection pooling and query caching; a real Hyperdrive path has not been exercised here. See [Hyperdrive local development](https://developers.cloudflare.com/hyperdrive/configuration/local-development/).

The Worker proof sets `shardLockDisableAdvisory: true` and verifies row-based shard leases. Cloudflare lists PostgreSQL advisory locks as unsupported by Hyperdrive, so the Worker-specific setting is required for that target. Emisoup's current VPS setup does not use this local Hyperdrive path. See [Hyperdrive's supported PostgreSQL features](https://developers.cloudflare.com/hyperdrive/reference/supported-databases-and-features/).

The local setup uses four PostgreSQL connections per runner/client layer. This is not a measurement of connection use through a production Hyperdrive pool or at Emisoup's full runner count.

### Application behavior not reproduced

The proof uses one synthetic counter workflow. It does not load Emisoup's real task registry, `TaskWorkflowLifecycle`, Twitch/YouTube/TikTok workflows, application repositories, provider clients, actors, crons, task reaper, or API proxy. The sample startup recovery only processes up to 20 active rows; it does not implement Emisoup's stale-task cleanup, unsupported-task handling, 200-row reconciliation page, or startup retry policy. Emisoup also has runner-storage refresh observability and application-specific task lifecycle/error handling that this proof does not include.

The activity writes one local PostgreSQL fact and is verified for idempotent replay. The proof has not killed a runner during an in-flight activity, tested external side effects, or shown exactly-once behavior across an interrupted side effect.

## Version and serialization results

| Effect packages                                          | Worker RPC encoding and health                                       | Result                                                                                                                                                                                                                                                                                  |
| -------------------------------------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `effect@4.0.2`, `@effect/sql-pg@4.0.2`                   | `RpcSerialization.layerSchemaBinary()` with `RunnerHealth.layerPing` | Full proof, restart recovery, mailbox claims, and Wrangler dry-run passed.                                                                                                                                                                                                              |
| `effect@4.0.0-beta.107`, `@effect/sql-pg@4.0.0-beta.107` | `RpcSerialization.layerJson` with `RunnerHealth.layerPing`           | A temporary compatibility copy passed the same workflow replay, restart recovery, mailbox, and dry-run checks. It is not a second tracked package target.                                                                                                                               |
| `4.0.0-beta.107` Worker port                             | MessagePack with active ping                                         | An earlier controlled Worker trial passed shard/RPC checks but the workflow dispatch returned Workerd's hung-request error. This does not show a failure in Emisoup's Node TCP setup; it means the default beta serialization combination remains unverified in this Worker adaptation. |

The tracked release proof uses SchemaBinary, which Effect documents as the binary RPC encoding. Its API is `RpcSerialization.layerSchemaBinary(options?)`, so the layer must be called. The beta package uses the `effect/unstable/...` import paths; the release package uses `effect/...`. See [Effect RPC serialization](https://effect.website/docs/v4/api/effect/rpc/RpcSerialization/).

## Local evidence and repeatable command sequence

The tracked setup uses `effect@4.0.2`, `@effect/sql-pg@4.0.2`, Wrangler `4.148.0`, and the ignored `.pg-repro.env` connection string for the dedicated local database `effect_cluster_workers_repro_20261009`.

1. Start `pnpm dev:pg` with the environment in `.pg-repro.env` loaded.
2. Run `pnpm prove -- http://localhost:8790`. It checks 32 shards, 16/16 ownership, runner-0 ingress to runner-1 execution, mailbox request/reply, a replayed workflow result, a completed poll, and the PostgreSQL activity counter.
3. Stop Wrangler and start it again with the same local persistence path and PostgreSQL database.
4. Run `pnpm prove -- http://localhost:8790 --require-existing`. It requires previous mailbox and DO counter rows, verifies the same workflow result, and checks that the staged task was recovered after runner initialization.
5. Stop Wrangler and run `pnpm prove:mailbox`. This exercises concurrent claims, stale claim release, delayed delivery, and reply cleanup.
6. Run `pnpm exec wrangler deploy --dry-run --config wrangler.pg.toml`. This checks bundling and binding configuration only; no deployment was performed.

Stable-package results from the final run:

- Initial proof: `runner0Owns=16`, `runner1Owns=16`, `executionRunner=runner-1`, workflow result `1`, workflow replay `1`, and no pre-existing recovery task.
- After restart: the DO counter advanced from `25` to `26`; the workflow result and PostgreSQL activity counter stayed at `1`; the task status became `complete` with result value `1`.
- Mailbox proof: 66 unique eligible concurrent claims, batch maximum 64, 64 claims recovered after the lock was released, one stale claim recovered, one delayed message delivered after its due time, and two messages released after reply cleanup.
- Dry-run: passed. It does not validate real Hyperdrive credentials, pooling, network access, or deployed DO recovery.

## Next minimal proofs

1. Test the release Worker bundle against a disposable real Hyperdrive resource and PostgreSQL database, including the intended connection limits. This requires a deployment-backed test and was not part of this local-only pass.
2. Stop one runner while a workflow activity is in flight, bring it back, and verify the workflow resumes without duplicating its PostgreSQL side effect.
3. Force a single runner DO to reconstruct while the other remains active; verify lock expiry, reassignment, and RPC recovery without restarting the whole Wrangler process.
4. If Worker hosting remains a target, port one actual Emisoup workflow and its task resolution contract into the Worker bundle. Keep Node TCP and Worker HTTP transport results separate.
