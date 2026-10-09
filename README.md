# Effect Cluster on Cloudflare Workers

Minimal proof of two Effect Cluster runners hosted by separate named Durable Objects, with a durable registry and mailbox in their own Durable Objects.

## What it proves

- `/prepare` starts both runners and waits until all 32 shards have one matching registry lock owner.
- `/increment` enters through runner 0. The proof script selects an entity owned by runner 1 and checks that the RPC handler runs there.
- Runner registration and shard locks live in the registry Durable Object's SQLite storage.
- Cluster requests and replies live in the mailbox Durable Object's SQLite storage. Snowflake IDs are stored as `TEXT` because the IDs exceed JavaScript's exact integer range.
- Counter values live in each runner's SQLite storage.

The custom mailbox driver also encodes shard IDs explicitly across the Durable Object RPC boundary. The stock SQLite adapter rounded Snowflake IDs when converting SQLite integers to JavaScript numbers, so a caller could wait for a different reply ID than the mailbox had stored.

## Local proof

```sh
pnpm install
pnpm dev:proof
```

In another terminal:

```sh
pnpm prove
```

The script checks shard ownership, runner-0-to-runner-1 RPC, the persisted counter, and a matched mailbox request/reply. Stop Wrangler with Ctrl-C, then start it again with `pnpm dev:proof` so it reuses `.wrangler/proof-state`. Run:

```sh
pnpm run prove -- --require-existing
```

This second run requires the registry, mailbox, and counter records from the prior process to exist before it sends another request.

## Deploy the demo Worker

```sh
pnpm run deploy:check
pnpm run deploy
pnpm run prove -- https://effect-cluster-workers-repro.astahmer.workers.dev
```

The deployed URL is <https://effect-cluster-workers-repro.astahmer.workers.dev>. This Worker is the standalone demo only; it does not deploy Emisoup.

## PostgreSQL-backed Worker variant

`wrangler.pg.toml` starts the same two Durable Object runners with Effect's SQL-backed `SqlRunnerStorage` and `SqlMessageStorage`. Both runners share PostgreSQL tables for registration, row-based shard leases, mailbox messages, and replies. `shardLockDisableAdvisory` is enabled because Hyperdrive does not support PostgreSQL advisory locks. The entity counter remains in each runner's Durable Object SQLite storage so this stays a small transport and recovery proof.

Cloudflare Workers can open outbound TCP connections, which lets the PG client reach a database. They do not accept inbound TCP connections, while `NodeClusterSocket` provides a TCP listener for runner RPC. The Worker variant therefore uses Effect's HTTP runner transport between Durable Objects and keeps the same SQL cluster stores.

For local runs, create a separate PostgreSQL database and owner role, then put its connection string in the ignored `.pg-repro.env` file:

```sh
CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_CLUSTER_DB=postgresql://<role>:<password>@127.0.0.1:5438/<database>?sslmode=disable
```

Start the Worker and run the shared proof script:

```sh
set -a
source .pg-repro.env
set +a
pnpm dev:pg
```

In another terminal, run `pnpm prove -- http://localhost:8790`. Stop and restart Wrangler with the same persistence path and database, then run `pnpm prove -- http://localhost:8790 --require-existing` to require the prior mailbox and counter records.

The Hyperdrive ID in `wrangler.pg.toml` is a local-only placeholder. This variant has not been deployed or tested through a real Hyperdrive pooler; local proof connects directly to PostgreSQL. Before any PG deployment, use a disposable database, replace the placeholder with its Hyperdrive configuration, and verify the installed `@effect/sql-pg` protocol against that Hyperdrive. Each active runner also holds a reserved SQL connection for row-based lease operations, so connection use must be checked at the intended runner count.

## Limits

- Restarting Wrangler against the same local persistence path checks reconstruction from persisted storage. It does not force one specific Durable Object eviction.
- `/prepare` activates both runners and checks ownership; it does not keep their in-memory Effect runtimes alive forever.
- One mailbox Durable Object is a shared throughput bottleneck in this proof.
- The 32-shard counter is a transport and recovery proof, not a production exactly-once or task-execution contract.
- Debug routes expose bounded metadata only; reply payloads are not returned.
