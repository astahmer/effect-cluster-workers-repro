# Effect Cluster on Cloudflare Workers

This repository has a small Durable Objects demo and a PostgreSQL-backed Effect Cluster proof. The PostgreSQL proof is intended to compare cluster behavior with Emisoup; it is not a deployment of Emisoup.

## PostgreSQL proof

The proof starts two named Durable Objects as runners, assigns 32 shards, and checks runner registration, health pings, shard leases, runner-to-runner RPC, PostgreSQL mailbox claims, workflow execution, and task recovery after a restart. Its recovery proof also interrupts one runner after an Activity commits a PostgreSQL effect but before the Activity returns. The surviving runner takes over the expired shards, retries the Activity, and keeps the effect count at one through a stable operation key. The RPC counter remains in Durable Object SQLite so both storage boundaries are visible.

For the Emisoup comparison, version results, exact differences, and remaining proof work, see [the capability parity notes](docs/capability-parity.md).

### Run it locally

Install the pinned dependencies:

```sh
pnpm install
```

Use a disposable local PostgreSQL database and owner role. Put its connection string in the ignored `.pg-repro.env` file:

```sh
CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_CLUSTER_DB=postgresql://<role>:<password>@127.0.0.1:5438/effect_cluster_workers_repro_20261009?sslmode=disable
```

Start Wrangler with the same local persistence directory each time:

```sh
set -a
source .pg-repro.env
set +a
pnpm dev:pg
```

The runner-interruption proof uses the ignored `.dev.vars` file to enable local-only reset controls:

```sh
REPRO_TEST_CONTROLS=local-only
```

In another terminal, run the first proof:

```sh
pnpm prove -- http://localhost:8790
```

Stop Wrangler, start it again with the same database and `.wrangler/pg-proof-state`, then run:

```sh
pnpm prove -- http://localhost:8790 --require-existing
```

The second run requires existing mailbox and Durable Object counter rows, checks the completed workflow result and PostgreSQL activity counter, and verifies that runner startup recovered the staged active task. The proof script also repeats a workflow request in one Worker process to check idempotent replay.

While Wrangler is running, interrupt one runner during an in-flight workflow Activity:

```sh
pnpm prove:runner-recovery -- http://localhost:8790
```

This takes about 35 seconds because the remaining runner must observe the 30-second shard lease expiry. Wrangler logs one expected `ctx.abort()` error. The script verifies that only the selected Durable Object is reconstructed, all 32 shards fail over, the committed PostgreSQL effect is not repeated, and the task ledger completes when the runner returns.

Stop Wrangler before running the independent mailbox claim exercise:

```sh
pnpm prove:mailbox
```

The Worker bundle can be checked without deployment:

```sh
pnpm exec wrangler deploy --dry-run --config wrangler.pg.toml
```

`wrangler.toml` is the separate SQLite-only demo. The PostgreSQL proof uses `wrangler.pg.toml`; its Hyperdrive ID is a placeholder. This work did not deploy either Worker or create a real Hyperdrive resource.
