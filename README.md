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

## Limits

- Restarting Wrangler against the same local persistence path checks reconstruction from persisted storage. It does not force one specific Durable Object eviction.
- `/prepare` activates both runners and checks ownership; it does not keep their in-memory Effect runtimes alive forever.
- One mailbox Durable Object is a shared throughput bottleneck in this proof.
- The 32-shard counter is a transport and recovery proof, not a production exactly-once or task-execution contract.
- Debug routes expose bounded metadata only; reply payloads are not returned.
