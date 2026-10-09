# Effect Cluster on Cloudflare Workers: minimal runner proof

A single Cloudflare Worker routes requests to one named Durable Object. The Durable Object hosts an Effect Cluster `Entity` and increments a value stored in its SQLite storage.

```sh
pnpm install
pnpm dev
curl -X POST http://localhost:8787/increment
curl -X POST http://localhost:8787/increment
```

The response should advance from `1` to `2`. This exercises Worker-to-DO RPC, Effect Cluster entity sharding/client dispatch, and durable state across requests.

Deploy with `pnpm deploy`. The Durable Object class is introduced by the `v1` migration in `wrangler.toml`.

This is deliberately a single-runner example. It uses Effect's `TestRunner.layer` for in-process sharding and message/runner storage. It does not prove cross-runner communication over `HttpRunner`, coordination among multiple Durable Objects, or recovery of Effect Cluster's internal mailbox state after eviction. Only the counter value is persisted in the Durable Object's SQLite storage.
