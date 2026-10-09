# Effect Cluster on Cloudflare Workers: minimal runner proof

A Cloudflare Worker forwards requests to one named Durable Object. That Durable Object hosts one Effect Cluster runner using Effect's HTTP runner protocol and serves a `Counter` entity from its fetch handler. The entity increments a value in Durable Object SQLite storage.

```sh
pnpm install
pnpm dev
curl -X POST http://localhost:8787/increment
curl http://localhost:8787/health
```

`/increment` should return `1`, then `2`. The same counter value survives a Wrangler restart because it is stored in Durable Object SQLite. The Effect entity uses in-process sharding and actor dispatch, while `/cluster` is the HTTP runner route exposed by `HttpRunner.layerHttpOptions`.

Deploy with `pnpm run deploy`. The Durable Object class is introduced by the `v1` migration in `wrangler.toml`.

The deployed proof is at <https://effect-cluster-workers-repro.astahmer.workers.dev>. Its `/increment` endpoint changes only this demo Durable Object's counter.

This is a deliberately small, single-runner proof. Runner and mailbox metadata use Effect's in-memory storage adapters; only the counter value survives Durable Object eviction. It demonstrates a real Effect Cluster entity running inside a deployed Durable Object, but does not prove cross-runner sharding, inter-DO HTTP RPC, durable mailbox recovery after eviction, or production task execution. A multi-runner version still needs durable shared `RunnerStorage` and `MessageStorage` plus an address-to-Durable-Object transport adapter.
