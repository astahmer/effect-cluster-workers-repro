import { DurableObject } from "cloudflare:workers"
import { Effect, Layer, Schema } from "effect"
import { Entity, TestRunner } from "effect/unstable/cluster"
import { Rpc } from "effect/unstable/rpc"

const Counter = Entity.make("Counter", [
  Rpc.make("Increment", { success: Schema.Number })
])

const makeCounterLayer = (storage) => Counter.toLayer({
  Increment: () => Effect.sync(() => {
    const currentValue = storage.sql
      .exec("SELECT value FROM counter WHERE id = 1")
      .one().value
    const nextValue = currentValue + 1
    storage.sql.exec("UPDATE counter SET value = ? WHERE id = 1", nextValue)
    return nextValue
  })
}).pipe(Layer.provideMerge(TestRunner.layer))

const increment = (storage) => Effect.gen(function*() {
  const makeClient = yield* Counter.client
  const client = makeClient("global")
  return yield* client.Increment()
}).pipe(Effect.provide(makeCounterLayer(storage)))

export class CounterRunner extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env)
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
    )
    this.ctx.storage.sql.exec(
      "INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)"
    )
  }

  increment() {
    return Effect.runPromise(increment(this.ctx.storage))
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url)

    if (url.pathname === "/health") {
      return Response.json({ ok: true })
    }

    if (url.pathname === "/increment" && request.method === "POST") {
      const runner = env.COUNTER_RUNNER.getByName("single-runner")
      const value = await runner.increment()
      return Response.json({ value, runner: "durable-object" })
    }

    return new Response("Not found", { status: 404 })
  }
}
