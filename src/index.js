import { DurableObject } from "cloudflare:workers"
import { Effect, Layer, Option, Schema } from "effect"
import {
  Entity,
  HttpRunner,
  MessageStorage,
  RunnerAddress,
  RunnerHealth,
  RunnerStorage,
  ShardingConfig
} from "effect/unstable/cluster"
import { FetchHttpClient, HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { Rpc, RpcSerialization } from "effect/unstable/rpc"

const runnerAddress = RunnerAddress.make("counter-runner.local", 8787)

const Counter = Entity.make("Counter", [
  Rpc.make("Increment", { success: Schema.Number })
])

const makeRunnerLayer = (storage) => {
  const clusterInfrastructure = Layer.mergeAll(
    MessageStorage.layerMemory,
    RunnerStorage.layerMemory,
    RunnerHealth.layerNoop,
    ShardingConfig.layer({ runnerAddress: Option.some(runnerAddress) }),
    RpcSerialization.layerJson,
    FetchHttpClient.layer
  )
  const runnerProtocol = HttpRunner.layerClientProtocolHttp({ path: "/cluster" }).pipe(
    Layer.provideMerge(clusterInfrastructure)
  )
  const clusterRunner = HttpRunner.layerHttpOptions({ path: "/cluster" }).pipe(
    Layer.provideMerge(runnerProtocol)
  )
  const counter = Counter.toLayer({
    Increment: () => Effect.sync(() => {
      const currentValue = storage.sql
        .exec("SELECT value FROM counter WHERE id = 1")
        .one().value
      const nextValue = currentValue + 1
      storage.sql.exec("UPDATE counter SET value = ? WHERE id = 1", nextValue)
      return nextValue
    })
  }).pipe(Layer.provideMerge(clusterRunner))
  const routes = Layer.mergeAll(
    HttpRouter.add("GET", "/health", HttpServerResponse.json({ ok: true })),
    HttpRouter.add("POST", "/increment", Effect.gen(function*() {
      const makeClient = yield* Counter.client
      const client = makeClient("global")
      const value = yield* client.Increment()
      return yield* HttpServerResponse.json({ value, runner: "effect-http-runner" })
    }))
  )

  return routes.pipe(Layer.provideMerge(counter))
}

export class CounterRunner extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env)
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
    )
    this.ctx.storage.sql.exec(
      "INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)"
    )
    this.handler = HttpRouter.toWebHandler(
      makeRunnerLayer(this.ctx.storage),
      { disableLogger: true }
    )
  }

  fetch(request) {
    return this.handler.handler(request)
  }
}

export default {
  async fetch(request, env) {
    const runner = env.COUNTER_RUNNER.getByName("single-runner")
    return runner.fetch(request)
  }
}
