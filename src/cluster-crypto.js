import { Crypto, Effect, Layer } from "effect";

export const cryptoLayer = Layer.succeed(Crypto.Crypto)(
	Crypto.make({
		randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
		digest: (algorithm, data) =>
			Effect.promise(
				async () => new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, data)),
			),
	}),
);
