import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"

// WHY: registration is the author's responsibility (ADR-0007). If a DO never
// calls registerSync, sync must fail LOUD — accessing the registry throws with a
// pointed message — rather than silently serving nothing.

describe("registerSync (ADR-0007) — fail loud when unregistered", () => {
  it("throws on registry access if registerSync was never called", async () => {
    const stub = env.UNREG_DO.get(env.UNREG_DO.idFromName("nope"))
    await runInDurableObject(stub, (instance) => {
      expect(() => (instance as unknown as { registry: unknown }).registry).toThrow(/registerSync/)
    })
  })
})
