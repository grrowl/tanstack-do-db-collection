// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects. Tests drive storage directly via
// `runInDurableObject`, so the DO stays bare; per-test schema setup lives in
// the tests. Gains a real registry + WebSocket lifecycle as milestones land.

import { DurableObject } from "cloudflare:workers"

export class TestDO extends DurableObject {}

export default {
  async fetch(): Promise<Response> {
    return new Response("test-worker")
  },
}
