// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects. Grows as milestones add behaviour to exercise.

import { DurableObject } from "cloudflare:workers"

/** Minimal DO; gains a sync-do registry + collections as milestones land. */
export class TestDO extends DurableObject {}

export default {
  async fetch(): Promise<Response> {
    return new Response("test-worker")
  },
}
