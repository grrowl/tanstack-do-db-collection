// tanstack-do-db-collection — server entry.
//
// The Durable Object base class and registry that own the data and move the
// diffs. Imports the workerd runtime (`cloudflare:workers`); not for browsers.
//
// Surface lands across milestones M1–M9 (see docs/adr/0001-sync-architecture.md
// and docs/adr/0002-adversarial-review-corrections.md):
//   - Registry: defineCollection (M1) / defineMutation / defineCommand (M3, M5)
//   - SyncDurableObject: hibernating-WS base class                  (M2, M3)

export { Registry } from "./registry.ts"
export type { CollectionDef } from "./registry.ts"
