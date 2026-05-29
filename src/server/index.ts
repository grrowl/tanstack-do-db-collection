// tanstack-do-db-collection — server entry.
//
// The Durable Object base class and registry that own the data and move the
// diffs. Imports the workerd runtime (`cloudflare:workers`); not for browsers.
//
// Surface lands across milestones M1–M9 (see docs/adr/0001-sync-architecture.md):
//   - Registry: defineCollection / defineMutation / defineCommand   (M1, M3, M5)
//   - SyncDurableObject: hibernating-WS base class                  (M2, M3)
//
// M0 establishes the package layout; implementation follows.

export {}
