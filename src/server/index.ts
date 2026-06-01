// tanstack-do-db-collection — server entry.
//
// The Durable Object base class and registry that own the data and move the
// diffs. Imports the workerd runtime (`cloudflare:workers`); not for browsers.
//
//   - SyncDurableObject: hibernating-WebSocket base class.
//   - SyncRegistry: defineCollection / defineMutation / defineCommand.

export { SyncRegistry } from "./registry.ts"
export type {
  CollectionDef,
  CommandCtx,
  CommandDef,
  MutationCtx,
  OpFor,
  MutationDef,
} from "./registry.ts"
export { SyncDurableObject } from "./sync-do.ts"
