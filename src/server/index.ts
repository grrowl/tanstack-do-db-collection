// tanstack-do-db-collection — server entry.
//
// The Durable Object base class and registry that own the data and move the
// diffs. Imports the workerd runtime (`cloudflare:workers`); not for browsers.
//
//   - SyncDurableObject: hibernating-WebSocket base class.
//   - defineSync: the object-schema authoring API (collection/command/schema).

export { assertValidCollection, compileSchema, defineSync } from "./registry.ts"
export type {
  CollectionDef,
  CollectionEntry,
  CollectionInput,
  CommandCtx,
  CommandEntry,
  CommandInput,
  CompiledSync,
  DeleteDef,
  DeleteOp,
  InsertDef,
  InsertOp,
  MutationCtx,
  Mutations,
  RuntimeCommandDef,
  RuntimeMutationDef,
  StandardSchemaV1,
  SyncSchema,
  UpdateDef,
  UpdateOp,
} from "./registry.ts"
export { SyncDurableObject } from "./sync-do.ts"
