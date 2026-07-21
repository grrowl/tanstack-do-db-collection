// tanstack-durable-object-sync — server entry.
//
// The Durable Object base class and registry that own the data and move the
// diffs. Imports the workerd runtime (`cloudflare:workers`); not for browsers.
//
//   - SyncDurableObject: hibernating-WebSocket base class.
//   - Syncable: the mixin factory; apply the sync machinery over any DO base.
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
export { Syncable, SYNC_TAG } from "./mixin.ts"
export type { DOCtor, SyncApi, SyncableOptions, SyncMixin } from "./mixin.ts"
