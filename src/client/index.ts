// tanstack-do-db-collection/client — browser entry.
//
// The TanStack DB collection-options creator and WebSocket transport. Depends
// on `@tanstack/db` (a peer): types throughout, plus its predicate compiler at
// runtime for the where-filter preflight.
//
//   - WebSocketTransport: one per DO; single appliedSeq cursor; awaitSeq.
//   - doCollectionOptions: build a TanStack DB CollectionConfig, optionally
//     server-filtered by a `where` predicate.

export {
  MutationRejectedError,
  WebSocketTransport,
} from "./transport.ts"
export type { SubHandler, Transport, TransportOptions, WebSocketLike } from "./transport.ts"
export { doCollectionOptions, WriteOutsideSubError } from "./do-collection.ts"
export type { DoCollectionOptions, DoSyncMeta } from "./do-collection.ts"
// SSR (experimental — tracks TanStack DB draft PR #1564; ADR-0011). Create one
// SsrSnapshotTransport PER REQUEST and pass `(req) => stub.readSnapshot(req)`.
export { SsrReadOnlyError, SsrSnapshotTransport } from "./ssr-transport.ts"
export type { SnapshotRead } from "./ssr-transport.ts"
