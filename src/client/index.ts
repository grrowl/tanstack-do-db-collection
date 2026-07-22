// tanstack-durable-object-sync/client — browser entry.
//
// The TanStack DB collection-options creator and WebSocket transport. Depends
// on `@tanstack/db` (a peer): types throughout, plus its predicate compiler at
// runtime for the where-filter preflight.
//
//   - WebSocketTransport: one per DO; single appliedSeq cursor; awaitSeq.
//   - doCollectionOptions: build a TanStack DB CollectionConfig, optionally
//     server-filtered by a `where` predicate.

export {
  defaultReconnectDelay,
  MutationRejectedError,
  WebSocketTransport,
} from "./transport.ts"
export type { ReconnectDelayFn, SubHandler, TransportOptions, WebSocketLike } from "./transport.ts"
export { doCollectionOptions, WriteOutsideSubError } from "./do-collection.ts"
export type { CollectionName, DoApiCollectionOptions, RowOf } from "./do-collection.ts"
