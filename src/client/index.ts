// tanstack-do-db-collection/client — browser entry.
//
// The TanStack DB collection-options creator and WebSocket transport. Imports
// `@tanstack/db` for types only; no @tanstack/db runtime dependency here.
//
// Surface lands across milestones M3–M9 (see docs/adr/):
//   - WebSocketTransport: one per DO; single appliedSeq cursor; awaitSeq   (M3)
//   - doCollectionOptions: build a TanStack DB CollectionConfig            (M3)

export {
  MutationRejectedError,
  WebSocketTransport,
} from "./transport.ts"
export type { SubHandler, TransportOptions, WebSocketLike } from "./transport.ts"
export { doCollectionOptions } from "./do-collection.ts"
export type { DoCollectionOptions } from "./do-collection.ts"
