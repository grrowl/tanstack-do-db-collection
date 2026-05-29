// tanstack-do-db-collection/client — browser entry.
//
// The TanStack DB collection-options creator and WebSocket transport. Imports
// `@tanstack/db`; not for the workerd runtime.
//
// Surface lands across milestones M3–M9 (see docs/adr/):
//   - WebSocketTransport: one per DO; single appliedSeq cursor; awaitSeq   (M3)
//   - doCollectionOptions: build a TanStack DB CollectionConfig            (M3)

export {
  MutationRejectedError,
  WebSocketTransport,
} from "./transport.ts"
export type { SubHandler, TransportOptions, WebSocketLike } from "./transport.ts"
