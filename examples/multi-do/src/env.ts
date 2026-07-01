// Shared binding env + identity for both DOs.
//
// The schema modules (`room-schema.ts`, `inbox-schema.ts`) bind these into
// `defineSync<Claims, Env>()`, and `worker.ts` types its DO subclasses + handler
// against the same `Env`. Kept in its own module so the schemas never have to
// import the worker (which would be a cycle: the worker imports the schemas).

export interface Claims {
  /** The connected user. The example trusts a `?user=` query param; a real app
   *  verifies a token at the Worker and forges a claims header (see README). */
  userId: string
}

export interface Env {
  ROOM_DO: DurableObjectNamespace
  INBOX_DO: DurableObjectNamespace
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}
