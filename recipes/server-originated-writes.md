# Write to a collection from the server

Most writes come from a client mutation. Sometimes the server itself needs to
write a row, e.g. a webhook updates a record, a scheduled job inserts data, or
you seed a table. Wrap those writes in `this.runSyncedWrite` so connected clients
see them.

## Recipe

```ts
export class RoomDO extends SyncDurableObject<Env, Claims> {
  override async fetch(req: Request): Promise<Response> {
    if (new URL(req.url).pathname === "/seed") {
      // Runs in a transaction and broadcasts the change to connected clients.
      this.runSyncedWrite((sql) => {
        sql.exec(
          "INSERT OR IGNORE INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
          ulid(), "system", "welcome", Date.now(),
        )
      })
      return new Response("seeded")
    }
    return super.fetch(req)
  }
}
```

## Why runSyncedWrite and not a plain sql.exec

A plain `sql.exec` writes the row and records the change, but the change is sent
to clients only on the next mutation or command. `runSyncedWrite` sends the
change now, so connected clients update right away (ADR-0006).

## Run work after a mutation commits

When you want work to run after a mutation has committed, e.g. delete a file in
R2 or add a job to a queue, put it in the mutation's `afterCommit`. It runs after
the client has its confirmation, it can be async, and the framework keeps the
Durable Object awake until it finishes.

```ts
delete: {
  execute: ({ op, sql }) => sql.exec("DELETE FROM files WHERE id = ?", op.key),
  afterCommit: async ({ op, env }) => { await env.BUCKET.delete(op.key) },
},
```

A thrown error in `afterCommit` is logged and dropped, and there is no retry, so
make the work idempotent. Write it so that a later run can finish whatever an
interrupted run left behind.

## Notes

- The function you pass to `runSyncedWrite` must be synchronous, because it runs
  in a transaction. Do any async work before the call. If the function returns a
  promise, the framework rejects it and rolls the write back.
- Do not poll on a timer while the Durable Object is idle, because that keeps it
  awake and stops it from hibernating. Start a server write from a real event,
  e.g. a webhook, an alarm, or a mutation.
- A server write has no client to confirm to, so there is no transaction id, no
  receipt, and no duplicate check. Make writes idempotent, e.g. `INSERT OR
  IGNORE` on the client-supplied key.

## See also

- ADR-0006 explains why a server write goes through `runSyncedWrite`.
- ADR-0004 explains `afterCommit`.
- `examples/board` seeds and bumps rows with `runSyncedWrite`.
