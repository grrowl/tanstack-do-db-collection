# When to use a mutation and when to use a command

A mutation is a typed write to one collection. It is an insert, an update, or a
delete. A command is a named call for anything that is not one of those three
writes. Use a mutation for ordinary row writes, and use a command for everything
else.

## Mutations

On the client you write through the collection, and the change shows right away,
before the server confirms it. This is an optimistic update, and the framework
rolls it back if the server rejects it.

```ts
messages.insert({ id: ulid(), author: me, content, created_at: Date.now() })
messages.update(id, (m) => { m.content = "edited" })
messages.delete(id)
```

On the server you define the matching handlers. A collection has at most an
insert, an update, and a delete. Each `execute` runs inside a transaction, so it
must be synchronous.

```ts
messages: sync.collection<Message>({
  pk: "id",
  mutations: {
    insert: {
      authorize: ({ user, op }) => {
        if (op.cols.author !== user.userId) throw new Error("not your message")
      },
      execute: ({ op, sql }) => sql.exec("INSERT INTO messages(...) VALUES (...)", op.cols.id /* ... */),
    },
    update: { execute: ({ op, sql }) => {/* op.cols is a partial patch */} },
    delete: { execute: ({ op, sql }) => sql.exec("DELETE FROM messages WHERE id = ?", op.key) },
  },
}),
```

## Commands

A command has any name you choose. You call it on the transport and await its
result.

```ts
// client
const { deleted } = await transport.call.clearRoom()
```

```ts
// server
commands: {
  clearRoom: sync.command()(({ sql }) => {
    const before = count(sql)
    sql.exec("DELETE FROM messages")
    return { deleted: before }
  }),
},
```

A command's `execute` runs outside a transaction, so it can be async. It can do
work a mutation cannot, such as calling another service, and it returns a value
to the caller. A command can also write rows, and those writes broadcast to
other clients as ordinary changes, the same as a mutation's writes.

## How they differ

- A mutation is an insert, update, or delete on a collection. A command has any
  name.
- A mutation is optimistic, so the change shows on the client at once and rolls
  back if the server rejects it. A command is not optimistic, so you await its
  result.
- A mutation's `execute` is synchronous and runs in a transaction. A command's
  `execute` can be async and runs outside a transaction.
- A mutation returns nothing to the caller. A command returns a result.
- Both can write rows that broadcast to other clients.

## When a command is the right choice

Use a command when the work is not a single typed row write, e.g. deleting many
rows at once, returning a computed value, or calling an external service. "Clear
the room" in the chat example is a command, because it deletes every message and
returns the count.

## Notes

- Denial is uniform across mutations and commands: an `authorize` throw, or a
  schema validation failure, surfaces its reason to the client (validation
  failures carry a `VALIDATION` code), so you can tell the user why. Only an
  `execute` error is sanitized to a generic message ("mutation failed" /
  "command failed") — by then the call is authorized, and a failure there may
  carry internal detail.
- You can type a command's args, either with a generic
  (`sync.command<{ before: number }>()(fn)`) or from a schema
  (`sync.command(zArgs, fn)`).
- A mutation's `execute` must be synchronous, because it runs inside the
  transaction that commits the row and its change-log entry together. Writing to
  the Durable Object's own SQLite is synchronous, so this is the normal case. Do
  any async work in `authorize` (it runs before the transaction), in
  `afterCommit` (it runs after the commit), or in a command (it runs outside a
  transaction).

## See also

- ADR-0014 describes the schema and why the mutation set is fixed.
- `examples/chat` has an insert mutation and the `clearRoom` command.
- `examples/multi-do` calls a command on each of two Durable Objects.
