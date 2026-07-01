# Share types between the server and the client

The schema you define on the server is also the client's contract. Export its
type, import it on the client, and the transport, the commands, and the
collections are all typed from it. No server code ships to the browser. Only the
type is used.

## Recipe

```ts
// server, e.g. src/schema.ts
const sync = defineSync<Claims, Env>()
export const schema = sync.schema({
  collections: { messages: sync.collection<Message>({ pk: "id", mutations: { /* ... */ } }) },
  commands: { clearRoom: sync.command()(({ sql }) => ({ deleted: clearAll(sql) })) },
})
export type Api = typeof schema
```

```ts
// client
import type { Api } from "../server/schema" // type only; no server code is bundled

const transport = new WebSocketTransport<Api>({ url })

// the command name, its args, and its result are all checked against the schema
const { deleted } = await transport.call.clearRoom()

// the row type is inferred from the schema and the table name
const messages = createCollection(
  doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
)
```

## How it works

The schema value carries the row type of each collection, and the arguments and
result of each command, in its type. `typeof schema` recovers them on the client.
The import is a type-only import, so the bundler removes it and no server code
reaches the browser.

## Two ways to type a collection's row

Pick whichever you prefer.

- Pass the row type as a generic: `sync.collection<Message>({ pk: "id", mutations })`.
- Infer it from a schema on the insert mutation: `sync.collection({ pk: "id",
  mutations: { insert: { schema: Message, execute } } })`. On this branch the
  schema only infers the type. A runtime check against the schema is a separate
  follow-up.

## Notes

- Keep the schema in a module that both sides import. The Durable Object imports
  the value to register it, and the client imports the type. Importing the type
  from the worker file works in these examples, but a published app should keep
  the schema in its own module so the browser bundle never pulls in server code.
- A schema with no commands has no callable commands, so `transport.call.anything`
  is a type error, not a silent call that does nothing.

## See also

- ADR-0014 describes the schema as the shared contract.
- `examples/chat` types its transport and collection from `ChatApi`.
