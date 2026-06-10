# ssr — tanstack-do-db-collection example

Server-side rendering end to end (ADR-0011): a TanStack Start app on Cloudflare
Workers reads a `todos` collection from the sync DO **without a WebSocket**,
dehydrates it into the route payload, hydrates in the browser for an instant
first paint, then goes live over the socket and converges — catch-up from the
dehydrated cursor delivers whatever changed while the HTML was in flight.
Stale-while-revalidate, never a flash of empty.

> **Experimental.** SSR support tracks TanStack DB **draft PR
> [#1564](https://github.com/TanStack/db/pull/1564)** (`DbClient`,
> `dehydrate`/`hydrate`, `collectionOptions`). This example installs the
> vendored PR builds from `../../vendor` and pins `@tanstack/db` via npm
> `overrides` so exactly **one** copy resolves — two copies break the
> Symbol-branded `collectionOptions`. The upstream hook signatures may change
> before release.

The example imports the library from source (`../../src`), so it always tracks
the current code. A published consumer would `import` from
`tanstack-do-db-collection` / `.../client` instead.

## Run

```sh
npm install
npm run dev      # vite dev with the Cloudflare plugin (runs in workerd)
```

Open the printed URL (default http://localhost:5173).

- `npm run build` — production build (client + worker)
- `npm run deploy` — build then `wrangler deploy`

## What to observe

- **View source** (not devtools — the raw response): the seeded todos are in
  the HTML, before any JavaScript runs. The dehydrated payload rides the route
  data — look for `collectionId:"todos"` and `syncMeta:{v:1,cursor:"…"}`, the
  resume point the browser catches up from.
- The status line flips `ssr → hydrated` on mount and shows `live` once the
  socket is up.
- Open a **second tab**: adds and toggles in one tab appear in the other over
  the WebSocket. Writes are optimistic — instant locally, confirmed on the
  single ordered stream.

## Shape

One worker serves everything (`src/server.ts`): WebSocket upgrades on `/sync/*`
go straight to the DO; every other request is the Start app via
`@tanstack/react-start/server-entry`.

- `src/todos-do.ts` — `TodosDO` (`todos` table + insert/update/delete
  mutations), seeded with three rows on first create.
- `src/routes/index.tsx` — the round trip. A `createServerFn` (server-only by
  construction; the browser gets the payload instead of re-running the read)
  builds a **per-request** `DbClient` + `SsrSnapshotTransport` over
  `stub.readSyncSnapshot`, preloads, and returns `db.dehydrate()`. The component
  hydrates a fresh `DbClient` from that payload and creates the *same*
  collection options around the environment's transport.
- `src/lib/todos.ts` — the one collection shape, three transports (the
  ADR-0011 D2 seam): snapshot in the loader, WebSocket in the browser, and an
  inert transport for the worker's React render pass — `hydrate()` already
  applied the rows, so that pass needs no data source. The collection id
  defaults to the table name (`todos`) everywhere; that match is what routes
  the dehydrated rows into the collection on hydrate.

Verified manually (curl shows the rows in raw HTML; a headless browser
confirmed hydration and cross-tab convergence). There is no automated e2e here
— the library's own `tests/ssr-*.test.ts` pin the contract.
