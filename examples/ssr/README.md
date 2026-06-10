# ssr — tanstack-do-db-collection example

Server-side rendering end to end (ADR-0011): a TanStack Start app on Cloudflare
Workers reads a `todos` collection from the sync DO **without a WebSocket**,
dehydrates it into the route payload, hydrates in the browser for an instant
first paint, then goes live over the socket and converges — catch-up from the
dehydrated cursor delivers whatever changed while the HTML was in flight.
Stale-while-revalidate, never a flash of empty.

> **Experimental.** SSR support tracks TanStack DB **draft PR
> [#1564](https://github.com/TanStack/db/pull/1564)** (`DbClient`,
> `dehydrate`/`hydrate`, `collectionOptions`, `useLiveSuspenseQuery`). This
> example installs the vendored PR builds from `../../vendor` and pins
> `@tanstack/db` via npm `overrides` so exactly **one** copy resolves — two
> copies break the Symbol-branded `collectionOptions`. The upstream hook
> signatures may change before release.

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

## Pages

`/` is a plain landing page; the two showcase pages live under a shared layout:

### `/live-query` — `useLiveQuery`

The baseline SSR experience. Data is present from the first (server) render;
the status line flips `ssr → hydrated` on mount and `catching up → live` once
the socket converges. Adds and toggles are optimistic — instant locally,
confirmed on the single ordered stream. Open a second tab to watch them sync.

### `/live-suspense-query` — `useLiveSuspenseQuery`

The same collection consumed through React Suspense. What it demonstrates:

- **Hydrated state does not suspend.** `db.hydrate()` applies the dehydrated
  rows as a committed synced transaction — but upstream hydration does NOT
  mark the collection ready; readiness is always the sync adapter's call.
  It's this library's hydrated path that calls `markReady()` *synchronously*
  at sync start, because the rows are already present — the explicit
  stale-while-revalidate contract (ADR-0011 D3). So the first paint never
  throws to the boundary, on the server or in the browser. View source: the
  raw HTML contains the todo rows, **not** the fallback, and the
  `fallback-count` on the page stays at 0 through hydration.
- **Query identity changes create a new derived collection.** The
  "show only open" toggle changes the `where` clause; the structured query IR
  is the identity, so flipping it swaps in a new live query (the re-suspension
  path in the hook). In practice the fallback still never commits here: the
  source collection is ready in memory, so the new derived query computes
  synchronously and `useLiveSuspenseQuery` never reaches the throw. With this
  library the fallback would only ever show for a source that isn't hydrated or
  synced yet — which this app, by construction, never has.

## Shape

One worker serves everything (`src/server.ts`): WebSocket upgrades on `/sync/*`
go straight to the DO; every other request is the Start app via
`@tanstack/react-start/server-entry`.

- `src/todos-do.ts` — `TodosDO` (`todos` table + insert/update/delete
  mutations), seeded with three rows on first create.
- `src/routes/_db.tsx` — the round trip, lifted to a **pathless layout** shared
  by both pages. Its loader calls one `createServerFn` (server-only by
  construction; the browser gets the payload instead of re-running the read)
  that builds a **per-request** `DbClient` + `SsrSnapshotTransport` over
  `stub.readSyncSnapshot`, preloads, and returns `db.dehydrate()`. The layout
  component hydrates a fresh `DbClient` from that payload — **once per tab** —
  and provides the collection to the pages. Lifting it means one loader, one
  DbClient, one socket; per-page DbClients would open a fresh WebSocket on
  every client-side navigation between the pages and never close the old one.
- `src/routes/_db.live-query.tsx`, `src/routes/_db.live-suspense-query.tsx` —
  the two consumers, reading the shared collection via `useTodos()`.
- `src/lib/todos.ts` — the one collection shape, three transports (the
  ADR-0011 D2 seam): snapshot in the loader, WebSocket in the browser, and an
  inert transport for the worker's React render pass — `hydrate()` already
  applied the rows, so that pass needs no data source. The collection id
  defaults to the table name (`todos`) everywhere; that match is what routes
  the dehydrated rows into the collection on hydrate.
- `src/lib/todos-context.ts` — the React context for the collection handle.
  Deliberately **not** exported from the `_db` route file: Start code-splits
  route files, so a context exported from one is evaluated twice (split
  component module vs. direct import) and the provider and consumers end up
  holding two different contexts.

Verified manually (curl shows the rows — and no Suspense fallback — in both
pages' raw HTML; a headless browser confirmed hydration, the filter toggle,
and zero-fallback first paint). There is no automated e2e here — the library's
own `tests/ssr-*.test.ts` pin the contract.
