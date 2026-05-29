# chat — tanstack-do-db-collection example

A minimal multi-client chat: a Cloudflare Worker + `SessionDO` (the sync DO) and
a React client using `useLiveQuery` over a DO-backed collection. Optimistic
sends, live cross-tab sync, reconnect — the whole stack end to end.

The example imports the library from source (`../../src`), so it always tracks
the current code. A published consumer would `import` from
`tanstack-do-db-collection` / `.../client` instead.

## Run

```sh
npm install
npm run dev      # builds the client bundle, then `wrangler dev`
```

Open the printed URL (default http://localhost:8787), then open a **second tab**
and watch messages sync live between them. Each tab gets a throwaway identity.

- `npm run build:client` — bundle the React client to `public/client.js` (esbuild)
- `npm run watch:client` — rebuild on change (run alongside `wrangler dev`)

## Shape

- `src/worker.ts` — `SessionDO` (one `messages` collection + an insert mutation
  authorized to the connected user) and the upgrade router (`/sync` → DO,
  everything else → static assets).
- `src/client.tsx` — one `WebSocketTransport` + a `messages` collection via
  `doCollectionOptions`, rendered by `useLiveQuery`.
