# Examples

## [chat](./chat)

A minimal multi-client chat — Worker + `SessionDO` + a React `useLiveQuery`
client. Showcases the whole stack end to end: optimistic **mutations**, live
cross-tab sync, reconnect, and a **command** (`clearRoom`) for the one action
that isn't a typed row write.

## [on-demand](./on-demand)

Categorised items where each category panel loads only when opened. Showcases
`syncMode: 'on-demand'` — the collection syncs only the subsets your live
queries request, via `loadSubset` / `unloadSubset` as panels mount and unmount.
Categories you never open are never synced.

## [board](./board)

A high-volume "task board": 5,000 tasks on **one** Durable Object. Showcases
windowed pagination at scale — a bounded window (top 50) with cursor `fetch` for
scroll-back; a **mutable** order key, so a bump arrives as **move-in /
move-out**; and **server-originated writes** (`runSyncedWrite`) via `/seed` and
`/bump`. It also surfaces the deferred bounded-window-under-churn limitation as a
live number.

## [multi-do](./multi-do)

Two **separate** Durable Objects (a room and an inbox) behind one Worker.
Showcases the multi-DO story: **one transport per DO**, a React
`SyncProvider` / `useSync` keyed by DO so each DO's typed `transport.call`
namespace stays disjoint (no command-name collisions), and a **cross-DO feed**
merged client-side (the DO never joins — ADR-0001).
