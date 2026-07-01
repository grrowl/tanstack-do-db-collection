// Landing page only — no DB here. The DbClient lives in the `_db` pathless
// layout so each showcase page exercises the SSR round trip on its own URL,
// while client-side navigation between them shares one socket.

import { createFileRoute, Link } from "@tanstack/react-router"

export const Route = createFileRoute("/")({
  component: Landing,
})

function Landing() {
  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2>tanstack-do-db SSR showcase</h2>
      <p style={{ color: "#666" }}>
        One todos collection in a Durable Object, server-rendered two ways. View
        source on either page: the rows are in the raw HTML.
      </p>
      <ul>
        <li>
          <Link to="/live-query">useLiveQuery</Link> — hydrate, paint, converge
          live; explicit <code>isReady</code> state.
        </li>
        <li>
          <Link to="/live-suspense-query">useLiveSuspenseQuery</Link> — same
          data via Suspense; hydrated state does not suspend on first paint.
        </li>
      </ul>
    </main>
  )
}
