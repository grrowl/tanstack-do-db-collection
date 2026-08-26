// Landing page only — no DB here. The DbClient is app-scoped (router context,
// see router.tsx), so each showcase page exercises its own SSR path while
// client-side navigation between them shares one collection and one socket.

import { createFileRoute, Link } from "@tanstack/react-router"

export const Route = createFileRoute("/")({
  component: Landing,
})

function Landing() {
  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2>tanstack-durable-object-sync SSR showcase</h2>
      <p style={{ color: "#666" }}>
        One todos collection in a Durable Object, server-rendered two ways.
      </p>
      <ul>
        <li>
          <Link to="/live-query">useLiveQuery</Link> — loader preload, dehydrate,
          hydrate, converge live; rows are in the raw HTML.
        </li>
        <li>
          <Link to="/live-suspense-query">useLiveSuspenseQuery</Link> — the query
          suspends during the server render and its result is streamed.
        </li>
      </ul>
    </main>
  )
}
