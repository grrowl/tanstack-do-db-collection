import { createRootRouteWithContext, HeadContent, Link, Outlet, Scripts } from "@tanstack/react-router"
import * as React from "react"
import type { RouterContext } from "../router.tsx"

export const Route = createRootRouteWithContext<RouterContext>()({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "tanstack-durable-object-sync SSR todos" },
    ],
  }),
  shellComponent: RootDocument,
  component: RootLayout,
})

function RootDocument({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function RootLayout() {
  return (
    <>
      <nav
        style={{
          fontFamily: "system-ui, sans-serif",
          maxWidth: 640,
          margin: "1rem auto 0",
          padding: "0 1rem",
          display: "flex",
          gap: 12,
        }}
      >
        <Link to="/">home</Link>
        <Link to="/live-query">useLiveQuery</Link>
        <Link to="/live-suspense-query">useLiveSuspenseQuery</Link>
      </nav>
      <Outlet />
    </>
  )
}
