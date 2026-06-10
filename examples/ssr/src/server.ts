// Custom worker entry (wrangler `main`): ONE worker serves both halves —
// WebSocket upgrades on /sync/* go straight to the DO, everything else is the
// TanStack Start app (SSR + assets). The Start handler never sees the upgrade,
// so hibernation stays intact.

import handler from "@tanstack/react-start/server-entry"
import type { Env } from "./todos-do.ts"

export { TodosDO } from "./todos-do.ts"

export default {
  fetch(req: Request, env: Env, ctx: ExecutionContext): Response | Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname.startsWith("/sync/")) {
      const room = url.pathname.slice("/sync/".length) || "main"
      return env.TODOS_DO.get(env.TODOS_DO.idFromName(room)).fetch(req)
    }
    return handler.fetch(req, env, ctx)
  },
} satisfies ExportedHandler<Env>
