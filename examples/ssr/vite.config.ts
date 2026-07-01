import { cloudflare } from "@cloudflare/vite-plugin"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  resolve: {
    // The library is imported from source (../../src), whose own `@tanstack/db`
    // import would resolve from the REPO root's node_modules — a second physical
    // copy. Two copies break the Symbol-branded collectionOptions and every
    // instanceof across the boundary. Dedupe forces one copy: this example's
    // vendored PR build.
    dedupe: ["@tanstack/db"],
  },
  plugins: [cloudflare({ viteEnvironment: { name: "ssr" } }), tanstackStart(), react()],
})
