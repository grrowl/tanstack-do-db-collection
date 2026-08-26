import { cloudflare } from "@cloudflare/vite-plugin"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  resolve: {
    // `tanstack-durable-object-sync` is a file: link to the repo root, whose
    // own `@tanstack/db` peer import would resolve from the ROOT node_modules
    // — a second physical copy. Two copies break the Symbol-branded
    // collectionOptions and every instanceof across the boundary. Dedupe
    // forces one copy: this example's.
    dedupe: ["@tanstack/db"],
  },
  plugins: [cloudflare({ viteEnvironment: { name: "ssr" } }), tanstackStart(), react()],
})
