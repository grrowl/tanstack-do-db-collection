import { defineWorkersProject } from "@cloudflare/vitest-pool-workers/config"

// Tests run inside workerd (via miniflare) so Durable Objects behave like
// production. Pure-TS units (e.g. the wire codec) run here too — workerd is a
// V8 isolate, so they execute identically and we keep a single runner.
export default defineWorkersProject({
  test: {
    include: ["tests/**/*.test.ts"],
    poolOptions: {
      workers: {
        singleWorker: true,
        isolatedStorage: true,
        main: "./tests/test-worker.ts",
        miniflare: {
          compatibilityDate: "2026-03-10",
          compatibilityFlags: ["nodejs_compat"],
          durableObjects: {
            TEST_DO: { className: "TestDO", useSQLite: true },
          },
        },
      },
    },
  },
})
