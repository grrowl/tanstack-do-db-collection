/// <reference types="@cloudflare/vitest-pool-workers" />

declare module "cloudflare:test" {
  interface ProvidedEnv {
    TEST_DO: DurableObjectNamespace
  }
}
