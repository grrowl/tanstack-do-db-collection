/// <reference types="@cloudflare/vitest-pool-workers" />

declare module "cloudflare:test" {
  interface ProvidedEnv {
    TEST_DO: DurableObjectNamespace
    SYNC_DO: DurableObjectNamespace
    UNREG_DO: DurableObjectNamespace
    MAINT_DO: DurableObjectNamespace
    SLOW_DO: DurableObjectNamespace
    LIMITS_DO: DurableObjectNamespace
  }
}
