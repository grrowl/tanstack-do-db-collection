# 0004 — Side effects go in a fire-and-forget `afterCommit`, not the transaction

**Status:** Accepted. Extends [ADR-0001](./0001-sync-architecture.md)'s mutation
model and [ADR-0002](./0002-adversarial-review-corrections.md) C6 (execute is
synchronous).

## Context

A mutation's `execute` runs inside `transactionSync` and is required to be
synchronous (ADR-0001 D11, ADR-0002 C6): it must complete inside the workerd
storage transaction, and a returned thenable is runtime-rejected. That is
correct for the durable, ordered, atomic write — but it has no room for the
external side effects real apps need: deleting an R2 object when a file row is
tombstoned, enqueuing a job, calling another service.

The two obvious homes are both wrong:

- **`authorize`** runs *before* the transaction and runs for ops that may then
  be rejected. An effect here fires before the write is durable, and fires even
  when the mutation is denied.
- **`execute`** is synchronous and inside the transaction. An external call
  there cannot be awaited, holds the write path, and is not atomic with the
  external system — the transaction can roll back but R2 cannot.

A `command` (`defineCommand`) *can* do async work in `execute` (it runs outside
any transaction), and remains the right tool for RPC-shaped operations. But it
is not the collection's optimistic mutation path, and its only durability is
client-retry-driven dedup. We still want post-work attached to ordinary
mutations.

## Decision

Add an optional **`afterCommit(ctx)`** to `MutationDef`, run **after** the
mutation commits and its `committed` receipt is sent, via `ctx.waitUntil` —
never on the client's critical path. It receives the same `ctx` as `execute`,
including the DO's **`env`** (see below), so it can reach bindings.

The library's guarantee is deliberately minimal: **runs once per committed
op, off the response path, isolated.** A throw is logged and dropped; it never
touches the already-committed mutation. There is:

- **no retry** — a thrown hook or a DO eviction mid-effect drops *that*
  invocation;
- **no ordering / no single-flight** — concurrent invocations may overlap;
- **no scheduler, no dirty-tracking, no boot trigger** in the library.

`env` is added to `MutationCtx` and `CommandCtx` (typed via a new `Env` generic
on `Registry`, defaulting to `unknown` so existing `Registry<TUser>` is
unchanged). Handlers previously had no way to reach bindings at all; this is the
prerequisite that makes external effects possible from `authorize`/`afterCommit`.

## Why the minimal hook is sufficient

The robustness lives in *user land*, not the library, and that is the right
split. The correct reconciler is **level-triggered**, not edge-triggered:

- **Edge-triggered** (the hook *is* the effect, bound to one commit): a missed
  edge — a throw, an eviction — is lost forever. No backstop.
- **Level-triggered** (the hook *triggers* an idempotent sweep that derives
  "what still needs doing" from state): a dropped invocation just leaves the
  work queued; the next trigger — the next such mutation, or a boot-time sweep
  in the user's `fetch`/constructor — finishes it.

So the sanctioned pattern is: `execute` writes the durable intent (a
`deleted_at` tombstone); `afterCommit` calls an idempotent reconciler that
queries un-purged tombstones, performs the effect (R2 delete is idempotent),
and marks `purged_at`. Because that reconciler is level-triggered and
idempotent, the library hook needs none of the orchestration — a dumb
fire-and-forget trigger is exactly enough.

This is why we rejected building a reconciler primitive (single-flight queue,
dirty re-run, boot hook) into the library: it is speculative complexity for a
concern the user is better placed to own, and the level-triggered design makes
the library's part trivial. We provide the interface (`afterCommit` + a boot
trigger the user adds in their own `fetch`), not the policy.

## Consequences

- Mutations gain async post-work without weakening `execute`'s synchronous,
  in-transaction contract.
- Handlers can reach `env` bindings (R2/KV/services) from `authorize`,
  `execute` (synchronous reads only), `afterCommit`, and command `execute`.
- The durable state change stays in `execute`; only the external effect is in
  `afterCommit`. Putting the state change in `afterCommit` would reintroduce the
  crash hole (effect with no durable record to drive recovery).
- **Residual, named not hidden:** a DO that is deleted and never accessed again
  never fires either trigger, so its un-purged tombstones leak in the external
  store. No in-DO design closes this — boot recovery needs a boot. The backstop
  is an external scheduled sweep (a cron Worker), out of this library's scope.
