// Collection registry. For M1 it holds collection definitions and enforces the
// client-supplied-key rule (ADR-0001 D9). defineMutation/defineCommand arrive
// with the sync + confirmation milestones.

import type { SqlStorage } from "@cloudflare/workers-types"
import type { MutOp, RowOp } from "../wire/frames.ts"
import { SYNC_PREFIX } from "./changes.ts"

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/

export interface CollectionDef {
  /** Table name; also the collection's wire identity. The author creates the
   *  table themselves (migration); the framework only reads + emits it. */
  table: string
  /** Primary-key column — must be a client-supplied TEXT key (ULID/UUIDv7).
   *  Enforced against the actual table by `registerSync` (ADR-0007). */
  pk: string
}

/** Context for a mutation handler. `env` is the DO's binding env, so handlers
 *  can reach external resources (R2/KV/services) — essential for `afterCommit`
 *  side effects and useful in `authorize`. `execute` runs inside
 *  `transactionSync`, so it cannot await env, but may read synchronous config. */
export interface MutationCtx<TUser, Env = unknown> {
  user: TUser
  op: MutOp
  sql: SqlStorage
  env: Env
}

export interface MutationDef<TUser, Env = unknown> {
  collection: string
  type: RowOp
  /** Runs BEFORE the transaction; may be async (read other rows, call out).
   *  Throw to deny — the frame is rejected and nothing is applied. */
  authorize?: (ctx: MutationCtx<TUser, Env>) => void | Promise<void>
  /** Runs INSIDE `transactionSync` — MUST be synchronous (ADR-0001 D11/C6). */
  execute: (ctx: MutationCtx<TUser, Env>) => void
  /**
   * Fire-and-forget async post-work, run via `ctx.waitUntil` AFTER the mutation
   * commits and its receipt is sent — never blocking the client. This is the
   * sanctioned home for external side effects a synchronous `execute` can't do
   * (delete an R2 object, enqueue a job). It receives the committed `env`/`sql`.
   *
   * It has no retry and no ordering guarantee: a thrown error or a DO eviction
   * mid-effect just drops THIS invocation. Make the work idempotent and
   * level-triggered (query "what still needs doing", act, mark done) so a later
   * trigger — the next such mutation, or a boot-time sweep in your DO — finishes
   * whatever a dropped invocation left. Don't put the durable state change here;
   * that belongs in `execute`.
   */
  afterCommit?: (ctx: MutationCtx<TUser, Env>) => unknown | Promise<unknown>
}

/** Context for a command handler. `execute` runs outside any transaction. */
export interface CommandCtx<TUser, Env = unknown> {
  user: TUser
  args: unknown
  sql: SqlStorage
  env: Env
}

export interface CommandDef<TUser, Env = unknown> {
  name: string
  authorize?: (ctx: CommandCtx<TUser, Env>) => void | Promise<void>
  /** Side-effecting command; may be async (so external effects can run inline,
   *  unlike a mutation's synchronous execute). Result is returned on `committed`. */
  execute: (ctx: CommandCtx<TUser, Env>) => unknown | Promise<unknown>
}

export class Registry<TUser = unknown, Env = unknown> {
  readonly collections = new Map<string, CollectionDef>()
  /** Keyed by `${collection}:${type}`. */
  readonly mutations = new Map<string, MutationDef<TUser, Env>>()
  readonly commands = new Map<string, CommandDef<TUser, Env>>()

  defineCollection(def: CollectionDef): this {
    assertValidCollection(def)
    if (this.collections.has(def.table)) {
      throw new Error(`collection '${def.table}' is already defined`)
    }
    this.collections.set(def.table, def)
    return this
  }

  defineMutation(def: MutationDef<TUser, Env>): this {
    if (!this.collections.has(def.collection)) {
      throw new Error(`defineMutation: unknown collection '${def.collection}' — define the collection first`)
    }
    const key = `${def.collection}:${def.type}`
    if (this.mutations.has(key)) throw new Error(`mutation '${key}' is already defined`)
    this.mutations.set(key, def)
    return this
  }

  defineCommand(def: CommandDef<TUser, Env>): this {
    if (this.commands.has(def.name)) throw new Error(`command '${def.name}' is already defined`)
    this.commands.set(def.name, def)
    return this
  }
}

/**
 * Validate the `{table, pk}` identifiers at registration. These strings are
 * interpolated raw into trigger DDL and `SELECT`s, so they must be safe
 * identifiers and must not collide with framework tables. The structural
 * constraint — the pk is a sole TEXT client-supplied key (no AUTOINCREMENT,
 * D9) — is enforced against the ACTUAL table by `registerSync` (ADR-0007),
 * since the author owns table creation and we no longer see a DDL string.
 */
export function assertValidCollection(def: CollectionDef): void {
  const { table, pk } = def
  if (!IDENT.test(table)) throw new Error(`invalid table name '${table}'`)
  if (!IDENT.test(pk)) throw new Error(`invalid pk name '${pk}'`)
  if (table.startsWith(SYNC_PREFIX)) {
    throw new Error(`table name '${table}' uses the reserved '${SYNC_PREFIX}' prefix`)
  }
}
