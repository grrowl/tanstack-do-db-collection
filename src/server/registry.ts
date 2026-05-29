// Collection registry. For M1 it holds collection definitions and enforces the
// client-supplied-key rule (ADR-0001 D9). defineMutation/defineCommand arrive
// with the sync + confirmation milestones.

import type { SqlStorage } from "@cloudflare/workers-types"
import type { MutOp, RowOp } from "../wire/frames.ts"
import { SYNC_PREFIX } from "./changes.ts"

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/

export interface CollectionDef {
  /** Table name; also the collection's wire identity. */
  table: string
  /** Primary-key column — must be a client-supplied TEXT key (ULID/UUIDv7). */
  pk: string
  /** A single `CREATE TABLE` statement. */
  ddl: string
}

/** Context for a mutation handler. `execute` runs inside `transactionSync`. */
export interface MutationCtx<TUser> {
  user: TUser
  op: MutOp
  sql: SqlStorage
}

export interface MutationDef<TUser> {
  collection: string
  type: RowOp
  /** Runs BEFORE the transaction; may be async (read other rows, call out).
   *  Throw to deny — the frame is rejected and nothing is applied. */
  authorize?: (ctx: MutationCtx<TUser>) => void | Promise<void>
  /** Runs INSIDE `transactionSync` — MUST be synchronous (ADR-0001 D11/C6). */
  execute: (ctx: MutationCtx<TUser>) => void
}

/** Context for a command handler. `execute` runs outside any transaction. */
export interface CommandCtx<TUser> {
  user: TUser
  args: unknown
  sql: SqlStorage
}

export interface CommandDef<TUser> {
  name: string
  authorize?: (ctx: CommandCtx<TUser>) => void | Promise<void>
  /** Side-effecting command; may be async. Result is returned on `committed`. */
  execute: (ctx: CommandCtx<TUser>) => unknown | Promise<unknown>
}

export class Registry<TUser = unknown> {
  readonly collections = new Map<string, CollectionDef>()
  /** Keyed by `${collection}:${type}`. */
  readonly mutations = new Map<string, MutationDef<TUser>>()
  readonly commands = new Map<string, CommandDef<TUser>>()

  defineCollection(def: CollectionDef): this {
    assertValidCollection(def)
    if (this.collections.has(def.table)) {
      throw new Error(`collection '${def.table}' is already defined`)
    }
    this.collections.set(def.table, def)
    return this
  }

  defineMutation(def: MutationDef<TUser>): this {
    if (!this.collections.has(def.collection)) {
      throw new Error(`defineMutation: unknown collection '${def.collection}' — define the collection first`)
    }
    const key = `${def.collection}:${def.type}`
    if (this.mutations.has(key)) throw new Error(`mutation '${key}' is already defined`)
    this.mutations.set(key, def)
    return this
  }

  defineCommand(def: CommandDef<TUser>): this {
    if (this.commands.has(def.name)) throw new Error(`command '${def.name}' is already defined`)
    this.commands.set(def.name, def)
    return this
  }
}

/** Split a column-list body on top-level commas (paren-depth aware). */
function splitColumns(body: string): Array<string> {
  const parts: Array<string> = []
  let depth = 0
  let start = 0
  for (let i = 0; i < body.length; i++) {
    const c = body[i]
    if (c === "(") depth++
    else if (c === ")") depth--
    else if (c === "," && depth === 0) {
      parts.push(body.slice(start, i))
      start = i + 1
    }
  }
  parts.push(body.slice(start))
  return parts.map((s) => s.trim()).filter((s) => s.length > 0)
}

const unquote = (s: string): string => s.replace(/["'`[\]]/g, "")

/**
 * Enforce the constraints distributed sync depends on:
 *  - valid identifiers (interpolated into trigger DDL),
 *  - not the reserved `_sync_` prefix,
 *  - no AUTOINCREMENT (server-assigned keys break optimistic id parity),
 *  - the pk is a single TEXT PRIMARY KEY (client-supplied, stable).
 * Fails loud at registration so a bad schema never reaches runtime.
 */
export function assertValidCollection(def: CollectionDef): void {
  const { table, pk, ddl } = def

  if (!IDENT.test(table)) throw new Error(`invalid table name '${table}'`)
  if (!IDENT.test(pk)) throw new Error(`invalid pk name '${pk}'`)
  if (table.startsWith(SYNC_PREFIX)) {
    throw new Error(`table name '${table}' uses the reserved '${SYNC_PREFIX}' prefix`)
  }
  if (/\bautoincrement\b/i.test(ddl)) {
    throw new Error(
      `collection '${table}': AUTOINCREMENT keys are not supported — distributed ` +
        `sync requires a client-supplied TEXT key (ULID/UUIDv7) so the optimistic ` +
        `id matches the confirmed id`,
    )
  }

  const open = ddl.indexOf("(")
  const close = ddl.lastIndexOf(")")
  if (open < 0 || close <= open) {
    throw new Error(`collection '${table}': DDL must be a single CREATE TABLE with a column list`)
  }
  const cols = splitColumns(ddl.slice(open + 1, close))

  let pkType: string | undefined
  let pkColIsPrimary = false
  let tableLevelPrimary = false
  const pkLower = pk.toLowerCase()

  for (const col of cols) {
    const upper = col.toUpperCase()
    if (/^(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY|CONSTRAINT)\b/.test(upper)) {
      const m = upper.match(/^PRIMARY\s+KEY\s*\(([^)]*)\)/)
      if (m) {
        const keys = m[1]!.split(",").map((k) => unquote(k.trim().toLowerCase()))
        if (keys.length === 1 && keys[0] === pkLower) tableLevelPrimary = true
      }
      continue
    }
    const tokens = col.split(/\s+/)
    const name = unquote(tokens[0] ?? "").toLowerCase()
    if (name === pkLower) {
      pkType = (tokens[1] ?? "").toUpperCase()
      pkColIsPrimary = /\bPRIMARY\s+KEY\b/.test(upper)
    }
  }

  if (pkType === undefined) {
    throw new Error(`collection '${table}': pk column '${pk}' not found in the DDL`)
  }
  if (pkType !== "TEXT") {
    throw new Error(
      `collection '${table}': pk column '${pk}' must be TEXT (client-supplied key); got '${pkType || "(no type)"}'`,
    )
  }
  if (!pkColIsPrimary && !tableLevelPrimary) {
    throw new Error(
      `collection '${table}': pk column '${pk}' must be the sole PRIMARY KEY`,
    )
  }
}
