// Mutation/command dedup (exactly-once under client retry). Every mut/call
// outcome is recorded by client-generated txId; a retry replays the stored
// outcome instead of re-running the handler (and re-doing its side effects).
//
// Retention is INDEPENDENT of changelog retention (ADR-0002 C5): a fully
// current client can still retry an old txId, so this table is swept on its
// own time-based horizon (M7), not tied to the compaction floor.

import type { SqlStorage } from "@cloudflare/workers-types"
import { decode as decodeValue, encode as encodeValue } from "../wire/codec.ts"

export interface SeenTx {
  ok: boolean
  cursor: string | null
  error: string | null
  /** Value-codec-encoded result for commands; null for none. */
  result: string | null
}

export function lookupTx(sql: SqlStorage, txId: string): SeenTx | null {
  const rows = Array.from(
    sql.exec<{ ok: number; cursor: string | null; error: string | null; result: string | null }>(
      "SELECT ok, cursor, error, result FROM _sync_seen_tx WHERE tx_id = ?",
      txId,
    ),
  )
  if (rows.length === 0) return null
  const r = rows[0]!
  return { ok: r.ok === 1, cursor: r.cursor, error: r.error, result: r.result }
}

export function recordTx(
  sql: SqlStorage,
  txId: string,
  ok: boolean,
  cursor: string | null,
  error: string | null,
  result: string | null,
): void {
  // INSERT OR IGNORE: the first recorded outcome wins; a racing retry never
  // clobbers it.
  sql.exec(
    "INSERT OR IGNORE INTO _sync_seen_tx(tx_id, ok, cursor, error, result, ts) VALUES (?, ?, ?, ?, ?, unixepoch()*1000)",
    txId,
    ok ? 1 : 0,
    cursor,
    error,
    result,
  )
}

/** Encode a command result for storage; null for `undefined`. Throws (via the
 *  value codec) on non-serializable values, surfaced as a rejected mutation. */
export function encodeResult(value: unknown): string | null {
  return value === undefined ? null : encodeValue(value)
}

export function decodeResult(stored: string | null): unknown {
  return stored === null ? undefined : decodeValue(stored)
}
