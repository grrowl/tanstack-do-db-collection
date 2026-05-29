// CDC substrate (ADR-0001 D12): one per-DO change log fed by AFTER triggers,
// plus the single monotonic `seq` cursor.
//
// `_sync_changes` is the one ordered stream — the source of truth for live
// deltas, reconnect catch-up, and write confirmation. A trigger's INSERT runs
// inside the same `transactionSync` SAVEPOINT as the user write, so the change
// row commits or rolls back atomically with it (no orphaned log rows).
//
// No before-image column: move-in/move-out is computed at emit time from the
// current row + the subscription predicate (ADR-0002 C4).

import type { SqlStorage } from "@cloudflare/workers-types"

export const SYNC_PREFIX = "_sync_"

export type ChangeOp = "insert" | "update" | "delete"

export interface ChangeRow {
  seq: number
  tbl: string
  key: string
  op: ChangeOp
  ts: number
  // Index signature lets ChangeRow satisfy sql.exec<T>()'s constraint.
  [k: string]: string | number
}

/** Create the framework-owned tables. Idempotent. */
export function initSchema(sql: SqlStorage): void {
  // seq INTEGER PRIMARY KEY AUTOINCREMENT is the monotonic cursor; SQLite
  // indexes the PK already. The composite index serves per-table catch-up.
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_changes (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              tbl TEXT NOT NULL,
              key TEXT NOT NULL,
              op  TEXT NOT NULL,
              ts  INTEGER NOT NULL
            )`)
  sql.exec(`CREATE INDEX IF NOT EXISTS _sync_changes_tbl_seq ON _sync_changes(tbl, seq)`)
  sql.exec(`CREATE TABLE IF NOT EXISTS _sync_meta (k TEXT PRIMARY KEY, v TEXT)`)
}

/**
 * Install AFTER INSERT/UPDATE/DELETE triggers copying change events into
 * `_sync_changes`. Idempotent. `tbl`/`pk` MUST be validated identifiers
 * (the Registry enforces this) — they are interpolated into DDL.
 *
 * Each statement is passed whole: splitting on `;` would sever the inner
 * `INSERT ...;` from its `END`.
 */
export function installTriggers(sql: SqlStorage, tbl: string, pk: string): void {
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS _sync_changes_${tbl}_ai AFTER INSERT ON ${tbl} BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(NEW.${pk} AS TEXT), 'insert', unixepoch()*1000);
     END`,
  )
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS _sync_changes_${tbl}_au AFTER UPDATE ON ${tbl} BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(NEW.${pk} AS TEXT), 'update', unixepoch()*1000);
     END`,
  )
  sql.exec(
    `CREATE TRIGGER IF NOT EXISTS _sync_changes_${tbl}_ad AFTER DELETE ON ${tbl} BEGIN
       INSERT INTO _sync_changes(tbl,key,op,ts) VALUES ('${tbl}', CAST(OLD.${pk} AS TEXT), 'delete', unixepoch()*1000);
     END`,
  )
}

/** Highest `seq` in the log — the current cursor / write-confirmation receipt. */
export function currentSeq(sql: SqlStorage): number {
  const rows = Array.from(
    sql.exec<{ s: number | null }>("SELECT MAX(seq) AS s FROM _sync_changes"),
  )
  return Number(rows[0]?.s ?? 0)
}

/** The "highest seq we've broadcast" watermark. */
export function getDrainCursor(sql: SqlStorage): number {
  const rows = Array.from(
    sql.exec<{ v: string }>("SELECT v FROM _sync_meta WHERE k='drain_cursor'"),
  )
  return rows.length === 0 ? 0 : Number(rows[0]!.v ?? 0)
}

export function setDrainCursor(sql: SqlStorage, seq: number): void {
  sql.exec(
    "INSERT INTO _sync_meta(k,v) VALUES('drain_cursor', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
    String(seq),
  )
}

/** All change rows with seq > cursor, in seq order. */
export function readChangesSince(sql: SqlStorage, cursor: number): Array<ChangeRow> {
  return Array.from(
    sql.exec<ChangeRow>(
      "SELECT seq, tbl, key, op, ts FROM _sync_changes WHERE seq > ? ORDER BY seq",
      cursor,
    ),
  )
}
