// WHY: a filtered subscription's membership must not depend on which evaluator
// decided it — the SQL snapshot path and the JS delta path must agree on every
// row, or clients diverge by connection timing. Specifically:
//   - The snapshot path filters rows in SQLite via sql-compiler.ts (lowered SQL).
//   - The delta path filters rows in JS via @tanstack/db's compileSingleRowExpression
//     + toBooleanPredicate in subscriptions.ts.
// SQL and JS famously disagree at the edges: NULL semantics (three-valued logic),
// LIKE case-sensitivity, and IN with NULLs. These cases pin the actual behavior
// and fail loudly if the two paths diverge.
//
// DIVERGENCES FOUND (plan 003 — needs maintainer decision + ADR):
//
// 1. `ne` — @tanstack/db's compileSingleRowExpression throws QueryCompilationError
//    for "ne" (it is not in its function registry). The SQL compiler accepts `ne`
//    (maps to !=), so `ne` predicates work at snapshot but crash the DO's
//    handleSub when compilePredicate runs (uncaught exception, the client hangs).
//    This is an OPERATOR FLOOR MISMATCH: sql-compiler.ts accepts ne but
//    compileSingleRowExpression rejects it — the floor is not consistent.
//    Severity: the `ne` operator is advertised as supported but silently breaks
//    the delta/reconnect path and leaves the subscription in a broken state.
//
// 2. `like` — SQLite LIKE is case-insensitive for ASCII by default.
//    @tanstack/db's JS evaluator is case-sensitive. Result: a row with body
//    "HELLO" matches `LIKE "hello%"` in the SQL snapshot but NOT in the JS delta,
//    so two clients can have different views of the same subscription depending
//    on when they connected.
//
// Failing cases are marked `it.fails()` so the suite stays green while the
// divergences stay visible (per plan 003 step 4). Fix options — see plan 003
// Maintenance notes — need a maintainer decision and an ADR.

import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 3000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))

// IR helpers — plain JSON matching the wire shape (type/name/args).
const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

// Seed rows:
//   n1 — body IS NULL  (tests NULL semantics in every case)
//   lo — body = "hello" (lowercase)
//   up — body = "HELLO" (uppercase — tests LIKE case-sensitivity)
//   x  — body = "x"    (value used in ne/eq/in tests)
type Seed = { id: string; body: string | null }
const SEEDS: Array<Seed> = [
  { id: "n1", body: null },
  { id: "lo", body: "hello" },
  { id: "up", body: "HELLO" },
  { id: "x", body: "x" },
]

/**
 * Measure snapshot membership for a `where` IR: insert seed rows server-side
 * via runInDurableObject (raw SQL — so body can be NULL), then sub from a
 * fresh socket and collect keys from `snap` frames until `snap-end`.
 *
 * Returns the sorted set of keys the SQL snapshot matched.
 */
async function snapshotMembers(room: string, where: unknown): Promise<Array<string>> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  await runInDurableObject(stub, (_i, s) => {
    for (const seed of SEEDS) {
      if (seed.body === null) {
        s.storage.sql.exec("INSERT INTO messages(id) VALUES (?)", seed.id)
      } else {
        s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", seed.id, seed.body)
      }
    }
  })
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  const frames = await collectUntil(ws, (f) => f.t === "snap-end")
  ws.close()
  return frames
    .filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap")
    .map((f) => f.key as string)
    .sort()
}

/**
 * Measure delta membership for a `where` IR: sub first (empty snapshot),
 * then insert seed rows via `mut` frames so they drain synchronously
 * (sync-do.ts:318). For each inserted key, the subscriber either receives a
 * `d`/insert (member) or `d`/delete (non-member / synthetic delete from the
 * always-emit rule, ADR-0002 C4) or nothing (effectively non-member).
 *
 * Two spellings of the NULL row are tested:
 *   - omitting `body` from `cols` — the mutation handler's `c.body` is undefined,
 *     which binds as NULL in SQLite. This is the normal client path.
 *   - explicit `body: null` in `cols` — same result in storage; both spellings
 *     must agree or there is a codec/binding divergence worth surfacing.
 *
 * Returns the sorted set of keys that arrived as non-delete deltas.
 */
async function deltaMembers(room: string, where: unknown): Promise<Array<string>> {
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  await collectUntil(ws, (f) => f.t === "snap-end")

  const members = new Set<string>()
  let txCounter = 0

  for (const seed of SEEDS) {
    const txId = `tx-${++txCounter}`
    // Two spellings for the NULL row:
    //   (a) omit body — c.body is undefined in the mutation handler, binds as NULL
    //   (b) explicit body: null — same storage result; both are wired to be identical
    // We use spelling (a) for n1 as the primary, note spelling (b) is equivalent.
    // If they diverge (e.g. a codec difference), only (a) is inserted here and
    // a separate explicit-null sub-case verifies (b).
    const cols: Record<string, unknown> =
      seed.body === null
        ? { id: seed.id } // omit body → undefined → NULL in SQLite
        : { id: seed.id, body: seed.body }

    send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "insert", key: seed.id, cols }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed" && f.txId === txId)

    // The always-emit rule (ADR-0002 C4): every changed key gets a `d` frame,
    // either insert/update (member) or delete (non-member). If no `d` arrives,
    // that's a protocol deviation — currently silent here; see STOP condition in plan 003.
    const delta = frames.find(
      (f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && (f.key as string) === seed.id,
    )
    if (delta && delta.op !== "delete") {
      members.add(seed.id)
    }
  }

  ws.close()
  return Array.from(members).sort()
}

/**
 * Same as deltaMembers but sends the NULL row with explicit `body: null` in cols.
 * WHY: MessagePack serialises null differently from a missing key; verify both
 * spellings produce the same membership result as omitting the key.
 */
async function deltaMembersExplicitNull(room: string, where: unknown): Promise<Array<string>> {
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  await collectUntil(ws, (f) => f.t === "snap-end")

  const members = new Set<string>()
  let txCounter = 0

  for (const seed of SEEDS) {
    const txId = `tx-${++txCounter}`
    // Explicit null spelling for the NULL row.
    const cols: Record<string, unknown> =
      seed.body === null
        ? { id: seed.id, body: null } // explicit null — should store NULL, same as omitting
        : { id: seed.id, body: seed.body }

    send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "insert", key: seed.id, cols }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed" && f.txId === txId)

    const delta = frames.find(
      (f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && (f.key as string) === seed.id,
    )
    if (delta && delta.op !== "delete") {
      members.add(seed.id)
    }
  }

  ws.close()
  return Array.from(members).sort()
}

// ─── The 6 parity cases ───────────────────────────────────────────────────────

describe("SQL/JS predicate parity (plan 003)", () => {
  // Case 1: ne(body, "x")
  // DIVERGENCE CONFIRMED (plan 003).
  // sql-compiler.ts accepts `ne` (maps to !=) so the snapshot path works fine.
  // @tanstack/db's compileSingleRowExpression does NOT know "ne" — it throws
  // QueryCompilationError, which is uncaught in handleSub (only
  // UnsupportedPredicateError from compileSubsetQuery is caught). The subscription
  // never registers; the client receives no snap-end and hangs until timeout.
  // Operator floor is inconsistent: ne is in COMPARATORS but not in the JS evaluator.
  // Fix path: see plan 003 Maintenance notes (option a or c).
  it.fails("ne: DIVERGENCE — @tanstack/db rejects 'ne', snapshot path works, JS path crashes (plan 003)", async () => {
    const where = fn("ne", ref("body"), val("x"))
    const snap = await snapshotMembers("pp-ne-snap", where)
    const delta = await deltaMembers("pp-ne-delta", where)
    const deltaExplicit = await deltaMembersExplicitNull("pp-ne-delta-explicit", where)

    // Both paths must agree — if they don't, divergence is confirmed.
    expect(snap, `ne: snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `ne: omit-null vs explicit-null spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(
      deltaExplicit,
    )
  })

  // Case 2: eq(body, "hello")
  // Sanity case: only "lo" matches; no NULL edge.
  // SQL: "hello" = 'hello' → true for lo; "HELLO" = 'hello' → false (case-sensitive = in SQLite).
  // JS: same. VERIFIED PASSING — both paths agree: snap=["lo"] delta=["lo"].
  it("eq: sanity case, exact match (no NULL edge)", async () => {
    const where = fn("eq", ref("body"), val("hello"))
    const snap = await snapshotMembers("pp-eq-snap", where)
    const delta = await deltaMembers("pp-eq-delta", where)

    expect(snap, `eq: snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
  })

  // Case 3: like(body, "hello%")
  // DIVERGENCE CONFIRMED (plan 003).
  // SQL LIKE is case-insensitive for ASCII by default: "HELLO" matches "hello%" → up included.
  // JS like in @tanstack/db is case-sensitive: "HELLO" does NOT match "hello%" → up excluded.
  // Observed: snap=["lo","up"] delta=["lo"] — a concrete membership divergence.
  // A client that connected during a snapshot sees "up"; one that connected fresh
  // (receiving via delta) does not. Clients silently diverge.
  // Fix path: see plan 003 Maintenance notes (option a — JS evaluator with SQLite semantics).
  it.fails("like: DIVERGENCE — SQLite LIKE is case-insensitive, JS evaluator is case-sensitive (plan 003)", async () => {
    const where = fn("like", ref("body"), val("hello%"))
    const snap = await snapshotMembers("pp-like-snap", where)
    const delta = await deltaMembers("pp-like-delta", where)

    // snap=["lo","up"] delta=["lo"] — "up" ("HELLO") matches in SQL but not in JS.
    expect(snap, `like: snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
  })

  // Case 4: gt(body, "a")
  // SQL: NULL > 'a' evaluates to NULL (three-valued) → n1 excluded.
  // JS: toBooleanPredicate(null) → false → excluded too.
  // VERIFIED PASSING — both paths agree: snap=["lo","up","x"] delta=["lo","up","x"].
  // (Note: string comparison in both paths; "HELLO" and "hello" are > "a" in both.)
  // Also verifies the explicit `body: null` spelling on the delta path produces the
  // same membership as omitting `body` — both spellings must agree (plan 003 step 2).
  it("gt: NULL body excluded on both paths (SQL three-valued logic)", async () => {
    const where = fn("gt", ref("body"), val("a"))
    const snap = await snapshotMembers("pp-gt-snap", where)
    const delta = await deltaMembers("pp-gt-delta", where)
    const deltaExplicit = await deltaMembersExplicitNull("pp-gt-delta-explicit", where)

    expect(snap, `gt: snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `gt: omit-null vs explicit-null spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(
      deltaExplicit,
    )
  })

  // Case 5: not(eq(body, "x"))
  // SQL: NOT (NULL = 'x') → NOT NULL → NULL → n1 excluded (three-valued NOT).
  // JS: toBooleanPredicate wraps the outer call — compileSingleRowExpression for
  //   not(eq(body,"x")) returns null for null body, and toBooleanPredicate(null) → false.
  // VERIFIED PASSING — both paths agree: snap=["lo","up"] delta=["lo","up"].
  // Also verifies the explicit `body: null` spelling on the delta path produces the
  // same membership as omitting `body` — both spellings must agree (plan 003 step 2).
  it("not(eq): NULL body excluded under three-valued NOT", async () => {
    const where = fn("not", fn("eq", ref("body"), val("x")))
    const snap = await snapshotMembers("pp-not-snap", where)
    const delta = await deltaMembers("pp-not-delta", where)
    const deltaExplicit = await deltaMembersExplicitNull("pp-not-delta-explicit", where)

    expect(snap, `not(eq): snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `not(eq): omit-null vs explicit-null spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(
      deltaExplicit,
    )
  })

  // Case 6: in(body, ["hello", "x"])
  // SQL: NULL IN ('hello', 'x') → NULL (three-valued) → n1 excluded.
  // JS: [].includes(null) → false → excluded (consistent with SQL).
  // Also: "HELLO" not in the list → up excluded. Only lo and x included.
  // VERIFIED PASSING — both paths agree: snap=["lo","x"] delta=["lo","x"].
  // Also verifies the explicit `body: null` spelling on the delta path produces the
  // same membership as omitting `body` — both spellings must agree (plan 003 step 2).
  it("in: NULL body excluded, exact match only (no case folding)", async () => {
    const where = fn("in", ref("body"), val(["hello", "x"]))
    const snap = await snapshotMembers("pp-in-snap", where)
    const delta = await deltaMembers("pp-in-delta", where)
    const deltaExplicit = await deltaMembersExplicitNull("pp-in-delta-explicit", where)

    expect(snap, `in: snapshot vs delta mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `in: omit-null vs explicit-null spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(
      deltaExplicit,
    )
  })
})
