// Cross-language wire-conformance harness (ADR-0019 D6).
//
// Runs the PRODUCTION TS codec (src/wire/frame-codec.ts, via node
// --experimental-strip-types) on both sides of the contract:
//
//   gen    — encode the fixture frames below and write them (base64) to
//            packages/do_sync_client/test/fixtures/wire_fixtures.json.
//            The Dart suite decodes these and asserts typed values.
//   verify — read packages/do_sync_client/test/fixtures/dart_emitted.json
//            (written by `dart test`), decode every Dart-emitted frame with the
//            production codec, and deep-compare against this script's frames.
//            Byte-identity is NOT asserted (msgpack shortest-form variance is
//            legal); VALUE-identity through the production decoder is the
//            contract.
//
// Usage:  node --experimental-strip-types scripts/wire-conformance.mjs gen
//         node --experimental-strip-types scripts/wire-conformance.mjs verify

import { readFileSync, writeFileSync, mkdirSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { createFrameCodec } from "../src/wire/frame-codec.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const fixturesDir = join(root, "packages/do_sync_client/test/fixtures")
const fixturesPath = join(fixturesDir, "wire_fixtures.json")
const dartEmittedPath = join(fixturesDir, "dart_emitted.json")

const codec = createFrameCodec()

// IR helpers matching the wire shape.
const ref = (col) => ({ type: "ref", path: [col] })
const val = (value) => ({ type: "val", value })
const fn = (name, ...args) => ({ type: "func", name, args })

// The fixture frames: every frame tag, every ADR-0019 D1 value type, and the
// integer boundaries where the JS encoder switches formats.
const FRAMES = {
  "sub-minimal": { t: "sub", subId: "s1", collection: "messages" },
  "sub-full": {
    t: "sub",
    subId: "messages#1",
    collection: "messages",
    where: fn("and", fn("eq", ref("author"), val("alice")), fn("gt", ref("created_at"), val(0))),
    limit: 50,
    since: "1234567890123",
  },
  unsub: { t: "unsub", subId: "messages#1" },
  "mut-insert-values": {
    t: "mut",
    txId: "tx-1",
    collection: "messages",
    ops: [
      {
        type: "insert",
        key: "01J0000000000000000000TEST",
        cols: {
          id: "01J0000000000000000000TEST",
          author: "alice",
          content: "héllo wörld \u{1F680}",
          created_at: 1753167000000, // integral beyond uint32 -> float64
          score: 1.5,
          active: true,
          note: null,
          big: 7n, // JS bigint -> uint64
          when: new Date(1753167000123),
          blob: new Uint8Array([0, 1, 2, 254, 255]),
        },
      },
    ],
  },
  "mut-update-partial": {
    t: "mut",
    txId: "tx-2",
    collection: "messages",
    ops: [{ type: "update", key: "k1", cols: { content: "edited", edited_at: 1753167000001 } }],
  },
  "mut-delete": {
    t: "mut",
    txId: "tx-3",
    collection: "messages",
    ops: [{ type: "delete", key: "k1" }],
  },
  call: { t: "call", txId: "tx-4", name: "clearRoom", args: { keep: 5, dry_run: false } },
  fetch: {
    t: "fetch",
    fetchId: "messages#fetch#1",
    collection: "messages",
    where: fn("eq", ref("category"), val("a")),
    cursor: {
      whereFrom: fn("lt", ref("created_at"), val(1753167000000)),
      whereCurrent: fn("eq", ref("created_at"), val(1753167000000)),
    },
    orderBy: [{ expression: ref("created_at"), direction: "desc" }],
    limit: 20,
  },
  snap: {
    t: "snap",
    sub: "messages#1",
    key: "k1",
    row: { id: "k1", author: "bob", content: "hi", created_at: 1753167000000, big: 9007199254740993n },
    seq: "41",
  },
  "snap-end": { t: "snap-end", sub: "messages#1", seq: "42" },
  "d-insert": {
    t: "d",
    sub: "messages#1",
    key: "k2",
    op: "insert",
    cols: { id: "k2", author: "carol", content: "yo", created_at: 1753167000002 },
    seq: "43",
  },
  "d-delete": { t: "d", sub: "messages#1", key: "k2", op: "delete", seq: "44" },
  uptodate: { t: "uptodate", seq: "45" },
  committed: { t: "committed", txId: "tx-1", seq: "46", result: { deleted: 3 } },
  rejected: { t: "rejected", txId: "tx-2", error: { code: "FRAME_TOO_LARGE", message: "no" } },
  "reset-sub": { t: "reset", sub: "messages#1" },
  "reset-all": { t: "reset" },
  page: {
    t: "page",
    fetchId: "messages#fetch#1",
    rows: [
      { id: "k3", created_at: 1753166000000 },
      { id: "k4", created_at: 1753165000000 },
    ],
    seq: "47",
  },
  "edge-ints": {
    t: "call",
    txId: "tx-5",
    name: "edge",
    args: {
      zero: 0,
      fixintMax: 127,
      u8: 200,
      u16: 40000,
      u32max: 4294967295,
      beyond32: 4294967296, // -> float64
      dateNow: 1753167000000, // -> float64
      maxSafe: 9007199254740991, // -> float64 (exact)
      negFixint: -32,
      i8: -100,
      i16: -30000,
      i32min: -2147483648,
      belowI32: -2147483649, // -> float64
      nan: NaN,
      inf: Infinity,
      ninf: -Infinity,
      f64: 3.141592653589793,
      bigPos: 9223372036854775807n, // int64 max, -> uint64 on the wire
      bigNeg: -42n, // -> int64
      bigU64: 18446744073709551615n, // uint64 max
    },
  },
  "long-string": {
    t: "call",
    txId: "tx-6",
    name: "strings",
    args: { s255: "a".repeat(255), s300: "b".repeat(300), uni: "日本語🎌 end" },
  },
  "big-bin": {
    t: "call",
    txId: "tx-7",
    name: "bin",
    args: { bin300: new Uint8Array(Array.from({ length: 300 }, (_, i) => i % 256)) },
  },
  "date-forms": {
    t: "call",
    txId: "tx-8",
    name: "dates",
    args: {
      wholeSec: new Date(1753167000000), // nsec==0 -> timestamp32
      withMs: new Date(1753167000123), // -> timestamp64
      pre1970: new Date(-86400000), // negative sec -> timestamp96
    },
  },
}

// Frames the DART side authors natively (from Dart values) and emits under
// these names; each must decode — via the production codec — to deep-equal the
// frame here. This proves the Dart ENCODER against authored (not just
// round-tripped) values.
const NATIVE_EXPECTED = {
  "native:mut-insert-values": FRAMES["mut-insert-values"],
  "native:sub-full": FRAMES["sub-full"],
  "native:edge-ints": FRAMES["edge-ints"],
  "native:date-forms": FRAMES["date-forms"],
}

function deepEqual(a, b, path = "$") {
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a) && Number.isNaN(b)) return null
    return a === b ? null : `${path}: ${a} !== ${b}`
  }
  if (typeof a !== typeof b) return `${path}: type ${typeof a} !== ${typeof b}`
  if (typeof a === "bigint") return a === b ? null : `${path}: ${a}n !== ${b}n`
  if (a === null || b === null) return a === b ? null : `${path}: ${a} !== ${b}`
  if (a instanceof Date || b instanceof Date) {
    if (!(a instanceof Date && b instanceof Date)) return `${path}: Date vs non-Date`
    return a.getTime() === b.getTime() ? null : `${path}: Date ${a.toISOString()} !== ${b.toISOString()}`
  }
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (!(a instanceof Uint8Array && b instanceof Uint8Array)) return `${path}: bytes vs non-bytes`
    if (a.length !== b.length) return `${path}: bytes length ${a.length} !== ${b.length}`
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return `${path}[${i}]: ${a[i]} !== ${b[i]}`
    return null
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!(Array.isArray(a) && Array.isArray(b))) return `${path}: array vs non-array`
    if (a.length !== b.length) return `${path}: length ${a.length} !== ${b.length}`
    for (let i = 0; i < a.length; i++) {
      const d = deepEqual(a[i], b[i], `${path}[${i}]`)
      if (d) return d
    }
    return null
  }
  if (typeof a === "object") {
    const ka = Object.keys(a).sort()
    const kb = Object.keys(b).sort()
    if (ka.join(",") !== kb.join(",")) return `${path}: keys [${ka}] !== [${kb}]`
    for (const k of ka) {
      const d = deepEqual(a[k], b[k], `${path}.${k}`)
      if (d) return d
    }
    return null
  }
  return a === b ? null : `${path}: ${String(a)} !== ${String(b)}`
}

const toB64 = (u8) => Buffer.from(u8).toString("base64")
const fromB64 = (s) => new Uint8Array(Buffer.from(s, "base64"))

const mode = process.argv[2]
if (mode === "gen") {
  const out = {}
  for (const [name, frame] of Object.entries(FRAMES)) {
    out[name] = toB64(codec.encode(frame))
  }
  mkdirSync(fixturesDir, { recursive: true })
  writeFileSync(fixturesPath, `${JSON.stringify(out, null, 2)}\n`)
  console.log(`wrote ${Object.keys(out).length} fixtures -> ${fixturesPath}`)
} else if (mode === "verify") {
  const emitted = JSON.parse(readFileSync(dartEmittedPath, "utf8"))
  let failures = 0
  let checked = 0
  const expectFor = (name) => {
    if (name.startsWith("native:")) return NATIVE_EXPECTED[name]
    return FRAMES[name]
  }
  for (const [name, b64] of Object.entries(emitted)) {
    const expected = expectFor(name)
    if (!expected) {
      console.error(`FAIL ${name}: no expected frame for this name`)
      failures++
      continue
    }
    // Canonicalize the expectation through the codec's own round-trip, so
    // encoding-lossy values (e.g. -0 -> 0) compare on post-wire semantics.
    const canonical = codec.decode(codec.encode(expected))
    let decoded
    try {
      decoded = codec.decode(fromB64(b64))
    } catch (e) {
      console.error(`FAIL ${name}: production codec cannot decode Dart bytes: ${e}`)
      failures++
      continue
    }
    const diff = deepEqual(canonical, decoded)
    if (diff) {
      console.error(`FAIL ${name}: ${diff}`)
      failures++
    }
    checked++
  }
  // Every TS fixture must have been round-tripped by the Dart side — a missing
  // name means the Dart suite silently skipped it (fail loud).
  for (const name of Object.keys(FRAMES)) {
    if (!(name in emitted)) {
      console.error(`FAIL missing: Dart suite did not emit round-trip for fixture '${name}'`)
      failures++
    }
  }
  for (const name of Object.keys(NATIVE_EXPECTED)) {
    if (!(name in emitted)) {
      console.error(`FAIL missing: Dart suite did not emit native frame '${name}'`)
      failures++
    }
  }
  if (failures > 0) {
    console.error(`\nconformance FAILED: ${failures} failure(s), ${checked} frames checked`)
    process.exit(1)
  }
  console.log(`conformance OK: ${checked} Dart-emitted frames decode identically in the production codec`)
} else {
  console.error("usage: node --experimental-strip-types scripts/wire-conformance.mjs <gen|verify>")
  process.exit(2)
}
