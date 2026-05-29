// Tagged value codec (ADR-0001 D17).
//
// Lossless transport + at-rest encoding for values JSON cannot represent on its
// own: bigint, Date, NaN, ±Infinity, -0, undefined, and Uint8Array. Naive
// `JSON.stringify` silently corrupts every one of these.
//
// Collision-proof by construction: special values are replaced by neutral
// placeholders in the payload, and their (path, type) is recorded in a separate
// meta list. Reconstruction walks the meta list — never the payload — so user
// data that happens to look like a tag (e.g. `{ $type: "bigint" }`) is never
// misinterpreted.

type TypeTag = "bigint" | "date" | "nan" | "inf" | "-inf" | "-0" | "undef" | "u8"

type Path = Array<string | number>

interface Envelope {
  /** Payload with special values replaced by JSON-safe placeholders. */
  d: unknown
  /** (path, type) entries for every replaced value. */
  m: Array<[Path, TypeTag]>
}

/** Identify a value that JSON cannot round-trip; null if it is JSON-safe. */
function classify(v: unknown): TypeTag | null {
  switch (typeof v) {
    case "bigint":
      return "bigint"
    case "undefined":
      return "undef"
    case "number":
      if (Number.isNaN(v)) return "nan"
      if (v === Infinity) return "inf"
      if (v === -Infinity) return "-inf"
      if (Object.is(v, -0)) return "-0"
      return null
    case "object":
      if (v === null) return null
      if (v instanceof Date) return "date"
      if (v instanceof Uint8Array) return "u8"
      return null
    case "function":
    case "symbol":
      throw new TypeError(`tanstack-do-db-collection: cannot encode a ${typeof v}`)
    default:
      return null
  }
}

function toPlaceholder(tag: TypeTag, v: unknown): unknown {
  switch (tag) {
    case "bigint":
      return (v as bigint).toString()
    case "date":
      return (v as Date).getTime()
    case "u8":
      return Array.from(v as Uint8Array)
    case "-0":
      return 0
    case "nan":
    case "inf":
    case "-inf":
    case "undef":
      return null
  }
}

function fromPlaceholder(tag: TypeTag, v: unknown): unknown {
  switch (tag) {
    case "bigint":
      return BigInt(v as string)
    case "date":
      return new Date(v as number)
    case "u8":
      return new Uint8Array(v as Array<number>)
    case "nan":
      return NaN
    case "inf":
      return Infinity
    case "-inf":
      return -Infinity
    case "-0":
      return -0
    case "undef":
      return undefined
  }
}

function walk(v: unknown, path: Path, meta: Envelope["m"]): unknown {
  const tag = classify(v)
  if (tag !== null) {
    meta.push([path.slice(), tag])
    return toPlaceholder(tag, v)
  }
  if (Array.isArray(v)) {
    return v.map((item, i) => walk(item, [...path, i], meta))
  }
  if (v !== null && typeof v === "object") {
    const out: Record<string, unknown> = {}
    for (const k of Object.keys(v as Record<string, unknown>)) {
      out[k] = walk((v as Record<string, unknown>)[k], [...path, k], meta)
    }
    return out
  }
  return v
}

/** Encode any supported value to a string safe for the wire or `value TEXT`. */
export function encode(value: unknown): string {
  const meta: Envelope["m"] = []
  const d = walk(value, [], meta)
  return JSON.stringify({ d, m: meta } satisfies Envelope)
}

/** Decode a string produced by `encode`, reconstructing special values. */
export function decode(text: string): unknown {
  const env = JSON.parse(text) as Envelope
  let root = env.d
  for (const [path, tag] of env.m) {
    if (path.length === 0) {
      root = fromPlaceholder(tag, root)
      continue
    }
    // Navigate to the parent of the target; placeholders never have
    // descendants in `m`, so this only traverses plain objects/arrays.
    let parent = root as Record<string | number, unknown>
    for (let i = 0; i < path.length - 1; i++) {
      parent = parent[path[i]!] as Record<string | number, unknown>
    }
    const last = path[path.length - 1]!
    parent[last] = fromPlaceholder(tag, parent[last])
  }
  return root
}
