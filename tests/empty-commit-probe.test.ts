import { createCollection } from "@tanstack/db"
import { describe, expect, it } from "vitest"

// PROBE (ADR-0002 C2): when a confirmed write lands in NO loaded window, no
// delta arrives, yet the completed *direct* optimistic upsert is retained until
// a later sync commit. Does an empty begin()/commit() run TanStack's clear path
// (state.ts:1167-1188) and drop the unconfirmed row? If yes, the adapter issues
// a post-mutation empty commit to retire no-window-match phantoms; if no, it
// must emit a targeted synced delete instead.

interface Row {
  id: string
  body: string
}

describe("@tanstack/db: empty sync commit clears a confirmed no-match direct insert", () => {
  it("an empty begin/commit retires a completed direct optimistic upsert", async () => {
    type Controls = {
      begin: () => void
      write: (m: { type: string; value?: unknown; key?: string }) => void
      commit: () => void
      markReady: () => void
    }
    let controls: Controls
    const c = createCollection<Row, string>({
      id: "empty-commit-probe",
      getKey: (r: Row) => r.id,
      startSync: true,
      sync: {
        rowUpdateMode: "partial",
        sync: (p: Controls) => {
          controls = p
          p.markReady()
        },
      },
      // Confirmed, but no synced row will ever arrive (the no-window-match case).
      onInsert: async () => {},
    } as never)

    const tx = c.insert({ id: "x", body: "1" }) as unknown as { isPersisted: { promise: Promise<unknown> } }
    await tx.isPersisted.promise

    // Retained as a completed direct optimistic upsert (no sync confirmed it).
    expect(c.get("x")).toMatchObject({ id: "x", body: "1" })

    // The decisive step: an empty sync commit.
    controls!.begin()
    controls!.commit()
    await Promise.resolve()
    await new Promise((r) => setTimeout(r, 0))

    expect(c.get("x")).toBeUndefined()
  })
})
