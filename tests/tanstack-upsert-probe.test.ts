import { createCollection } from "@tanstack/db"
import { describe, expect, it } from "vitest"

// PROBE (M5 move-in): when a filtered row moves INTO view via an underlying
// UPDATE, the server has no before-image, so it would emit op="update" for a
// key the client doesn't hold. This checks whether TanStack DB's sync
// write({type:"update"}) for an absent key upserts (so the actual op is safe)
// or drops it (so the server must emit insert on match). Also confirms a full
// createCollection runs in workerd.

interface Row {
  id: string
  body: string
}

describe("@tanstack/db sync write semantics in workerd", () => {
  it("update for an absent key — upsert or drop?", async () => {
    type Controls = {
      begin: () => void
      write: (m: { type: string; value?: unknown; key?: string }) => void
      commit: () => void
      markReady: () => void
    }
    let controls: Controls
    const c = createCollection<Row, string>({
      id: "upsert-probe",
      getKey: (r: Row) => r.id,
      startSync: true,
      sync: {
        rowUpdateMode: "partial",
        sync: (params: Controls) => {
          controls = params
          params.markReady()
        },
      },
    } as never)

    controls!.begin()
    controls!.write({ type: "insert", value: { id: "a", body: "1" } })
    controls!.commit()

    controls!.begin()
    controls!.write({ type: "update", value: { id: "b", body: "2" } }) // 'b' never inserted
    controls!.commit()

    await Promise.resolve()
    await new Promise((r) => setTimeout(r, 0))

    expect(c.get("a")).toMatchObject({ id: "a", body: "1" })
    // The decisive assertion: does the move-in update create 'b'?
    expect(c.get("b")).toMatchObject({ id: "b", body: "2" })
  })
})
