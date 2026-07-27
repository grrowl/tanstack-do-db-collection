import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import { SYNC_TAG } from "../src/server/mixin.ts"
import { type FakeHost, testSchema } from "./test-worker.ts"

// WHY: the mixin's entire reason to exist is cohosting — one DO serving both its
// framework's WebSocket surface AND tddc's sync protocol with no cross-talk
// (ADR-0015). These drive the mixin over a FAKE partyserver-like base (owns
// `__pk` sockets, filters foreign sockets, exposes a `sql` tagged template,
// upgrades on `/_host`) and pin the safety properties the cohosting proof rests
// on: tag-partitioned sockets, path-partitioned upgrades, `super` delegation of
// foreign traffic, no `sql` shadow, and trigger safety on a host with its own
// tables. The fake stands in for the real `agents` package so CI never carries a
// ~13 MB pre-1.0 dependency; a real-`agents` smoke gates version bumps.
//
// Scope note: these tests assert the tag partition (`getWebSockets(SYNC_TAG)`
// vs. `getWebSockets(HOST_TAG)`) on the SAME live instance — the cohosting
// contract this file exists for. The wake-time restore across a real
// eviction-and-reconstruct cycle is covered in `tests/hibernation.test.ts`
// (`evictDurableObject`, unlocked by the vitest 4 migration; ADR-0019 —
// whose real evictions exposed and fixed the subscription-registry loss).

const codec = createFrameCodec()
const HOST_TAG = "__host"

async function openWs(path: string, headers: Record<string, string> = {}): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket", ...headers } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<Array<ServerFrame>> {
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

/** Resolve true iff NO message arrives within `ms` — the honest way to assert a
 *  frame was NOT delivered to a socket (used for cross-delivery and auto-response
 *  negatives). */
function noMessageWithin(ws: WebSocket, ms: number): Promise<boolean> {
  return new Promise((resolve) => {
    const onMsg = (): void => {
      clearTimeout(timer)
      ws.removeEventListener("message", onMsg)
      resolve(false)
    }
    const timer = setTimeout(() => {
      ws.removeEventListener("message", onMsg)
      resolve(true)
    }, ms)
    ws.addEventListener("message", onMsg)
  })
}

const sub = (subId: string, collection: string): ClientFrame => ({ t: "sub", subId, collection })
const insert = (txId: string, collection: string, id: string, body: string): ClientFrame => ({
  t: "mut",
  txId,
  collection,
  ops: [{ type: "insert", key: id, cols: { id, body } }],
})

describe("Syncable over a partyserver-like host (ADR-0015)", () => {
  it("sync and host sockets coexist; each protocol's frames reach only its own sockets", async () => {
    const room = "coexist"
    const hostWs = await openWs(`/host/${room}/_host`)
    const syncWs = await openWs(`/host/${room}/_sync`, { "x-user": "alice" })

    // A sync frame on the sync socket is handled by the mixin (snap-end back)…
    syncWs.send(codec.encode(sub("s1", "messages")))
    const frames = await collectUntil(syncWs, (f) => f.t === "snap-end")
    expect(frames.at(-1)?.t).toBe("snap-end")

    // …and a plain string on the host socket is delegated to the host handler.
    hostWs.send("hello-host")
    await new Promise((r) => setTimeout(r, 100))

    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))
    await runInDurableObject(stub, (instance, state) => {
      const host = instance as unknown as FakeHost
      // The host saw ONLY its own frame — never the sync `sub`.
      expect(host.hostInbox).toEqual(["hello-host"])
      // Sockets are tag-partitioned: exactly one each, the discriminator the
      // wake-time restore keys on.
      expect(state.getWebSockets(SYNC_TAG).length).toBe(1)
      expect(state.getWebSockets(HOST_TAG).length).toBe(1)
      expect(state.getWebSockets().length).toBe(2)
    })
    hostWs.close()
    syncWs.close()
  })

  it("a broadcast reaches only the sync socket, never the host socket", async () => {
    const room = "broadcast-iso"
    const hostWs = await openWs(`/host/${room}/_host`)
    const syncWs = await openWs(`/host/${room}/_sync`, { "x-user": "bob" })

    syncWs.send(codec.encode(sub("s1", "messages")))
    await collectUntil(syncWs, (f) => f.t === "snap-end")

    // The host socket must receive nothing while the sync write fans out.
    const hostSilent = noMessageWithin(hostWs, 500)
    syncWs.send(codec.encode(insert("t1", "messages", "m1", "hi")))
    const out = await collectUntil(syncWs, (f) => f.t === "committed")

    // The sync client got its delta + receipt…
    expect(out.some((f) => f.t === "d" || f.t === "snap")).toBe(true)
    expect(out.some((f) => f.t === "committed")).toBe(true)
    // …and the host socket stayed silent (the broadcaster never touched it).
    expect(await hostSilent).toBe(true)
    hostWs.close()
    syncWs.close()
  })

  it("a non-sync upgrade reaches super.fetch (host claims /_host); /_sync never reaches the host", async () => {
    const room = "fetch-split"
    // /_host is claimed by the delegated host fetch.
    const hostWs = await openWs(`/host/${room}/_host`)
    // A non-upgrade request falls through to the host, which 404s its own way.
    const res = await SELF.fetch(`https://example.com/host/${room}/status`)
    expect(res.status).toBe(404)
    expect(await res.text()).toBe("host: not found")
    // /_sync is claimed by the mixin — the host never records a socket for it.
    const syncWs = await openWs(`/host/${room}/_sync`, { "x-user": "carol" })
    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))
    await runInDurableObject(stub, (_i, state) => {
      expect(state.getWebSockets(HOST_TAG).length).toBe(1) // only the /_host socket
      expect(state.getWebSockets(SYNC_TAG).length).toBe(1) // only the /_sync socket
    })
    hostWs.close()
    syncWs.close()
  })

  it("a host socket's message is delegated to super.webSocketMessage and handled by the host", async () => {
    const room = "delegate-msg"
    const hostWs = await openWs(`/host/${room}/_host`)
    hostWs.send("host-frame")
    await new Promise((r) => setTimeout(r, 100))
    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))
    await runInDurableObject(stub, (instance) => {
      expect((instance as unknown as FakeHost).hostInbox).toContain("host-frame")
    })
    hostWs.close()
  })

  it("does not shadow the host's `sql` tagged-template method", async () => {
    const room = "no-sql-shadow"
    await openWs(`/host/${room}/_sync`) // force construction
    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))
    await runInDurableObject(stub, (instance) => {
      const host = instance as unknown as FakeHost
      // `this.sql` is still the host's tagged-template FUNCTION, not a SqlStorage
      // getter — if the mixin defined a `sql` getter this would throw.
      expect(typeof host.sql).toBe("function")
      expect(host.sql<{ one: number }>`SELECT 1 AS one`).toEqual([{ one: 1 }])
    })
  })

  it("installs triggers only on registered tables; a host-owned table gets none and the reaper spares host triggers", async () => {
    const room = "trigger-safety"
    await openWs(`/host/${room}/_sync`) // registerSync runs in the constructor
    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))
    await runInDurableObject(stub, (instance, state) => {
      const sql = state.storage.sql
      const triggers = new Set(
        Array.from(sql.exec("SELECT name FROM sqlite_master WHERE type='trigger'")).map((r) => r.name as string),
      )
      // Registered table: triggers present. Host-owned table: none.
      expect(triggers.has("_sync_changes_messages_ai")).toBe(true)
      expect([...triggers].some((n) => n.includes("cf_agents_state"))).toBe(false)

      // A host write emits no CDC row (no trigger on the unregistered table).
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM _sync_changes"))[0]!.c as number
      sql.exec("INSERT INTO cf_agents_state(key, value) VALUES ('k', 'v')")
      const after = Array.from(sql.exec("SELECT count(*) AS c FROM _sync_changes"))[0]!.c as number
      expect(after).toBe(before)

      // The reaper only drops `_sync_changes_*`; a host-named trigger survives a
      // re-register (GLOB treats `_` literally — ADR-0008). Re-registering the
      // same schema is exactly what a real author's constructor does on wake.
      sql.exec(`CREATE TRIGGER cf_agents_guard AFTER INSERT ON cf_agents_state BEGIN SELECT 1; END`)
      ;(instance as unknown as { sync: { registerSync(s: typeof testSchema): void } }).sync.registerSync(testSchema)
      const stillThere = Array.from(
        sql.exec("SELECT name FROM sqlite_master WHERE type='trigger' AND name = 'cf_agents_guard'"),
      )
      expect(stillThere.length).toBe(1)
      // And the registered-table triggers are still present after the reap.
      const msgTrig = Array.from(
        sql.exec("SELECT name FROM sqlite_master WHERE type='trigger' AND name = '_sync_changes_messages_ai'"),
      )
      expect(msgTrig.length).toBe(1)
    })
  })

  it("bare DO treats a legacy untagged socket as sync (0.4.0 → mixin migration)", async () => {
    // A socket accepted by 0.4.0 carried NO tag. After the mixin upgrade, such a
    // socket can wake out of hibernation; the bare DO must still treat it as sync
    // (it owns every socket), or a pre-existing client's frames are silently
    // ignored. Simulate it by accepting an untagged socket directly.
    const room = "legacy-untagged"
    await openWs(`/sync/${room}`) // construct the DO + its schema
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, async (instance, state) => {
      const pair = new WebSocketPair()
      const server = pair[1]
      state.acceptWebSocket(server) // NO tag — exactly what 0.4.0 did
      const client = pair[0]
      client.accept()
      const got = new Promise<ServerFrame>((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("untagged socket frame ignored")), 2000)
        client.addEventListener(
          "message",
          (e) => { clearTimeout(t); resolve(codec.decode(e.data as ArrayBuffer) as ServerFrame) },
          { once: true },
        )
      })
      // The mixin must PROCESS the frame (a snap-end back), not delegate/ignore it.
      const handler = instance as unknown as { webSocketMessage(ws: WebSocket, m: ArrayBuffer): Promise<void> }
      await handler.webSocketMessage(server, codec.encode(sub("s1", "messages")) as unknown as ArrayBuffer)
      expect((await got).t).toBe("snap-end")
    })
  })

  it("configure({ caseSensitiveLike: false }) is a real toggle, not a dead option", async () => {
    const room = "toggle-pragma"
    await openWs(`/sync/${room}`)
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (instance, state) => {
      const like = (): unknown => Array.from(state.storage.sql.exec("SELECT ('A' LIKE 'a') AS m"))[0]!.m
      expect(like()).toBe(0) // bare-DO default ON → case-sensitive
      ;(instance as unknown as { sync: { configure(o: { caseSensitiveLike: boolean }): void } }).sync.configure({
        caseSensitiveLike: false,
      })
      expect(like()).toBe(1) // toggled OFF → case-insensitive
    })
  })

  it("SyncDurableObject (bare DO base) keeps 0.4.0 defaults: auto-response ON, case-sensitive LIKE ON", async () => {
    const room = "bare-defaults"
    const ws = await openWs(`/sync/${room}`)
    // Auto-response ON: a literal "ping" pongs without reaching a handler.
    ws.send("ping")
    const pong = await new Promise<string | ArrayBuffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no pong")), 2000)
      ws.addEventListener("message", (e) => { clearTimeout(t); resolve(e.data as string) }, { once: true })
    })
    expect(pong).toBe("pong")
    // Pragma ON: LIKE is case-sensitive.
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (_i, state) => {
      expect(Array.from(state.storage.sql.exec("SELECT ('A' LIKE 'a') AS m"))[0]!.m).toBe(0)
    })
    ws.close()
  })

  it("over a non-DO base the two DO-global side effects default OFF, and `configure` opts them back on", async () => {
    // Default OFF: pragma off → LIKE is case-insensitive; auto-response off → a
    // literal "ping" is NOT pong'd (it is decoded as a frame and dropped).
    const offRoom = "sidefx-off"
    const offWs = await openWs(`/host/${offRoom}/_sync`)
    const noPong = noMessageWithin(offWs, 500)
    offWs.send("ping")
    expect(await noPong).toBe(true)
    await runInDurableObject(env.HOST_DO.get(env.HOST_DO.idFromName(offRoom)), (_i, state) => {
      expect(Array.from(state.storage.sql.exec("SELECT ('A' LIKE 'a') AS m"))[0]!.m).toBe(1)
    })
    offWs.close()

    // Opted ON via configure(): pragma on → case-sensitive; auto-response on → pong.
    const onRoom = "sidefx-on"
    const onWs = await openWs(`/host-optin/${onRoom}/_sync`)
    onWs.send("ping")
    const pong = await new Promise<string | ArrayBuffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no pong")), 2000)
      onWs.addEventListener("message", (e) => { clearTimeout(t); resolve(e.data as string) }, { once: true })
    })
    expect(pong).toBe("pong")
    await runInDurableObject(env.HOST_OPTIN_DO.get(env.HOST_OPTIN_DO.idFromName(onRoom)), (_i, state) => {
      expect(Array.from(state.storage.sql.exec("SELECT ('A' LIKE 'a') AS m"))[0]!.m).toBe(0)
    })
    onWs.close()
  })
})
