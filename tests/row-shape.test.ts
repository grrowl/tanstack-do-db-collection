// WHY: the client row IS the raw `SELECT *` row — no mapping layer, no
// auto-parsing, no type coercion between SQLite and the collection. Apps key
// off exact snake_case column names, treat JSON TEXT as strings, and rely on
// scalar fidelity (INTEGER/REAL/NULL) surviving the wire. BLOB columns come
// out of workerd's SqlStorage as bare ArrayBuffer, which the codecs normalize
// to Uint8Array at emission (ADR-0017, issue #27) — before that, msgpack fell
// through to encodeMap and the client silently received {}. If any of these
// guarantees drift, synced apps corrupt data without an error.

import { createCollection } from "@tanstack/db";
import { env, runInDurableObject, SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  doCollectionOptions,
  type WebSocketLike,
  WebSocketTransport,
} from "../src/client/index.ts";
import type { SqlStorage } from "@cloudflare/workers-types";
import type { TestApi } from "./test-worker.ts";

type ServerApi = { runSyncedWrite: <T>(fn: (sql: SqlStorage) => T) => T };
const api = (i: unknown): ServerApi => i as unknown as ServerApi;

function realTransport(room: string): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, {
        headers: { Upgrade: "websocket" },
      });
      const ws = res.webSocket;
      if (!ws) throw new Error("no webSocket");
      ws.accept();
      return ws as unknown as WebSocketLike;
    },
  });
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((r) => setTimeout(r, 5));
  }
}

const TS = 1753056000000; // ms epoch, well under 2^53
const BIG = 9007199254740993n; // 2^53 + 1 — NOT representable as a JS number

describe("client row shape is the raw SELECT * row", () => {
  it("snapshot rows carry snake_case SQL column names verbatim; JSON TEXT stays a string; INTEGER/REAL/NULL survive; BLOB arrives as Uint8Array", async () => {
    const room = "row-shape-1";
    const t = realTransport(room);
    await t.connect(); // constructs the DO -> tables exist, registerSync ran

    // Widen the registered `messages` table with realistic columns.
    // The declared client Row type (MsgRow = {id, body}) knows NOTHING of these.
    await runInDurableObject(
      env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
      (_i, s) => {
        const sql = s.storage.sql;
        sql.exec("ALTER TABLE messages ADD COLUMN parent_ids TEXT");
        sql.exec("ALTER TABLE messages ADD COLUMN tool_output TEXT");
        sql.exec("ALTER TABLE messages ADD COLUMN created_at INTEGER");
        sql.exec("ALTER TABLE messages ADD COLUMN score REAL");
        sql.exec("ALTER TABLE messages ADD COLUMN is_done INTEGER");
        sql.exec("ALTER TABLE messages ADD COLUMN payload BLOB");
        sql.exec(
          "INSERT INTO messages(id, body, parent_ids, tool_output, created_at, score, is_done, payload) VALUES (?,?,?,?,?,?,?,?)",
          "m1",
          "hello",
          '["p1","p2"]',
          '{"result":{"ok":true},"n":42}',
          TS,
          0.5,
          1,
          new Uint8Array([1, 2, 3]).buffer,
        );
        // Row with NULLs and a beyond-2^53 integer stored via SQL literal.
        sql.exec(
          `INSERT INTO messages(id, body, created_at) VALUES ('m2', 'nulls', ${BIG})`,
        );
      },
    );

    const messages = createCollection(
      doCollectionOptions({
        transport: t,
        table: "messages",
        getKey: (m) => m.id,
      }),
    );
    await messages.preload();

    const row = messages.get("m1") as Record<string, unknown>;
    expect(row).toBeDefined();

    // 1. Raw SQL column names, snake_case, no mapping layer. (TanStack DB's
    // get() decorates rows with enumerable $-prefixed metadata keys — ignore.)
    expect(
      Object.keys(row)
        .filter((k) => !k.startsWith("$"))
        .sort(),
    ).toEqual([
      "body",
      "created_at",
      "id",
      "is_done",
      "parent_ids",
      "payload",
      "score",
      "tool_output",
    ]);
    expect("parentIds" in row).toBe(false);

    // 2. JSON TEXT arrives as a STRING — no auto-parse anywhere.
    expect(row.parent_ids).toBe('["p1","p2"]');
    expect(typeof row.tool_output).toBe("string");
    expect(JSON.parse(row.tool_output as string)).toEqual({
      result: { ok: true },
      n: 42,
    });

    // 3. Scalars: INTEGER -> number, REAL -> number, boolean-as-0/1 -> number.
    expect(row.created_at).toBe(TS);
    expect(typeof row.created_at).toBe("number");
    expect(row.score).toBe(0.5);
    expect(row.is_done).toBe(1); // number 1, NOT boolean true
    expect(typeof row.is_done).toBe("number");

    // 4. BLOB: workerd's SqlStorage yields a bare ArrayBuffer; the codecs
    // normalize it at emission so the client always receives a Uint8Array with
    // the exact bytes (ADR-0017, issue #27). Anything else here means BLOBs
    // are being corrupted silently again.
    expect(row.payload).toBeInstanceOf(Uint8Array);
    expect(Array.from(row.payload as Uint8Array)).toEqual([1, 2, 3]);

    // 5. NULL columns arrive as JS null.
    const m2 = messages.get("m2") as Record<string, unknown>;
    expect(m2.parent_ids).toBeNull();
    expect(m2.tool_output).toBeNull();
    expect(m2.score).toBeNull();

    // 6. INTEGER beyond 2^53: workerd's SqlStorage returns JS numbers, so
    // 2^53+1 rounds to 2^53 BEFORE the wire is involved. Known-lossy; see
    // issue #10 (wontfix) — pinned so a behaviour change is noticed.
    expect(m2.created_at).toBe(9007199254740992);

    t.close();
  });

  it("live delta rows (server-originated UPDATE) carry raw column names, string JSON, and Uint8Array BLOBs", async () => {
    const room = "row-shape-2";
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room));
    const t = realTransport(room);
    await t.connect();

    await runInDurableObject(stub, (_i, s) => {
      const sql = s.storage.sql;
      sql.exec("ALTER TABLE messages ADD COLUMN tool_output TEXT");
      sql.exec("ALTER TABLE messages ADD COLUMN updated_at INTEGER");
      sql.exec("ALTER TABLE messages ADD COLUMN payload BLOB");
      sql.exec(
        "INSERT INTO messages(id, body, tool_output, updated_at) VALUES ('s1','v1',NULL,1)",
      );
    });

    const messages = createCollection(
      doCollectionOptions({
        transport: t,
        table: "messages",
        getKey: (m) => m.id,
      }),
    );
    await messages.preload();
    expect((messages.get("s1") as Record<string, unknown>).body).toBe("v1");

    // Streaming-style server write: frequent row UPDATE via runSyncedWrite.
    await runInDurableObject(stub, (instance) => {
      api(instance).runSyncedWrite((sql) =>
        sql.exec(
          "UPDATE messages SET body = ?, tool_output = ?, updated_at = ?, payload = ? WHERE id = ?",
          "v2",
          '{"chunks":["a","b"]}',
          TS,
          new Uint8Array([9, 8, 7]).buffer,
          "s1",
        ),
      );
    });

    await waitFor(
      () => (messages.get("s1") as Record<string, unknown>).body === "v2",
    );
    const row = messages.get("s1") as Record<string, unknown>;
    // Delta hydration is SELECT * too — raw snake_case keys, string JSON,
    // number int, and the same BLOB -> Uint8Array normalization as snapshots.
    expect(
      Object.keys(row)
        .filter((k) => !k.startsWith("$"))
        .sort(),
    ).toEqual(["body", "id", "payload", "tool_output", "updated_at"]);
    expect(row.tool_output).toBe('{"chunks":["a","b"]}');
    expect(typeof row.tool_output).toBe("string");
    expect(row.updated_at).toBe(TS);
    expect(row.payload).toBeInstanceOf(Uint8Array);
    expect(Array.from(row.payload as Uint8Array)).toEqual([9, 8, 7]);

    t.close();
  });
});
