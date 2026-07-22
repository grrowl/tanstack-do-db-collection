# do_sync_client

Dart client for
[`tanstack-durable-object-sync`](../../README.md) — the same wire protocol the
TS browser client speaks, against an **unmodified** `SyncDurableObject`. Built
for Flutter (any non-web Dart VM target today); design rationale in
[ADR-0019](../../docs/adr/0019-client-agnostic-wire-contract-dart-client.md).

Where the TS client leans on TanStack DB for its reactive layer, this package
substitutes the platform-native equivalents: a synced **mirror** only server
frames write to, a pending-mutation **overlay** shadowed over it by pk (the
client-supplied-key invariant makes optimistic id == confirmed id, so rollback
is just dropping the overlay), and `Stream`s that emit **only at commit
boundaries** — never a half-applied batch. No TanStack port, no IVM: filter,
sort, and join client-side in Dart (or feed the boundary-committed changes into
Drift/SQLite later).

Zero runtime dependencies. The MessagePack codec is hand-rolled to the exact
dialect of the TS endpoints and pinned by cross-language conformance fixtures
(see below).

## Use

```dart
import 'package:do_sync_client/do_sync_client.dart';

final transport = WebSocketTransport(
  open: ioOpen('wss://host/sync/$sessionId'),
);

// One transport per DO, shared by every collection on it.
final messages = SyncCollection(transport, 'messages');
await messages.ready; // first snapshot applied

// Reactive reads: merged (synced + optimistic) state at every boundary.
messages.changes.listen((rows) => render(rows.values));

// Optimistic writes: instant locally, completes on the server's `committed`
// riding the same stream (no second ack channel). Rejection rolls back.
await messages.insert({
  'id': ulid(), // client-supplied pk — you mint it
  'author': userId,
  'content': 'hello',
  'created_at': DateTime.now().millisecondsSinceEpoch,
});

// Commands (escape-hatch writes with results):
final result = await transport.sendCall('clearRoom'); // {deleted: n}

// Filtered eager sync; out-of-filter writes are rejected before any I/O.
final mine = SyncCollection(transport, 'messages', where: eq('author', userId));
```

The predicate builder (`eq`, `gt`, `gte`, `lt`, `lte`, `like`, `inList`,
`and`, `or`, `not`) emits the ADR-0013 operator floor — the set the server's
SQL and JS evaluators are verified to agree on. Anything else the server
rejects with a `reset`, and this package's evaluator refuses to compile.

### Value mapping (ADR-0019 D1)

| wire | JS | Dart |
|---|---|---|
| int formats (≤32-bit) | `number` | `int` |
| float64 — incl. **all integers beyond 32-bit** (e.g. `Date.now()` values) | `number` | `double` |
| int64/uint64 | `bigint` | `BigInt` |
| timestamp ext −1 | `Date` | `DateTime` (UTC) |
| bin / ext 0 | `Uint8Array` | `Uint8List` |

A Dart `int` above 32 bits encodes as float64 (so JS peers read a `number`);
ints ≥ 2^53 are rejected loudly — use `BigInt` for true 64-bit values.

### What's deferred

`syncMode: 'on-demand'` / `loadSubset` windows and the atomic cursor `fetch`
(ADR-0003/0005); typed command/row codegen from the TS schema; a `package:web`
socket adapter; a Drift persistence adapter. See ADR-0019 D4.

## Tests

```sh
dart test                      # unit + conformance suites (offline)
```

Cross-language conformance (run from the repo root; production TS codec on
both sides):

```sh
npm run conformance:gen        # TS encodes golden frames -> test/fixtures/
dart test test/conformance_test.dart   # Dart decodes, asserts, re-encodes
npm run conformance:verify     # TS decodes Dart bytes, deep-compares
```

Live end-to-end against a real DO in workerd:

```sh
cd examples/chat && npx wrangler dev --port 8787   # terminal 1
DO_SYNC_E2E_URL=ws://localhost:8787 dart test test/e2e_test.dart
```
