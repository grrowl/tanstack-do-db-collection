// Live end-to-end against a REAL SyncDurableObject served by workerd
// (ADR-0019 D6.3) — the "speaks the same protocol" proof.
//
// Requires the chat example running locally:
//   cd examples/chat && npx wrangler dev --port 8787
// then:
//   DO_SYNC_E2E_URL=ws://localhost:8787 dart test test/e2e_test.dart
//
// Skipped (not failed) when DO_SYNC_E2E_URL is unset, so the offline suite
// stays green; CI wires both halves together.

@Tags(['e2e'])
library;

import 'dart:io' show Platform;
import 'dart:math';

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

void main() {
  final base = Platform.environment['DO_SYNC_E2E_URL'];

  (WebSocketTransport, SyncCollection) client(String room, String user) {
    final transport = WebSocketTransport(
      open: ioOpen('$base/sync?room=$room&user=$user'),
      timeout: const Duration(seconds: 10),
    );
    final messages = SyncCollection(transport, 'messages');
    return (transport, messages);
  }

  Map<String, Object?> message(String id, String author, String content) => {
        'id': id,
        'author': author,
        'content': content,
        'created_at': DateTime.now().millisecondsSinceEpoch,
      };

  test('snapshot, live delta, optimistic confirm, reject+rollback, command', () async {
    final room = 'dart-e2e-${Random().nextInt(1 << 32)}';

    // --- Client A: empty snapshot, then a confirmed optimistic insert.
    final (ta, a) = client(room, 'alice');
    await a.ready.timeout(const Duration(seconds: 10));
    expect(a.rows, isEmpty);

    await a.insert(message('m1', 'alice', 'hello from Dart'));
    // committed arrived; C1 ordering means the delta is already applied.
    expect(a.get('m1'), isNotNull);
    expect(a.get('m1')!['content'], 'hello from Dart');

    // --- Client B: snapshot carries A's row with the D1 type mapping.
    final (tb, b) = client(room, 'bob');
    await b.ready.timeout(const Duration(seconds: 10));
    expect(b.get('m1'), isNotNull, reason: 'snapshot must include the earlier insert');
    expect(b.get('m1')!['author'], 'alice');
    // created_at was written as a JS-number-range int; SQLite INTEGER comes
    // back within 32 bits as int, beyond as float64 — accept num, require the
    // exact value both clients hold.
    final aCreated = (a.get('m1')!['created_at'] as num).toInt();
    expect((b.get('m1')!['created_at'] as num).toInt(), aCreated);

    // --- Live delta: A inserts, B sees it without resubscribing.
    final bSawM2 = b.changes.firstWhere((rows) => rows.containsKey('m2'));
    await a.insert(message('m2', 'alice', 'second'));
    await bSawM2.timeout(const Duration(seconds: 10));
    expect(b.get('m2')!['content'], 'second');

    // --- Authorization rejection: author != connected user -> rejected frame,
    // typed exception, overlay rollback.
    await expectLater(
      b.insert(message('m3', 'not-bob', 'forged')),
      throwsA(isA<MutationRejectedException>()),
    );
    expect(b.get('m3'), isNull, reason: 'rejected optimistic row must roll back');
    expect(a.get('m3'), isNull);

    // --- Command round-trip + fan-out: clearRoom returns its result on
    // committed and the deletes broadcast to both clients.
    final aEmpty = a.changes.firstWhere((rows) => rows.isEmpty);
    final result = await tb.sendCall('clearRoom');
    expect((result as Map<Object?, Object?>)['deleted'], 2);
    await aEmpty.timeout(const Duration(seconds: 10));
    expect(b.rows, isEmpty);

    // --- Cursors advanced and agree on the single stream.
    expect(BigInt.parse(ta.appliedCursor) > BigInt.zero, isTrue);

    a.dispose();
    b.dispose();
    ta.close();
    tb.close();
  }, skip: base == null ? 'set DO_SYNC_E2E_URL (see header) to run against a live DO' : false);

  test('second transport starts from the durable snapshot (server persistence)', () async {
    final room = 'dart-e2e-persist-${Random().nextInt(1 << 32)}';
    final (ta, a) = client(room, 'alice');
    await a.ready.timeout(const Duration(seconds: 10));
    await a.insert(message('p1', 'alice', 'persisted'));
    a.dispose();
    ta.close();

    final (tc, c) = client(room, 'carol');
    await c.ready.timeout(const Duration(seconds: 10));
    expect(c.get('p1'), isNotNull, reason: 'a fresh socket must see DO-persisted state');
    c.dispose();
    tc.close();
  }, skip: base == null ? 'set DO_SYNC_E2E_URL (see header) to run against a live DO' : false);
}
