// SyncCollection semantics — each test pins one of the ADR-carried invariants
// listed at the top of lib/src/collection.dart. The test plays the server
// through a FakeSocket, so the full transport->collection path runs.

import 'dart:async' show unawaited;

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

import 'fake_socket.dart';

void main() {
  late FakeSocket socket;
  late List<FakeSocket> opened;
  late WebSocketTransport transport;

  setUp(() {
    opened = [];
    transport = WebSocketTransport(
      open: () async {
        socket = FakeSocket();
        opened.add(socket);
        return socket;
      },
      timeout: const Duration(milliseconds: 500),
      reconnectDelay: (a, c, r) => Duration.zero,
    );
  });

  Future<String> subIdOf(SyncCollection c) async {
    await pump();
    return socket.sent.whereType<SubFrame>().last.subId;
  }

  test('snapshot applies atomically at snap-end; ready completes there', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    var readyDone = false;
    unawaited(c.ready.then((_) => readyDone = true));

    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1', 'v': 1}, seq: '10'));
    socket.receive(SnapFrame(sub: sub, key: 'k2', row: const {'id': 'k2', 'v': 2}, seq: '10'));
    await pump();
    expect(c.rows, isEmpty, reason: 'mid-batch state must never be visible');
    expect(readyDone, isFalse);

    socket.receive(SnapEndFrame(sub: sub, seq: '10'));
    await pump();
    expect(readyDone, isTrue);
    expect(c.rows.keys, unorderedEquals(['k1', 'k2']));
    expect(c.get('k1'), {'id': 'k1', 'v': 1});
  });

  test('changes stream emits only at boundaries, with merged state', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    final emissions = <Map<String, Row>>[];
    final subn = c.changes.listen(emissions.add);

    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1'}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    socket.receive(DeltaFrame(sub: sub, key: 'k2', op: RowOp.insert, cols: const {'id': 'k2'}, seq: '2'));
    socket.receive(DeltaFrame(sub: sub, key: 'k3', op: RowOp.insert, cols: const {'id': 'k3'}, seq: '3'));
    await pump();
    expect(emissions, hasLength(1), reason: 'deltas buffer until the uptodate boundary');

    socket.receive(const UptodateFrame(seq: '3'));
    await pump();
    expect(emissions, hasLength(2));
    expect(emissions.last.keys, unorderedEquals(['k1', 'k2', 'k3']));
    await subn.cancel();
  });

  test('held-key catch-up insert applies as an upsert (ADR-0002 C4)', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1', 'v': 'old', 'extra': true}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    // Deleted-and-reinserted while away arrives as `insert` for a HELD key:
    // must replace (the delta's cols are the full new row), not throw.
    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.insert, cols: const {'id': 'k1', 'v': 'new'}, seq: '5'));
    socket.receive(const UptodateFrame(seq: '5'));
    await pump();
    expect(c.get('k1'), {'id': 'k1', 'v': 'new'}, reason: 'full-row replace, stale columns gone');
  });

  test('update deltas merge partially; an absent-key update upserts (move-in)', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1', 'a': 1, 'b': 2}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.update, cols: const {'b': 3}, seq: '2'));
    socket.receive(DeltaFrame(sub: sub, key: 'k9', op: RowOp.update, cols: const {'id': 'k9', 'a': 9}, seq: '3'));
    socket.receive(const UptodateFrame(seq: '3'));
    await pump();
    expect(c.get('k1'), {'id': 'k1', 'a': 1, 'b': 3}, reason: 'top-level partial merge (C6)');
    expect(c.get('k9'), {'id': 'k9', 'a': 9}, reason: 'move-in upsert (C4)');
  });

  test('delete deltas remove; a delete then insert in one batch nets to the insert', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1'}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.delete, seq: '2'));
    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.insert, cols: const {'id': 'k1', 'v': 2}, seq: '3'));
    socket.receive(const UptodateFrame(seq: '3'));
    await pump();
    expect(c.get('k1'), {'id': 'k1', 'v': 2});
  });

  test('reset truncates, completes ready (rejected-sub terminal), and a re-snapshot replaces', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1'}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();
    expect(c.rows, hasLength(1));

    // Compaction/rotation (or stale reconnect): reset, then a fresh snapshot
    // on the SAME sub (the server's below-floor path sends exactly this).
    socket.receive(ResetFrame(sub: sub));
    await pump();
    expect(c.rows, isEmpty, reason: 'reset truncates');
    await c.ready; // must be complete — reset is also the rejected-sub terminal

    socket.receive(SnapFrame(sub: sub, key: 'k2', row: const {'id': 'k2'}, seq: '9'));
    socket.receive(SnapEndFrame(sub: sub, seq: '9'));
    await pump();
    expect(c.rows.keys, ['k2'], reason: 're-snapshot replaces the mirror');
  });

  test('optimistic insert shows immediately, confirms via delta, overlay retires', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    final fut = c.insert({'id': 'k1', 'v': 'mine'});
    expect(c.get('k1'), {'id': 'k1', 'v': 'mine'}, reason: 'optimistic overlay is instant');
    await pump();
    final mut = socket.sent.whereType<MutFrame>().single;
    expect(mut.ops.single.type, RowOp.insert);

    // Server-side C1 ordering: matched delta flushes BEFORE committed.
    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.insert, cols: const {'id': 'k1', 'v': 'mine'}, seq: '2'));
    socket.receive(const UptodateFrame(seq: '2'));
    socket.receive(CommittedFrame(txId: mut.txId, seq: '2'));
    await fut;
    expect(c.get('k1'), {'id': 'k1', 'v': 'mine'}, reason: 'row survives overlay retirement (now mirror-backed)');
  });

  test('rejected mutation rolls the overlay back and rethrows', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    final fut = c.insert({'id': 'k1', 'v': 'nope'});
    expect(c.get('k1'), isNotNull);
    await pump();
    final mut = socket.sent.whereType<MutFrame>().single;
    socket.receive(RejectedFrame(txId: mut.txId, code: 'DENIED', message: 'no'));
    await expectLater(fut, throwsA(isA<MutationRejectedException>()));
    expect(c.get('k1'), isNull, reason: 'rollback IS dropping the overlay');
  });

  test('optimistic update merges over the mirror; delete hides the row', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1', 'a': 1, 'b': 2}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    final updateFut = c.update('k1', {'b': 9});
    expect(c.get('k1'), {'id': 'k1', 'a': 1, 'b': 9});
    final deleteFut = c.delete('k1');
    expect(c.get('k1'), isNull);
    await pump();

    // Reject both: state must fall back to the untouched mirror.
    for (final mut in socket.sent.whereType<MutFrame>()) {
      socket.receive(RejectedFrame(txId: mut.txId, message: 'no'));
    }
    await expectLater(updateFut, throwsA(isA<MutationRejectedException>()));
    await expectLater(deleteFut, throwsA(isA<MutationRejectedException>()));
    expect(c.get('k1'), {'id': 'k1', 'a': 1, 'b': 2});
  });

  test('update of an unknown key fails loud', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();
    expect(() => c.update('ghost', {'a': 1}), throwsStateError);
  });

  test('eager where preflight rejects out-of-filter writes before any I/O', () async {
    final c = SyncCollection(transport, 'messages', where: eq('author', 'alice'));
    final sub = await subIdOf(c);
    expect(socket.sent.whereType<SubFrame>().last.where, isNotNull, reason: 'where rides the sub frame');
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    await expectLater(
      c.insert({'id': 'k1', 'author': 'mallory'}),
      throwsA(isA<WriteOutsideSubException>()),
    );
    expect(socket.sent.whereType<MutFrame>(), isEmpty, reason: 'preflight fires before send');
    expect(c.get('k1'), isNull, reason: 'no stranded optimistic row');

    // An in-filter write goes through.
    final ok = c.insert({'id': 'k2', 'author': 'alice'});
    await pump();
    final mut = socket.sent.whereType<MutFrame>().single;
    socket.receive(DeltaFrame(sub: sub, key: 'k2', op: RowOp.insert, cols: const {'id': 'k2', 'author': 'alice'}, seq: '2'));
    socket.receive(const UptodateFrame(seq: '2'));
    socket.receive(CommittedFrame(txId: mut.txId, seq: '2'));
    await ok;
    expect(c.get('k2'), isNotNull);
  });

  test('reconnect catch-up flows into the same collection (deltas + uptodate)', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1'}, seq: '5'));
    socket.receive(SnapEndFrame(sub: sub, seq: '5'));
    await pump();

    socket.dropFromServer();
    await pump(50);
    expect(opened, hasLength(2));
    final resub = socket.sent.whereType<SubFrame>().single;
    expect(resub.since, '5');

    // Server catch-up: latest-op-per-key deltas, then uptodate. k1 was
    // deleted while away; k2 appeared.
    socket.receive(DeltaFrame(sub: sub, key: 'k1', op: RowOp.delete, seq: '8'));
    socket.receive(DeltaFrame(sub: sub, key: 'k2', op: RowOp.insert, cols: const {'id': 'k2'}, seq: '8'));
    socket.receive(const UptodateFrame(seq: '8'));
    await pump();
    expect(c.rows.keys, ['k2']);
    expect(transport.appliedCursor, '8');
  });

  test('multi-op mutate is one frame, one overlay, retired together', () async {
    final c = SyncCollection(transport, 'messages');
    final sub = await subIdOf(c);
    socket.receive(SnapFrame(sub: sub, key: 'k1', row: const {'id': 'k1', 'v': 1}, seq: '1'));
    socket.receive(SnapEndFrame(sub: sub, seq: '1'));
    await pump();

    final fut = c.mutate([
      MutOp.insert('k2', {'id': 'k2'}),
      MutOp.update('k1', {'v': 2}),
      MutOp.delete('k1'),
    ]);
    expect(c.rows.keys, ['k2'], reason: 'ops apply in order within the overlay');
    await pump();
    final mut = socket.sent.whereType<MutFrame>().single;
    expect(mut.ops, hasLength(3));
    socket.receive(RejectedFrame(txId: mut.txId, message: 'no'));
    await expectLater(fut, throwsA(isA<MutationRejectedException>()));
    expect(c.rows.keys, ['k1'], reason: 'whole transaction rolls back together');
  });
}
