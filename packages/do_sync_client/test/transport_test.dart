// Transport semantics — each test pins a wire-contract invariant the TS
// client also holds (ADR-0002 C1 cursor, ADR-0016 reconnect, ADR-0018 guard).

import 'dart:typed_data';

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

import 'fake_socket.dart';

class RecordingHandler implements SubHandler {
  final List<String> events = [];
  @override
  void onSnap(Object? key, Object? row) => events.add('snap:$key');
  @override
  void onSnapEnd() => events.add('snap-end');
  @override
  void onDelta(RowOp op, Object? key, Map<String, Object?>? cols) => events.add('d:${op.name}:$key');
  @override
  void onUptodate() => events.add('uptodate');
  @override
  void onReset() => events.add('reset');
}

void main() {
  late FakeSocket socket;
  late List<FakeSocket> opened;

  WebSocketTransport makeTransport({
    Duration timeout = const Duration(seconds: 5),
    ReconnectDelayFn? reconnectDelay,
    void Function(int?, String?)? onClosed,
  }) {
    opened = [];
    return WebSocketTransport(
      open: () async {
        socket = FakeSocket();
        opened.add(socket);
        return socket;
      },
      timeout: timeout,
      reconnectDelay: reconnectDelay ?? (attempt, code, reason) => Duration.zero,
      onClosed: onClosed,
    );
  }

  test('cursor advances only at commit boundaries, never on snap/d frames', () async {
    final t = makeTransport();
    final h = RecordingHandler();
    await t.subscribe('s1', 'messages', h);
    expect(t.appliedCursor, '0');

    socket.receive(const SnapFrame(sub: 's1', key: 'k1', row: {'id': 'k1'}, seq: '41'));
    socket.receive(const DeltaFrame(sub: 's1', key: 'k2', op: RowOp.insert, cols: {'id': 'k2'}, seq: '42'));
    await pump();
    expect(t.appliedCursor, '0', reason: 'snap/d frames must not move the cursor');

    socket.receive(const SnapEndFrame(sub: 's1', seq: '41'));
    await pump();
    expect(t.appliedCursor, '41');

    socket.receive(const UptodateFrame(seq: '42'));
    await pump();
    expect(t.appliedCursor, '42');
    expect(h.events, ['snap:k1', 'd:insert:k2', 'snap-end', 'uptodate']);
  });

  test('sendMut completes with the committed result and advances the cursor', () async {
    final t = makeTransport();
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.sendMut(const MutFrame(txId: 'tx-1', collection: 'messages', ops: []));
    await pump();
    expect(socket.sent.whereType<MutFrame>().single.txId, 'tx-1');
    socket.receive(const CommittedFrame(txId: 'tx-1', seq: '7', result: {'n': 1}));
    final result = await fut;
    expect((result as Map<Object?, Object?>)['n'], 1);
    expect(t.appliedCursor, '7');
  });

  test('rejected surfaces a typed MutationRejectedException with the code', () async {
    final t = makeTransport();
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.sendMut(const MutFrame(txId: 'tx-2', collection: 'messages', ops: []));
    await pump();
    socket.receive(const RejectedFrame(txId: 'tx-2', code: 'DENIED', message: 'author mismatch'));
    await expectLater(
      fut,
      throwsA(isA<MutationRejectedException>()
          .having((e) => e.code, 'code', 'DENIED')
          .having((e) => e.message, 'message', 'author mismatch')),
    );
  });

  test('confirmation timeout rejects and clears the waiter', () async {
    final t = makeTransport(timeout: const Duration(milliseconds: 50));
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.sendMut(const MutFrame(txId: 'tx-3', collection: 'messages', ops: []));
    await expectLater(fut, throwsA(isA<SyncTimeoutException>()));
    // A late committed for the timed-out tx must not blow up.
    socket.receive(const CommittedFrame(txId: 'tx-3', seq: '9'));
    await pump();
    expect(t.appliedCursor, '9');
  });

  test('sendCall mints a txId and resolves with the command result', () async {
    final t = makeTransport();
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.sendCall('clearRoom', {'keep': 1});
    await pump();
    final call = socket.sent.whereType<CallFrame>().single;
    expect(call.name, 'clearRoom');
    expect(call.txId, isNotEmpty);
    socket.receive(CommittedFrame(txId: call.txId, seq: '3', result: const {'deleted': 2}));
    expect(((await fut) as Map<Object?, Object?>)['deleted'], 2);
  });

  test('awaitSeq resolves once a boundary reaches the target', () async {
    final t = makeTransport();
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.awaitSeq('10');
    socket.receive(const UptodateFrame(seq: '9'));
    await pump();
    socket.receive(const UptodateFrame(seq: '10'));
    await fut; // completes; times out otherwise
    await t.awaitSeq('5'); // already-passed target resolves immediately
  });

  test('oversize frame is rejected pre-send with FRAME_TOO_LARGE (ADR-0018)', () async {
    final t = makeTransport();
    final big = Uint8List(maxFrameBytes + 1);
    await expectLater(
      t.sendMut(MutFrame(txId: 'tx-4', collection: 'messages', ops: [
        MutOp.insert('k', {'blob': big}),
      ])),
      throwsA(isA<MutationRejectedException>().having((e) => e.code, 'code', 'FRAME_TOO_LARGE')),
    );
    // Guard fires before any socket exists: nothing was opened.
    expect(opened, isEmpty);
  });

  test('unexpected drop reconnects and resubscribes carrying since (ADR-0016)', () async {
    final t = makeTransport();
    final h = RecordingHandler();
    await t.subscribe('s1', 'messages', h, where: eq('author', 'a'), limit: 5);
    socket.receive(const SnapEndFrame(sub: 's1', seq: '20'));
    await pump();

    final first = socket;
    first.dropFromServer(); // network blip, no close code
    await pump(50);

    expect(opened, hasLength(2), reason: 'transport must reopen after the drop');
    final resub = opened[1].sent.whereType<SubFrame>().single;
    expect(resub.subId, 's1');
    expect(resub.since, '20', reason: 'resubscribe must carry the applied cursor');
    expect(resub.where, isNotNull, reason: 'predicate must survive the reconnect');
    expect(resub.limit, 5);
  });

  test('default backoff never wraps to zero at high attempt counts', () {
    // The int `<<` version overflowed around attempt 57, collapsing a long
    // outage into a zero-delay reconnect hot loop (adversarial review). The
    // policy must saturate at the cap forever, like the TS double arithmetic.
    final policy = defaultReconnectDelay(const Duration(milliseconds: 250));
    for (final attempt in [1, 10, 56, 57, 63, 64, 100, 1000]) {
      var sawPositive = false;
      for (var i = 0; i < 50; i++) {
        final d = policy(attempt, null, null);
        expect(d, isNotNull);
        expect(d!.inMilliseconds, greaterThanOrEqualTo(0));
        expect(d.inMilliseconds, lessThanOrEqualTo(30000));
        if (d.inMilliseconds > 0) sawPositive = true;
      }
      expect(sawPositive, isTrue, reason: 'attempt $attempt: jitter ceiling collapsed to zero');
    }
  });

  test('application close 4000-4999 is terminal: no retry, onClosed fires', () async {
    int? closedCode;
    final t = makeTransport(
      reconnectDelay: defaultReconnectDelay(const Duration(milliseconds: 1)),
      onClosed: (code, reason) => closedCode = code,
    );
    await t.subscribe('s1', 'messages', RecordingHandler());
    socket.dropFromServer(4403, 'auth rejected');
    await pump(50);
    expect(closedCode, 4403);
    expect(opened, hasLength(1), reason: 'terminal close must not reconnect');
  });

  test('intentional close() suppresses reconnect and fails pending work', () async {
    final t = makeTransport();
    await t.subscribe('s1', 'messages', RecordingHandler());
    final fut = t.sendMut(const MutFrame(txId: 'tx-5', collection: 'messages', ops: []));
    await pump();
    t.close();
    await expectLater(fut, throwsA(isA<TransportClosedException>()));
    await pump(50);
    expect(opened, hasLength(1));
  });

  test('undecodable and non-binary inbound messages are dropped silently', () async {
    final t = makeTransport();
    final h = RecordingHandler();
    await t.subscribe('s1', 'messages', h);
    socket.receiveRaw('a text frame');
    socket.receiveRaw(Uint8List.fromList([0xc1])); // reserved, never valid
    socket.receive(const UptodateFrame(seq: '1'));
    await pump();
    expect(t.appliedCursor, '1', reason: 'stream must survive garbage frames');
  });

  test('reset without sub fans out to every handler', () async {
    final t = makeTransport();
    final h1 = RecordingHandler();
    final h2 = RecordingHandler();
    await t.subscribe('s1', 'a', h1);
    await t.subscribe('s2', 'b', h2);
    socket.receive(const ResetFrame());
    await pump();
    expect(h1.events, ['reset']);
    expect(h2.events, ['reset']);
  });

  test('unsubscribe stops delivery and sends unsub', () async {
    final t = makeTransport();
    final h = RecordingHandler();
    await t.subscribe('s1', 'messages', h);
    t.unsubscribe('s1');
    socket.receive(const DeltaFrame(sub: 's1', key: 'k', op: RowOp.delete, seq: '2'));
    await pump();
    expect(socket.sent.whereType<UnsubFrame>().single.subId, 's1');
    expect(h.events, isEmpty);
  });
}
