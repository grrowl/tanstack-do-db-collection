// In-memory SyncSocket for transport/collection tests: the test plays the
// server by pushing encoded frames into `receive` and inspecting `sent`.

import 'dart:async';
import 'dart:typed_data';

import 'package:do_sync_client/do_sync_client.dart';

class FakeSocket implements SyncSocket {
  final List<Frame> sent = [];
  final StreamController<Object?> _in = StreamController<Object?>();
  final Completer<void> _done = Completer<void>();
  void Function(Frame frame)? onSend;

  @override
  int? closeCode;
  @override
  String? closeReason;

  bool get isClosed => _done.isCompleted;

  /// Server -> client: deliver one frame.
  void receive(Frame frame) => _in.add(encodeFrame(frame));

  /// Server -> client: deliver raw bytes (for undecodable-input tests).
  void receiveRaw(Object? data) => _in.add(data);

  /// Server-side close (or network drop) with an optional close code.
  void dropFromServer([int? code, String? reason]) {
    closeCode = code;
    closeReason = reason;
    if (!_done.isCompleted) _done.complete();
    _in.close();
  }

  @override
  void send(Uint8List data) {
    if (isClosed) throw StateError('send on closed socket');
    final frame = decodeFrame(data);
    sent.add(frame);
    onSend?.call(frame);
  }

  @override
  void close([int? code, String? reason]) => dropFromServer(code, reason);

  @override
  Stream<Object?> get messages => _in.stream;

  @override
  Future<void> get done => _done.future;
}

/// Let queued microtasks/stream events settle.
Future<void> pump([int rounds = 10]) async {
  for (var i = 0; i < rounds; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}
