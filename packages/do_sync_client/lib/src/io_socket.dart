// dart:io WebSocket adapter — the default socket opener for VM/Flutter
// (non-web) targets. Web support would add a package:web adapter behind the
// same SyncSocket interface; not needed yet.

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'transport.dart';

class IoSyncSocket implements SyncSocket {
  IoSyncSocket._(this._ws) {
    // A buffering controller rather than asBroadcastStream: events that arrive
    // before the transport attaches its listener are held, not dropped.
    _ws.listen(
      (Object? data) => _controller.add(data),
      onError: (Object e) {
        // Socket errors surface as a close; the transport's reconnect path
        // owns recovery.
        if (!_doneCompleter.isCompleted) _doneCompleter.complete();
        _controller.close();
      },
      onDone: () {
        if (!_doneCompleter.isCompleted) _doneCompleter.complete();
        _controller.close();
      },
      cancelOnError: false,
    );
  }

  final WebSocket _ws;
  final StreamController<Object?> _controller = StreamController<Object?>();
  final Completer<void> _doneCompleter = Completer<void>();

  /// Open and CONNECT a socket. Completes once the upgrade succeeds — the
  /// transport's `open` contract is "returns a connected socket".
  static Future<IoSyncSocket> connect(String url, {Map<String, dynamic>? headers}) async {
    final ws = await WebSocket.connect(url, headers: headers);
    return IoSyncSocket._(ws);
  }

  @override
  void send(Uint8List data) => _ws.add(data);

  @override
  void close([int? code, String? reason]) {
    _ws.close(code, reason);
  }

  @override
  Stream<Object?> get messages => _controller.stream;

  @override
  Future<void> get done => _doneCompleter.future;

  @override
  int? get closeCode => _ws.closeCode;

  @override
  String? get closeReason => _ws.closeReason;
}

/// Convenience opener for [WebSocketTransport]:
/// `WebSocketTransport(open: ioOpen('wss://host/sync/room'))`.
Future<SyncSocket> Function() ioOpen(String url, {Map<String, dynamic>? headers}) =>
    () => IoSyncSocket.connect(url, headers: headers);
