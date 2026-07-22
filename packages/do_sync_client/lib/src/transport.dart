// WebSocketTransport — the Dart port of `src/client/transport.ts`, one per
// Durable Object.
//
// The single-cursor half of the inversion (ADR-0002 C1). It tracks exactly one
// position, `appliedSeq`, advanced only at commit boundaries (snap-end /
// uptodate / committed) — never on individual snap/d frames — so the cursor is
// always a contiguous applied prefix. There is no second ("acked") cursor.
//
// Confirmation rides the same stream: `sendMut`/`sendCall` complete when the
// server's `committed` for that txId arrives. Because the server flushes a
// mutation's matched deltas before its `committed` (server-side C1 ordering),
// by the time `committed` is processed the deltas are already applied and
// `appliedSeq >= commitSeq` holds.
//
// The socket opener is injectable: the default opens `dart:io`'s WebSocket
// (see io_socket.dart); tests provide an in-memory socket.

import 'dart:async';
import 'dart:math';
import 'dart:typed_data';

import 'wire/frame_codec.dart';
import 'wire/frames.dart';

/// Minimal structural socket — satisfied by `dart:io` WebSocket (via
/// [IoSyncSocket]) and by test fakes.
abstract interface class SyncSocket {
  /// Send one binary frame.
  void send(Uint8List data);

  void close([int? code, String? reason]);

  /// Inbound messages: [Uint8List]/[List<int>] for binary frames.
  Stream<Object?> get messages;

  /// Completes when the socket closes (either side). Close code/reason are
  /// readable afterwards.
  Future<void> get done;
  int? get closeCode;
  String? get closeReason;
}

/// Cloudflare's inbound WebSocket edge cap, ~1 MiB (ADR-0018). An
/// infrastructure FACT, not a knob — mirrors the TS client's constant.
const int maxFrameBytes = 1048576;

class MutationRejectedException implements Exception {
  MutationRejectedException(this.message, [this.code]);
  final String message;
  final String? code;
  @override
  String toString() => 'MutationRejectedException(${code ?? '-'}): $message';
}

class TransportClosedException implements Exception {
  @override
  String toString() => 'TransportClosedException: transport closed';
}

class SyncTimeoutException implements Exception {
  SyncTimeoutException(this.message);
  final String message;
  @override
  String toString() => 'SyncTimeoutException: $message';
}

/// A collection adapter's view of one subscription's inbound frames.
abstract interface class SubHandler {
  void onSnap(Object? key, Object? row);
  void onSnapEnd();
  void onDelta(RowOp op, Object? key, Map<String, Object?>? cols);
  void onUptodate();
  void onReset();
}

/// Reconnect delay policy (ADR-0016). Called once per reconnect attempt with
/// the 1-based attempt number (reset after each successful open) and, when the
/// drop came from a socket close, that close's code/reason. Return the delay
/// before the next attempt, or `null` to stop reconnecting (terminal — the
/// transport surfaces it via `onClosed`).
typedef ReconnectDelayFn = Duration? Function(int attempt, int? closeCode, String? closeReason);

/// The default reconnect policy: capped exponential backoff with full jitter —
/// a uniform delay in [0, min(cap, base*2^(attempt-1))], cap 30s — and
/// application close codes (4000-4999) are terminal: the server closed
/// deliberately (e.g. an auth rejection), so retrying cannot succeed.
ReconnectDelayFn defaultReconnectDelay(Duration base, {Duration cap = const Duration(seconds: 30)}) {
  final capMs = max(cap.inMilliseconds, base.inMilliseconds).toDouble();
  final rng = Random();
  return (attempt, closeCode, closeReason) {
    if (closeCode != null && closeCode >= 4000 && closeCode <= 4999) return null;
    // Double arithmetic like the TS policy (baseMs * 2 ** (attempt-1)): the
    // exponential saturates at the cap instead of wrapping — 64-bit int `<<`
    // here would overflow to <= 0 around attempt 57 and turn a long outage
    // into a zero-delay reconnect hot loop (adversarial review).
    final ceiling = min(capMs, base.inMilliseconds * pow(2.0, attempt - 1).toDouble());
    return Duration(milliseconds: (rng.nextDouble() * ceiling).floor());
  };
}

class _SeqWaiter {
  _SeqWaiter(this.target, this.completer, this.timer);
  final BigInt target;
  final Completer<void> completer;
  final Timer timer;
}

class _TxWaiter {
  _TxWaiter(this.completer, this.timer);
  final Completer<Object?> completer;
  final Timer timer;
}

class _SubEntry {
  _SubEntry(this.handler, this.collection, this.where, this.orderBy, this.limit);
  final SubHandler handler;
  final String collection;
  final Object? where;
  final Object? orderBy;
  final int? limit;
}

class WebSocketTransport {
  WebSocketTransport({
    required Future<SyncSocket> Function() open,
    this.timeout = const Duration(seconds: 5),
    ReconnectDelayFn? reconnectDelay,
    this.onClosed,
  })  : _open = open,
        _reconnectDelay = reconnectDelay ?? defaultReconnectDelay(const Duration(milliseconds: 250));

  final Future<SyncSocket> Function() _open;
  final Duration timeout;
  final ReconnectDelayFn _reconnectDelay;

  /// Called when an unexpected drop is TERMINAL — the policy returned `null`
  /// (default: any 4000-4999 application close) — so the app can tell
  /// "re-auth needed" from a transient blip the transport is still retrying.
  final void Function(int? code, String? reason)? onClosed;

  SyncSocket? _ws;
  Future<void>? _connectFuture;
  StreamSubscription<Object?>? _messagesSub;

  final Map<String, _SubEntry> _handlers = {};
  BigInt _appliedSeq = BigInt.zero;
  final List<_SeqWaiter> _seqWaiters = [];
  final Map<String, _TxWaiter> _pendingTx = {};
  final Map<String, ({Completer<List<Object?>> completer, Timer timer})> _pendingFetches = {};

  bool _intentionallyClosed = false;
  bool _reconnecting = false;
  int _reconnectAttempt = 0;
  Timer? _reconnectTimer;

  /// Bumped by close(). A connect() body captures it before awaiting open();
  /// a mismatch after the await means close() ran mid-flight — the resolved
  /// socket must be discarded, not installed.
  int _closeEpoch = 0;

  final Random _rng = Random.secure();

  /// Highest committed position the client has applied (stringified bigint).
  String get appliedCursor => _appliedSeq.toString();

  Future<void> connect() {
    if (_ws != null) return Future.value();
    final existing = _connectFuture;
    if (existing != null) return existing;
    final future = _doConnect();
    _connectFuture = future;
    // A socket that never OPENED fires no close event, so the close-handler
    // recovery path can't run. Clear the cached failure so the next connect()
    // starts fresh, and re-arm the timer while subscriptions are live.
    future.catchError((Object _) {
      _connectFuture = null;
      if (!_intentionallyClosed && _handlers.isNotEmpty) {
        _scheduleReconnect();
      }
    });
    return future;
  }

  Future<void> _doConnect() async {
    final epoch = _closeEpoch;
    final ws = await _open();
    if (epoch != _closeEpoch) {
      // close() ran while open() was in flight: discard the orphan.
      try {
        ws.close();
      } catch (_) {}
      return;
    }
    _messagesSub = ws.messages.listen((data) => _onMessage(data));
    unawaited(ws.done.then((_) {
      // Only the CURRENT socket's close may detach/reconnect. A stale socket's
      // late close must not null the live connection.
      if (!identical(_ws, ws)) return;
      _ws = null;
      _connectFuture = null;
      if (!_intentionallyClosed && _handlers.isNotEmpty) {
        _scheduleReconnect(ws.closeCode, ws.closeReason);
      }
    }));
    _ws = ws;
    // A successful open resets the backoff and supersedes any pending timer.
    _reconnectAttempt = 0;
    _clearReconnectTimer();
    // On a reconnect, re-establish every subscription from our single applied
    // cursor so the server serves a windowed catch-up rather than a snapshot.
    if (_reconnecting) {
      _reconnecting = false;
      _resubscribeAll();
    }
  }

  /// Consult the policy and either arm the next reconnect attempt or stop.
  void _scheduleReconnect([int? closeCode, String? closeReason]) {
    // Set at SCHEDULING time, not in the timer: a demand-driven connect()
    // inside the reconnect window must run the resubscribe path too. Also set
    // on a TERMINAL close, so an app-driven connect() after e.g. re-auth
    // still resubscribes from the cursor.
    _reconnecting = true;
    _clearReconnectTimer();
    final delay = _reconnectDelay(++_reconnectAttempt, closeCode, closeReason);
    if (delay == null) {
      onClosed?.call(closeCode, closeReason);
      return;
    }
    _reconnectTimer = Timer(delay, () {
      _reconnectTimer = null;
      connect().catchError((Object _) {
        // next attempt retries via the connect-failure path above
      });
    });
  }

  void _clearReconnectTimer() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  /// Re-send a `sub` for every registered subscription, carrying `since`.
  void _resubscribeAll() {
    final since = appliedCursor;
    for (final entry in _handlers.entries) {
      _sendFrame(SubFrame(
        subId: entry.key,
        collection: entry.value.collection,
        where: entry.value.where,
        orderBy: entry.value.orderBy,
        limit: entry.value.limit,
        since: since,
      ));
    }
  }

  void close() {
    _intentionallyClosed = true;
    _closeEpoch++;
    _clearReconnectTimer();
    for (final w in List.of(_seqWaiters)) {
      w.timer.cancel();
      w.completer.completeError(TransportClosedException());
    }
    _seqWaiters.clear();
    for (final w in _pendingTx.values) {
      w.timer.cancel();
      w.completer.completeError(TransportClosedException());
    }
    _pendingTx.clear();
    for (final w in _pendingFetches.values) {
      w.timer.cancel();
      w.completer.completeError(TransportClosedException());
    }
    _pendingFetches.clear();
    try {
      _ws?.close();
    } catch (_) {}
    _messagesSub?.cancel();
    _messagesSub = null;
    _ws = null;
    _connectFuture = null;
  }

  Future<void> subscribe(
    String subId,
    String collection,
    SubHandler handler, {
    Object? where,
    Object? orderBy,
    int? limit,
  }) async {
    _handlers[subId] = _SubEntry(handler, collection, where, orderBy, limit);
    await connect();
    _sendFrame(SubFrame(subId: subId, collection: collection, where: where, orderBy: orderBy, limit: limit));
  }

  void unsubscribe(String subId) {
    _handlers.remove(subId);
    if (_ws != null) _sendFrame(UnsubFrame(subId: subId));
  }

  /// Send a `mut` frame; completes with the commit receipt's `result` (if any)
  /// when the server's `committed` for its txId arrives.
  Future<Object?> sendMut(MutFrame frame) => _sendAwaitingReceipt(frame, frame.txId);

  /// Invoke a server command by name. Builds the `call` frame internally and
  /// mints the txId. Completes with the command's result on `committed`.
  Future<Object?> sendCall(String name, [Object? args]) {
    final txId = _uuid4();
    return _sendAwaitingReceipt(CallFrame(txId: txId, name: name, args: args), txId);
  }

  /// Resolves once `appliedSeq >= target`.
  Future<void> awaitSeq(String target) {
    final t = BigInt.parse(target);
    if (_appliedSeq >= t) return Future.value();
    final completer = Completer<void>();
    late final Timer timer;
    timer = Timer(timeout, () {
      _seqWaiters.removeWhere((w) => identical(w.completer, completer));
      completer.completeError(SyncTimeoutException('awaitSeq timeout: target=$target'));
    });
    _seqWaiters.add(_SeqWaiter(t, completer, timer));
    return completer.future;
  }

  Future<Object?> _sendAwaitingReceipt(Frame frame, String txId) async {
    // Pre-send size guard (ADR-0018): an oversize frame may never reach the DO
    // past the ~1 MiB edge cap — reject typed and immediate rather than
    // waiting out the confirmation timeout. Encode once; bytes are reused.
    final encoded = encodeFrame(frame);
    if (encoded.length > maxFrameBytes) {
      throw MutationRejectedException(
        'frame too large (${encoded.length} bytes > the $maxFrameBytes-byte inbound WebSocket edge cap)',
        'FRAME_TOO_LARGE',
      );
    }
    await connect();
    final completer = Completer<Object?>();
    final timer = Timer(timeout, () {
      _pendingTx.remove(txId);
      completer.completeError(SyncTimeoutException('confirmation timeout: txId=$txId'));
    });
    _pendingTx[txId] = _TxWaiter(completer, timer);
    // A socket may refuse a send synchronously. Clean up the waiter/timer
    // before rethrowing, or the stale entry lingers with an armed timeout.
    try {
      _sendRaw(encoded);
    } catch (e) {
      timer.cancel();
      _pendingTx.remove(txId);
      rethrow;
    }
    return completer.future;
  }

  void _sendFrame(Frame frame) => _sendRaw(encodeFrame(frame));

  void _sendRaw(Uint8List data) {
    final ws = _ws;
    if (ws == null) throw StateError('transport not connected');
    ws.send(data);
  }

  void _onMessage(Object? data) {
    Frame frame;
    try {
      final bytes = switch (data) {
        Uint8List d => d,
        List<int> d => Uint8List.fromList(d),
        _ => throw const FormatException('non-binary message'),
      };
      frame = decodeFrame(bytes);
    } catch (_) {
      return; // undecodable frames are dropped, mirroring the TS client
    }
    switch (frame) {
      case SnapFrame f:
        _handlers[f.sub]?.handler.onSnap(f.key, f.row);
      case SnapEndFrame f:
        _handlers[f.sub]?.handler.onSnapEnd();
        _advance(f.seq);
      case DeltaFrame f:
        _handlers[f.sub]?.handler.onDelta(f.op, f.key, f.cols);
      case UptodateFrame f:
        for (final entry in _handlers.values) {
          entry.handler.onUptodate();
        }
        _advance(f.seq);
      case CommittedFrame f:
        final w = _pendingTx.remove(f.txId);
        if (w != null) {
          w.timer.cancel();
          w.completer.complete(f.result);
        }
        _advance(f.seq);
      case RejectedFrame f:
        final w = _pendingTx.remove(f.txId);
        if (w != null) {
          w.timer.cancel();
          w.completer.completeError(MutationRejectedException(f.message, f.code));
        }
      case PageFrame f:
        final w = _pendingFetches.remove(f.fetchId);
        if (w != null) {
          w.timer.cancel();
          w.completer.complete(f.rows);
        }
      case ResetFrame f:
        final sub = f.sub;
        if (sub != null) {
          _handlers[sub]?.handler.onReset();
        } else {
          for (final entry in _handlers.values) {
            entry.handler.onReset();
          }
        }
      // Client->server frames arriving inbound are protocol violations; drop.
      case SubFrame _:
      case UnsubFrame _:
      case MutFrame _:
      case CallFrame _:
      case FetchFrame _:
        return;
    }
  }

  void _advance(String seq) {
    final s = BigInt.parse(seq);
    if (s > _appliedSeq) _appliedSeq = s;
    if (_seqWaiters.isEmpty) return;
    _seqWaiters.removeWhere((w) {
      if (_appliedSeq >= w.target) {
        w.timer.cancel();
        w.completer.complete();
        return true;
      }
      return false;
    });
  }

  String _uuid4() {
    final b = Uint8List(16);
    for (var i = 0; i < 16; i++) {
      b[i] = _rng.nextInt(256);
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    String h(int i) => b[i].toRadixString(16).padLeft(2, '0');
    return '${h(0)}${h(1)}${h(2)}${h(3)}-${h(4)}${h(5)}-${h(6)}${h(7)}-${h(8)}${h(9)}-${h(10)}${h(11)}${h(12)}${h(13)}${h(14)}${h(15)}';
  }
}
