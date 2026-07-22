// SyncCollection — the Dart substitute for the TS client's
// `doCollectionOptions` + TanStack DB (ADR-0019 D2): a synced MIRROR only
// server frames write to, plus a pending-mutation OVERLAY shadowed over it by
// pk at read time. Eager mode (ADR-0019 D4).
//
// The invariants this file carries (each guarded by a test):
//   - Batch atomicity: frames buffer between boundaries and apply in one step
//     at snap-end/uptodate; readers/streams never observe a half-applied batch
//     (the begin()/commit() contract of ADR-0002 C1, without TanStack).
//   - Held-key catch-up insert applies as the upsert it semantically is
//     (ADR-0002 C4 — the scroll-back DuplicateKeySyncError lesson).
//   - reset => truncate + await re-snapshot; also the terminal signal for a
//     rejected sub, so `ready` must complete there too.
//   - Overlay retirement: `committed` drops the pending tx (its deltas are
//     already in the mirror — C1 ordering); `rejected`/timeout drop it too,
//     which IS the rollback. No empty-commit dance (that was TanStack's
//     direct-upsert retention, ADR-0002 C2 — see ADR-0019 D2).
//   - Eager `where` preflight: a write outside the filter would never be
//     confirmed by a delta — reject before any I/O (WriteOutsideSubError
//     parity, via the ADR-0013 floor evaluator).

import 'dart:async';

import 'predicate.dart';
import 'transport.dart';
import 'wire/frames.dart';

class WriteOutsideSubException implements Exception {
  WriteOutsideSubException(this.message);
  final String message;
  @override
  String toString() => 'WriteOutsideSubException: $message';
}

int _subSeq = 0;

/// One pending optimistic transaction: the ops of a single `mut` frame.
class _PendingTx {
  _PendingTx(this.ops);
  final List<MutOp> ops;
}

class SyncCollection {
  SyncCollection(
    this.transport,
    this.table, {
    String Function(Row row)? getKey,
    Object? where,
  })  : _getKey = getKey ?? _defaultGetKey,
        _where = where,
        _matches = compilePredicate(where),
        _subId = '$table#${++_subSeq}' {
    _ready = Completer<void>();
    // A failed first connect is not terminal: the transport's reconnect
    // machinery retries while this subscription is registered, so swallow the
    // rejection here rather than surfacing an unhandled async error.
    unawaited(transport.subscribe(_subId, table, _Handler(this), where: _where).then<void>(
      (_) {},
      onError: (Object _) {},
    ));
  }

  final WebSocketTransport transport;
  final String table;
  final String Function(Row row) _getKey;
  final Object? _where;
  final bool Function(Row row) _matches;
  final String _subId;

  static String _defaultGetKey(Row row) => row['id'] as String;

  // Synced state: only server frames write here.
  final Map<String, Row> _mirror = {};
  // Buffered batch between boundaries (staged writes + staged deletes).
  final Map<String, Row?> _staged = {};
  bool _sawSnapStart = false;
  // Pending optimistic transactions in send order.
  final Map<String, _PendingTx> _pending = {};

  late Completer<void> _ready;
  final StreamController<Map<String, Row>> _changes = StreamController.broadcast();
  int _mutSeq = 0;

  /// Completes at the first snapshot boundary (or terminal reset).
  Future<void> get ready => _ready.future;

  /// Emits the merged (mirror + overlay) state at every commit boundary and on
  /// every optimistic change — never mid-batch.
  Stream<Map<String, Row>> get changes => _changes.stream;

  /// Merged snapshot: mirror shadowed by pending optimistic ops, in send order.
  Map<String, Row> get rows {
    final out = <String, Row>{for (final e in _mirror.entries) e.key: Map.of(e.value)};
    for (final tx in _pending.values) {
      for (final op in tx.ops) {
        final key = op.key as String;
        switch (op.type) {
          case RowOp.insert:
            out[key] = Map.of(op.cols!);
          case RowOp.update:
            final base = out[key];
            if (base != null) {
              base.addAll(op.cols!);
            } else {
              out[key] = Map.of(op.cols!);
            }
          case RowOp.delete:
            out.remove(key);
        }
      }
    }
    return out;
  }

  Row? get(String key) => rows[key];

  void _emit() {
    if (_changes.hasListener) _changes.add(rows);
  }

  // --- Inbound (server) side -------------------------------------------------

  void _onSnap(Object? key, Object? row) {
    // First snap frame of a (re-)snapshot: the batch REPLACES the mirror, so
    // stage a truncate marker by clearing into a fresh staging generation.
    if (!_sawSnapStart) {
      _sawSnapStart = true;
      _staged.clear();
    }
    _staged[key as String] = (row as Map<Object?, Object?>).cast<String, Object?>();
  }

  void _onSnapEnd() {
    // Snapshot boundary: replace the mirror with the staged snapshot rows
    // atomically. (A catch-up with no snap frames is just an empty batch.)
    if (_sawSnapStart) {
      _mirror.clear();
      _sawSnapStart = false;
    }
    _applyStaged();
    if (!_ready.isCompleted) _ready.complete();
    _emit();
  }

  void _onDelta(RowOp op, Object? key, Map<String, Object?>? cols) {
    final k = key as String;
    switch (op) {
      case RowOp.delete:
        _staged[k] = null;
      case RowOp.insert:
        // Full row; a held-key insert (deleted-and-reinserted while away,
        // arriving as the latest op per key) is an upsert — replace.
        _staged[k] = Map<String, Object?>.of(cols!);
      case RowOp.update:
        // Top-level partial patch (rowUpdateMode: partial — shallow merge,
        // ADR-0002 C6); an update for an absent key upserts (the move-in
        // contract, C4). Merge over staged-then-mirror state.
        final base = _staged.containsKey(k) ? _staged[k] : _mirror[k];
        _staged[k] = {...?base, ...cols!};
    }
  }

  void _onUptodate() {
    _applyStaged();
    _emit();
  }

  void _onReset() {
    // Compaction/rotation reset: truncate; the server re-snapshots on this
    // same sub. Also the ONLY terminal signal for a rejected sub (unsupported
    // predicate / unknown collection), so `ready` must complete here or a
    // waiting caller hangs forever.
    _staged.clear();
    _sawSnapStart = false;
    _mirror.clear();
    if (!_ready.isCompleted) _ready.complete();
    _emit();
  }

  void _applyStaged() {
    for (final e in _staged.entries) {
      final row = e.value;
      if (row == null) {
        _mirror.remove(e.key);
      } else {
        _mirror[e.key] = row;
      }
    }
    _staged.clear();
  }

  // --- Outbound (mutation) side ---------------------------------------------

  /// Optimistically insert a full row; completes on the server's `committed`.
  Future<void> insert(Row row) => mutate([MutOp.insert(_getKey(row), row)]);

  /// Optimistically apply a top-level partial update to an existing row.
  Future<void> update(String key, Map<String, Object?> changes) {
    if (rows[key] == null) {
      throw StateError("update of unknown key '$key' in '$table'");
    }
    return mutate([MutOp.update(key, changes)]);
  }

  /// Optimistically delete a row.
  Future<void> delete(String key) => mutate([MutOp.delete(key)]);

  /// Send one optimistic transaction (one `mut` frame, possibly multi-op).
  /// The overlay applies immediately; it retires on `committed` (the deltas
  /// have already landed in the mirror) and rolls back on rejection/timeout.
  Future<void> mutate(List<MutOp> ops) async {
    final effective = rows;
    for (final op in ops) {
      if (_where == null || op.type == RowOp.delete) continue;
      // Eager filtered preflight on the full modified row, like the TS client.
      final modified = switch (op.type) {
        RowOp.insert => op.cols!,
        RowOp.update => {...?effective[op.key as String], ...op.cols!},
        RowOp.delete => throw StateError('unreachable'),
      };
      if (!_matches(modified)) {
        throw WriteOutsideSubException(
          "write to '$table' (key '${op.key}') falls outside the collection's where filter",
        );
      }
    }
    final txId = '$table#mut#${++_mutSeq}#${DateTime.now().microsecondsSinceEpoch}';
    _pending[txId] = _PendingTx(ops);
    _emit();
    try {
      await transport.sendMut(MutFrame(txId: txId, collection: table, ops: ops));
    } finally {
      // committed OR rejected/timeout/closed: drop the overlay. On success the
      // authoritative rows are already in the mirror (C1: deltas flush before
      // committed); on failure this IS the rollback.
      _pending.remove(txId);
      _emit();
    }
  }

  /// Tear down: unsubscribe from the server and close the change stream.
  void dispose() {
    transport.unsubscribe(_subId);
    _changes.close();
  }
}

/// Adapter object so the transport's SubHandler surface stays an interface.
class _Handler implements SubHandler {
  _Handler(this.c);
  final SyncCollection c;
  @override
  void onSnap(Object? key, Object? row) => c._onSnap(key, row);
  @override
  void onSnapEnd() => c._onSnapEnd();
  @override
  void onDelta(RowOp op, Object? key, Map<String, Object?>? cols) => c._onDelta(op, key, cols);
  @override
  void onUptodate() => c._onUptodate();
  @override
  void onReset() => c._onReset();
}
