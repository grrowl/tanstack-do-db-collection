// Wire protocol frames — the Dart mirror of `src/wire/frames.ts` (ADR-0001
// §single-ordered-stream, ADR-0002 §2, ADR-0019 D1).
//
// One ordered stream per DO. The client tracks a single cursor (appliedSeq);
// `seq` is opaque (a stringified bigint). Confirmation rides the same stream:
// `committed`/`rejected` correlate to a client `txId`; there is no second ack
// channel.
//
// Field names and shapes match the TS union exactly; optional fields are
// OMITTED from the wire map when null (the TS decoder reads an absent key as
// `undefined`, the same "not provided" the TS client sends).

/// `insert` | `update` | `delete`.
enum RowOp {
  insert,
  update,
  delete;

  static RowOp fromWire(String s) => switch (s) {
        'insert' => RowOp.insert,
        'update' => RowOp.update,
        'delete' => RowOp.delete,
        _ => throw FormatException('unknown row op: $s'),
      };
}

/// One row operation inside a `mut` frame. `cols` is the full row for an
/// insert, a top-level partial for an update, absent for a delete.
class MutOp {
  const MutOp.insert(this.key, Map<String, Object?> this.cols) : type = RowOp.insert;
  const MutOp.update(this.key, Map<String, Object?> this.cols) : type = RowOp.update;
  const MutOp.delete(this.key) : type = RowOp.delete, cols = null;

  final RowOp type;
  final Object? key;
  final Map<String, Object?>? cols;

  Map<String, Object?> toWire() => {
        'type': type.name,
        'key': key,
        if (cols != null) 'cols': cols,
      };
}

sealed class Frame {
  const Frame();

  Map<String, Object?> toWire();

  /// Decode a wire map into a typed frame. Unknown tags throw [FormatException]
  /// — the transport drops undecodable messages, mirroring the TS client.
  static Frame fromWire(Map<String, Object?> m) {
    final t = m['t'];
    return switch (t) {
      'sub' => SubFrame(
          subId: m['subId'] as String,
          collection: m['collection'] as String,
          where: m['where'],
          orderBy: m['orderBy'],
          limit: (m['limit'] as num?)?.toInt(),
          offset: (m['offset'] as num?)?.toInt(),
          since: m['since'] as String?,
        ),
      'unsub' => UnsubFrame(subId: m['subId'] as String),
      'mut' => MutFrame(
          txId: m['txId'] as String,
          collection: m['collection'] as String,
          ops: (m['ops'] as List<Object?>).map((o) {
            final op = o as Map<String, Object?>;
            final type = RowOp.fromWire(op['type'] as String);
            return switch (type) {
              RowOp.insert => MutOp.insert(op['key'], (op['cols'] as Map<String, Object?>?) ?? const {}),
              RowOp.update => MutOp.update(op['key'], (op['cols'] as Map<String, Object?>?) ?? const {}),
              RowOp.delete => MutOp.delete(op['key']),
            };
          }).toList(),
        ),
      'call' => CallFrame(txId: m['txId'] as String, name: m['name'] as String, args: m['args']),
      'fetch' => FetchFrame(
          fetchId: m['fetchId'] as String,
          collection: m['collection'] as String,
          where: m['where'],
          cursor: m['cursor'] as Map<String, Object?>?,
          orderBy: m['orderBy'],
          limit: (m['limit'] as num?)?.toInt(),
        ),
      'snap' => SnapFrame(sub: m['sub'] as String, key: m['key'], row: m['row'], seq: m['seq'] as String),
      'snap-end' => SnapEndFrame(sub: m['sub'] as String, seq: m['seq'] as String),
      'd' => DeltaFrame(
          sub: m['sub'] as String,
          key: m['key'],
          op: RowOp.fromWire(m['op'] as String),
          cols: m['cols'] as Map<String, Object?>?,
          seq: m['seq'] as String,
        ),
      'uptodate' => UptodateFrame(seq: m['seq'] as String),
      'committed' => CommittedFrame(txId: m['txId'] as String, seq: m['seq'] as String, result: m['result']),
      'rejected' => RejectedFrame(
          txId: m['txId'] as String,
          code: (m['error'] as Map<String, Object?>?)?['code'] as String?,
          message: ((m['error'] as Map<String, Object?>?)?['message'] as String?) ?? 'rejected',
        ),
      'reset' => ResetFrame(sub: m['sub'] as String?),
      'page' => PageFrame(
          fetchId: m['fetchId'] as String,
          rows: m['rows'] as List<Object?>,
          seq: m['seq'] as String,
        ),
      _ => throw FormatException('unknown frame tag: $t'),
    };
  }
}

// --- Client -> server -------------------------------------------------------

class SubFrame extends Frame {
  const SubFrame({
    required this.subId,
    required this.collection,
    this.where,
    this.orderBy,
    this.limit,
    this.offset,
    this.since,
  });
  final String subId;
  final String collection;
  final Object? where;
  final Object? orderBy;
  final int? limit;
  final int? offset;
  final String? since;

  @override
  Map<String, Object?> toWire() => {
        't': 'sub',
        'subId': subId,
        'collection': collection,
        if (where != null) 'where': where,
        if (orderBy != null) 'orderBy': orderBy,
        if (limit != null) 'limit': limit,
        if (offset != null) 'offset': offset,
        if (since != null) 'since': since,
      };
}

class UnsubFrame extends Frame {
  const UnsubFrame({required this.subId});
  final String subId;

  @override
  Map<String, Object?> toWire() => {'t': 'unsub', 'subId': subId};
}

class MutFrame extends Frame {
  const MutFrame({required this.txId, required this.collection, required this.ops});
  final String txId;
  final String collection;
  final List<MutOp> ops;

  @override
  Map<String, Object?> toWire() => {
        't': 'mut',
        'txId': txId,
        'collection': collection,
        'ops': ops.map((o) => o.toWire()).toList(),
      };
}

class CallFrame extends Frame {
  const CallFrame({required this.txId, required this.name, this.args});
  final String txId;
  final String name;
  final Object? args;

  @override
  // `args` is always present (the TS client sends `undefined` args as an
  // explicit member; the server reads both null and absent as "no args").
  Map<String, Object?> toWire() => {'t': 'call', 'txId': txId, 'name': name, 'args': args};
}

class FetchFrame extends Frame {
  const FetchFrame({
    required this.fetchId,
    required this.collection,
    this.where,
    this.cursor,
    this.orderBy,
    this.limit,
  });
  final String fetchId;
  final String collection;
  final Object? where;
  final Map<String, Object?>? cursor; // { whereFrom, whereCurrent }
  final Object? orderBy;
  final int? limit;

  @override
  Map<String, Object?> toWire() => {
        't': 'fetch',
        'fetchId': fetchId,
        'collection': collection,
        if (where != null) 'where': where,
        if (cursor != null) 'cursor': cursor,
        if (orderBy != null) 'orderBy': orderBy,
        if (limit != null) 'limit': limit,
      };
}

// --- Server -> client -------------------------------------------------------

class SnapFrame extends Frame {
  const SnapFrame({required this.sub, required this.key, required this.row, required this.seq});
  final String sub;
  final Object? key;
  final Object? row;
  final String seq;

  @override
  Map<String, Object?> toWire() => {'t': 'snap', 'sub': sub, 'key': key, 'row': row, 'seq': seq};
}

class SnapEndFrame extends Frame {
  const SnapEndFrame({required this.sub, required this.seq});
  final String sub;
  final String seq;

  @override
  Map<String, Object?> toWire() => {'t': 'snap-end', 'sub': sub, 'seq': seq};
}

class DeltaFrame extends Frame {
  const DeltaFrame({required this.sub, required this.key, required this.op, this.cols, required this.seq});
  final String sub;
  final Object? key;
  final RowOp op;
  final Map<String, Object?>? cols;
  final String seq;

  @override
  Map<String, Object?> toWire() => {
        't': 'd',
        'sub': sub,
        'key': key,
        'op': op.name,
        if (cols != null) 'cols': cols,
        'seq': seq,
      };
}

class UptodateFrame extends Frame {
  const UptodateFrame({required this.seq});
  final String seq;

  @override
  Map<String, Object?> toWire() => {'t': 'uptodate', 'seq': seq};
}

class CommittedFrame extends Frame {
  const CommittedFrame({required this.txId, required this.seq, this.result});
  final String txId;
  final String seq;
  final Object? result;

  @override
  Map<String, Object?> toWire() => {
        't': 'committed',
        'txId': txId,
        'seq': seq,
        if (result != null) 'result': result,
      };
}

class RejectedFrame extends Frame {
  const RejectedFrame({required this.txId, this.code, required this.message});
  final String txId;
  final String? code;
  final String message;

  @override
  Map<String, Object?> toWire() => {
        't': 'rejected',
        'txId': txId,
        'error': {if (code != null) 'code': code, 'message': message},
      };
}

class ResetFrame extends Frame {
  const ResetFrame({this.sub});
  final String? sub;

  @override
  Map<String, Object?> toWire() => {'t': 'reset', if (sub != null) 'sub': sub};
}

class PageFrame extends Frame {
  const PageFrame({required this.fetchId, required this.rows, required this.seq});
  final String fetchId;
  final List<Object?> rows;
  final String seq;

  @override
  Map<String, Object?> toWire() => {'t': 'page', 'fetchId': fetchId, 'rows': rows, 'seq': seq};
}

/// A decoded row is always a string-keyed map on this wire.
typedef Row = Map<String, Object?>;
