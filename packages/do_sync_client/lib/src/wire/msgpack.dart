// MessagePack codec speaking the exact dialect of the TS wire endpoints:
// `@msgpack/msgpack` with `useBigInt64` plus the ext registrations of
// `src/wire/frame-codec.ts` (ADR-0019 D1/D5). Hand-rolled so every mapping
// below is a deliberate contract decision, pinned by the cross-language
// conformance fixtures — not an upstream package's incidental behavior.
//
// Value mapping (ADR-0019 D1, empirically probed against @msgpack/msgpack):
//   int formats (≤32-bit)  <->  Dart int          (JS integral number)
//   float64                <->  Dart double        (JS number — including ALL
//                               integers beyond the 32-bit formats; a Dart int
//                               outside int32/uint32 range therefore encodes as
//                               float64, and ints ≥ 2^53 are rejected loudly)
//   uint64/int64 (cf/d3)   <->  Dart BigInt        (JS bigint)
//   timestamp ext -1       <->  Dart DateTime (UTC) (JS Date)
//   bin                    <->  Dart Uint8List      (JS Uint8Array)
//   ext 0                  -->  Dart Uint8List      (server-side bare
//                               ArrayBuffer, ADR-0017; decode-only)

import 'dart:convert' show utf8;
import 'dart:typed_data';

/// Thrown when a value cannot be encoded losslessly onto the wire.
class MsgpackEncodeError extends Error {
  MsgpackEncodeError(this.message);
  final String message;
  @override
  String toString() => 'MsgpackEncodeError: $message';
}

/// Thrown when wire bytes cannot be decoded (truncated, unknown ext, …).
class MsgpackDecodeError extends Error {
  MsgpackDecodeError(this.message);
  final String message;
  @override
  String toString() => 'MsgpackDecodeError: $message';
}

/// Largest integer a float64 represents exactly. A Dart int beyond the 32-bit
/// msgpack formats must ride as float64 to decode as a JS `number` (matching
/// what a JS client would have written), so anything above this would silently
/// lose precision — reject instead (fail loud). True 64-bit values belong in
/// [BigInt] (int64/uint64 on the wire, `bigint` in JS).
const int _maxSafeInteger = 9007199254740991; // 2^53 - 1

Uint8List msgpackEncode(Object? value) {
  final b = BytesBuilder(copy: false);
  _encode(value, b);
  return b.takeBytes();
}

void _encode(Object? v, BytesBuilder b) {
  if (v == null) {
    b.addByte(0xc0);
  } else if (v is bool) {
    b.addByte(v ? 0xc3 : 0xc2);
  } else if (v is int) {
    _encodeInt(v, b);
  } else if (v is double) {
    _encodeFloat64(v, b);
  } else if (v is BigInt) {
    _encodeBigInt(v, b);
  } else if (v is String) {
    _encodeString(v, b);
  } else if (v is Uint8List) {
    _encodeBin(v, b);
  } else if (v is DateTime) {
    _encodeTimestamp(v, b);
  } else if (v is List<Object?>) {
    _encodeArrayHeader(v.length, b);
    for (final item in v) {
      _encode(item, b);
    }
  } else if (v is Map<Object?, Object?>) {
    _encodeMapHeader(v.length, b);
    for (final entry in v.entries) {
      final key = entry.key;
      if (key is! String) {
        throw MsgpackEncodeError('map keys must be String, got ${key.runtimeType}');
      }
      _encodeString(key, b);
      _encode(entry.value, b);
    }
  } else {
    throw MsgpackEncodeError('cannot encode ${v.runtimeType}');
  }
}

// Mirrors @msgpack/msgpack's number encoding: int formats up to 32 bits, then
// float64 — NOT int64 (int64/uint64 mean `bigint` on the JS side).
void _encodeInt(int v, BytesBuilder b) {
  if (v >= 0) {
    if (v < 0x80) {
      b.addByte(v);
    } else if (v < 0x100) {
      b.addByte(0xcc);
      b.addByte(v);
    } else if (v < 0x10000) {
      b.addByte(0xcd);
      b.add(_be(2, (d) => d.setUint16(0, v)));
    } else if (v <= 0xFFFFFFFF) {
      b.addByte(0xce);
      b.add(_be(4, (d) => d.setUint32(0, v)));
    } else {
      _requireSafe(v);
      _encodeFloat64(v.toDouble(), b);
    }
  } else {
    if (v >= -32) {
      b.addByte(0x100 + v); // negative fixint
    } else if (v >= -128) {
      b.addByte(0xd0);
      b.add(_be(1, (d) => d.setInt8(0, v)));
    } else if (v >= -32768) {
      b.addByte(0xd1);
      b.add(_be(2, (d) => d.setInt16(0, v)));
    } else if (v >= -2147483648) {
      b.addByte(0xd2);
      b.add(_be(4, (d) => d.setInt32(0, v)));
    } else {
      _requireSafe(v);
      _encodeFloat64(v.toDouble(), b);
    }
  }
}

void _requireSafe(int v) {
  if (v > _maxSafeInteger || v < -_maxSafeInteger) {
    throw MsgpackEncodeError(
      'int $v exceeds 2^53-1 and would lose precision as a JS number; '
      'use BigInt for true 64-bit values (wire int64/uint64, JS bigint)',
    );
  }
}

void _encodeFloat64(double v, BytesBuilder b) {
  b.addByte(0xcb);
  b.add(_be(8, (d) => d.setFloat64(0, v)));
}

void _encodeBigInt(BigInt v, BytesBuilder b) {
  if (v >= BigInt.zero) {
    if (v.bitLength > 64) {
      throw MsgpackEncodeError('BigInt $v does not fit in uint64');
    }
    b.addByte(0xcf);
    b.add(_be(8, (d) {
      d.setUint32(0, (v >> 32).toInt());
      d.setUint32(4, (v & BigInt.from(0xFFFFFFFF)).toInt());
    }));
  } else {
    if (v < BigInt.from(-9223372036854775808)) {
      throw MsgpackEncodeError('BigInt $v does not fit in int64');
    }
    b.addByte(0xd3);
    b.add(_be(8, (d) => d.setInt64(0, v.toInt())));
  }
}

void _encodeString(String v, BytesBuilder b) {
  final bytes = utf8.encode(v);
  final n = bytes.length;
  if (n < 32) {
    b.addByte(0xa0 | n);
  } else if (n < 0x100) {
    b.addByte(0xd9);
    b.addByte(n);
  } else if (n < 0x10000) {
    b.addByte(0xda);
    b.add(_be(2, (d) => d.setUint16(0, n)));
  } else {
    b.addByte(0xdb);
    b.add(_be(4, (d) => d.setUint32(0, n)));
  }
  b.add(bytes);
}

void _encodeBin(Uint8List v, BytesBuilder b) {
  final n = v.length;
  if (n < 0x100) {
    b.addByte(0xc4);
    b.addByte(n);
  } else if (n < 0x10000) {
    b.addByte(0xc5);
    b.add(_be(2, (d) => d.setUint16(0, n)));
  } else {
    b.addByte(0xc6);
    b.add(_be(4, (d) => d.setUint32(0, n)));
  }
  b.add(v);
}

// msgpack timestamp extension (type -1), smallest-form like @msgpack/msgpack:
// timestamp32 when nsec==0 and sec fits uint32; timestamp64 when sec fits
// 34 bits; timestamp96 otherwise (pre-1970 dates land here).
void _encodeTimestamp(DateTime v, BytesBuilder b) {
  final micros = v.microsecondsSinceEpoch;
  var sec = micros ~/ 1000000;
  var rem = micros - sec * 1000000;
  if (rem < 0) {
    sec -= 1;
    rem += 1000000;
  }
  final nsec = rem * 1000;
  if (sec >= 0 && sec < 0x400000000) {
    if (nsec == 0 && sec <= 0xFFFFFFFF) {
      b.addByte(0xd6); // fixext4
      b.addByte(0xff);
      b.add(_be(4, (d) => d.setUint32(0, sec)));
    } else {
      b.addByte(0xd7); // fixext8
      b.addByte(0xff);
      // data64 = nsec (30 bits) << 34 | sec (34 bits); assemble as two u32s to
      // stay clear of signed-64 overflow.
      final hi = (nsec << 2) | (sec >> 32);
      final lo = sec & 0xFFFFFFFF;
      b.add(_be(8, (d) {
        d.setUint32(0, hi);
        d.setUint32(4, lo);
      }));
    }
  } else {
    b.addByte(0xc7); // ext8
    b.addByte(12);
    b.addByte(0xff);
    b.add(_be(12, (d) {
      d.setUint32(0, nsec);
      d.setInt64(4, sec);
    }));
  }
}

void _encodeArrayHeader(int n, BytesBuilder b) {
  if (n < 16) {
    b.addByte(0x90 | n);
  } else if (n < 0x10000) {
    b.addByte(0xdc);
    b.add(_be(2, (d) => d.setUint16(0, n)));
  } else {
    b.addByte(0xdd);
    b.add(_be(4, (d) => d.setUint32(0, n)));
  }
}

void _encodeMapHeader(int n, BytesBuilder b) {
  if (n < 16) {
    b.addByte(0x80 | n);
  } else if (n < 0x10000) {
    b.addByte(0xde);
    b.add(_be(2, (d) => d.setUint16(0, n)));
  } else {
    b.addByte(0xdf);
    b.add(_be(4, (d) => d.setUint32(0, n)));
  }
}

Uint8List _be(int len, void Function(ByteData) fill) {
  final d = ByteData(len);
  fill(d);
  return d.buffer.asUint8List();
}

Object? msgpackDecode(Uint8List bytes) {
  final r = _Reader(bytes);
  final v = r.read();
  if (r.offset != bytes.length) {
    throw MsgpackDecodeError('trailing bytes after value (${bytes.length - r.offset})');
  }
  return v;
}

class _Reader {
  _Reader(this.bytes) : data = ByteData.sublistView(bytes);
  final Uint8List bytes;
  final ByteData data;
  int offset = 0;

  int _u8() => data.getUint8(offset++);

  int _u16() {
    final v = data.getUint16(offset);
    offset += 2;
    return v;
  }

  int _u32() {
    final v = data.getUint32(offset);
    offset += 4;
    return v;
  }

  Uint8List _bytes(int n) {
    final v = Uint8List.sublistView(bytes, offset, offset + n);
    offset += n;
    // Copy out of the frame buffer, mirroring the TS codec's ext-decode note:
    // a small value must not pin the whole inbound frame alive.
    return Uint8List.fromList(v);
  }

  Object? read() {
    if (offset >= bytes.length) throw MsgpackDecodeError('truncated input');
    final b = _u8();
    if (b < 0x80) return b; // positive fixint
    if (b >= 0xe0) return b - 0x100; // negative fixint
    if (b >= 0xa0 && b < 0xc0) return _str(b & 0x1f); // fixstr
    if (b >= 0x90 && b < 0xa0) return _array(b & 0x0f); // fixarray
    if (b >= 0x80 && b < 0x90) return _map(b & 0x0f); // fixmap
    switch (b) {
      case 0xc0:
        return null;
      case 0xc2:
        return false;
      case 0xc3:
        return true;
      case 0xc4:
        return _bytes(_u8());
      case 0xc5:
        return _bytes(_u16());
      case 0xc6:
        return _bytes(_u32());
      case 0xc7:
        final n = _u8();
        return _ext(_i8(), n);
      case 0xc8:
        final n = _u16();
        return _ext(_i8(), n);
      case 0xc9:
        final n = _u32();
        return _ext(_i8(), n);
      case 0xca:
        final v = data.getFloat32(offset);
        offset += 4;
        return v;
      case 0xcb:
        final v = data.getFloat64(offset);
        offset += 8;
        return v;
      case 0xcc:
        return _u8();
      case 0xcd:
        return _u16();
      case 0xce:
        return _u32();
      case 0xcf: // uint64 -> BigInt (JS bigint; ADR-0019 D1)
        final hi = _u32();
        final lo = _u32();
        return (BigInt.from(hi) << 32) | BigInt.from(lo);
      case 0xd0:
        return _i8();
      case 0xd1:
        final v = data.getInt16(offset);
        offset += 2;
        return v;
      case 0xd2:
        final v = data.getInt32(offset);
        offset += 4;
        return v;
      case 0xd3: // int64 -> BigInt (JS bigint)
        final v = data.getInt64(offset);
        offset += 8;
        return BigInt.from(v);
      case 0xd4:
        return _ext(_i8(), 1);
      case 0xd5:
        return _ext(_i8(), 2);
      case 0xd6:
        return _ext(_i8(), 4);
      case 0xd7:
        return _ext(_i8(), 8);
      case 0xd8:
        return _ext(_i8(), 16);
      case 0xd9:
        return _str(_u8());
      case 0xda:
        return _str(_u16());
      case 0xdb:
        return _str(_u32());
      case 0xdc:
        return _array(_u16());
      case 0xdd:
        return _array(_u32());
      case 0xde:
        return _map(_u16());
      case 0xdf:
        return _map(_u32());
      default:
        throw MsgpackDecodeError('unknown format byte 0x${b.toRadixString(16)}');
    }
  }

  int _i8() {
    final v = data.getInt8(offset);
    offset += 1;
    return v;
  }

  String _str(int n) {
    final v = utf8.decode(Uint8List.sublistView(bytes, offset, offset + n));
    offset += n;
    return v;
  }

  List<Object?> _array(int n) => List<Object?>.generate(n, (_) => read(), growable: false);

  Map<String, Object?> _map(int n) {
    final out = <String, Object?>{};
    for (var i = 0; i < n; i++) {
      final key = read();
      if (key is! String) {
        throw MsgpackDecodeError('map key must be String, got ${key.runtimeType}');
      }
      out[key] = read();
    }
    return out;
  }

  Object _ext(int type, int n) {
    if (type == -1) return _timestamp(n);
    // Ext 0 is the TS codec's bare-ArrayBuffer normalization (ADR-0017):
    // payload is the raw bytes, decoded to Uint8List exactly like bin.
    if (type == 0) return _bytes(n);
    throw MsgpackDecodeError('unsupported ext type $type');
  }

  DateTime _timestamp(int n) {
    switch (n) {
      case 4:
        final sec = _u32();
        return DateTime.fromMillisecondsSinceEpoch(sec * 1000, isUtc: true);
      case 8:
        final hi = _u32();
        final lo = _u32();
        final nsec = hi >> 2;
        final sec = ((hi & 0x3) << 32) | lo;
        return DateTime.fromMicrosecondsSinceEpoch(sec * 1000000 + nsec ~/ 1000, isUtc: true);
      case 12:
        final nsec = _u32();
        final sec = data.getInt64(offset);
        offset += 8;
        return DateTime.fromMicrosecondsSinceEpoch(sec * 1000000 + nsec ~/ 1000, isUtc: true);
      default:
        throw MsgpackDecodeError('bad timestamp length $n');
    }
  }
}
