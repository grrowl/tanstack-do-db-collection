// Frame <-> wire bytes: binary MessagePack, the dialect of ADR-0019 D1.
// The JSON debug codec of the TS package is deliberately not mirrored — binary
// is the wire contract; JSON was a debugging convenience.

import 'dart:typed_data';

import 'frames.dart';
import 'msgpack.dart';

Uint8List encodeFrame(Frame frame) => msgpackEncode(frame.toWire());

Frame decodeFrame(Uint8List bytes) {
  final v = msgpackDecode(bytes);
  if (v is! Map<String, Object?>) {
    throw const FormatException('frame is not a map');
  }
  return Frame.fromWire(v);
}
