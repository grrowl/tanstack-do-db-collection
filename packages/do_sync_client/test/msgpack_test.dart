// Codec unit tests: Dart-side round-trips and fail-loud edges. The
// cross-language contract itself is pinned by conformance_test.dart +
// scripts/wire-conformance.mjs — these cover what those can't (error paths,
// Dart-only type decisions).

import 'dart:typed_data';

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

Object? rt(Object? v) => msgpackDecode(msgpackEncode(v));

void main() {
  group('round-trips', () {
    test('scalars', () {
      expect(rt(null), isNull);
      expect(rt(true), true);
      expect(rt(false), false);
      expect(rt(0), 0);
      expect(rt(127), 127);
      expect(rt(128), 128);
      expect(rt(65535), 65535);
      expect(rt(65536), 65536);
      expect(rt(4294967295), 4294967295);
      expect(rt(-1), -1);
      expect(rt(-32), -32);
      expect(rt(-33), -33);
      expect(rt(-128), -128);
      expect(rt(-129), -129);
      expect(rt(-32768), -32768);
      expect(rt(-2147483648), -2147483648);
      expect(rt('x'), 'x');
      expect(rt(''), '');
      expect(rt(1.5), 1.5);
    });

    test('int beyond 32-bit rides as float64 and returns as double', () {
      // The ADR-0019 D1 asymmetry, on purpose: a JS peer would have written
      // the same bytes for the same value.
      expect(rt(4294967296), 4294967296.0);
      expect(rt(4294967296), isA<double>());
      expect(rt(-2147483649), -2147483649.0);
      expect(rt(9007199254740991), 9007199254740991.0);
    });

    test('BigInt <-> int64/uint64', () {
      expect(rt(BigInt.from(5)), BigInt.from(5));
      expect(rt(BigInt.from(-42)), BigInt.from(-42));
      expect(rt(BigInt.parse('9223372036854775807')), BigInt.parse('9223372036854775807'));
      expect(rt(BigInt.parse('18446744073709551615')), BigInt.parse('18446744073709551615'));
      expect(rt(BigInt.parse('-9223372036854775808')), BigInt.parse('-9223372036854775808'));
    });

    test('DateTime through all three timestamp forms', () {
      final wholeSec = DateTime.fromMillisecondsSinceEpoch(1753167000000, isUtc: true);
      final withMs = DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true);
      final pre1970 = DateTime.fromMillisecondsSinceEpoch(-86400000, isUtc: true);
      final far = DateTime.fromMillisecondsSinceEpoch(0x400000000 * 1000 + 500, isUtc: true);
      expect(rt(wholeSec), wholeSec);
      expect(rt(withMs), withMs);
      expect(rt(pre1970), pre1970);
      expect(rt(far), far); // sec >= 2^34 -> timestamp96
      // Form check: whole seconds use the 4-byte form, ms the 8-byte form.
      expect(msgpackEncode(wholeSec)[0], 0xd6);
      expect(msgpackEncode(withMs)[0], 0xd7);
      expect(msgpackEncode(pre1970)[0], 0xc7);
    });

    test('bytes, lists, maps across length-format boundaries', () {
      final bin300 = Uint8List.fromList(List.generate(300, (i) => i % 256));
      expect(rt(bin300), bin300);
      expect(rt('a' * 255), 'a' * 255);
      expect(rt('b' * 300), 'b' * 300);
      final list20 = List<Object?>.generate(20, (i) => i);
      expect(rt(list20), list20);
      final map20 = {for (var i = 0; i < 20; i++) 'k$i': i};
      expect(rt(map20), map20);
      expect(rt({'nested': {'deep': [1, 'two', null]}}), {
        'nested': {'deep': [1, 'two', null]},
      });
    });

    test('NaN and infinities', () {
      expect((rt(double.nan) as double).isNaN, isTrue);
      expect(rt(double.infinity), double.infinity);
      expect(rt(double.negativeInfinity), double.negativeInfinity);
    });
  });

  group('fail loud', () {
    test('int above 2^53-1 is rejected, not silently rounded', () {
      expect(() => msgpackEncode(9007199254740992), throwsA(isA<MsgpackEncodeError>()));
      expect(() => msgpackEncode(-9007199254740992), throwsA(isA<MsgpackEncodeError>()));
    });

    test('BigInt outside 64 bits is rejected', () {
      expect(() => msgpackEncode(BigInt.two.pow(64)), throwsA(isA<MsgpackEncodeError>()));
      expect(
        () => msgpackEncode(-BigInt.two.pow(63) - BigInt.one),
        throwsA(isA<MsgpackEncodeError>()),
      );
    });

    test('non-String map keys are rejected on encode and decode', () {
      expect(() => msgpackEncode({1: 'x'}), throwsA(isA<MsgpackEncodeError>()));
      // fixmap(1), key = fixint 1, value = fixint 2
      expect(
        () => msgpackDecode(Uint8List.fromList([0x81, 0x01, 0x02])),
        throwsA(isA<MsgpackDecodeError>()),
      );
    });

    test('unknown ext types and truncated input are rejected', () {
      // fixext1, type 42
      expect(
        () => msgpackDecode(Uint8List.fromList([0xd4, 42, 0x00])),
        throwsA(isA<MsgpackDecodeError>()),
      );
      // str16 claiming 100 bytes with none present
      expect(
        () => msgpackDecode(Uint8List.fromList([0xda, 0x00, 0x64])),
        throwsA(anything),
      );
      // trailing garbage after a complete value
      expect(
        () => msgpackDecode(Uint8List.fromList([0xc0, 0xc0])),
        throwsA(isA<MsgpackDecodeError>()),
      );
    });

    test('unsupported Dart types are rejected', () {
      expect(() => msgpackEncode(Object()), throwsA(isA<MsgpackEncodeError>()));
      expect(() => msgpackEncode(#symbol), throwsA(isA<MsgpackEncodeError>()));
    });
  });

  group('server-dialect decode', () {
    test('ext 0 (bare ArrayBuffer normalization, ADR-0017) decodes to bytes', () {
      // fixext4 would be d6 00; use ext8 form: c7 len=3 type=0 payload
      final decoded = msgpackDecode(Uint8List.fromList([0xc7, 3, 0x00, 9, 8, 7]));
      expect(decoded, Uint8List.fromList([9, 8, 7]));
    });

    test('decoded bytes are copies, not views pinning the frame buffer', () {
      final frame = msgpackEncode({'b': Uint8List.fromList([1, 2, 3])});
      final decoded = (msgpackDecode(frame) as Map<String, Object?>)['b'] as Uint8List;
      frame[frame.length - 1] = 99; // mutate the wire buffer
      expect(decoded, Uint8List.fromList([1, 2, 3]));
    });
  });
}
