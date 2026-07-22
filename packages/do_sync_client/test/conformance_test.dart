// Cross-language conformance, Dart half (ADR-0019 D6).
//
// Decodes every golden frame the production TS codec emitted
// (fixtures/wire_fixtures.json, regenerate with
// `node --experimental-strip-types scripts/wire-conformance.mjs gen`),
// asserts the typed values landed per the ADR-0019 D1 mapping, then re-encodes
// each decoded frame — plus four natively-authored frames — into
// fixtures/dart_emitted.json for the TS side to verify
// (`node --experimental-strip-types scripts/wire-conformance.mjs verify`).

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

Map<String, String> loadFixtures() {
  final file = File('test/fixtures/wire_fixtures.json');
  return (jsonDecode(file.readAsStringSync()) as Map<String, dynamic>).cast<String, String>();
}

void main() {
  final fixtures = loadFixtures();
  final emitted = <String, String>{};

  Frame decodeFixture(String name) {
    final b64 = fixtures[name];
    if (b64 == null) fail('missing fixture $name — regenerate with wire-conformance.mjs gen');
    return decodeFrame(base64Decode(b64));
  }

  tearDownAll(() {
    // Every fixture round-trips through the Dart codec; the TS verify step
    // decodes these bytes with the production codec and deep-compares.
    for (final name in fixtures.keys) {
      emitted[name] = base64Encode(encodeFrame(decodeFixture(name)));
    }
    File('test/fixtures/dart_emitted.json')
        .writeAsStringSync('${const JsonEncoder.withIndent('  ').convert(emitted)}\n');
  });

  group('typed decode of TS-encoded frames', () {
    test('sub-minimal', () {
      final f = decodeFixture('sub-minimal') as SubFrame;
      expect(f.subId, 's1');
      expect(f.collection, 'messages');
      expect(f.where, isNull);
      expect(f.since, isNull);
    });

    test('sub-full carries the predicate IR verbatim', () {
      final f = decodeFixture('sub-full') as SubFrame;
      expect(f.limit, 50);
      expect(f.since, '1234567890123');
      final where = f.where as Map<String, Object?>;
      expect(where['type'], 'func');
      expect(where['name'], 'and');
      final args = where['args'] as List<Object?>;
      final eqNode = args[0] as Map<String, Object?>;
      expect(eqNode['name'], 'eq');
      expect((eqNode['args'] as List<Object?>)[0], {
        'type': 'ref',
        'path': ['author'],
      });
    });

    test('mut-insert-values: every D1 value type lands as its Dart type', () {
      final f = decodeFixture('mut-insert-values') as MutFrame;
      expect(f.txId, 'tx-1');
      final op = f.ops.single;
      expect(op.type, RowOp.insert);
      final cols = op.cols!;
      expect(cols['author'], 'alice');
      expect(cols['content'], 'héllo wörld 🚀');
      // Integral JS number beyond uint32 rides as float64 -> Dart double.
      expect(cols['created_at'], isA<double>());
      expect(cols['created_at'], 1753167000000.0);
      expect(cols['score'], 1.5);
      expect(cols['active'], true);
      expect(cols['note'], isNull);
      // JS bigint -> uint64 -> Dart BigInt.
      expect(cols['big'], BigInt.from(7));
      // JS Date -> timestamp ext -> Dart DateTime (UTC).
      expect(cols['when'], DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true));
      expect(cols['blob'], Uint8List.fromList([0, 1, 2, 254, 255]));
    });

    test('edge-ints: format-boundary integers, NaN/Inf, int64/uint64 extremes', () {
      final f = decodeFixture('edge-ints') as CallFrame;
      final args = (f.args as Map<Object?, Object?>).cast<String, Object?>();
      expect(args['zero'], 0);
      expect(args['fixintMax'], 127);
      expect(args['u8'], 200);
      expect(args['u16'], 40000);
      expect(args['u32max'], 4294967295);
      expect(args['beyond32'], 4294967296.0); // float64 on the wire
      expect(args['maxSafe'], 9007199254740991.0);
      expect(args['negFixint'], -32);
      expect(args['i8'], -100);
      expect(args['i16'], -30000);
      expect(args['i32min'], -2147483648);
      expect(args['belowI32'], -2147483649.0);
      expect((args['nan'] as double).isNaN, isTrue);
      expect(args['inf'], double.infinity);
      expect(args['ninf'], double.negativeInfinity);
      expect(args['f64'], 3.141592653589793);
      expect(args['bigPos'], BigInt.parse('9223372036854775807'));
      expect(args['bigNeg'], BigInt.from(-42));
      expect(args['bigU64'], BigInt.parse('18446744073709551615'));
    });

    test('date-forms: timestamp32/64/96 all decode', () {
      final f = decodeFixture('date-forms') as CallFrame;
      final args = (f.args as Map<Object?, Object?>).cast<String, Object?>();
      expect(args['wholeSec'], DateTime.fromMillisecondsSinceEpoch(1753167000000, isUtc: true));
      expect(args['withMs'], DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true));
      expect(args['pre1970'], DateTime.fromMillisecondsSinceEpoch(-86400000, isUtc: true));
    });

    test('long-string and big-bin cross their length-format boundaries', () {
      final s = decodeFixture('long-string') as CallFrame;
      final sArgs = (s.args as Map<Object?, Object?>).cast<String, Object?>();
      expect((sArgs['s255'] as String).length, 255);
      expect((sArgs['s300'] as String).length, 300);
      expect(sArgs['uni'], '日本語🎌 end');
      final b = decodeFixture('big-bin') as CallFrame;
      final bArgs = (b.args as Map<Object?, Object?>).cast<String, Object?>();
      final bin = bArgs['bin300'] as Uint8List;
      expect(bin.length, 300);
      expect(bin[0], 0);
      expect(bin[299], 299 % 256);
    });

    test('server frames: snap / d / uptodate / committed / rejected / reset / page', () {
      final snap = decodeFixture('snap') as SnapFrame;
      expect(snap.key, 'k1');
      expect(snap.seq, '41');
      expect((snap.row as Map<Object?, Object?>)['big'], BigInt.parse('9007199254740993'));

      final dIns = decodeFixture('d-insert') as DeltaFrame;
      expect(dIns.op, RowOp.insert);
      expect(dIns.cols!['author'], 'carol');

      final dDel = decodeFixture('d-delete') as DeltaFrame;
      expect(dDel.op, RowOp.delete);
      expect(dDel.cols, isNull);

      expect((decodeFixture('uptodate') as UptodateFrame).seq, '45');

      final committed = decodeFixture('committed') as CommittedFrame;
      expect(committed.txId, 'tx-1');
      expect((committed.result as Map<Object?, Object?>)['deleted'], 3);

      final rejected = decodeFixture('rejected') as RejectedFrame;
      expect(rejected.code, 'FRAME_TOO_LARGE');
      expect(rejected.message, 'no');

      expect((decodeFixture('reset-sub') as ResetFrame).sub, 'messages#1');
      expect((decodeFixture('reset-all') as ResetFrame).sub, isNull);

      final page = decodeFixture('page') as PageFrame;
      expect(page.rows, hasLength(2));
      expect(page.seq, '47');
    });
  });

  group('natively-authored Dart frames (encoder against authored values)', () {
    test('author the fixture frames from Dart values', () {
      // These must decode — in the PRODUCTION TS codec — to deep-equal the
      // frames wire-conformance.mjs builds under the same names.
      final native = <String, Frame>{
        'native:mut-insert-values': MutFrame(
          txId: 'tx-1',
          collection: 'messages',
          ops: [
            MutOp.insert('01J0000000000000000000TEST', {
              'id': '01J0000000000000000000TEST',
              'author': 'alice',
              'content': 'héllo wörld 🚀',
              'created_at': 1753167000000, // Dart int beyond uint32 -> float64
              'score': 1.5,
              'active': true,
              'note': null,
              'big': BigInt.from(7),
              'when': DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true),
              'blob': Uint8List.fromList([0, 1, 2, 254, 255]),
            }),
          ],
        ),
        'native:sub-full': SubFrame(
          subId: 'messages#1',
          collection: 'messages',
          where: and([
            eq('author', 'alice'),
            gt('created_at', 0),
          ]),
          limit: 50,
          since: '1234567890123',
        ),
        'native:edge-ints': CallFrame(
          txId: 'tx-5',
          name: 'edge',
          args: {
            'zero': 0,
            'fixintMax': 127,
            'u8': 200,
            'u16': 40000,
            'u32max': 4294967295,
            'beyond32': 4294967296,
            'dateNow': 1753167000000,
            'maxSafe': 9007199254740991,
            'negFixint': -32,
            'i8': -100,
            'i16': -30000,
            'i32min': -2147483648,
            'belowI32': -2147483649,
            'nan': double.nan,
            'inf': double.infinity,
            'ninf': double.negativeInfinity,
            'f64': 3.141592653589793,
            'bigPos': BigInt.parse('9223372036854775807'),
            'bigNeg': BigInt.from(-42),
            'bigU64': BigInt.parse('18446744073709551615'),
          },
        ),
        'native:date-forms': CallFrame(
          txId: 'tx-8',
          name: 'dates',
          args: {
            'wholeSec': DateTime.fromMillisecondsSinceEpoch(1753167000000, isUtc: true),
            'withMs': DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true),
            'pre1970': DateTime.fromMillisecondsSinceEpoch(-86400000, isUtc: true),
          },
        ),
      };
      for (final entry in native.entries) {
        final bytes = encodeFrame(entry.value);
        // Sanity: our own decoder round-trips what we author.
        expect(decodeFrame(bytes), isA<Frame>());
        emitted[entry.key] = base64Encode(bytes);
      }
    });
  });
}
