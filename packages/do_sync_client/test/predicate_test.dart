// Floor-evaluator parity (ADR-0013 via ADR-0019 D3).
//
// The seed set and per-operator membership expectations mirror
// tests/predicate-parity.test.ts row-for-row: n1 (body NULL), lo ("hello"),
// up ("HELLO"), x ("x"). The TS test proves SQL and @tanstack/db agree on
// these; this suite proves the Dart evaluator lands on the same rows, so all
// THREE membership deciders agree.

import 'dart:typed_data';

import 'package:do_sync_client/do_sync_client.dart';
import 'package:test/test.dart';

const seeds = <Map<String, Object?>>[
  {'id': 'n1', 'body': null},
  {'id': 'lo', 'body': 'hello'},
  {'id': 'up', 'body': 'HELLO'},
  {'id': 'x', 'body': 'x'},
];

List<String> members(Map<String, Object?> where) {
  final p = compilePredicate(where);
  return [
    for (final s in seeds)
      if (p(s)) s['id']! as String,
  ]..sort();
}

void main() {
  group('parity with tests/predicate-parity.test.ts', () {
    test('eq: exact match, case-sensitive, NULL excluded', () {
      expect(members(eq('body', 'hello')), ['lo']);
    });

    test('like: case-sensitive (HELLO excluded), NULL excluded', () {
      expect(members(like('body', 'hello%')), ['lo']);
    });

    test("gt: NULL excluded (three-valued) and uppercase sorts below 'a'", () {
      expect(members(gt('body', 'a')), ['lo', 'x']);
    });

    test('not(eq): NULL body excluded under three-valued NOT', () {
      expect(members(not(eq('body', 'x'))), ['lo', 'up']);
    });

    test('in: NULL body excluded, exact match only (no case folding)', () {
      expect(members(inList('body', ['hello', 'x'])), ['lo', 'x']);
    });

    test('ne is off-floor: fails loud at compile, like the server reset', () {
      expect(
        () => compilePredicate({
          'type': 'func',
          'name': 'ne',
          'args': [ref('body'), val('x')],
        }),
        throwsA(isA<UnsupportedPredicateError>()),
      );
    });

    test('ilike is off-floor too', () {
      expect(
        () => compilePredicate({
          'type': 'func',
          'name': 'ilike',
          'args': [ref('body'), val('h%')],
        }),
        throwsA(isA<UnsupportedPredicateError>()),
      );
    });
  });

  group('three-valued composition', () {
    test('and: false short-circuits over UNKNOWN; UNKNOWN otherwise excludes', () {
      // body IS NULL row: (body = 'x') is UNKNOWN. UNKNOWN AND false = false;
      // UNKNOWN AND true = UNKNOWN -> excluded either way, but not(...) flips
      // only definite values.
      expect(members(and([eq('body', 'hello'), eq('id', 'lo')])), ['lo']);
      expect(members(and([eq('body', 'hello'), eq('id', 'up')])), isEmpty);
    });

    test('or: true short-circuits over UNKNOWN', () {
      expect(members(or([eq('body', 'hello'), eq('id', 'n1')])), ['lo', 'n1']);
    });

    test('not(or(...)) keeps UNKNOWN unknown', () {
      // For n1: or(eq(body,'a'), eq(body,'b')) = UNKNOWN; not(UNKNOWN) =
      // UNKNOWN -> excluded.
      expect(members(not(or([eq('body', 'a'), eq('body', 'b')]))), ['lo', 'up', 'x']);
    });
  });

  group('operand edge cases pinned by @tanstack/db semantics', () {
    test('like with non-string column value is false, not an error', () {
      final p = compilePredicate(like('n', 'h%'));
      expect(p({'n': 5}), isFalse);
    });

    test('in against a non-list is false; NULL element never matches', () {
      final p = compilePredicate(inList('body', ['a', null]));
      expect(p({'body': 'a'}), isTrue);
      expect(p({'body': null}), isFalse); // value UNKNOWN
    });

    test('eq normalizes DateTime to epoch ms (matching normalizeValue)', () {
      final when = DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true);
      final p = compilePredicate(eq('when', when));
      expect(p({'when': DateTime.fromMillisecondsSinceEpoch(1753167000123, isUtc: true)}), isTrue);
      expect(p({'when': 1753167000123}), isTrue); // ms number vs Date literal
    });

    test('like pattern regex specials are escaped; % and _ are wildcards', () {
      final p = compilePredicate(like('s', r'a.c%'));
      expect(p({'s': 'a.cdef'}), isTrue);
      expect(p({'s': 'aXcdef'}), isFalse); // '.' is literal, not regex any
      final u = compilePredicate(like('s', 'a_c'));
      expect(u({'s': 'abc'}), isTrue);
      expect(u({'s': 'abbc'}), isFalse);
    });

    test('absent key is UNKNOWN (JS undefined), excluded like NULL', () {
      expect(compilePredicate(eq('missing', 'v'))(const {}), isFalse);
      expect(compilePredicate(not(eq('missing', 'v')))(const {}), isFalse);
    });

    test('null where matches everything', () {
      expect(compilePredicate(null)(const {}), isTrue);
    });

    test('numeric comparison crosses int/double like JS numbers', () {
      expect(compilePredicate(gt('n', 1))({'n': 1.5}), isTrue);
      expect(compilePredicate(eq('n', 5))({'n': 5.0}), isTrue);
      expect(compilePredicate(gt('n', 1))({'n': double.nan}), isFalse);
    });
  });

  group('adversarial-review parity pins', () {
    test('eq does NOT bridge BigInt and num (JS: 5n === 5 is false)…', () {
      // An int64 column arrives as Dart BigInt (JS bigint); a numeric literal
      // must not eq-match it, or the Dart preflight admits writes the server's
      // delta evaluator will move-out.
      expect(compilePredicate(eq('big', 5))({'big': BigInt.from(5)}), isFalse);
      expect(compilePredicate(eq('big', BigInt.from(5)))({'big': BigInt.from(5)}), isTrue);
    });

    test('…but relational ops DO coerce BigInt vs num (JS: 5n > 3 is true)', () {
      expect(compilePredicate(gt('big', 3))({'big': BigInt.from(5)}), isTrue);
      expect(compilePredicate(lt('big', 3))({'big': BigInt.from(5)}), isFalse);
    });

    test('relational ops coerce Date vs number like JS valueOf', () {
      final d300 = DateTime.fromMillisecondsSinceEpoch(300, isUtc: true);
      expect(compilePredicate(gt('t', 200))({'t': d300}), isTrue);
      expect(compilePredicate(gt('t', d300))({'t': 400}), isTrue);
      expect(compilePredicate(lt('t', d300))({'t': 400}), isFalse);
    });

    test('in matches small blobs by content, large blobs by identity (JS threshold 128)', () {
      final small = Uint8List.fromList(List.filled(64, 7));
      final smallCopy = Uint8List.fromList(List.filled(64, 7));
      expect(compilePredicate(inList('b', [smallCopy]))({'b': small}), isTrue);

      final large = Uint8List.fromList(List.filled(200, 7));
      final largeCopy = Uint8List.fromList(List.filled(200, 7));
      // JS: normalizeValue leaves >128-byte arrays as objects; `===` is
      // identity, so a decoded wire value never matches -> false.
      expect(compilePredicate(inList('b', [largeCopy]))({'b': large}), isFalse);
      // eq is the byte-comparing path (areValuesEqual): still true.
      expect(compilePredicate(eq('b', largeCopy))({'b': large}), isTrue);
    });

    test('gt(-0.0, 0) is false like JS (compareTo would order them)', () {
      expect(compilePredicate(gt('n', 0))({'n': -0.0}), isFalse);
      expect(compilePredicate(lt('n', 0))({'n': -0.0}), isFalse);
      expect(compilePredicate(gte('n', 0))({'n': -0.0}), isTrue);
      expect(compilePredicate(eq('n', 0))({'n': -0.0}), isTrue);
    });
  });
}
