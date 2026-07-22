// Floor-predicate evaluator — the Dart mirror of `@tanstack/db`'s
// `compileSingleRowExpression` + `toBooleanPredicate`, restricted to the
// ADR-0013 operator floor. ADR-0013's "one evaluator is the source of truth"
// discipline extends here (ADR-0019 D3): membership decided by this evaluator
// must agree row-for-row with the server's two paths, and the parity cases in
// test/predicate_test.dart mirror tests/predicate-parity.test.ts.
//
// Semantics ported from @tanstack/db dist/esm/query/compiler/evaluators.js:
// three-valued logic (null = UNKNOWN, collapsed to false by
// [toBooleanPredicate]), case-sensitive LIKE with string-only operands,
// normalize(Date -> epoch ms) for eq/in, byte-wise Uint8List equality.

import 'dart:typed_data';

/// Thrown for an operator outside the ADR-0013 floor. The server would answer
/// such a subscription with `reset`; the client fails loud at compile time
/// instead of quietly diverging.
class UnsupportedPredicateError extends Error {
  UnsupportedPredicateError(this.message);
  final String message;
  @override
  String toString() => 'UnsupportedPredicateError: $message';
}

/// SQL WHERE truth-collapse: only an explicit `true` includes the row
/// (UNKNOWN/null excludes, matching toBooleanPredicate).
bool toBooleanPredicate(Object? result) => result == true;

/// Compile a `where` IR into a row predicate. `null` where matches everything
/// (the TS client's compilePredicate does the same).
bool Function(Map<String, Object?> row) compilePredicate(Object? where) {
  if (where == null) return (_) => true;
  final eval = _compile(where);
  return (row) => toBooleanPredicate(eval(row));
}

typedef _Eval = Object? Function(Map<String, Object?> row);

_Eval _compile(Object? expr) {
  if (expr is! Map<String, Object?>) {
    throw UnsupportedPredicateError('expression is not an IR node: $expr');
  }
  switch (expr['type']) {
    case 'val':
      final value = expr['value'];
      return (_) => value;
    case 'ref':
      final path = (expr['path'] as List<Object?>).cast<String>();
      // Single-row ref: walk the path, null-propagating (an absent key is
      // UNKNOWN, same as JS `undefined`).
      return (row) {
        Object? value = row;
        for (final prop in path) {
          if (value == null) return null;
          if (value is! Map<String, Object?>) return null;
          value = value[prop];
        }
        return value;
      };
    case 'func':
      return _compileFunc(expr['name'] as String, (expr['args'] as List<Object?>?) ?? const []);
    default:
      throw UnsupportedPredicateError('unknown IR node type: ${expr['type']}');
  }
}

_Eval _compileFunc(String name, List<Object?> args) {
  final compiled = args.map(_compile).toList();
  switch (name) {
    case 'eq':
      final a = compiled[0], b = compiled[1];
      return (row) {
        final va = _normalize(a(row));
        final vb = _normalize(b(row));
        if (va == null || vb == null) return null;
        return _valuesEqual(va, vb);
      };
    case 'gt':
      return _comparison(compiled, (c) => c > 0);
    case 'gte':
      return _comparison(compiled, (c) => c >= 0);
    case 'lt':
      return _comparison(compiled, (c) => c < 0);
    case 'lte':
      return _comparison(compiled, (c) => c <= 0);
    case 'and':
      return (row) {
        var hasUnknown = false;
        for (final arg in compiled) {
          final r = arg(row);
          if (r == false) return false;
          if (r == null) hasUnknown = true;
        }
        return hasUnknown ? null : true;
      };
    case 'or':
      return (row) {
        var hasUnknown = false;
        for (final arg in compiled) {
          final r = arg(row);
          if (r == true) return true;
          if (r == null) hasUnknown = true;
        }
        return hasUnknown ? null : false;
      };
    case 'not':
      final arg = compiled[0];
      return (row) {
        final r = arg(row);
        if (r == null) return null;
        return r != true;
      };
    case 'in':
      final valueEval = compiled[0], arrayEval = compiled[1];
      return (row) {
        // JS parity: `in` matches via normalizeValue(item) === value — NOT
        // areValuesEqual — so bytes only match when small enough to normalize
        // to a string key (see _normalizeIn). eq is the byte-comparing one.
        final value = _normalizeIn(valueEval(row));
        final array = arrayEval(row);
        if (value == null) return null;
        if (array is! List<Object?>) return false;
        return array.any((item) {
          final n = _normalizeIn(item);
          return n != null && n == value;
        });
      };
    case 'like':
      final valueEval = compiled[0], patternEval = compiled[1];
      return (row) {
        final value = valueEval(row);
        final pattern = patternEval(row);
        if (value == null || pattern == null) return null;
        return _like(value, pattern);
      };
    default:
      throw UnsupportedPredicateError('operator outside the ADR-0013 floor: $name');
  }
}

// gt/gte/lt/lte: JS `a > b` over the value types this wire carries. Same-type
// num/String/bool/DateTime compare; a type mismatch is false (JS coercion
// comparisons across types are off-contract — the floor is column vs literal
// of the column's type).
_Eval _comparison(List<_Eval> compiled, bool Function(int) test) {
  final a = compiled[0], b = compiled[1];
  return (row) {
    final va = a(row);
    final vb = b(row);
    if (va == null || vb == null) return null;
    final c = _compareValues(va, vb);
    if (c == null) return false;
    return test(c);
  };
}

int? _compareValues(Object? a, Object? b) {
  // JS relational operators coerce Date -> ms (valueOf), so `created_at > 200`
  // works against a Date on either side. Mirror that BEFORE the typed cases.
  if (a is DateTime && (b is num || b is DateTime)) {
    return _compareValues(a.millisecondsSinceEpoch, b is DateTime ? b.millisecondsSinceEpoch : b);
  }
  if (b is DateTime && a is num) {
    return _compareValues(a, b.millisecondsSinceEpoch);
  }
  if (a is num && b is num) {
    // Direct </> like JS, NOT compareTo: IEEE semantics (NaN compares false
    // against everything) and -0 == 0 (compareTo orders -0 below 0, which
    // would make gt(-0.0, 0) true — adversarial review).
    if (a < b) return -1;
    if (a > b) return 1;
    if (a == b) return 0;
    return null; // NaN involved
  }
  if (a is BigInt && b is BigInt) return a.compareTo(b);
  // JS DOES coerce bigint <-> number for relational operators (5n > 3).
  if (a is BigInt && b is num) {
    if (b.isNaN) return null;
    final ad = a.toDouble();
    return ad == b ? 0 : (ad < b ? -1 : 1);
  }
  if (a is num && b is BigInt) {
    final c = _compareValues(b, a);
    return c == null ? null : -c;
  }
  if (a is String && b is String) return a.compareTo(b); // code-unit order, like JS
  if (a is bool && b is bool) return (a ? 1 : 0).compareTo(b ? 1 : 0);
  return null;
}

/// `in` normalization, mirroring @tanstack/db's normalizeValue exactly:
/// Date -> epoch ms; bytes <= 128 long -> a string key (so small blobs match
/// by content); LARGER bytes stay as-is, where JS `===` is object identity —
/// two decoded wire values never match, and neither do two Dart [Uint8List]s
/// under `==`. eq is different: it runs areValuesEqual, which byte-compares.
Object? _normalizeIn(Object? v) {
  if (v is DateTime) return v.millisecondsSinceEpoch;
  if (v is Uint8List && v.length <= 128) return '__u8__${v.join(',')}';
  return v;
}

/// eq normalization (normalizeValue without the byte-stringing shortcut —
/// byte equality is handled structurally in [_valuesEqual], matching
/// areValuesEqual which byte-compares any length).
Object? _normalize(Object? v) {
  if (v is DateTime) return v.millisecondsSinceEpoch;
  return v;
}

bool _valuesEqual(Object? a, Object? b) {
  if (a is Uint8List && b is Uint8List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
  // NO num <-> BigInt bridge: JS `5n === 5` is false, and eq uses strict
  // equality after normalize. (Relational ops DO coerce — see _compareValues.)
  return a == b; // Dart ==: int 5 == double 5.0, matching JS === on numbers
}

// SQL LIKE -> anchored regex, ported byte-for-byte from evaluateLike:
// non-string operands are false; escape regex specials, then % -> .*, _ -> .;
// dotAll so % and _ cross newlines.
bool _like(Object? value, Object? pattern) {
  if (value is! String || pattern is! String) return false;
  final escaped = pattern.replaceAllMapped(
    RegExp(r'[.*+?^${}()|[\]\\]'),
    (m) => '\\${m[0]}',
  );
  final regex = RegExp(
    '^${escaped.replaceAll('%', '.*').replaceAll('_', '.')}\$',
    dotAll: true,
  );
  return regex.hasMatch(value);
}
