// Predicate IR builder — emits the `@tanstack/db` BasicExpression node shape
// ({type: ref|val|func}) restricted to the ADR-0013 operator floor, which
// ADR-0019 D1 froze as part of the wire contract:
//   { eq, gt, gte, lt, lte, like, in, and, or, not }
//
// Anything else is rejected by the server with a `reset` (fail loud), so this
// builder simply doesn't offer it.
//
//   where: and([eq('author', 'alice'), gt('created_at', 0)])

Map<String, Object?> ref(String column) => {
      'type': 'ref',
      'path': [column],
    };

Map<String, Object?> val(Object? value) => {'type': 'val', 'value': value};

Map<String, Object?> _fn(String name, List<Object?> args) => {
      'type': 'func',
      'name': name,
      'args': args,
    };

/// An IR operand: pass a raw node ([ref]/[val]/a nested func) through, wrap
/// anything else as a literal. Lets call sites write `eq('col', 42)`.
Object? _operand(Object? v) {
  if (v is Map<String, Object?> && v['type'] is String) return v;
  return val(v);
}

/// Column-or-node on the left: a bare [String] means a column reference.
Object? _lhs(Object? v) {
  if (v is String) return ref(v);
  return _operand(v);
}

Map<String, Object?> eq(Object? column, Object? value) => _fn('eq', [_lhs(column), _operand(value)]);
Map<String, Object?> gt(Object? column, Object? value) => _fn('gt', [_lhs(column), _operand(value)]);
Map<String, Object?> gte(Object? column, Object? value) => _fn('gte', [_lhs(column), _operand(value)]);
Map<String, Object?> lt(Object? column, Object? value) => _fn('lt', [_lhs(column), _operand(value)]);
Map<String, Object?> lte(Object? column, Object? value) => _fn('lte', [_lhs(column), _operand(value)]);

/// SQL LIKE, case-SENSITIVE (ADR-0013 D3); the pattern must be a string
/// literal — the server rejects anything else.
Map<String, Object?> like(Object? column, String pattern) => _fn('like', [_lhs(column), val(pattern)]);

/// `column IN (values)`. NULL never matches (three-valued logic, ADR-0013).
Map<String, Object?> inList(Object? column, List<Object?> values) =>
    _fn('in', [_lhs(column), val(values)]);

Map<String, Object?> and(List<Map<String, Object?>> args) => _fn('and', args);
Map<String, Object?> or(List<Map<String, Object?>> args) => _fn('or', args);
Map<String, Object?> not(Map<String, Object?> arg) => _fn('not', [arg]);
