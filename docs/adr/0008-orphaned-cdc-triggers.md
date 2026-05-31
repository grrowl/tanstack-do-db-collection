# 0008 — Orphaned CDC triggers when a collection is removed (backlog)

**Status:** Proposed — backlog. Records a known limitation of
[ADR-0007](./0007-author-owned-schema-register-sync.md)'s `registerSync` and the
preferred fix; not yet implemented.

## Context

`registerSync` installs three triggers per registered collection
(`_sync_changes_<tbl>_{ai,au,ad}`, via `CREATE TRIGGER IF NOT EXISTS`). It only
ever **adds** — it never drops triggers. So if an author removes a collection
from the registry in a later deploy (or renames its table), the old triggers
persist on the now-unregistered table.

Consequence: writes to that orphaned table keep firing the triggers and landing
rows in `_sync_changes`. Those rows are drained but have **no subscriber and no
handler** — the table is no longer a collection. Effects:

- **Change-log bloat / compaction churn** — an actively-written orphan table
  grows `_sync_changes` for nothing, and compaction keeps collapsing entries no
  client will ever consume.
- **A correctness smell** — a table that is no longer a synced collection still
  emits sync changes, which is surprising and could mask a genuine
  "forgot to register" bug.

**Severity: low.** It requires both removing a collection (uncommon) *and*
continuing to write the orphaned table. No data corruption — the changes simply
have no consumer. But the change-log growth is unbounded in that state.

## Options

1. **Reap on `registerSync`.** Enumerate existing `_sync_*` triggers from
   `sqlite_master`, diff against the registered collections, and `DROP` triggers
   for tables not in the registry. `registerSync` already owns the trigger
   lifecycle, so it should own removal too — install the registered set, drop the
   stale set. Self-healing on the next deploy; only ever touches the namespaced
   `_sync_*` triggers it created (safe). Risk: a DO constructed with a
   *temporarily* reduced registry (e.g. a feature flag) would drop-then-reinstall
   — churn, but correct.
2. **Explicit `deregisterCollection(table)` (or `dropSync`).** Author opts in to
   removal. No magic, but the author must remember — and forgetting is exactly
   how the orphan arises in the first place.
3. **Do nothing; document.** Orphan triggers are benign short of the bloat case.

## Decision (deferred)

Prefer **Option 1** — reaping in `registerSync`, gated to the `_sync_*` prefix it
owns. It keeps the "the author declares the desired set; the framework
reconciles" model already established for triggers, and needs no new API. Deferred
until a real need surfaces (someone removes a collection in anger).

When implemented, add a test: register A+B → write both → remove B from the
registry → `registerSync` → B's triggers are gone and B writes no longer hit
`_sync_changes`, while A is untouched.
