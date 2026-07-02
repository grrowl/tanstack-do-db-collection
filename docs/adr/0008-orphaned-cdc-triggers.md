# 0008 — Orphaned CDC triggers when a collection is removed (backlog)

**Status:** Accepted — implemented. Records a limitation of
[ADR-0007](./0007-author-owned-schema-register-sync.md)'s `registerSync` and the
fix now shipped: Option 1, reaping in `registerSync`.
[ADR-0015](./0015-syncable-mixin.md) cites this GLOB `_sync_changes_*` namespace
as its trigger-collision-safety proof: on a host that owns unregistered tables
(`cf_agents_*`, `assistant_*`), the reaper's literal-`_` GLOB can never drop a
host trigger, and unregistered tables get no capture triggers at all.

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

## Decision

**Option 1** — reconcile in `registerSync`. Trigger install and reap are merged
into one `ensureTriggers(sql, collections)` (in `changes.ts`): validate +
install each registered table's triggers, build the expected trigger-name set,
then `DROP` any `_sync_changes_*` trigger not in it. `registerSync` calls it on
every wake, so the author declares the desired set and the framework reconciles
— install the registered set, drop the stale set — with no new API.

**Scope: trigger reap only.** Dropping a collection's triggers stops *new*
orphan rows; the rows already in `_sync_changes` for that table are left as-is.
After compaction that residue is bounded to ~distinct keys (not the "unbounded"
the Context warns of, which only holds *while* triggers keep firing) and has no
consumer, so it is benign. A destructive purge of those rows is deliberately
**not** done here: under a transiently-reduced registry (misconfig, partial
rollout) reaping triggers is cheap and self-healing, but purging rows would be
irreversible. If row reclamation is ever wanted it should be a separate,
explicitly author-invoked step — not a side effect of reconnecting.

Implementation details that matter:

- **Set-diff, never name-parsing.** Table names may contain `_`, so a trigger
  name can't be reliably split back into its table. Membership against the
  expected set sidesteps that entirely.
- **`GLOB '_sync_changes_*'`, not `LIKE`.** In `LIKE`, `_` is a single-char
  wildcard; `GLOB` treats it literally, so the scan matches exactly our
  namespace and can never touch an author's trigger.

This also fixes a sharper case than bloat — a *correctness* bug. `ALTER TABLE …
RENAME` keeps a table's triggers attached across the rename (same trigger
**name**, now firing on the renamed table), and our trigger body hardcodes the
original table name as a string **literal** in the change row. So a rename +
re-register under an add-only `registerSync` would leave the old-named trigger
firing alongside the freshly-installed one: every write to the renamed table
logs **twice**, one row mislabelled with the old table name. Reaping by name
removes the stale trigger. This SQLite behaviour is pinned by a probe test (it's
load-bearing for the fix), so it can't silently change under us.

Tests (`tests/ensure-triggers.test.ts`): register A+B → write both → re-register
A only → B's triggers gone from `sqlite_master` and a B write hits nothing in
`_sync_changes`, while A still captures; idempotency (re-running reaps nothing);
namespace safety (an author trigger survives reaping with an empty registry); a
rename probe pinning the duplicate-trigger hazard, and that one reconcile after a
rename leaves the renamed table capturing exactly once.
