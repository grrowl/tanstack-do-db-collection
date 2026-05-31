# Architecture Decision Records

Decisions are recorded as ADRs (see
[0000](./0000-record-architecture-decisions.md) for the process). They are
append-mostly: when a decision changes, a new ADR supersedes the old one and
explains the displacement.

| # | Title | Status |
|---|---|---|
| [0000](./0000-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0001](./0001-sync-architecture.md) | Sync architecture: single-ordered-stream over a Durable Object | Accepted (amended by 0002) |
| [0002](./0002-adversarial-review-corrections.md) | Corrections from adversarial review: ordering, shaping, retention | Accepted |
| [0003](./0003-atomic-cursor-fetch.md) | Cursor load-more is one atomic fetch, not two | Accepted (naming amended by 0005) |
| [0004](./0004-after-commit-hook.md) | Side effects go in a fire-and-forget `afterCommit`, not the transaction | Accepted |
| [0005](./0005-fetch-frame-mirrors-loadsubsetoptions.md) | The cursor `fetch` frame mirrors TanStack's `LoadSubsetOptions` | Accepted |
| [0006](./0006-server-originated-writes.md) | Server-originated writes: `runSyncedWrite` | Accepted (init caveat retired by 0007) |
| [0007](./0007-author-owned-schema-register-sync.md) | Author-owned schema; `registerSync` wires the sync | Accepted |
| [0008](./0008-orphaned-cdc-triggers.md) | Orphaned CDC triggers when a collection is removed | Proposed (backlog) |
