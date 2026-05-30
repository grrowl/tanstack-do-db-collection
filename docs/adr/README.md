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
| [0003](./0003-atomic-cursor-fetch.md) | Cursor load-more is one atomic fetch, not two | Accepted |
| [0004](./0004-after-commit-hook.md) | Side effects go in a fire-and-forget `afterCommit`, not the transaction | Accepted |
