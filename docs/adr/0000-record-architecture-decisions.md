# 0000 — Record architecture decisions

**Status:** Accepted

## Context

This library makes a number of load-bearing architectural choices that are
non-obvious and, in several cases, deliberately reject a tempting alternative.
A reader (or a future maintainer) needs to know not just *what* was decided
but *why*, and *what was rejected*.

## Decision

We keep Architecture Decision Records (ADRs) — short markdown documents,
numbered sequentially, in `docs/adr/`. Each records a single decision with its
context, the decision itself, the alternatives considered, and the
consequences.

ADRs are **append-mostly**. We do not rewrite history. When a decision is
superseded, we add a new ADR that references and supersedes the old one and
explains the displacement.

## Consequences

- The rationale travels with the code, in the repo, under version control.
- `git log` and the ADR set together tell the story of how the library was
  built — a goal of this project.
- New contributors can read the ADRs before reaching for something in the
  rejected set.
