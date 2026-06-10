# vendor/ — branch-only packed builds of TanStack DB draft PR #1564

These tarballs exist **only on `feat/ssr`** and are removed when upstream
ships (npm canary or merged release). They are devDependency / example
inputs — the published package never depends on them.

## Provenance

Built from [TanStack/db PR #1564](https://github.com/TanStack/db/pull/1564)
("Add SSR DbClient and live query identity", draft, author @tannerlinsley):

| field | value |
| --- | --- |
| upstream head | `132d53a9f03e9d0df442b2d15c74e5931925b77b` |
| upstream commit date | 2026-05-30 10:40:24 -0600 |
| fetched via | `git fetch origin pull/1564/head` |
| built | 2026-06-10, `pnpm@11.1.0`, `pnpm --filter "@tanstack/react-db..." build` |
| packages | `@tanstack/db@0.6.7` → `tanstack-db-0.6.7-pr1564.tgz` · `@tanstack/react-db@0.1.85` → `tanstack-react-db-0.1.85-pr1564.tgz` |

Everything `tests/ssr-*.test.ts` and `examples/ssr` validate is validated
**against exactly this upstream commit**. When the PR is force-pushed or
revised, rebuild the tarballs, update this table, and re-run the suite —
green tests against stale tarballs prove nothing about the current draft.

## Consumption gotcha

The public npm registry has a REAL `@tanstack/db@0.6.7`, and the react-db
tarball pins it as a regular dependency. Anything installing these tarballs
MUST force resolution to the vendored file (root `package.json` does this
implicitly via the `file:` devDependency; `examples/ssr` needs explicit npm
`overrides` plus vite `resolve.dedupe`) — two copies of `@tanstack/db` break
the Symbol-branded `collectionOptions`.

## Exit plan

When upstream publishes: delete this directory, point the devDependency and
example at the published version, drop the example's overrides, and rebase
`feat/ssr` to remove the tarball commits from history (they are large blobs;
the branch is rebased-not-merged until then anyway).
