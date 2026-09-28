# west-discovery — 1.0.0 Release Plan

Runbook for merging `integration` → `main` and cutting the `v1.0.0` production release.
Repo: `git@github.com:esgf2-us/west-discovery.git`

> **Status (updated 2026-09-28):** The `release/1.0.0` branch has been created off
> `integration`, and the prep changes (version, LICENSE, CHANGELOG) are written to the
> working tree but **not yet committed**. Remaining: commit, open the PR, merge, tag,
> release. See §3 for exactly what's done vs. outstanding.

---

## 1. Status snapshot (verified 2026-09-28)

| Check | Result |
|---|---|
| Release candidate branch | `integration` — **58 commits ahead of `origin/main`, 0 behind** |
| Release branch | `release/1.0.0` created off `integration`; prep changes staged in working tree, **not yet committed** |
| Unit tests | **200 passed** (re-verified after version bump) |
| Coverage | **95%** (`src/stac_fastapi/globus_search`) |
| `black --check` | Clean (19 files, incl. edited `__init__.py`) |
| Secrets | `.env` is gitignored and **not** tracked — no leak |

The code is in good shape. The gaps below are release *hygiene*, not code defects.

---

## 2. Blockers & gaps to close before tagging 1.0.0

| # | Item | Status | Notes |
|---|---|---|---|
| 1 | **Version defined** | ✅ Done | `[project]` table with `version = "1.0.0"` added to `pyproject.toml`; `__version__ = "1.0.0"` in `__init__.py`. TOML parses; tests still pass. Uncommitted. |
| 2 | **LICENSE** | ✅ Done | `LICENSE` (MIT) added at repo root. Confirm the copyright line (`2026 ESGF2-US contributors`) matches ESGF/UChicago policy before committing. |
| 3 | **CHANGELOG** | ✅ Done | `CHANGELOG.md` added with the 1.0.0 section. Uncommitted. |
| 4 | CI/CD | ⏭️ Deferred | No `.github/`. You opted out for 1.0.0. A stale `add-ci-cd` branch exists on origin if you revisit. |
| 5 | Lint config gaps | ⚠️ Optional | No `ruff`/`isort` config in `pyproject.toml`; run with defaults they report style noise that disagrees with the project's actual `.flake8` (line-length 88, ignores E203/W503/W504) and with `black`. Not release-blocking, but worth a follow-up. |
| 6 | `globus_sdk` deprecation | ℹ️ Note | Tests emit `RemovedInV4Warning: 'SearchQuery' is deprecated`. Fine for 1.0.0 on `globus_sdk==3.62.0`; track before any v4 bump. |
| 7 | ~40 stale origin branches | ⏭️ Post-release | Cleanup listed in §8. |

The three release-hygiene blockers (1–3) are now resolved in the working tree of
`release/1.0.0`. What remains is committing them, then the merge/tag/release flow.

---

## 3. Runbook

### Phase 0 — Prep branch ✅ Done

`release/1.0.0` has been created off `integration`.

### Phase 1 — Add version ✅ Done

`[project]` table added to `pyproject.toml` and `__version__ = "1.0.0"` set in
`src/stac_fastapi/globus_search/__init__.py`. (Content reference in §4/§5.)

### Phase 2 — Add LICENSE ✅ Done

`LICENSE` (MIT) created at repo root. **Confirm the copyright line** before
committing (see §5).

### Phase 3 — Add CHANGELOG ✅ Done

`CHANGELOG.md` created (see §4).

### Phase 4 — Commit prep, then open PR ⬜ Outstanding

The prep edits are in the working tree of `release/1.0.0` but not committed. The
commit must be run in your local environment (the agent sandbox mount blocks the
`.git/*.lock` cleanup git needs, so it can't run `git add`/`git commit` there).

```bash
cd ~/Projects/python/globus/west-discovery

# one-time cleanup of stale locks + agent probe artifacts (safe to run)
rm -f .git/index.lock .git/packed-refs.lock .git/refs/heads/_probe_branch.lock
rm -f _b _c _perm_test
git branch -D _probe_branch 2>/dev/null || true

# verify green
python -m pytest            # expect 200 passed
black --check src tests     # expect clean

git status                  # expect: pyproject.toml + __init__.py modified; LICENSE, CHANGELOG.md, RELEASE_PLAN_1.0.0.md new
git add pyproject.toml src/stac_fastapi/globus_search/__init__.py LICENSE CHANGELOG.md RELEASE_PLAN_1.0.0.md
git commit -m "chore(release): prepare 1.0.0 (version, LICENSE, CHANGELOG)"
git push -u origin release/1.0.0
```

Then open a PR **`release/1.0.0` → `main`** on GitHub (per your choice to merge via PR).
Title: `Release 1.0.0`. Use the CHANGELOG as the PR description.

### Phase 5 — Merge

Review, approve, and merge the PR into `main` (use a merge commit to preserve history).
Then sync locally:

```bash
git checkout main && git pull
```

### Phase 6 — Tag & GitHub release

```bash
git tag -a v1.0.0 -m "west-discovery 1.0.0"
git push origin v1.0.0
```

Create the GitHub Release from tag `v1.0.0`, titled `1.0.0`, body = the CHANGELOG
1.0.0 section. Attach a built Docker image reference if you publish one.

### Phase 7 — Post-release

- Fast-forward `integration` to `main` so they don't diverge:
  ```bash
  git checkout integration && git merge --ff-only main && git push
  ```
- Prune stale branches (§8).

---

## 4. CHANGELOG.md content (applied to `CHANGELOG.md`)

```markdown
# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-09-28

First production release. A STAC API backed by Globus Search, exposing ESGF
climate datasets as STAC Collections and Items with CQL2 filtering, aggregation,
free-text search, and queryables derived from ESGF controlled vocabularies.

### Added
- Queryables endpoint generated from esgvoc, per `collection_id`.
- CQL2 filter support for both bare and collection-qualified property names.
- Accept either `filter` or `filter_expr` on search and aggregate endpoints.
- Support for the `cmip6test` collection.
- `apply_datetime_filter` for start/end temporal extent.

### Changed
- Converted to a multi-stage Dockerfile; dropped Poetry in favor of
  `requirements.txt` / `requirements-dev.txt`.
- Moved all HTTP error handling out of the database layer into the view layer;
  replaced bare assertions and silent failures with proper HTTP errors.
- STAC-standard `numberMatched` / `numberReturned` on ItemCollection responses.
- Transparent qualification of CQL2 property names.
- README rewritten to reflect the current state of the project.

### Fixed
- Resolve collection namespace for CQL2 filters in aggregate.
- Resolve alternate/replica name via `assets.alternate:name`.
- Restore CQL2 field-name namespacing and fail loudly when unresolvable.
- CMIP6Test aggregation namespace and STAC validation.

### Testing
- Unit test coverage expanded to ~95%+ (200 tests); added error-handling paths;
  queryables tests made independent of the installed esgvoc CV.

[1.0.0]: https://github.com/esgf2-us/west-discovery/releases/tag/v1.0.0
```

---

## 5. LICENSE content (applied to `LICENSE`)

```
MIT License

Copyright (c) 2026 ESGF2-US contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

> Confirm the copyright holder before committing. If ESGF/UChicago policy
> prefers a named institution, substitute it on the copyright line.

---

## 6. Pre-flight checklist (tick before you tag)

- [x] `[project]` table with `version = "1.0.0"` added to `pyproject.toml`
- [x] `__version__ = "1.0.0"` in `__init__.py`
- [x] `LICENSE` (MIT) added — ⚠️ copyright holder still to be confirmed
- [x] `CHANGELOG.md` added
- [x] `python -m pytest` → 200 passed
- [x] `black --check src tests` → clean
- [ ] Prep changes committed on `release/1.0.0` (see Phase 4)
- [ ] Docker image builds: `docker build --target production -t discovery-api:1.0.0 .`
- [ ] PR `release/1.0.0` → `main` opened, reviewed, approved
- [ ] Merged to `main`
- [ ] `v1.0.0` tag pushed
- [ ] GitHub Release published
- [ ] `integration` fast-forwarded to `main`

---

## 7. Optional follow-ups (not blocking 1.0.0)

- Add `[tool.ruff]` and `[tool.isort]` (`profile = "black"`) config to `pyproject.toml`
  so lint tooling agrees with `black` and `.flake8`.
- Add a GitHub Actions CI workflow (ruff/black/pytest on push + PR). The
  `origin/add-ci-cd` branch is a starting point.
- Plan the `globus_sdk` v4 migration (`SearchQuery` → `SearchQueryV1`).

---

## 8. Stale branch cleanup (post-release)

~40 branches exist on `origin`. After 1.0.0, delete merged/dead ones, e.g.:

```bash
# review first
git branch -r --merged origin/main

# delete a remote branch once confirmed merged/abandoned
git push origin --delete <branch-name>
```

Candidates (confirm each before deleting): `add-caching`, `add-ci-cd`,
`add-collection-filter`, `add-like-filter`, `add-queryables`, `add-tests`,
`add_aggregation`, `better-readme`, `bug/more-dynamic-aggregations`,
`enhance-queryables`, `esgf-data-challenge-*`, `esgvoc-collections`,
`fix-*`, `formats`, `free-text`, `multi-stage-docker`, `proper-error-returns*`,
`support-item-collection`, `task/add-post-search`, `unit-tests`,
`update-unit-tests`, `update-version-6.0.0`.
```
