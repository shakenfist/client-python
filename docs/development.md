# Merging and releasing

How a change to the client gets tested, merged to `develop`, and
released to PyPI. The one-time release infrastructure setup is in
[RELEASE-SETUP.md](https://github.com/shakenfist/client-python/blob/develop/RELEASE-SETUP.md).

## Continuous integration

`functional-tests.yml` is the CI workflow that merges and releases
depend on. It runs on pull requests, on merge queue entries, and when
dispatched (a "please retest" comment dispatches it through
`pr-retest.yml`). Its jobs:

| Job | What it does |
|-----|--------------|
| Check paths | Decides whether the change touches anything outside `docs/` and `mkdocs.yml`. A documentation-only change skips the two jobs below. |
| `sanity_checks` | flake8 on the changed files, an install of the requirements, and the unit tests (`tox -epy3`). |
| Smoke cluster | Deploys a single-node Shaken Fist cluster with this change's client and the server from shakenfist `develop`, through `smoke-cluster.yml` in shakenfist/actions, and runs the smoke suite against it. |
| Automated reviewer | Reviews the pull request once the sanity checks pass. Pull requests only, and advisory. |
| Can enqueue, Can merge | The checks the merge queue requires, described below. |

Documentation-only changes are filtered by a job rather than by
`paths-ignore`, so that the workflow always reports: a required check
in a workflow that never ran would block the merge forever.

## Merging

Pull requests merge through a merge queue on `develop`, not with the
merge button directly. A pull request can be queued once its "Can
enqueue" check passes. The queue then runs `functional-tests.yml`
again on the commit it is about to merge, smoke cluster included, and
merges only when "Can merge" passes. That second run is the one a
release counts (see below), so a change merged around the queue, by an
administrator bypassing it, cannot be released until a later commit has
gone through the queue.

"Can enqueue" and "Can merge" each require every other job in the
workflow to succeed. The one exception is a documentation-only change,
where the skipped sanity checks and smoke cluster count as passed. A
new job in `functional-tests.yml` belongs in the `needs:` of both.

The queue builds one entry at a time (`max_entries_to_build: 1` in the
`develop` ruleset). The workflow's concurrency groups depend on that:
a queue run shares a group with whatever the queue builds next, so
that a superseded run is cancelled and gives back its runner and its
smoke cluster on the shared under-cloud. Raising the setting would make
each entry cancel the one ahead of it, so revisit those groups first.

## Releasing

Pushing a `v*` tag triggers `release.yml`. Its jobs, in order:

1. `ci-gate` checks that `functional-tests.yml` succeeded on the tagged
   commit, in its merge queue run. A run started by hand, as
   `pr-retest.yml` does with `workflow_dispatch`, does not count, so to
   recover from a flaky queue run, use "Re-run jobs" on that merge
   queue run itself. How the gate decides is documented with the
   action, as
   [`require-ci-success`](https://github.com/shakenfist/actions/blob/main/docs/actions.md#require-ci-success).
   It has no override.
2. `build` builds the sdist and wheel, then runs `twine check` and
   [`check-dist`](https://github.com/shakenfist/actions/blob/main/docs/actions.md#check-dist),
   which fails if the wheel ships a test directory. It runs only if
   the gate passed.
3. The publishing jobs wait for approval in the `release` environment,
   re-sign the tag with gitsign, and publish to PyPI with attestations.

So tag a commit that is on `develop` and went through the merge queue,
and wait for its `functional-tests.yml` run to finish first. A gate
failure means nothing was signed or published, so every failure is
recoverable: the template's
[recovery table](https://github.com/shakenfist/development/blob/main/templates/release-automation/README.md#recovering-from-a-gate-failure)
says what to do for each cause, which is usually moving the tag or
re-running CI and then the `ci-gate` job.

Running `release.yml` by hand with `workflow_dispatch` skips the gate
and the publishing jobs, and only builds and checks the packages.

The version comes from the tag through setuptools_scm.

## What the wheel ships

The wheel holds the `shakenfist_client` package and nothing else: the
unit tests in `shakenfist_client/tests/` run from the checkout and are
excluded. `pyproject.toml` sets `include-package-data = false`, because
otherwise setuptools_scm offers every tracked file to the wheel as data
and the excluded tests come straight back. The package reads no data
files at runtime; if that changes, declare them under
`[tool.setuptools.package-data]`, or they will not ship.
