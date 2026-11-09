# Releases

Merges to `master` run the local release workflow. Commitizen calculates the next
stable version from Conventional Commits. The workflow checks the selected source,
builds one wheel and one sdist on Python 3.11, and retains those original bytes,
notes, manifest, and prepared Git bundle before publication. Release automation
runs on Python 3.12 with uv 0.13.1 and Commitizen 4.13.9.

The publisher pushes the prepared stable version commit and annotated tag together.
It uploads the originals to qlty first, then uploads the same files to PyPI. Both
registries must serve the exact original SHA256 hashes before the GitHub Release
can complete. A stable release then posts the retained notes to Slack. Prereleases
publish to both registries without moving `master` or posting to Slack.

## Setup

Infrastructure PR [#1801](https://github.com/fyndiq/infrastructure/pull/1801)
is merged. Before enabling releases, verify that its applied resources permit
this repository to use the `qlty-publish` environment and workload identity for
`gha-qlty-publish@fyndiq.iam.gserviceaccount.com`.

The environment must contain `RELEASE_APP_ID`, `RELEASE_APP_KEY`, `PYPI_API_TOKEN`,
and `SLACK_DEPLOYS_BOT_TOKEN`. The GitHub App must be installed on this repository,
with contents write access and the protected branch bypass used for release
commits. The PyPI token must permit publishing `eventsourcing-helpers`. Check
credential usability without printing secret values. Infrastructure merge and
secret names alone do not verify access.

The App token performs Git and Release writes. The workflow token reads and
retains Actions artifacts and dispatches source-race retries. Complete credentials,
IAM access, and baseline bootstrap before enabling the release workflow and
required release-preview checks.

Before enabling release checks, a maintainer must create and push the annotated tag `v2.4.1` at
`b2c760e0af9690fde74c351c76250c5c73eab570`. Verify the target commit before pushing. The existing public wheel contains 55
package and test source files; each matches that commit byte for byte.
The narrowly scoped bootstrap accepts only this repository, version, tag, and
commit, together with the existing public wheel
`eventsourcing_helpers-2.4.1-py3-none-any.whl` and SHA256
`7b274910f609f872f2c49dd2ebbac1bd26c657050b312d03c95dc666c4dbc037`.
That legacy release has no sdist and does not require a qlty backfill. Every new
release requires exactly one wheel and one sdist in both registries.

## Installation

Create or activate a virtual environment before installing. From public PyPI:

```sh
uv venv
uv pip install --index https://pypi.org/simple eventsourcing-helpers
```

For qlty, configure Google Application Default Credentials with access to its
Artifact Registry repository, then install the Google keyring helper and package:

```sh
uv tool install --with keyrings.google-artifactregistry-auth==1.1.2 keyring==25.7.0
uv pip install --index https://oauth2accesstoken@europe-north1-python.pkg.dev/fyndiq/qlty/simple/ --keyring-provider subprocess eventsourcing-helpers
```

Both registries serve the same distribution bytes for each new release. Pin a
version with `eventsourcing-helpers==<version>` when reproducibility is required.
To install a prerelease, specify its exact version in that requirement.

### uv projects using qlty

The `uv pip install --index` command above gives qlty priority but permits fallback
to public PyPI when a package is absent. For a uv project that must resolve both
`eventsourcing-helpers` and its transitive dependency `confluent-kafka-helpers`
only from qlty, merge this configuration into the consuming project's root
`pyproject.toml`. Preserve its other dependencies and uv settings. If `qlty` is
already defined, reuse it without changing its `explicit` or `default` settings;
other private dependencies may rely on them:

```toml
[project]
dependencies = ["eventsourcing-helpers>=2.4.2,<3"]

[tool.uv]
keyring-provider = "subprocess"
constraint-dependencies = ["confluent-kafka-helpers>=1.0.0"]

[[tool.uv.index]]
name = "qlty"
url = "https://oauth2accesstoken@europe-north1-python.pkg.dev/fyndiq/qlty/simple/"
explicit = true

[tool.uv.sources]
eventsourcing-helpers = { index = "qlty" }
confluent-kafka-helpers = { index = "qlty" }
```

Use the same Google Application Default Credentials and keyring helper described
above. In this example, the explicit index serves only packages assigned to it;
other dependencies can still resolve from public PyPI.

A library's published dependency metadata names its dependencies without carrying
its uv source configuration into consuming projects. Configure both source
mappings in the consumer. With uv 0.13.5, the versioned root constraint also
activates the source mapping for the transitive Kafka helper; the mapping alone
or a constraint without a version specifier does not. The `>=1.0.0` constraint
matches the requirement published by `eventsourcing-helpers` 2.4.2. It keeps the
Kafka helper transitive without adding another version restriction. See uv's
[dependency sources](https://docs.astral.sh/uv/concepts/projects/dependencies/#dependency-sources)
and [constraints](https://docs.astral.sh/uv/reference/settings/#constraint-dependencies).

Update the lockfile, upgrading the two helpers if they are already locked:

```sh
uv lock --upgrade-package eventsourcing-helpers --upgrade-package confluent-kafka-helpers
```

Before installing, inspect both helpers' entries in `uv.lock`. Each
`source.registry` must be
`https://europe-north1-python.pkg.dev/fyndiq/qlty/simple/`, and its wheel and sdist
URLs, where present, must point to that Artifact Registry repository. Check the
selected versions and review other lockfile changes before committing it.
Then verify installation from the lockfile with a fresh cache:

```sh
uv sync --frozen --no-cache
```

## Manual releases and recovery

Run the Release workflow on `master`. Leave all inputs empty for a stable release.
For a prerelease, supply all three inputs: `source_ref` (branch, tag, or SHA),
`target_version` (stable `major.minor.patch`), and `phase` (`alpha`, `beta`, or `rc`).
The workflow fixes the source SHA and allocates the next unused sequence across
Git tags and both registries. It preserves that allocation across reruns.

After preparation or artifact-upload failures, rerun all jobs so inspection can
select the original retained candidate. Failed or missing preparation evidence
requires restoring originals before another preparation attempt.

For interrupted publication, dispatch with `resume_tag` alone. The workflow uses
the retained originals and version, verifies existing files in each registry, and
continues missing uploads. A partial upload, hash disagreement, missing original,
or lost publication evidence blocks newer releases. Do not rebuild distributions,
replace retained assets, delete published files, move tags, or choose another
version to bypass a failed release. Restore missing originals or correct access,
then resume the same tag. Actions transport artifacts expire after 90 days;
the draft GitHub Release retains the canonical originals for recovery.

For a failed Slack announcement after a completed stable release, dispatch with
`announce_tag` alone. The workflow verifies the tag, retained notes, both registry
publications, and Slack history before posting. A retry can reconcile a successful
post whose response was lost. Leave `release_retry` at `0`; source-race retries are
bounded and managed by automation.

Source checks and builds have no publishing secrets. The publisher executes the
orchestrator from the trusted workflow snapshot and reads selected-source
metadata as data; it never executes the selected source or its build backend.
PyPI reads use only HTTPS public PyPI hosts, without credentials, and hash the
actual downloaded bytes. Only the PyPI upload child receives the PyPI token.
