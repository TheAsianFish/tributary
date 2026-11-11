# Releasing

A release is a `v*` tag on `main`. Two workflows react to it:

| Workflow | On a `v*` tag | Run by hand on a branch (dry run) |
|---|---|---|
| `release.yml` | builds the sdist and wheel, `twine check --strict`, installs the wheel in a clean venv and runs it (`make dist`); checks that the tag matches `pyproject.toml`'s version and that CHANGELOG.md has its section; publishes to PyPI by trusted publishing; creates the GitHub release with the changelog section and the files | the build and every check; the files are uploaded to the run as the `dist` artifact; nothing is published |
| `images.yml` | builds `tributary-api` and `tributary-stream` for linux/amd64 and linux/arm64 and pushes them to `ghcr.io/<owner>/` with the version, `MAJOR.MINOR`, the short SHA and (for a final release) `latest` | both images build for both platforms; nothing is pushed |

The package is `tributary-platform` on PyPI (the name `tributary` belongs to
an unrelated project); the import name and the command are `tributary`.

## Once, before the first release

1. **PyPI trusted publisher.** On pypi.org, under *Your account → Publishing →
   Add a new pending publisher*, enter: PyPI project name
   `tributary-platform`, owner `TheAsianFish`, repository `tributary`,
   workflow `release.yml`, environment `pypi`. No token is stored anywhere.
   (If trusted publishing is not an option, create a project-scoped API token
   and store it as the `PYPI_API_TOKEN` secret of the `pypi` environment, and
   add `with: password: ${{ secrets.PYPI_API_TOKEN }}` to the publish step.)
2. **The `pypi` environment.** In the repository's *Settings → Environments*,
   create `pypi` and add yourself as a required reviewer, so a pushed tag
   waits for a click before anything reaches PyPI. Under *Deployment branches
   and tags*, allow only tags matching `v*`.
3. **Tag protection.** Under *Settings → Rules → Rulesets*, add a tag ruleset
   for `v*` that restricts creation, update and deletion to maintainers, so
   nobody else can trigger a publish.
4. **GHCR.** Nothing to configure: `images.yml` pushes with the workflow's
   `GITHUB_TOKEN` (`packages: write`). After the first push, open each
   package's settings on GitHub and set its visibility (public if the images
   should be pullable without logging in) and link it to the repository.

## Every release

1. **Dry-run on the release branch.** Push the branch, then run both
   workflows by hand on it (*Actions → Release → Run workflow*, and the same
   for *Images*), or from the command line:

   ```bash
   gh workflow run release.yml --ref <branch>
   gh workflow run images.yml --ref <branch>
   gh run watch
   ```

   Both must be green. Locally, `make dist` runs the same package checks.
2. **Version and changelog.** In `pyproject.toml`, set `version` to the
   release (`0.1.0`), and set the same value in `tributary/__init__.py`. Move the
   `[Unreleased]` entries into a `## [X.Y.Z] - YYYY-MM-DD` section of
   CHANGELOG.md dated the day you tag, and update the compare links at the
   bottom. Run `make check`.
3. **Merge to `main`** through the pull request, with CI green.
4. **Tag the merge commit and push the tag:**

   ```bash
   git switch main && git pull --ff-only
   git tag -a v0.1.0 -m "Tributary 0.1.0"
   git push origin v0.1.0
   ```

   The tag must point at the commit on `main` (a squash merge creates a new
   commit, so tag after merging, never the branch's commit).
5. **Approve the `pypi` deployment** when the run asks for it. Then check:
   `pip install tributary-platform==X.Y.Z` in a fresh venv, `tributary
   version`, the GitHub release page, and
   `docker pull ghcr.io/theasianfish/tributary-api:X.Y.Z`.
6. **Open the next cycle:** set the version to the next `.dev0`
   (`0.2.0.dev0`) and add an empty `[Unreleased]` section.

## If something goes wrong

- **The build or a check fails on the tag:** nothing was published. Delete
  the tag (`git push --delete origin vX.Y.Z && git tag -d vX.Y.Z`), fix it on
  `main` and tag again.
- **PyPI succeeded, but the GitHub release or an image failed:** re-run the
  failed job from the run page. PyPI never accepts the same version twice, so
  never re-tag a version that reached PyPI.
- **A bad version reached PyPI:** yank it on pypi.org (*Manage → Releases →
  Yank*) with a reason, then release a fixed `X.Y.Z+1`. Do not delete it.
- **A bad image was pushed:** push a fixed release. To move `latest` back, run
  `docker buildx imagetools create -t ghcr.io/<owner>/tributary-api:latest
  ghcr.io/<owner>/tributary-api:<good version>`.

## What ships

- **Wheel and sdist:** the `tributary` package only (the CLI, the agent, the
  API with the `api` extra, the stream job with the `spark` extra, dbt and
  Airflow support with the `warehouse` and `airflow` extras). The sample,
  BIRD files, recordings and benchmark results stay in the repository; the
  commands that need them (`produce`, `warehouse refresh`, the evaluations)
  run from a clone.
- **Images:** `tributary-api` (the API, non-root, read-only root filesystem in
  Compose) and `tributary-stream` (the Spark job and the producer, with the
  sample baked in). The Airflow and MinIO images are built locally by
  `docker compose` and not published.
