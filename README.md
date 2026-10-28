# Chronoflux

Chronoflux tracks and compares CI build times across GitLab CI, Konflux, and eventually GitHub Actions. See the [design document](docs/design.md) for the architecture and data model, and the [PoC plan](docs/plan.md) for the first milestone and proposed CLI.

## Local CLI

The first implementation is a Python CLI for the `rhaiis-containers` scope. Use `uv.lock` for reproducible local dependencies (including the BigQuery client). Copy `chronoflux.example.toml` to `chronoflux.toml` (gitignored) and adjust it for your environment before running the CLI:

```sh
uv sync --extra bigquery --extra test --frozen
.venv/bin/chronoflux doctor
.venv/bin/chronoflux backfill --scope rhaiis-containers --source gitlab \
  --since 2025-02-01T00:00:00Z --until 2025-03-01T00:00:00Z --dry-run
.venv/bin/chronoflux backfill --scope rhaiis-containers --source konflux \
  --since 2025-02-01T00:00:00Z --until 2025-03-01T00:00:00Z --dry-run
.venv/bin/chronoflux compare --scope rhaiis-containers --sha <commit> --format table
```

For GitLab backfills, the containers project requires an authenticated GitLab API token. With an existing `glab` login and Google application-default credentials, load a bounded interval after checking access and previewing its size:

```sh
export GITLAB_TOKEN="$(glab config get token --host gitlab.com)"
.venv/bin/chronoflux doctor --source gitlab
.venv/bin/chronoflux doctor --source bigquery
.venv/bin/chronoflux backfill --source gitlab \
  --since 2026-10-19T16:30:00Z --until 2026-10-19T16:35:00Z --dry-run
.venv/bin/chronoflux backfill --source gitlab \
  --since 2026-10-19T16:30:00Z --until 2026-10-19T16:35:00Z
```

`backfill` can be rerun safely for the same interval; it upserts by source ID. Use a larger interval only after checking the dry-run counts. To load Konflux, first log into p02 and configure `KUBEARCHIVE_URL` and `KUBEARCHIVE_TOKEN`, then run `doctor --source konflux` before `backfill --source konflux`. Set `CHRONOFLUX_KONFLUX_SOURCES=archive` to skip the live API, for example when an older window lives entirely in KubeArchive and there is no current oc login.

GitLab uses `GITLAB_TOKEN` when set (public project reads can work without it). Live Konflux reads use the current `oc` context and verify it matches the configured p02 API server. KubeArchive needs `KUBEARCHIVE_URL` and, if required, `KUBEARCHIVE_TOKEN`; the CLI expects the standard namespaced Tekton Kubernetes list API with `metadata.continue` pagination. The scope's `label_selectors` are applied server-side to both live and archive listings — the KubeArchive API ignores `fieldSelector` during pagination, so label selectors are the only way to bound an archive walk, and the walk also stops once a page predates the backfill window (KubeArchive serves runs newest-first). Set `TEKTON_API_VERSION` if the cluster serves a different version. BigQuery uses the configured `aipcc-cicd.chronoflux` dataset by default, with `BIGQUERY_PROJECT` and `BIGQUERY_DATASET` available as overrides. Bootstrap the dataset and tables with `scripts/bootstrap-bigquery.sh aipcc-cicd APPROVED_REGION`; it sets the dataset description to link back to this repository. Authenticate to BigQuery before running the script. Chronoflux's write path also creates missing tables, but never creates the dataset or changes cloud IAM.

The default `gitlab_project_id = 0` resolves the numeric ID from GitLab at runtime. `variant_components` maps GitLab image-job variants to Konflux components: each containers-repo SHA builds exactly one component named `rhaiis-<variant>-ubi9` plus a release-stream suffix (for example `-3-6`) chosen by the merge request's target branch, so entries use a trailing `*` to match the suffix. This convention was verified against real archived runs in October 2026 (MRs !1458, !1541, !1542, !1546, !1559); if the naming ever changes, keep mappings exact instead. `compare` selects the highest GitLab job ID per pipeline/name and the latest completed Konflux run per SHA/component/platform; it reports retries/runs and excludes failed, incomplete, or unmatched rows from successful timing summaries. It suppresses median below 3 samples and p90 below 10. `doctor` does not test BigQuery write permission and checks only the first page of each Konflux list. Run tests with `.venv/bin/python -m pytest`.

The fixtures are synthetic redacted examples, not captures from MR !1458, but their annotation and label keys mirror the real p02 conventions verified in October 2026. p02/KubeArchive field conventions, the MR !1458 mapping, and the component/platform mapping are validated against real data; BigQuery reads and writes work with personal application-default credentials. What remains before treating a comparison as accepted: broader sample coverage, validation of multi-platform builds, and a documented comparison of trigger parity.
