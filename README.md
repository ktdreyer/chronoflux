# Chronoflux

Chronoflux tracks and compares CI build times across GitLab CI, Konflux, and eventually GitHub Actions. See the [design document](docs/design.md) for the architecture and data model, and the [PoC plan](docs/plan.md) for the first milestone and proposed CLI.

## Local CLI

The first implementation is a Python CLI for the `rhaiis-containers` scope. Install the optional BigQuery client only when using persistence or comparisons:

```sh
python -m pip install -e '.[bigquery]'
chronoflux doctor
chronoflux backfill --scope rhaiis-containers --source gitlab \
  --since 2025-02-01T00:00:00Z --until 2025-03-01T00:00:00Z --dry-run
chronoflux backfill --scope rhaiis-containers --source konflux \
  --since 2025-02-01T00:00:00Z --until 2025-03-01T00:00:00Z --dry-run
chronoflux compare --scope rhaiis-containers --sha <commit> --format table
```

GitLab uses `GITLAB_TOKEN` when set (public project reads can work without it). Live Konflux reads use the current `oc` context and verify it matches the configured p02 API server. KubeArchive needs `KUBEARCHIVE_URL` and, if required, `KUBEARCHIVE_TOKEN`; the CLI currently expects the standard namespaced Tekton Kubernetes list API with `metadata.continue` pagination. Set `TEKTON_API_VERSION` if the cluster serves a different version. BigQuery uses the configured `aipcc-cicd.chronoflux` dataset by default, with `BIGQUERY_PROJECT` and `BIGQUERY_DATASET` available as overrides. Bootstrap the dataset and tables with `scripts/bootstrap-bigquery.sh aipcc-cicd APPROVED_REGION`; it sets the dataset description to link back to this repository. Authenticate to BigQuery before running the script. Chronoflux's write path also creates missing tables, but never creates the dataset or changes cloud IAM.

The default `gitlab_project_id = 0` resolves the numeric ID from GitLab at runtime. Add `variant_components` entries to `chronoflux.toml` only after checking real Konflux component labels; without a verified mapping, comparisons remain unmatched instead of guessing. `compare` selects the highest GitLab job ID per pipeline/name and the latest completed Konflux run per SHA/component/platform; it reports retries/runs and excludes failed, incomplete, or unmatched rows from successful timing summaries. It suppresses median below 3 samples and p90 below 10. `doctor` does not test BigQuery write permission. Run tests with `python -m pytest`.

The fixtures are synthetic redacted examples, not captures from MR !1458. This implementation has fixture-backed extraction, dry-run backfills, BigQuery DDL/upserts/checkpoints, and a comparison query, but p02/KubeArchive field conventions, the MR !1458 mapping, component/platform mapping, and BigQuery access still need validation against real data before treating results as an accepted comparison.
