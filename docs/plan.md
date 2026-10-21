# Chronoflux PoC plan

## Outcome and scope

Answer [RHAI-10822](https://redhat.atlassian.net/browse/RHAI-10822): for the same `rhaiis/containers` commit and image variant, how long does the GitLab image job take compared with the Konflux build, and where does Konflux spend time? Start with GitLab CI and Konflux in `ai-tenant`; defer GitHub Actions, other repositories, dashboards, and alerts. Follow the [design](design.md) for the data model and recovery requirements.

**PoC deliverable:** A repeatable CLI run that loads several weeks of GitLab pipelines/jobs and Konflux PipelineRuns/TaskRuns into BigQuery, then prints a comparison with source URLs, sample size, exclusions, and separate wall-clock and execution metrics. Validate the result on [MR !1458](https://gitlab.com/redhat/rhel-ai/rhaiis/containers/-/merge_requests/1458), where the ROCm job succeeded although the GitLab pipeline failed for another job. The GitLab ROCm job and Konflux build have the same SHA and a single x86_64 build platform; use this as a first diagnostic, not as a performance conclusion.

## Delivery sequence

| Phase | Work | Exit criterion |
| --- | --- | --- |
| 0. Prove access and inspect data | Confirm read access to GitLab pipeline/job APIs, **live and archived** PipelineRuns/TaskRuns in p02 `ai-tenant`, and a writable PoC BigQuery dataset. Inspect real examples for commit, component, parent TaskRun, platform, and timestamp fields. Test local Google credentials and the intended OpenShift-to-GCP identity separately. | `chronoflux doctor` checks each dependency; a short recorded mapping identifies the actual fields and archive pagination/retention behavior. If p02 or archive access is unavailable, stop here rather than designing around commit-status timestamps. |
| 1. Extract one known interval | Build CLI, configuration for one scope, and GitLab and Konflux extractors. Page GitLab jobs with `include_retried=true`; retrieve both live and archived Konflux objects and deduplicate by cluster/namespace/UID. Parse Tekton conditions and job names without guessing missing platforms. | `backfill --dry-run` finds MR !1458's GitLab image job, Konflux PipelineRun, and associated TaskRuns. Sanitized API fixtures test parsing, pagination, retries, and live/archive duplication. No BigQuery dependency for this check. |
| 2. Persist and recover | Create the four source tables plus staging and sync-state/results tables. Deduplicate staging keys before BigQuery `MERGE`. Implement bounded backfills and checkpoint-based `sync`, including revisiting incomplete runs. | Running the same backfill twice yields the same logical rows; a pending-to-completed update changes its row; a failed or partial poll does not advance its checkpoint; two missed hourly schedules can be recovered. GitLab retries remain distinct rows. |
| 3. Compare like with like | Add a BigQuery query and CLI output for matching SHA + variant/platform, selecting a documented GitLab retry attempt. Report GitLab job time, Konflux PipelineRun wall time, Konflux TaskRun breakdown, and unmatched/failed/cancelled counts. | `compare --sha <MR-1458-SHA>` links to the correct GitLab ROCm job and Konflux build without equating GitLab's failed **pipeline** with its successful ROCm **job**. A wider sample reports medians and p90s only when sample sizes justify them. Do not call a Konflux multi-platform index an x86_64-only job; label work that cannot be paired. |
| 4. Automate only after validation | Containerize the *same* CLI; deploy a CronJob with namespaced p02 read permissions, archive access, read-only GitLab token, and GCP identity. Run `sync` on a schedule. | Scheduled and manual runs use the same ingestion code; sync results expose counts/checkpoints/errors; a deliberately missed run and a live-to-archive transition recover without loss or duplicate rows. |

After the PoC, decide whether to add GitHub Actions workflows **and jobs**, more repositories, and operational alerts. Those are separate follow-on milestones, not prerequisites for the first comparison. This PoC does not prove trigger/check parity.

### Decisions to settle in phase 0

* **Run location:** Prefer p02 so a namespaced service account can read `ai-tenant`; a CronJob elsewhere needs separate p02 authentication. Confirm KubeArchive access for the same identity.
* **BigQuery ownership:** Identify a PoC GCP project, dataset owner, region, and retention/cost limits. Provision the dataset and tables outside the runtime ingestion identity; do not give the CronJob dataset-admin privileges for convenience.
* **Identity:** Confirm an actual projected p02 service-account token works with GCP Workload Identity Federation before depending on it for deployment. Local development may use personal credentials without committing them.
* **Matching:** Verify whether TaskRun parameters/results identify the platform for a multi-platform build. If not, compare the overall Konflux image/index completion separately from per-architecture GitLab jobs rather than inventing a per-platform Konflux time.

## Proposed CLI

Single executable, `chronoflux`, with a configured scope (`rhaiis-containers`) that names the GitLab host/project and Konflux cluster/namespace/application. Keep credentials in environment/cluster secrets, not the configuration file. All times are UTC ISO 8601; `--since` is inclusive and `--until` exclusive. Backfills select runs by source creation time. Comparison date ranges select GitLab pipelines by creation time, then join matching Konflux builds by SHA even if they were created later.

| Command | Purpose |
| --- | --- |
| `chronoflux doctor [--source gitlab|konflux|bigquery]` | Check credentials, API access, archive access/pagination, and BigQuery read/write capability; print no secrets. |
| `chronoflux backfill --scope rhaiis-containers --source gitlab|konflux --since TIME --until TIME [--dry-run]` | Discover and ingest an explicit creation-time interval, independently of checkpoints. `--dry-run` prints counts and sample IDs without writes. Repeatable; uses the same extractor/loader as `sync`. |
| `chronoflux sync --scope rhaiis-containers [--source gitlab|konflux] [--dry-run]` | Scheduled incremental collection using per-source checkpoints, overlap, and reconciliation of incomplete runs. Only successful loads advance checkpoints. |
| `chronoflux compare --scope rhaiis-containers (--sha SHA | --since TIME --until TIME) [--variant NAME] [--format table|json|csv]` | Read BigQuery, show paired job/build evidence and unmatched runs. `--sha` is for debugging a known change; a date range gives a summary with sample sizes and exclusions. Never writes. |

Do not add separate `collect`, `load`, or `schema migrate` commands for the PoC: they create multiple paths to maintain. Manage table DDL as reviewed SQL files; use `backfill --dry-run` and `doctor` for development checks. `backfill` should not move the routine `sync` checkpoint forward merely because a historical interval was processed.

## Initial work items

1. **Access spike:** get p02 and KubeArchive read access; request the BigQuery PoC dataset and test WIF feasibility. This is the critical blocker.
2. **Fixtures and mappings:** save redacted GitLab job/pipeline and live/archive Konflux examples for MR !1458; document component/platform and parent-task mappings.
3. **CLI and GitLab extraction:** implement `doctor`, GitLab pagination/retry handling, and dry-run backfill with tests.
4. **Konflux extraction:** implement live/archive lookup and TaskRun association with tests for missing/duplicate objects.
5. **BigQuery load and replay:** add SQL schemas, deduplicated upserts, checkpoints, and failure/restart tests.
6. **Comparison and rollout:** implement `compare`, validate it against URLs and timestamps, then containerize and schedule `sync`.

Keep these as small reviewable changes in order; do not begin with a general dashboard or three-platform framework.
