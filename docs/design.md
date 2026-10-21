# Cross-Platform CI Build Time Tracker

## 1. Purpose and scope

We want to compare build times across GitLab CI, Konflux, and eventually GitHub Actions. A scheduled collector will load run metadata into platform-specific BigQuery tables. We can then build reporting views without assuming that a GitLab pipeline, a GitLab image job, and a Konflux multi-platform PipelineRun do the same work.

**First milestone (RHAI-10822):** Collect GitLab pipelines and image jobs, and Konflux PipelineRuns and TaskRuns, for `rhaiis/containers`. Include both live and archived Konflux runs. Produce a query that pairs completed image builds by source SHA, component, and platform, and reports time to a usable image as well as task-level time. Verify the data against a small set of known runs before expanding coverage.

**Later:** Add GitHub Actions workflows and jobs for repositories after migration; expand the repository list only after the first comparison works. The target system remains cross-platform, but GitHub integration must not delay the GitLab–Konflux comparison.

Goals:

* Retain run and job/task timestamps, status, source identity, and URLs; do not collect logs or secrets.
* Recover from long builds, delayed API updates, missed schedules, retries, and Konflux resource cleanup without silently losing runs.
* Keep source-specific, typed schemas; store the fields needed to join comparable work even before adding a cross-platform view.
* Measure queue/wait, execution, and end-to-end time separately where the source exposes them. Distinguish a component image build from an entire pipeline with unrelated jobs or integration tests.

Non-goals: real-time event streaming, triggering CI, and asserting that runs with the same SHA necessarily perform equivalent work.

## 2. Architecture and collection

An OpenShift CronJob runs a containerized collector (Python or Go) on a schedule, initially hourly. Each extractor paginates through its source API with bounded concurrency, handles rate limits and transient errors, and loads records into BigQuery staging tables. A `MERGE` upserts them into source-specific tables. The collector has no local durable state; BigQuery stores its ingestion checkpoints and sync results.

**Discovery and recovery:** A two-hour lookback alone is not a correctness guarantee. A run can last longer than two hours, be updated later, or finish while the collector is down. For each source:

1. Record the last *successfully completed* discovery checkpoint in BigQuery, per repository/namespace and source. Advance it only after the corresponding load succeeds. On restart, replay from the checkpoint with an overlap to accommodate delayed indexing. Paginate all results, and split GitHub `created` ranges when needed to stay below its 1,000-result filtered-search limit.
2. Re-read recently discovered and still-incomplete runs until they reach a terminal state; upsert changed statuses and timestamps. Periodically reconcile a wider recent interval and provide an explicit backfill command for older gaps. Source API filtering differs: GitHub workflow listing supports `created` ranges; GitLab project pipelines support `created_after` and `updated_after`; do not assume each API supports a completion-time filter.
3. Query **both** the live Konflux Kubernetes API and KubeArchive. Konflux cleans up PipelineRuns and TaskRuns; the live API alone cannot supply a reliable history or recover from a long outage. Deduplicate records seen in both places by resource identity. Validate KubeArchive authentication, pagination, filters, retention, and which TaskRuns it preserves during the first milestone.
4. Write a sync result (source, scope, start/end, checkpoint, record counts, and errors) even when a successful poll finds no new runs. A failed or partial poll must not advance its checkpoint.

For initial deployment, restrict extraction to `rhaiis/containers` and its Konflux application/components in `ai-tenant`. Do not infer that a pipeline is absent just because its final status has not appeared yet.

### Source extractors

* **GitLab:** List project pipelines (`/projects/:id/pipelines`), fetch pipeline details and paginated jobs (`/projects/:id/pipelines/:pipeline_id/jobs?include_retried=true`). The jobs endpoint omits retried jobs by default; collect every attempt, keyed by job ID. Include image jobs and other jobs so we can identify what held up a pipeline. Capture `sha`, `source`, `ref`, `created_at`, `started_at`, `finished_at`, `duration`, and queue metrics when present. A failed pipeline may still contain a successful image job. Preserve the raw job name and runner information; derive variant/architecture separately, since not every job is an image build.
* **Konflux:** Query PipelineRuns and TaskRuns in the configured namespace from Kubernetes and KubeArchive. Read `metadata.creationTimestamp`, `metadata.uid`, `status.startTime`, `status.completionTime`, the `Succeeded` condition's `status` and `reason`, repository/commit/PR/branch annotations, component labels, and pipeline task names. Associate TaskRuns through owner references or Tekton labels and retain the source of each observation (live or archived). Inspect the actual resources before fixing mappings; names and annotation conventions may change.
* **GitHub Actions (later phase):** List workflow runs (`/repos/{owner}/{repo}/actions/runs`, filtered with `created`, **not** `created_badge`) and paginated jobs, including rerun attempts. Capture `head_sha`, event, branch/PR, run ID, attempt, and job `started_at`/`completed_at`. Workflow `updated_at` is the last update, **not** an authoritative completion timestamp; do not report it as one.

## 3. BigQuery data model

Create a `chronoflux` dataset in the chosen GCP project, with a dataset description linking to https://github.com/ktdreyer/chronoflux and an `app:chronoflux` label. Partition tables by `DATE(created_at)` (Konflux: `metadata.creationTimestamp`) so incomplete runs also have a partition. Keep the source host or cluster in every identity; names alone are not globally unique. Timestamps are nullable until known. Retain a source URL and `ingested_at` on each record. Use `FLOAT64` for fractional seconds where supplied, rather than rounding away short queue times.

BigQuery does **not** enforce primary-key uniqueness. Before each `MERGE`, deduplicate the staging batch to one row per composite identity (for example, with `ROW_NUMBER()` ordered by observation time). An upsert must update a previously incomplete run when it finishes, while preserving distinct reruns/retries. Never merge directly from a batch containing duplicate keys.

### `gitlab_pipelines`

| Field | Type | Notes |
| --- | --- | --- |
| `gitlab_host`, `pipeline_id` | STRING, INT64 | Composite identity. |
| `project_id`, `project_path`, `web_url` | INT64, STRING, STRING | Source location. |
| `sha`, `ref`, `source`, `status` | STRING | Commit, ref, event source, result. Preserve MR refs and tag refs as supplied. |
| `created_at`, `started_at`, `finished_at`, `updated_at` | TIMESTAMP | Use `finished_at`, not `updated_at`, for completion. |
| `duration_seconds`, `queued_duration_seconds` | FLOAT64 | Source-reported pipeline metrics when present; not a substitute for job timing or a sum of job durations. |
| `ingested_at` | TIMESTAMP | Last successful ingestion of this row. |

### `gitlab_jobs`

| Field | Type | Notes |
| --- | --- | --- |
| `gitlab_host`, `project_id`, `job_id` | STRING, INT64, INT64 | Composite identity; retries have new job IDs. |
| `pipeline_id`, `name`, `stage`, `status`, `web_url` | INT64, STRING, STRING, STRING, STRING | Join to pipeline; retain the original job name even if parsing rules change. |
| `job_kind`, `variant`, `architecture` | STRING | Nullable derived fields; only parse names we recognize (for example `build-image-mr-rocm-x86_64`), and distinguish build jobs from lint/verification. Never guess a platform for an unrecognized name. |
| `allow_failure`, `failure_reason` | BOOL, STRING | Distinguish optional failures from blocking failures and infra failures from script failures. |
| `runner_id`, `runner_tags` | INT64, ARRAY<STRING> | Optional runner identity and requested tags for investigating queue bottlenecks; do not treat tags as proof of the resulting image architecture. |
| `created_at`, `started_at`, `finished_at` | TIMESTAMP | Separate wait from execution. |
| `duration_seconds`, `queued_duration_seconds` | FLOAT64 | Source-reported job metrics when available. `created_at` → `started_at` can also include waiting for prerequisite jobs; do not label that entire interval as runner queue time. |
| `ingested_at` | TIMESTAMP | Last ingestion. |

### `konflux_pipelineruns`

| Field | Type | Notes |
| --- | --- | --- |
| `cluster`, `namespace`, `resource_uid`, `pipelinerun_name` | STRING | Composite identity uses cluster + namespace + UID; retain name for joins and URLs. |
| `repository`, `commit_sha`, `event`, `source_ref`, `target_branch`, `pull_request_number`, `component`, `application` | STRING | Preserve the original annotations/labels used to derive these; distinguish tag and branch events. |
| `web_url`, `observation_source` | STRING | Konflux UI URL; live or KubeArchive. |
| `condition_status`, `condition_reason` | STRING | Tekton `Succeeded` condition: `True`, `False`, or `Unknown`, plus reason. Do not store `Succeeded` as the result value. |
| `created_at`, `start_time`, `completion_time`, `ingested_at` | TIMESTAMP | Creation and execution are different clocks. |

### `konflux_taskruns`

| Field | Type | Notes |
| --- | --- | --- |
| `cluster`, `namespace`, `resource_uid`, `taskrun_name` | STRING | Composite identity uses cluster + namespace + UID. |
| `pipelinerun_uid`, `pipelinerun_name`, `pipeline_task`, `web_url` | STRING | Join to parent; keep the task's pipeline name rather than inferring it from the TaskRun name. |
| `condition_status`, `condition_reason`, `observation_source` | STRING | Preserve failures and cancellation reasons. |
| `created_at`, `start_time`, `completion_time`, `ingested_at` | TIMESTAMP | Task creation, start, finish, ingestion. |

### `github_workflow_runs` (later phase)

| Field | Type | Notes |
| --- | --- | --- |
| `github_host`, `repository`, `run_id`, `run_attempt` | STRING, STRING, INT64, INT64 | Identity includes attempt; a rerun must not overwrite an earlier attempt. |
| `head_sha`, `head_branch`, `event`, `workflow_name`, `html_url` | STRING | `head_branch` may be null. |
| `status`, `conclusion` | STRING | Distinguish incomplete from failed. |
| `created_at`, `run_started_at`, `updated_at`, `ingested_at` | TIMESTAMP | `updated_at` is not a guaranteed finish time. |

Add a `github_jobs` table with job ID, attempt, parent run ID, timestamps, status, and conclusion when implementing GitHub ingestion; do **not** use workflow timestamps alone to compare component builds. Add a small sync-state/results table for checkpoints and monitoring, separate from the run tables.

### Comparison rules

A reporting view may expose common fields (`sha`, event, component, platform, status, created, started, finished, URL), but must retain source-specific detail. For RHAI-10822, map a GitLab image job's parsed variant and architecture to the Konflux component's `build-platforms`; a Konflux multi-platform build may have no one-to-one job equivalent. Group by comparable work, not merely by SHA. For retried GitLab jobs, retain all attempts but select an explicit attempt policy (for example, the latest job ID per pipeline and job name) for the headline comparison; report retry counts separately. Report time from trigger/creation to a usable image separately from build-task duration and from optional integration checks. Exclude or report failed, cancelled, missing, and unmatched builds separately; do not silently treat them as fast successes. Compare medians and p90s over a stated sample window, with cache/build-mode differences called out.

## 4. Access and deployment decisions

* **Konflux location:** Prefer a collector running on the Konflux p02 cluster if approved. Give its service account a **namespaced Role** with `get`/`list` on PipelineRuns and TaskRuns in `ai-tenant`; add `watch` only if needed. Confirm it can also read the relevant KubeArchive API. If the CronJob instead runs on another OpenShift cluster, design and test a separate p02 authentication path; its local service account cannot automatically read p02.
* **GitLab/GitHub:** Use scoped read-only credentials, preferably app tokens where supported. Store credentials in OpenShift Secrets and limit the collector's access to those Secrets. Do not write tokens or raw API responses containing secrets to BigQuery or logs.
* **GCP:** Validate Workload Identity Federation end to end with the *actual* cluster service-account token issuer, audience, projected token, and GCP IAM bindings before committing to it. Grant the impersonated GCP identity only the BigQuery permissions needed for staging, merge, and sync-state access. Do not assume OpenShift service-account federation works merely because the pod has a token.
* **Deployment:** Package the collector as an image; deploy a ServiceAccount, narrowly scoped access, Secret(s), configuration, and a CronJob (initially `0 * * * *`). Set a bounded job timeout and prevent overlapping runs, or serialize merges per source if overlap is permitted. Provide a manually runnable backfill with an explicit time range.

## 5. Monitoring and acceptance

Track last successful sync **per source and scope**, discovered/completed run counts, failed polls, API rate limits, latest source update, and lag from source update to ingestion. Alert on failed syncs, stalled checkpoints, and unexpected gaps in TaskRuns for completed PipelineRuns. A table's maximum run timestamp alone is not a freshness signal when no builds have occurred; use the sync-results table as well.

The first milestone is done when we can:

1. Reproduce the GitLab ROCm job and Konflux PipelineRun from [MR !1458](https://gitlab.com/redhat/rhel-ai/rhaiis/containers/-/merge_requests/1458) with source timestamps and links, including the fact that its GitLab pipeline failed for a *different* image job.
2. Identify the Konflux TaskRuns contributing to that build and distinguish waiting, execution, and image-index completion where the data permits it.
3. Show that restarting the collector, retrying a run, missing more than one schedule, and moving a run from live Kubernetes into KubeArchive do not create duplicates or lose results. Demonstrate a bounded backfill.
4. Produce a matched-variant comparison over multiple completed runs, with sample size and exclusions visible. 

## References

* [RHAI-10822 issue](https://redhat.atlassian.net/browse/RHAI-10822)
* [GitHub workflow runs REST API](https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-repository)
* [GitLab pipelines REST API](https://docs.gitlab.com/api/pipelines/)
* [GitLab jobs REST API](https://docs.gitlab.com/api/jobs/)
* [Konflux KubeArchive documentation](https://konflux.pages.redhat.com/docs/users/faq/kubearchive.html)
