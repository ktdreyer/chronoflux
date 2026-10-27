# Looker Studio dashboard setup

The `aipcc-cicd.chronoflux.ci_build_comparison` BigQuery view holds one row
per GitLab image job / Konflux build pairing (plus unmatched rows from either
side). Build the RHAI-10822 dashboard once; it refreshes from BigQuery
automatically as `chronoflux sync`/`backfill` loads more data.

## Create the report

1. Open <https://lookerstudio.google.com/navigation/report/new>.
2. In **Add data to report**, pick **BigQuery** and authorize with your Red
   Hat Google account (the one with access to the `aipcc-cicd` project).
3. Select **My Projects → aipcc-cicd → chronoflux → ci_build_comparison**
   and click **Add**.

## Recommended charts

Apply this filter to the report (or to each chart) so summaries only cover
comparable, successful builds:

* `match_status` = `matched`
* `job_status` = `success`
* `pipeline_condition_status` = `True`

1. **Headline bar chart** — Dimension: `variant`; Metrics: average
   `gitlab_wall_seconds`, average `konflux_wall_seconds`,
   average `konflux_execution_seconds`. Sort by Konflux average descending.
2. **Ratio scorecard** — Metric: `AVG(konflux_wall_seconds) /
   AVG(gitlab_wall_seconds)` (create a calculated field
   `AVG(konflux_wall_seconds) / AVG(gitlab_wall_seconds)`).
3. **Time series** — Dimension: `job_created_at` (day); Metrics: average
   `gitlab_wall_seconds` and `konflux_wall_seconds`; shows drift over time.
4. **Sample size table** — Dimensions: `variant`, `platform`; Metrics: record
   count, average `retry_count`, average `konflux_run_count`. Keep this next
   to the charts so sample sizes stay visible.
5. **Detail table** — Dimensions: `sha`, `variant`, `component`,
   `match_status`, `job_status`; Metrics: `gitlab_wall_seconds`,
   `konflux_wall_seconds`; enable "Allow links" on `job_url`/`konflux_url`
   for click-through evidence.

## Notes

* Rows are pre-selected per the retry policy (highest GitLab job ID per
  pipeline/name; latest completed Konflux run per SHA/component/platform).
* `konflux_execution_seconds` excludes queue/wait; `konflux_wall_seconds`
  is creation-to-completion. `gitlab_wall_seconds` is job creation to finish.
* Unmatched rows (`gitlab_only`, `konflux_only`) are retained in the view so
  trigger-parity gaps stay visible; filter them out for timing summaries.
