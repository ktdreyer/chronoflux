CREATE TABLE IF NOT EXISTS `{dataset}.gitlab_pipelines` (
  gitlab_host STRING, pipeline_id INT64, project_id INT64, project_path STRING, web_url STRING,
  sha STRING, ref STRING, source STRING, status STRING, created_at TIMESTAMP, started_at TIMESTAMP,
  finished_at TIMESTAMP, updated_at TIMESTAMP, duration_seconds FLOAT64, queued_duration_seconds FLOAT64,
  ingested_at TIMESTAMP
) PARTITION BY DATE(created_at);

CREATE TABLE IF NOT EXISTS `{dataset}.gitlab_jobs` (
  gitlab_host STRING, project_id INT64, job_id INT64, pipeline_id INT64, name STRING, stage STRING,
  status STRING, web_url STRING, job_kind STRING, variant STRING, architecture STRING, allow_failure BOOL,
  failure_reason STRING, runner_id INT64, runner_tags ARRAY<STRING>, created_at TIMESTAMP,
  started_at TIMESTAMP, finished_at TIMESTAMP, duration_seconds FLOAT64, queued_duration_seconds FLOAT64,
  ingested_at TIMESTAMP
) PARTITION BY DATE(created_at);

CREATE TABLE IF NOT EXISTS `{dataset}.konflux_pipelineruns` (
  cluster STRING, namespace STRING, resource_uid STRING, pipelinerun_name STRING, repository STRING,
  commit_sha STRING, event STRING, source_ref STRING, target_branch STRING, pull_request_number STRING,
  component STRING, application STRING, web_url STRING, observation_source STRING, condition_status STRING,
  condition_reason STRING, created_at TIMESTAMP, start_time TIMESTAMP, completion_time TIMESTAMP,
  ingested_at TIMESTAMP
) PARTITION BY DATE(created_at);

CREATE TABLE IF NOT EXISTS `{dataset}.konflux_taskruns` (
  cluster STRING, namespace STRING, resource_uid STRING, taskrun_name STRING, pipelinerun_uid STRING,
  pipelinerun_name STRING, pipeline_task STRING, platform STRING, web_url STRING,
  condition_status STRING, condition_reason STRING, observation_source STRING, created_at TIMESTAMP,
  start_time TIMESTAMP, completion_time TIMESTAMP, ingested_at TIMESTAMP
) PARTITION BY DATE(created_at);

CREATE TABLE IF NOT EXISTS `{dataset}.sync_state` (
  source STRING, scope STRING, checkpoint_at TIMESTAMP, updated_at TIMESTAMP, ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{dataset}.sync_results` (
  result_id STRING, source STRING, scope STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
  checkpoint_at TIMESTAMP, status STRING, pipeline_count INT64, job_count INT64,
  pipelinerun_count INT64, taskrun_count INT64, error STRING, ingested_at TIMESTAMP
);
