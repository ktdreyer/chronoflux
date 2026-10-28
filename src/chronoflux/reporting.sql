-- Reporting view for the RHAI-10822 GitLab vs Konflux comparison.
-- Mirrors compare.sql with the rhaiis-containers scope baked in and no
-- window/sha/variant filters; dashboards filter client-side.
-- Create or replace with:
--   CREATE OR REPLACE VIEW `aipcc-cicd.chronoflux.ci_build_comparison` AS <this SQL>
WITH mappings AS (
  SELECT JSON_VALUE(entry, '$.variant') AS variant,
         JSON_VALUE(entry, '$.component') AS component,
         ENDS_WITH(JSON_VALUE(entry, '$.component'), '*') AS prefix_match,
         REGEXP_REPLACE(JSON_VALUE(entry, '$.component'), r'\*$', '') AS prefix_base
  FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON('[{"variant":"cpu","component":"rhaiis-cpu-ubi9*"},{"variant":"cpu-pz","component":"rhaiis-cpu-pz-ubi9*"},{"variant":"cuda","component":"rhaiis-cuda-ubi9*"},{"variant":"gaudi","component":"rhaiis-gaudi-ubi9*"},{"variant":"model-opt-cuda","component":"rhaiis-model-opt-cuda-ubi9*"},{"variant":"neuron","component":"rhaiis-neuron-ubi9*"},{"variant":"rocm","component":"rhaiis-rocm-ubi9*"},{"variant":"rubin","component":"rhaiis-rubin-ubi9*"},{"variant":"spyre","component":"rhaiis-spyre-ubi9*"},{"variant":"tpu","component":"rhaiis-tpu-ubi9*"},{"variant":"vllm-omni-cuda","component":"rhaiis-vllm-omni-cuda-ubi9*"}]'))) AS entry
), gl AS (
  SELECT p.sha, j.variant, j.architecture,
         m.component AS mapped_component, m.prefix_base AS mapped_prefix,
         j.status AS job_status, j.web_url AS job_url,
         j.created_at AS job_created_at, j.started_at AS job_started_at, j.finished_at AS job_finished_at,
         j.duration_seconds AS job_duration_seconds, j.queued_duration_seconds, j.allow_failure, j.failure_reason,
         p.status AS pipeline_status,
         p.web_url AS pipeline_url, j.pipeline_id, j.name,
         ROW_NUMBER() OVER (PARTITION BY p.pipeline_id, j.name ORDER BY j.job_id DESC) AS attempt_rank,
         COUNT(*) OVER (PARTITION BY p.pipeline_id, j.name) - 1 AS retry_count
  FROM `aipcc-cicd.chronoflux.gitlab_jobs` j
  JOIN `aipcc-cicd.chronoflux.gitlab_pipelines` p
    ON p.gitlab_host=j.gitlab_host AND p.pipeline_id=j.pipeline_id
  LEFT JOIN mappings m ON m.variant=j.variant
  WHERE p.project_path='redhat/rhel-ai/rhaiis/containers' AND p.gitlab_host='gitlab.com'
    AND j.job_kind='build-image'
 ), run_tasks AS (
  -- Build-container TaskRuns for different architectures run concurrently;
  -- MAX captures the critical path instead of summing parallel work.
  SELECT t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name,
         STRING_AGG(CONCAT(COALESCE(t.pipeline_task, t.taskrun_name), ':', COALESCE(t.condition_status, 'Unknown'),
                           ' [create→start=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.start_time, t.created_at, MICROSECOND), 1000000.0) AS STRING), '?'),
                           's, execution=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.completion_time, t.start_time, MICROSECOND), 1000000.0) AS STRING), '?'), 's]'),
                     '\n' ORDER BY t.start_time) AS task_summary,
         MAX(IF(t.pipeline_task='prefetch-dependencies', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS prefetch_seconds,
         MAX(IF(t.pipeline_task='build-container', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS build_critical_seconds,
         MAX(IF(t.pipeline_task='build-container' AND t.platform='x86_64', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS build_x86_seconds,
         MAX(IF(t.pipeline_task='build-container' AND t.platform IS NOT NULL AND t.platform!='x86_64', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS build_other_seconds,
         MAX(IF(t.pipeline_task='build-image-index', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS index_seconds,
         MAX(IF(t.pipeline_task='build-source-image', TIMESTAMP_DIFF(t.completion_time, t.start_time, SECOND), NULL)) AS source_image_seconds
  FROM `aipcc-cicd.chronoflux.konflux_taskruns` t
  GROUP BY t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name
), run_platforms AS (
  SELECT DISTINCT t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name, t.platform
  FROM `aipcc-cicd.chronoflux.konflux_taskruns` t
  WHERE t.platform IS NOT NULL
), k AS (
  SELECT p.commit_sha AS sha, p.component, rp.platform,
         p.condition_status AS pipeline_condition_status, p.condition_reason AS pipeline_condition_reason,
         p.web_url AS konflux_url, p.created_at AS konflux_created_at, p.start_time AS konflux_start_time,
         p.completion_time AS konflux_completion_time, p.resource_uid AS pipelinerun_uid,
         ROW_NUMBER() OVER (PARTITION BY p.commit_sha, p.component, rp.platform
                            ORDER BY p.completion_time DESC NULLS LAST, p.created_at DESC) AS konflux_attempt_rank,
         COUNT(*) OVER (PARTITION BY p.commit_sha, p.component, rp.platform) AS konflux_run_count,
          rt.task_summary, rt.prefetch_seconds, rt.build_critical_seconds,
          rt.build_x86_seconds, rt.build_other_seconds, rt.index_seconds, rt.source_image_seconds
  FROM `aipcc-cicd.chronoflux.konflux_pipelineruns` p
  LEFT JOIN run_platforms rp
    ON rp.cluster=p.cluster AND rp.namespace=p.namespace AND
       (rp.pipelinerun_uid=p.resource_uid OR (rp.pipelinerun_uid IS NULL AND rp.pipelinerun_name=p.pipelinerun_name))
  LEFT JOIN run_tasks rt
    ON rt.cluster=p.cluster AND rt.namespace=p.namespace AND
       (rt.pipelinerun_uid=p.resource_uid OR (rt.pipelinerun_uid IS NULL AND rt.pipelinerun_name=p.pipelinerun_name))
  WHERE p.cluster='stone-prod-p02' AND p.namespace='ai-tenant'
    -- Ad-hoc tests can inherit a component label without building it. Only
    -- actual component PipelineRuns belong in the comparison.
    AND STARTS_WITH(p.pipelinerun_name, CONCAT(p.component, '-on-'))
)
SELECT COALESCE(gl.sha,k.sha) AS sha, gl.variant, gl.architecture, k.component, k.platform,
       gl.job_status, gl.pipeline_status, k.pipeline_condition_status, k.pipeline_condition_reason,
       gl.allow_failure, gl.failure_reason,
       k.konflux_run_count,
       gl.job_url, gl.pipeline_url, k.konflux_url,
       gl.job_created_at, gl.job_started_at, gl.job_finished_at, gl.job_duration_seconds,
       gl.queued_duration_seconds,
       SAFE_DIVIDE(TIMESTAMP_DIFF(gl.job_finished_at, gl.job_created_at, MICROSECOND), 1000000.0) AS gitlab_wall_seconds,
       k.konflux_created_at, k.konflux_start_time, k.konflux_completion_time,
       SAFE_DIVIDE(TIMESTAMP_DIFF(k.konflux_completion_time, k.konflux_created_at, MICROSECOND), 1000000.0) AS konflux_wall_seconds,
       SAFE_DIVIDE(TIMESTAMP_DIFF(k.konflux_completion_time, k.konflux_start_time, MICROSECOND), 1000000.0) AS konflux_execution_seconds,
        k.task_summary, gl.retry_count,
        k.prefetch_seconds AS konflux_prefetch_seconds,
        k.build_critical_seconds AS konflux_build_critical_seconds,
        k.build_x86_seconds AS konflux_build_x86_seconds,
        k.build_other_seconds AS konflux_build_other_seconds,
        k.index_seconds AS konflux_index_seconds,
        k.source_image_seconds AS konflux_source_image_seconds,
       CASE WHEN gl.pipeline_id IS NULL THEN 'konflux_only'
            WHEN k.pipelinerun_uid IS NULL THEN 'gitlab_only'
            ELSE 'matched' END AS match_status
FROM gl FULL OUTER JOIN k
  ON gl.sha=k.sha
  AND (k.component=gl.mapped_component
       OR (gl.mapped_prefix IS NOT NULL AND STARTS_WITH(k.component, gl.mapped_prefix)))
  AND gl.architecture=k.platform
WHERE (gl.attempt_rank=1 OR gl.attempt_rank IS NULL)
  AND (k.konflux_attempt_rank=1 OR k.konflux_attempt_rank IS NULL)
