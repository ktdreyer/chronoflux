WITH mappings AS (
  SELECT JSON_VALUE(entry, '$.variant') AS variant,
         JSON_VALUE(entry, '$.component') AS component,
         -- A trailing '*' means the component is matched by prefix, covering
         -- release-stream components (rhaiis-rocm-ubi9-3-6, ...-fast1, ...).
         ENDS_WITH(JSON_VALUE(entry, '$.component'), '*') AS prefix_match,
         REGEXP_REPLACE(JSON_VALUE(entry, '$.component'), r'\*$', '') AS prefix_base
  FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@component_map))) AS entry
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
  FROM {gitlab_jobs} j JOIN {gitlab_pipelines} p
    ON p.gitlab_host=j.gitlab_host AND p.pipeline_id=j.pipeline_id
  LEFT JOIN mappings m ON m.variant=j.variant
  WHERE p.project_path=@project_path AND p.gitlab_host=@gitlab_host
    AND j.job_kind='build-image'
    AND (@sha IS NULL OR p.sha=@sha)
    AND (@sha IS NOT NULL OR (p.created_at>=@since AND p.created_at<@until))
    AND (@variant IS NULL OR j.variant=@variant)
), run_tasks AS (
  -- One summary per PipelineRun: every TaskRun, regardless of platform, so
  -- queue and execution time stay visible for platform-agnostic tasks.
  SELECT t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name,
         STRING_AGG(CONCAT(COALESCE(t.pipeline_task, t.taskrun_name), ':', COALESCE(t.condition_status, 'Unknown'),
                           ' [create→start=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.start_time, t.created_at, MICROSECOND), 1000000.0) AS STRING), '?'),
                           's, execution=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.completion_time, t.start_time, MICROSECOND), 1000000.0) AS STRING), '?'), 's]'),
                    '\n' ORDER BY t.start_time) AS task_summary
  FROM {konflux_taskruns} t
  GROUP BY t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name
), run_platforms AS (
  -- Platforms come only from TaskRuns that carry one (build-container);
  -- runs without a platform-bearing TaskRun stay unmatched.
  SELECT DISTINCT t.cluster, t.namespace, t.pipelinerun_uid, t.pipelinerun_name, t.platform
  FROM {konflux_taskruns} t
  WHERE t.platform IS NOT NULL
), k AS (
  SELECT p.commit_sha AS sha, p.component, rp.platform,
         p.condition_status AS pipeline_condition_status, p.condition_reason AS pipeline_condition_reason,
         p.web_url AS konflux_url, p.created_at AS konflux_created_at, p.start_time AS konflux_start_time,
         p.completion_time AS konflux_completion_time, p.resource_uid AS pipelinerun_uid,
         ROW_NUMBER() OVER (PARTITION BY p.commit_sha, p.component, rp.platform
                            ORDER BY p.completion_time DESC NULLS LAST, p.created_at DESC) AS konflux_attempt_rank,
         COUNT(*) OVER (PARTITION BY p.commit_sha, p.component, rp.platform) AS konflux_run_count,
         rt.task_summary
  FROM {konflux_pipelineruns} p
  LEFT JOIN run_platforms rp
    ON rp.cluster=p.cluster AND rp.namespace=p.namespace AND
       (rp.pipelinerun_uid=p.resource_uid OR (rp.pipelinerun_uid IS NULL AND rp.pipelinerun_name=p.pipelinerun_name))
  LEFT JOIN run_tasks rt
    ON rt.cluster=p.cluster AND rt.namespace=p.namespace AND
       (rt.pipelinerun_uid=p.resource_uid OR (rt.pipelinerun_uid IS NULL AND rt.pipelinerun_name=p.pipelinerun_name))
  WHERE p.cluster=@cluster AND p.namespace=@namespace
    AND (@sha IS NULL OR p.commit_sha=@sha)
    AND (@variant IS NULL OR EXISTS (SELECT 1 FROM mappings m WHERE m.variant=@variant
         AND (p.component=m.component OR (m.prefix_match AND STARTS_WITH(p.component, m.prefix_base)))))
    AND (@sha IS NOT NULL OR EXISTS (
      SELECT 1 FROM {gitlab_pipelines} scope_pipeline
      WHERE scope_pipeline.project_path=@project_path AND scope_pipeline.gitlab_host=@gitlab_host
        AND scope_pipeline.sha=p.commit_sha AND scope_pipeline.created_at>=@since AND scope_pipeline.created_at<@until
    ))
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
ORDER BY sha, variant, component;
