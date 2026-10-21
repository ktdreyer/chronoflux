WITH mappings AS (
  SELECT JSON_VALUE(entry, '$.variant') AS variant, JSON_VALUE(entry, '$.component') AS component
  FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@component_map))) AS entry
), gl AS (
  SELECT p.sha, j.variant, j.architecture, j.status AS job_status, j.web_url AS job_url,
         j.created_at AS job_created_at, j.started_at AS job_started_at, j.finished_at AS job_finished_at,
         j.duration_seconds AS job_duration_seconds, j.queued_duration_seconds, j.allow_failure, j.failure_reason,
         p.status AS pipeline_status,
         p.web_url AS pipeline_url, j.pipeline_id, j.name,
         ROW_NUMBER() OVER (PARTITION BY p.pipeline_id, j.name ORDER BY j.job_id DESC) AS attempt_rank,
         COUNT(*) OVER (PARTITION BY p.pipeline_id, j.name) - 1 AS retry_count
  FROM {gitlab_jobs} j JOIN {gitlab_pipelines} p
    ON p.gitlab_host=j.gitlab_host AND p.pipeline_id=j.pipeline_id
  WHERE p.project_path=@project_path AND p.gitlab_host=@gitlab_host
    AND j.job_kind='build-image'
    AND (@sha IS NULL OR p.sha=@sha)
    AND (@sha IS NOT NULL OR (p.created_at>=@since AND p.created_at<@until))
    AND (@variant IS NULL OR j.variant=@variant)
), k AS (
  SELECT p.commit_sha AS sha, p.component, t.platform,
         p.condition_status AS pipeline_condition_status, p.condition_reason AS pipeline_condition_reason,
         p.web_url AS konflux_url, p.created_at AS konflux_created_at, p.start_time AS konflux_start_time,
         p.completion_time AS konflux_completion_time, p.resource_uid AS pipelinerun_uid,
         ROW_NUMBER() OVER (PARTITION BY p.commit_sha, p.component, t.platform
                            ORDER BY p.completion_time DESC NULLS LAST, p.created_at DESC) AS konflux_attempt_rank,
         COUNT(*) OVER (PARTITION BY p.commit_sha, p.component, t.platform) AS konflux_run_count,
          STRING_AGG(CONCAT(COALESCE(t.pipeline_task, t.taskrun_name), ':', COALESCE(t.condition_status, 'Unknown'),
                            ' [create→start=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.start_time, t.created_at, MICROSECOND), 1000000.0) AS STRING), '?'),
                            's, execution=', COALESCE(CAST(SAFE_DIVIDE(TIMESTAMP_DIFF(t.completion_time, t.start_time, MICROSECOND), 1000000.0) AS STRING), '?'), 's]'),
                    '\n' ORDER BY t.start_time) AS task_summary
  FROM {konflux_pipelineruns} p LEFT JOIN {konflux_taskruns} t
    ON t.cluster=p.cluster AND t.namespace=p.namespace AND
       (t.pipelinerun_uid=p.resource_uid OR (t.pipelinerun_uid IS NULL AND t.pipelinerun_name=p.pipelinerun_name))
  WHERE p.cluster=@cluster AND p.namespace=@namespace
    AND (@sha IS NULL OR p.commit_sha=@sha)
    AND (@variant IS NULL OR EXISTS (SELECT 1 FROM mappings m WHERE m.variant=@variant AND m.component=p.component))
    AND (@sha IS NOT NULL OR EXISTS (
      SELECT 1 FROM {gitlab_pipelines} scope_pipeline
      WHERE scope_pipeline.project_path=@project_path AND scope_pipeline.gitlab_host=@gitlab_host
        AND scope_pipeline.sha=p.commit_sha AND scope_pipeline.created_at>=@since AND scope_pipeline.created_at<@until
    ))
  GROUP BY p.commit_sha, p.component, t.platform, p.condition_status, p.condition_reason, p.web_url,
           p.created_at, p.start_time, p.completion_time, p.resource_uid
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
  AND EXISTS (SELECT 1 FROM mappings m WHERE m.variant=gl.variant AND m.component=k.component)
 AND gl.architecture=k.platform
WHERE (gl.attempt_rank=1 OR gl.attempt_rank IS NULL)
  AND (k.konflux_attempt_rank=1 OR k.konflux_attempt_rank IS NULL)
ORDER BY sha, variant, component;
