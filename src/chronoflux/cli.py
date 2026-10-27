from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median, quantiles
from typing import Any
from urllib.parse import urlparse

from .config import Config, Scope, load_config
from .extractors import GitLabExtractor, KonfluxExtractor, SourceError, parse_time
from .storage import BigQueryStore


def _time(value: str) -> datetime:
    try:
        parsed = parse_time(value)
    except SourceError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    if parsed is None:
        raise argparse.ArgumentTypeError("time must be ISO 8601 with a timezone")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chronoflux", description="Collect and compare GitLab and Konflux build timings")
    parser.add_argument("--config", default="chronoflux.toml", help="TOML configuration file")
    commands = parser.add_subparsers(dest="command", required=True)
    doctor = commands.add_parser("doctor", help="Check source and BigQuery access without writing")
    doctor.add_argument("--source", choices=("gitlab", "konflux", "bigquery"))
    doctor.add_argument("--scope", default="rhaiis-containers")
    backfill = commands.add_parser("backfill", help="Collect an explicit creation-time interval")
    backfill.add_argument("--scope", default="rhaiis-containers")
    backfill.add_argument("--source", choices=("gitlab", "konflux"), required=True)
    backfill.add_argument("--since", type=_time, required=True, help="Inclusive UTC ISO 8601 time")
    backfill.add_argument("--until", type=_time, required=True, help="Exclusive UTC ISO 8601 time")
    backfill.add_argument("--dry-run", action="store_true")
    sync = commands.add_parser("sync", help="Incrementally collect recent and changed runs")
    sync.add_argument("--scope", default="rhaiis-containers")
    sync.add_argument("--source", choices=("gitlab", "konflux"))
    sync.add_argument("--dry-run", action="store_true")
    compare = commands.add_parser("compare", help="Compare image jobs with Konflux builds from BigQuery")
    compare.add_argument("--scope", default="rhaiis-containers")
    selector = compare.add_mutually_exclusive_group(required=True)
    selector.add_argument("--sha")
    compare.add_argument("--since", type=_time)
    compare.add_argument("--until", type=_time)
    compare.add_argument("--variant")
    compare.add_argument("--format", choices=("table", "json", "csv"), default="table")
    return parser


def _store(config: Config) -> BigQueryStore:
    if not config.bq_project:
        raise RuntimeError("Set BIGQUERY_PROJECT or [bigquery].project; dataset must be provisioned separately")
    return BigQueryStore(config.bq_project, config.bq_dataset)


def _source_extract(scope: Scope, source: str, since: datetime, until: datetime, updated_after: datetime | None = None) -> dict[str, list[dict[str, Any]]]:
    if source == "gitlab":
        extractor = GitLabExtractor(scope.gitlab_host, scope.gitlab_project, os.getenv("GITLAB_TOKEN"))
        project_id = scope.gitlab_project_id
        if project_id <= 0:
            project_id = int(extractor.project_info()["id"])
        pipelines, jobs = extractor.extract(since, until, project_id, scope.gitlab_project, updated_after)
        return {"gitlab_pipelines": pipelines, "gitlab_jobs": jobs}
    extractor = KonfluxExtractor(scope.cluster, scope.namespace, expected_server=scope.api_server)
    pipelineruns, taskruns = extractor.extract(since, until)
    return {"konflux_pipelineruns": pipelineruns, "konflux_taskruns": taskruns}


def _summary(rows: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    ids: list[str] = []
    id_fields = {
        "gitlab_pipelines": "pipeline_id",
        "gitlab_jobs": "job_id",
        "konflux_pipelineruns": "resource_uid",
        "konflux_taskruns": "resource_uid",
    }
    for kind, records in rows.items():
        for row in records[:5]:
            ids.append(f"{kind}:{row.get(id_fields[kind])}")
    return {"counts": {name: len(items) for name, items in rows.items()}, "sample_ids": ids}


def _run_backfill(args: argparse.Namespace, scope: Scope, config: Config) -> int:
    if args.since >= args.until:
        raise ValueError("--since must be earlier than --until")
    rows = _source_extract(scope, args.source, args.since, args.until)
    result = _summary(rows)
    result.update({"source": args.source, "scope": scope.name, "dry_run": args.dry_run})
    if not args.dry_run:
        store = _store(config)
        store.ensure_tables()
        for table, records in rows.items():
            store.merge(table, records)
    print(json.dumps(result, indent=2, default=str))
    return 0


def _run_sync(args: argparse.Namespace, scope: Scope, config: Config) -> int:
    store = None if args.dry_run else _store(config)
    sources = [args.source] if args.source else ["gitlab", "konflux"]
    now = datetime.now(timezone.utc)
    if store:
        store.ensure_tables()
    overall = 0
    for source in sources:
        started = datetime.now(timezone.utc)
        checkpoint = store.checkpoint(source, scope.name) if store else None
        # A rolling reconciliation window catches incomplete/late-updated resources;
        # checkpoints extend discovery farther back after an outage.
        since = min(checkpoint - timedelta(hours=2), now - timedelta(days=30)) if checkpoint else now - timedelta(days=30)
        try:
            rows = _source_extract(scope, source, since, now, now - timedelta(days=30) if source == "gitlab" else None)
            if store:
                for table, records in rows.items():
                    store.merge(table, records)
            success = True
            error = None
        except Exception as caught:
            rows = {}
            success = False
            error = str(caught)
            overall = 1
        ended = datetime.now(timezone.utc)
        result_row = {
            "result_id": str(uuid.uuid4()), "source": source, "scope": scope.name,
            "started_at": started.isoformat(), "ended_at": ended.isoformat(),
            "checkpoint_at": ended.isoformat() if success else checkpoint.isoformat() if checkpoint else None,
            "status": "success" if success else "failed",
            "pipeline_count": len(rows.get("gitlab_pipelines", [])), "job_count": len(rows.get("gitlab_jobs", [])),
            "pipelinerun_count": len(rows.get("konflux_pipelineruns", [])), "taskrun_count": len(rows.get("konflux_taskruns", [])),
            "error": error,
        }
        if store:
            store.write_sync_result(result_row, advance_checkpoint=success)
        print(json.dumps({**result_row, "dry_run": args.dry_run}, indent=2, default=str))
    return overall


def _print_compare(rows: list[dict[str, Any]], output_format: str) -> None:
    statuses: dict[str, int] = {}
    for row in rows:
        status = row.get("match_status") or "unknown"
        statuses[status] = statuses.get(status, 0) + 1
    usable = [row for row in rows if row.get("match_status") == "matched"
              and row.get("job_status") == "success" and row.get("pipeline_condition_status") == "True"
              and row.get("gitlab_wall_seconds") is not None and row.get("konflux_wall_seconds") is not None]
    timing: dict[str, dict[str, float]] = {}
    for metric in ("gitlab_wall_seconds", "konflux_wall_seconds"):
        values = sorted(float(row[metric]) for row in usable)
        if values:
            timing[metric] = {"sample_count": len(values)}
            timing[metric]["median"] = median(values) if len(values) >= 3 else None
            timing[metric]["p90"] = quantiles(values, n=10, method="inclusive")[8] if len(values) >= 10 else None
    if output_format == "json":
        print(json.dumps({"rows": rows, "summary": {
            "row_count": len(rows), "match_status_counts": statuses,
            "eligible_successful_paired_samples": len(usable), "timing_seconds": timing,
            "excluded_from_timing_summary": len(rows) - len(usable),
        }}, indent=2, default=str))
        return
    if output_format == "csv":
        if rows:
            writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return
    if not rows:
        print("No comparison rows found.")
        return
    columns = ("sha", "variant", "architecture", "component", "platform", "job_status", "pipeline_status", "pipeline_condition_status", "job_duration_seconds", "queued_duration_seconds", "gitlab_wall_seconds", "konflux_execution_seconds", "konflux_wall_seconds", "retry_count", "konflux_run_count", "match_status", "job_url", "konflux_url")
    widths = {name: min(max(len(name), *(len(str(row.get(name) or "")) for row in rows)), 48) for name in columns}
    print("  ".join(name.ljust(widths[name]) for name in columns))
    for row in rows:
        print("  ".join(str(row.get(name) or "")[:widths[name]].ljust(widths[name]) for name in columns))
    print(f"Rows: {len(rows)}; matched: {statuses.get('matched', 0)}; unmatched: {len(rows) - statuses.get('matched', 0)}")
    failed_jobs = sum(row.get("job_status") == "failed" for row in rows)
    canceled_jobs = sum(row.get("job_status") == "canceled" for row in rows)
    failed_runs = sum(row.get("pipeline_condition_status") == "False" for row in rows)
    canceled_runs = sum((row.get("pipeline_condition_reason") or "").lower() in {"cancelled", "canceled"} for row in rows)
    print(f"GitLab failed/canceled jobs: {failed_jobs}/{canceled_jobs}; Konflux failed/canceled PipelineRuns: {failed_runs}/{canceled_runs}")
    for row in rows:
        task_summary = row.get("task_summary")
        if task_summary:
            print(f"\nKonflux TaskRuns for {row.get('sha') or '?'} / {row.get('component') or '?'}:")
            print("  " + str(task_summary).replace("\n", "\n  "))
    for metric in ("gitlab_wall_seconds", "konflux_wall_seconds"):
        if metric in timing:
            median_text = f"{timing[metric]['median']:.3f}s" if timing[metric]["median"] is not None else "suppressed (<3 samples)"
            p90_text = f"{timing[metric]['p90']:.3f}s" if timing[metric]["p90"] is not None else "suppressed (<10 samples)"
            print(f"Eligible successful paired samples: {len(usable)}; {metric}: median={median_text} p90={p90_text}")
        else:
            print(f"Eligible successful paired samples: 0; {metric}: no completed matched samples")
    excluded = len(rows) - len(usable)
    print(f"Excluded from timing summary: {excluded} (unmatched, failed, incomplete, or missing timestamps); thresholds: median >=3 samples, p90 >=10")


def _run_compare(args: argparse.Namespace, scope: Scope, config: Config) -> int:
    if args.sha is None and (args.since is None or args.until is None or args.since >= args.until):
        raise ValueError("date comparison requires --since < --until")
    store = _store(config)
    rows = store.compare({
        "project_path": scope.gitlab_project, "gitlab_host": urlparse(scope.gitlab_host).netloc,
        "cluster": scope.cluster, "namespace": scope.namespace, "sha": args.sha,
        "since": args.since, "until": args.until, "variant": args.variant,
        "component_map": scope.variant_components,
    })
    _print_compare(rows, args.format)
    return 0


def _run_doctor(args: argparse.Namespace, scope: Scope, config: Config) -> int:
    checks = [args.source] if args.source else ["gitlab", "konflux", "bigquery"]
    failures = 0
    for source in checks:
        try:
            if source == "gitlab":
                extractor = GitLabExtractor(scope.gitlab_host, scope.gitlab_project, os.getenv("GITLAB_TOKEN"))
                extractor.check()
                detail = "GitLab project, pipeline, and retried-job APIs reachable (read access only)"
            elif source == "konflux":
                extractor = KonfluxExtractor(scope.cluster, scope.namespace, expected_server=scope.api_server)
                extractor.check("live")
                try:
                    extractor.check("archive")
                except Exception as archive_error:
                    raise SourceError(f"live PipelineRun/TaskRun lists succeeded; archive check failed: {archive_error}") from archive_error
                detail = "live and KubeArchive PipelineRun/TaskRun list endpoints reachable"
            else:
                store = _store(config)
                store.doctor()
                detail = "BigQuery query and dataset metadata access succeeded; write permission not tested"
            print(f"PASS {source}: {detail}")
        except Exception as error:
            failures += 1
            print(f"FAIL {source}: {error}")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.scope not in config.scopes:
            raise ValueError(f"Unknown scope {args.scope!r}; configured scopes: {', '.join(config.scopes)}")
        scope = config.scopes[args.scope]
        if args.command == "doctor":
            return _run_doctor(args, scope, config)
        if args.command == "backfill":
            return _run_backfill(args, scope, config)
        if args.command == "sync":
            return _run_sync(args, scope, config)
        return _run_compare(args, scope, config)
    except Exception as error:
        print(f"chronoflux: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
