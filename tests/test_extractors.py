import json
import io
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.error import HTTPError

import pytest

from chronoflux import extractors
from chronoflux.extractors import GitLabExtractor, KonfluxExtractor, map_pipelinerun, map_taskrun, parse_image_job


FIXTURES = Path(__file__).parent / "fixtures"
SINCE = datetime(2025, 2, 12, 0, tzinfo=timezone.utc)
UNTIL = datetime(2025, 2, 13, 0, tzinfo=timezone.utc)


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def test_gitlab_job_listing_requests_retried_jobs_and_pages(monkeypatch):
    calls = []

    def fake_get(url, headers):
        calls.append((url, headers))
        query = parse_qs(urlparse(url).query)
        assert query["include_retried"] == ["true"]
        if query["page"] == ["1"]:
            return [{"id": i} for i in range(100)]
        return [fixture("gitlab_image_job.json")]

    monkeypatch.setattr(extractors, "_get_json", fake_get)
    extractor = GitLabExtractor("https://gitlab.example.invalid", "group/project", "redacted-token")
    rows = extractor.jobs(18290001)
    assert len(rows) == 101
    assert rows[-1]["id"] == 990001
    assert len(calls) == 2
    assert all(call[1]["PRIVATE-TOKEN"] == "redacted-token" for call in calls)


def test_http_client_retries_rate_limit_with_bounded_attempts(monkeypatch):
    calls = []

    def fake_urlopen(*_args, **_kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise HTTPError("https://api.example.invalid", 429, "rate limited", {"Retry-After": "1"}, None)
        return io.BytesIO(b'{"ok": true}')

    monkeypatch.setattr(extractors.urllib.request, "urlopen", fake_urlopen)
    delays = []
    monkeypatch.setattr(extractors.time, "sleep", delays.append)
    assert extractors._get_json("https://api.example.invalid") == {"ok": True}
    assert delays == [1.0]
    assert len(calls) == 2


def test_gitlab_maps_pipeline_and_image_job_independently(monkeypatch):
    extractor = GitLabExtractor("https://gitlab.example.invalid", "group/project")
    monkeypatch.setattr(extractor, "pipelines", lambda *_args, **_kwargs: [fixture("gitlab_pipeline.json")])
    monkeypatch.setattr(extractor, "pipeline_detail", lambda _pipeline_id: fixture("gitlab_pipeline.json"))
    monkeypatch.setattr(extractor, "jobs", lambda _pipeline_id: [fixture("gitlab_image_job.json")])
    pipelines, jobs = extractor.extract(SINCE, UNTIL, 8, "group/project")
    assert pipelines[0]["status"] == "failed"
    assert jobs[0]["status"] == "success"
    assert jobs[0]["variant"] == "rocm"
    assert jobs[0]["architecture"] == "x86_64"
    assert jobs[0]["queued_duration_seconds"] == 8.2


def test_job_parser_does_not_guess_unknown_names_or_platforms():
    assert parse_image_job("build-image-mr-rocm-x86_64") == ("rocm", "x86_64")
    assert parse_image_job("verify-image-rocm-x86_64") == (None, None)
    assert parse_image_job("build-image-custom-rocm-something") == (None, None)


def test_tekton_mappings_keep_identity_conditions_and_task_parent():
    pipeline = map_pipelinerun(fixture("pipelinerun.json"), "p02", "ai-tenant", "live")
    task = map_taskrun(fixture("taskrun.json"), "p02", "ai-tenant", "archive")
    assert pipeline["resource_uid"] == "11111111-2222-3333-4444-555555555555"
    assert pipeline["commit_sha"] == fixture("gitlab_pipeline.json")["sha"]
    assert pipeline["condition_status"] == "True"
    assert pipeline["component"] == "verified-component"
    assert task["pipelinerun_uid"] == pipeline["resource_uid"]
    assert task["pipeline_task"] == "build-image"
    assert task["platform"] == "x86_64"
    assert task["condition_reason"] == "Succeeded"
    assert task["observation_source"] == "archive"


def test_konflux_pagination_and_live_archive_deduplication(monkeypatch):
    pipeline = fixture("pipelinerun.json")
    taskrun = fixture("taskrun.json")
    extractor = KonfluxExtractor("p02", "ai-tenant", archive_url="https://archive.example.invalid")
    calls = []

    def page(source, resource, continuation):
        calls.append((source, resource, continuation))
        items = [pipeline] if resource == "pipelineruns" else [taskrun]
        if source == "live" and continuation is None:
            return {"items": items, "metadata": {"continue": "next-page"}}
        return {"items": items, "metadata": {}}

    monkeypatch.setattr(extractor, "_live_page", lambda resource, token: page("live", resource, token))
    monkeypatch.setattr(extractor, "_archive_page", lambda resource, token: page("archive", resource, token))
    runs, tasks = extractor.extract(SINCE, UNTIL)
    assert len(runs) == len(tasks) == 1
    assert runs[0]["observation_source"] == "live"
    assert tasks[0]["observation_source"] == "live"
    assert ("live", "pipelineruns", "next-page") in calls


def test_unknown_succeeded_condition_remains_unknown():
    resource = fixture("pipelinerun.json")
    resource["status"]["conditions"][0]["status"] = "Unknown"
    mapped = map_pipelinerun(resource, "p02", "ai-tenant", "live")
    assert mapped["condition_status"] == "Unknown"


def test_konflux_refuses_resources_without_stable_uid(monkeypatch):
    extractor = KonfluxExtractor("p02", "ai-tenant", archive_url="https://archive.example.invalid")
    malformed = fixture("pipelinerun.json")
    malformed["metadata"].pop("uid")
    monkeypatch.setattr(extractor, "_live_page", lambda resource, _token: {"items": [malformed], "metadata": {}})
    monkeypatch.setattr(extractor, "_archive_page", lambda resource, _token: {"items": [], "metadata": {}})
    with pytest.raises(extractors.SourceError, match="missing metadata.uid"):
        extractor.extract(SINCE, UNTIL)
