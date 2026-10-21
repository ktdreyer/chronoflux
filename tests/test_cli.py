import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from chronoflux import cli
from chronoflux.config import load_config


def test_backfill_dry_run_prints_counts_without_bigquery(monkeypatch, capsys, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text('''
[scopes.test]
gitlab_host = "https://gitlab.example.invalid"
gitlab_project = "group/project"
gitlab_project_id = 12
cluster = "p02"
namespace = "ai-tenant"
''')
    rows = {"gitlab_pipelines": [{"pipeline_id": 18}], "gitlab_jobs": [{"job_id": 19}]}
    monkeypatch.setattr(cli, "_source_extract", lambda *_args: rows)
    monkeypatch.setattr(cli, "_store", lambda *_args: (_ for _ in ()).throw(AssertionError("BigQuery should not be used")))
    args = SimpleNamespace(
        since=datetime(2025, 1, 1, tzinfo=timezone.utc), until=datetime(2025, 1, 2, tzinfo=timezone.utc),
        source="gitlab", dry_run=True,
    )
    assert cli._run_backfill(args, load_config(config_path).scopes["test"], load_config(config_path)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["counts"] == {"gitlab_pipelines": 1, "gitlab_jobs": 1}
    assert result["dry_run"] is True


def test_config_keeps_scope_component_map():
    config_path = Path(__file__).parents[1] / "chronoflux.toml"
    scope = load_config(config_path).scopes["rhaiis-containers"]
    assert scope.namespace == "ai-tenant"
    assert scope.api_server == "https://api.konflux.example.com:6443"
    assert scope.variant_components == {}


class FakeStore:
    def __init__(self, checkpoint):
        self.saved_results = []
        self.checkpoint_value = checkpoint
        self.created = False

    def ensure_tables(self):
        self.created = True

    def checkpoint(self, _source, _scope):
        return self.checkpoint_value

    def merge(self, _table, _rows):
        return 0

    def write_sync_result(self, row, advance_checkpoint=False):
        self.saved_results.append((row, advance_checkpoint))


def test_sync_failure_records_result_without_checkpoint_advance(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text('''
[scopes.test]
gitlab_host = "https://gitlab.example.invalid"
gitlab_project = "group/project"
gitlab_project_id = 12
cluster = "p02"
namespace = "ai-tenant"
[bigquery]
project = "test-project"
''')
    store = FakeStore(datetime(2025, 1, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(cli, "_store", lambda _config: store)
    monkeypatch.setattr(cli, "_source_extract", lambda *_args: (_ for _ in ()).throw(RuntimeError("temporary API failure")))
    args = SimpleNamespace(dry_run=False, source="gitlab")
    config = load_config(config_path)
    assert cli._run_sync(args, config.scopes["test"], config) == 1
    assert store.created
    assert store.saved_results[0][0]["status"] == "failed"
    assert store.saved_results[0][1] is False
    assert store.saved_results[0][0]["checkpoint_at"] == "2025-01-01T00:00:00+00:00"
    assert "temporary API failure" in capsys.readouterr().out


def test_sync_success_with_no_new_records_advances_checkpoint(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text('''
[scopes.test]
gitlab_host = "https://gitlab.example.invalid"
gitlab_project = "group/project"
gitlab_project_id = 12
cluster = "p02"
namespace = "ai-tenant"
[bigquery]
project = "test-project"
''')
    store = FakeStore(None)
    monkeypatch.setattr(cli, "_store", lambda _config: store)
    monkeypatch.setattr(cli, "_source_extract", lambda *_args: {"gitlab_pipelines": [], "gitlab_jobs": []})
    args = SimpleNamespace(dry_run=False, source="gitlab")
    config = load_config(config_path)
    assert cli._run_sync(args, config.scopes["test"], config) == 0
    assert store.saved_results[0][0]["status"] == "success"
    assert store.saved_results[0][1] is True
    assert store.saved_results[0][0]["pipeline_count"] == 0
    assert "success" in capsys.readouterr().out


def test_sync_dry_run_needs_no_bigquery_and_never_writes(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text('''
[scopes.test]
gitlab_host = "https://gitlab.example.invalid"
gitlab_project = "group/project"
gitlab_project_id = 12
cluster = "p02"
namespace = "ai-tenant"
''')
    monkeypatch.setattr(cli, "_store", lambda *_args: (_ for _ in ()).throw(AssertionError("dry-run must not create a store")))
    monkeypatch.setattr(cli, "_source_extract", lambda *_args: {"konflux_pipelineruns": [], "konflux_taskruns": []})
    args = SimpleNamespace(dry_run=True, source="konflux")
    config = load_config(config_path)
    assert cli._run_sync(args, config.scopes["test"], config) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["dry_run"] is True
    assert result["status"] == "success"


def test_compare_summary_excludes_failed_and_unmatched_rows(capsys):
    rows = [{
        "sha": f"sha-{index}", "variant": "rocm", "architecture": "x86_64", "component": "rocm", "platform": "x86_64",
        "job_status": "success", "pipeline_condition_status": "True", "gitlab_wall_seconds": 10.0 + index,
        "konflux_wall_seconds": 12.0 + index, "retry_count": 1, "match_status": "matched",
    } for index in range(3)]
    rows.append({"sha": "failed", "job_status": "failed", "pipeline_condition_status": "False", "match_status": "gitlab_only"})
    cli._print_compare(rows, "json")
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["eligible_successful_paired_samples"] == 3
    assert output["summary"]["timing_seconds"]["gitlab_wall_seconds"]["median"] == 11
    assert output["summary"]["timing_seconds"]["gitlab_wall_seconds"]["p90"] is None
    assert output["summary"]["excluded_from_timing_summary"] == 1
