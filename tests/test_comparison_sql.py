from pathlib import Path

import pytest


SQL_DIR = Path(__file__).resolve().parents[1] / "src" / "chronoflux"


@pytest.mark.parametrize("filename", ["compare.sql", "reporting.sql"])
def test_comparisons_exclude_mislabeled_test_runs(filename):
    sql = (SQL_DIR / filename).read_text()

    # An entitlement-test PipelineRun can carry the rhaiis-cuda-ubi9 label;
    # its name, not just its label, must identify the component it built.
    assert "STARTS_WITH(p.pipelinerun_name, CONCAT(p.component, '-on-'))" in sql


@pytest.mark.parametrize("filename", ["compare.sql", "reporting.sql"])
def test_task_breakdown_uses_parallel_build_critical_path(filename):
    sql = (SQL_DIR / filename).read_text()

    assert "MAX(IF(t.pipeline_task='build-container'," in sql
    assert "k.build_critical_seconds AS konflux_build_critical_seconds" in sql
    assert "k.source_image_seconds AS konflux_source_image_seconds" in sql
