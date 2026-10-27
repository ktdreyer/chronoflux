from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TABLE_KEYS = {
    "gitlab_pipelines": ["gitlab_host", "pipeline_id"],
    "gitlab_jobs": ["gitlab_host", "project_id", "job_id"],
    "konflux_pipelineruns": ["cluster", "namespace", "resource_uid"],
    "konflux_taskruns": ["cluster", "namespace", "resource_uid"],
    "sync_state": ["source", "scope"],
    "sync_results": ["result_id"],
}


class BigQueryStore:
    def __init__(self, project: str, dataset: str):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", dataset):
            raise ValueError("BigQuery dataset must be an identifier")
        self.dataset = dataset
        try:
            from google.cloud import bigquery
        except ImportError as error:
            raise RuntimeError("Install Chronoflux with the 'bigquery' extra to use BigQuery") from error
        self.bigquery = bigquery
        self.client = bigquery.Client(project=project or None)
        self.project = project or self.client.project
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", self.project):
            raise ValueError("BigQuery project must be an identifier")
        self.prefix = f"`{self.project}.{self.dataset}."

    def table(self, name: str) -> str:
        if name not in TABLE_KEYS:
            raise ValueError(f"Unknown table {name}")
        return f"{self.prefix}{name}`"

    def ensure_tables(self) -> None:
        sql_path = Path(__file__).with_name("schema.sql")
        for statement in sql_path.read_text().split(";"):
            statement = statement.strip()
            if statement:
                self.client.query(statement.replace("{dataset}", f"{self.project}.{self.dataset}")).result()

    def merge(self, table: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if table not in TABLE_KEYS:
            raise ValueError(f"Unknown table {table}")
        keys = TABLE_KEYS[table]
        missing = [key for row in rows for key in keys if row.get(key) is None]
        if missing:
            raise ValueError(f"Cannot merge {table}: composite identity fields must not be null")
        enriched = [{**row, "ingested_at": datetime.now(timezone.utc).isoformat()} for row in rows]
        target = self.table(table)
        staging_name = f"{table}_staging_{uuid.uuid4().hex}"
        staging = f"`{self.project}.{self.dataset}.{staging_name}`"
        target_table = self.client.get_table(target.strip("`"))
        self.client.create_table(self.bigquery.Table(staging.strip("`"), schema=target_table.schema))
        try:
            load = self.client.load_table_from_json(enriched, staging.strip("`"), job_config=self.bigquery.LoadJobConfig(schema=target_table.schema, write_disposition="WRITE_TRUNCATE"))
            load.result()
            key_match = " AND ".join(f"T.{key}=S.{key}" for key in keys)
            update = ", ".join(f"{field.name}=S.{field.name}" for field in target_table.schema if field.name not in keys)
            columns = ", ".join(field.name for field in target_table.schema)
            values = ", ".join(f"S.{field.name}" for field in target_table.schema)
            sql = f"""
              MERGE {target} T
              USING (SELECT * EXCEPT(_row_number) FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(keys)} ORDER BY ingested_at DESC) AS _row_number
                FROM {staging}
              ) WHERE _row_number=1) S
              ON {key_match}
              WHEN MATCHED THEN UPDATE SET {update}
              WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
            """
            self.client.query(sql).result()
        finally:
            self.client.delete_table(staging.strip("`"), not_found_ok=True)
        return len(rows)

    def checkpoint(self, source: str, scope: str) -> datetime | None:
        sql = f"SELECT checkpoint_at FROM {self.table('sync_state')} WHERE source=@source AND scope=@scope LIMIT 1"
        config = self.bigquery.QueryJobConfig(query_parameters=[
            self.bigquery.ScalarQueryParameter("source", "STRING", source),
            self.bigquery.ScalarQueryParameter("scope", "STRING", scope),
        ])
        rows = list(self.client.query(sql, job_config=config).result())
        return rows[0]["checkpoint_at"] if rows and rows[0]["checkpoint_at"] else None

    def write_sync_result(self, row: dict[str, Any], advance_checkpoint: bool = False) -> None:
        self.merge("sync_results", [row])
        if advance_checkpoint:
            self.merge("sync_state", [{
                "source": row["source"], "scope": row["scope"],
                "checkpoint_at": row.get("checkpoint_at"), "updated_at": row.get("ended_at"),
            }])

    def compare(self, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        sql = Path(__file__).with_name("compare.sql").read_text()
        params = [
            self.bigquery.ScalarQueryParameter("project_path", "STRING", parameters["project_path"]),
            self.bigquery.ScalarQueryParameter("gitlab_host", "STRING", parameters["gitlab_host"]),
            self.bigquery.ScalarQueryParameter("cluster", "STRING", parameters["cluster"]),
            self.bigquery.ScalarQueryParameter("namespace", "STRING", parameters["namespace"]),
            self.bigquery.ScalarQueryParameter("sha", "STRING", parameters.get("sha")),
            self.bigquery.ScalarQueryParameter("since", "TIMESTAMP", parameters.get("since")),
            self.bigquery.ScalarQueryParameter("until", "TIMESTAMP", parameters.get("until")),
            self.bigquery.ScalarQueryParameter("variant", "STRING", parameters.get("variant")),
            self.bigquery.ScalarQueryParameter("component_map", "STRING", json.dumps([
                {"variant": variant, "component": component}
                for variant, component in parameters["component_map"].items()
            ])),
        ]
        sql = sql.replace("{gitlab_pipelines}", self.table("gitlab_pipelines")).replace("{gitlab_jobs}", self.table("gitlab_jobs")).replace("{konflux_pipelineruns}", self.table("konflux_pipelineruns")).replace("{konflux_taskruns}", self.table("konflux_taskruns"))
        config = self.bigquery.QueryJobConfig(query_parameters=params)
        return [dict(row.items()) for row in self.client.query(sql, job_config=config).result()]

    def doctor(self) -> None:
        self.client.query("SELECT 1").result()
        self.client.get_dataset(f"{self.project}.{self.dataset}")
