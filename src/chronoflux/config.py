from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Scope:
    name: str
    gitlab_host: str
    gitlab_project: str
    gitlab_project_id: int
    cluster: str
    namespace: str
    api_server: str = ""
    application: str = ""
    variant_components: dict[str, str] = field(default_factory=dict)
    label_selectors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Config:
    scopes: dict[str, Scope]
    bq_project: str = ""
    bq_dataset: str = "chronoflux"


def load_config(path: str | Path) -> Config:
    with Path(path).open("rb") as stream:
        raw: dict[str, Any] = tomllib.load(stream)
    scopes = {
        name: Scope(name=name, **values)
        for name, values in raw.get("scopes", {}).items()
    }
    bq = raw.get("bigquery", {})
    return Config(
        scopes=scopes,
        bq_project=os.getenv("BIGQUERY_PROJECT", bq.get("project", "")),
        bq_dataset=os.getenv("BIGQUERY_DATASET", bq.get("dataset", "chronoflux")),
    )
