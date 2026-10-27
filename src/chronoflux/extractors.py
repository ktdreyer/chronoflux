from __future__ import annotations

import json
import http.client
import os
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable


class SourceError(RuntimeError):
    pass


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SourceError(f"Invalid source timestamp {value!r}") from error
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def in_window(value: str | None, since: datetime, until: datetime) -> bool:
    parsed = parse_time(value)
    return parsed is not None and since <= parsed < until


def _get_json(url: str, headers: dict[str, str] | None = None) -> Any:
    request = urllib.request.Request(url, headers={"Accept": "application/json", **(headers or {})})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            if error.code != 429 and not 500 <= error.code < 600:
                raise SourceError(f"GET {url}: HTTP {error.code}") from error
            if attempt == 2:
                raise SourceError(f"GET {url}: HTTP {error.code} after 3 attempts") from error
            retry_after = error.headers.get("Retry-After", "")
            delay = min(float(retry_after), 10.0) if retry_after.isdigit() else 0.25 * (2 ** attempt)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt == 2:
                raise SourceError(f"GET {url}: {error} after 3 attempts") from error
            time.sleep(0.25 * (2 ** attempt))
        # KubeArchive serves large chunked responses that the server sometimes
        # drops mid-transfer; retry those like any other transient failure.
        except (http.client.HTTPException, ConnectionError) as error:
            if attempt == 2:
                raise SourceError(f"GET {url}: {error} after 3 attempts") from error
            time.sleep(0.25 * (2 ** attempt))
        except json.JSONDecodeError as error:
            raise SourceError(f"GET {url}: invalid JSON response") from error
    raise SourceError(f"GET {url}: request failed")


class GitLabExtractor:
    def __init__(self, host: str, project: str, token: str | None = None):
        self.host = host.rstrip("/")
        self.project = urllib.parse.quote(project, safe="")
        self.headers = {"PRIVATE-TOKEN": token} if token else {}

    @property
    def project_url(self) -> str:
        return f"{self.host}/api/v4/projects/{self.project}"

    def _pages(self, url: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            separator = "&" if "?" in url else "?"
            batch = _get_json(f"{url}{separator}per_page=100&page={page}", self.headers)
            if not isinstance(batch, list):
                raise SourceError("GitLab list endpoint returned a non-list response")
            rows.extend(batch)
            if len(batch) < 100:
                return rows
            page += 1

    def pipelines(self, since: datetime, until: datetime, updated_after: datetime | None = None) -> list[dict[str, Any]]:
        after = urllib.parse.quote(since.isoformat().replace("+00:00", "Z"))
        before = urllib.parse.quote(until.isoformat().replace("+00:00", "Z"))
        result = self._pages(f"{self.project_url}/pipelines?order_by=updated_at&sort=asc&created_after={after}&created_before={before}")
        selected = {int(row["id"]): row for row in result if in_window(row.get("created_at"), since, until)}
        if updated_after:
            updated = urllib.parse.quote(updated_after.isoformat().replace("+00:00", "Z"))
            rows = self._pages(f"{self.project_url}/pipelines?order_by=updated_at&sort=asc&updated_after={updated}&updated_before={before}")
            selected.update({int(row["id"]): row for row in rows})
        return list(selected.values())

    def pipeline_detail(self, pipeline_id: int) -> dict[str, Any]:
        return _get_json(f"{self.project_url}/pipelines/{pipeline_id}", self.headers)

    def jobs(self, pipeline_id: int) -> list[dict[str, Any]]:
        return self._pages(f"{self.project_url}/pipelines/{pipeline_id}/jobs?include_retried=true")

    def project_info(self) -> dict[str, Any]:
        return _get_json(self.project_url, self.headers)

    def check(self) -> None:
        self.project_info()
        pipelines = _get_json(f"{self.project_url}/pipelines?per_page=1", self.headers)
        if pipelines:
            self._pages(f"{self.project_url}/pipelines/{int(pipelines[0]['id'])}/jobs?include_retried=true")

    def extract(self, since: datetime, until: datetime, project_id: int, project_path: str, updated_after: datetime | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        pipelines: list[dict[str, Any]] = []
        jobs: list[dict[str, Any]] = []
        host = urllib.parse.urlparse(self.host).netloc or self.host
        for item in self.pipelines(since, until, updated_after):
            detail = self.pipeline_detail(int(item["id"]))
            created = detail.get("created_at", item.get("created_at"))
            is_reconcile = updated_after is not None and parse_time(detail.get("updated_at")) is not None and parse_time(detail.get("updated_at")) >= updated_after
            if not in_window(created, since, until) and not is_reconcile:
                continue
            pipelines.append({
                "gitlab_host": host, "pipeline_id": int(detail["id"]), "project_id": project_id,
                "project_path": project_path, "web_url": detail.get("web_url"), "sha": detail.get("sha"),
                "ref": detail.get("ref"), "source": detail.get("source"), "status": detail.get("status"),
                "created_at": created, "started_at": detail.get("started_at"),
                "finished_at": detail.get("finished_at"), "updated_at": detail.get("updated_at"),
                "duration_seconds": detail.get("duration"), "queued_duration_seconds": detail.get("queued_duration"),
            })
            for job in self.jobs(int(detail["id"])):
                variant, architecture = parse_image_job(job.get("name", ""))
                runner = job.get("runner") or {}
                jobs.append({
                    "gitlab_host": host, "project_id": project_id, "job_id": int(job["id"]),
                    "pipeline_id": int(detail["id"]), "name": job.get("name"), "stage": job.get("stage"),
                    "status": job.get("status"), "web_url": job.get("web_url"),
                    "job_kind": "build-image" if job.get("name", "").startswith("build-image-") else None,
                    "variant": variant, "architecture": architecture,
                    "allow_failure": job.get("allow_failure"), "failure_reason": job.get("failure_reason"),
                    "runner_id": runner.get("id"), "runner_tags": job.get("tag_list") or [],
                    "created_at": job.get("created_at"), "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"), "duration_seconds": job.get("duration"),
                    "queued_duration_seconds": job.get("queued_duration"),
                })
        return pipelines, jobs


_IMAGE_JOB = re.compile(r"^build-image-(?:mr|push|schedule|web|api|trigger)-(.+)-(x86_64|aarch64|ppc64le|s390x)$")


def parse_image_job(name: str) -> tuple[str | None, str | None]:
    match = _IMAGE_JOB.fullmatch(name)
    return (match.group(1), match.group(2)) if match else (None, None)


def _value_from(items: Iterable[dict[str, Any]], names: set[str]) -> str | None:
    for item in items:
        if item.get("name") in names:
            value = item.get("value")
            if value is not None:
                return str(value)
    return None


def _first(mapping: dict[str, Any], names: tuple[str, ...]) -> str | None:
    for name in names:
        value = mapping.get(name)
        if value not in (None, ""):
            return str(value)
    return None


def map_condition(status: dict[str, Any]) -> tuple[str | None, str | None]:
    for condition in status.get("conditions", []):
        if condition.get("type") == "Succeeded":
            return condition.get("status"), condition.get("reason")
    return None, None


def map_pipelinerun(resource: dict[str, Any], cluster: str, namespace: str, observation_source: str) -> dict[str, Any]:
    metadata = resource.get("metadata", {})
    status = resource.get("status", {})
    annotations = metadata.get("annotations", {})
    labels = metadata.get("labels", {})
    params = resource.get("spec", {}).get("params", []) or status.get("pipelineSpec", {}).get("params", [])
    condition_status, condition_reason = map_condition(status)
    # Annotation and label keys verified against p02 ai-tenant resources in October 2026.
    # PAC v0.50 uses pipelinesascode.tekton.dev/*; a newer test deployment uses
    # pac.test.appstudio.openshift.io/*, so both styles are accepted.
    return {
        "cluster": cluster, "namespace": namespace, "resource_uid": metadata.get("uid"),
        "pipelinerun_name": metadata.get("name"),
        "repository": _first(annotations, ("pipelinesascode.tekton.dev/repo-url", "pipelinesascode.tekton.dev/repo", "pac.test.appstudio.openshift.io/repo-url", "build.appstudio.redhat.com/source-repo")),
        "commit_sha": _first(annotations, ("build.appstudio.redhat.com/commit_sha", "pipelinesascode.tekton.dev/sha", "pac.test.appstudio.openshift.io/sha", "build.appstudio.redhat.com/source-sha")) or _value_from(params, {"revision", "git-revision", "commit"}),
        "event": _first(annotations, ("pipelinesascode.tekton.dev/event-type", "pac.test.appstudio.openshift.io/event-type", "pipelinesascode.tekton.dev/event")),
        "source_ref": _first(annotations, ("pipelinesascode.tekton.dev/source-branch", "pac.test.appstudio.openshift.io/source-branch", "build.appstudio.redhat.com/source-branch")) or _value_from(params, {"branch", "ref"}),
        "target_branch": _first(annotations, ("build.appstudio.redhat.com/target_branch", "pipelinesascode.tekton.dev/branch", "pipelinesascode.tekton.dev/target-branch")),
        "pull_request_number": _first(annotations, ("build.appstudio.redhat.com/pull_request_number", "pipelinesascode.tekton.dev/pr-number", "pac.test.appstudio.openshift.io/pr-number")),
        "component": _first(labels, ("appstudio.openshift.io/component", "appstudio.redhat.com/component", "app.kubernetes.io/component")),
        "application": _first(labels, ("appstudio.openshift.io/application", "appstudio.redhat.com/application", "app.kubernetes.io/part-of")),
        "web_url": _first(annotations, ("pipelinesascode.tekton.dev/log-url", "pac.test.appstudio.openshift.io/log-url", "appstudio.redhat.com/pipelinerun-url")),
        "observation_source": observation_source, "condition_status": condition_status,
        "condition_reason": condition_reason, "created_at": metadata.get("creationTimestamp"),
        "start_time": status.get("startTime"), "completion_time": status.get("completionTime"),
    }


def _normalize_platform(value: str) -> str:
    """Normalize Konflux platform values ('linux/x86_64', 'linux-x86_64') to
    GitLab architecture names ('x86_64')."""
    platform = value.strip().rsplit("/", 1)[-1]
    if platform.startswith("linux-"):
        platform = platform.split("-", 1)[1]
    return platform


def map_taskrun(resource: dict[str, Any], cluster: str, namespace: str, observation_source: str) -> dict[str, Any]:
    metadata = resource.get("metadata", {})
    status = resource.get("status", {})
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})
    owner = next((ref for ref in metadata.get("ownerReferences", []) if ref.get("kind") == "PipelineRun"), {})
    condition_status, condition_reason = map_condition(status)
    task_name = _first(labels, ("tekton.dev/pipelineTask", "tekton.dev/pipelineTaskName"))
    task_name = task_name or _first(annotations, ("tekton.dev/pipelineTask",))
    # Only build taskruns carry the platform: the 'build-container' task has a
    # 'build.appstudio.redhat.com/target-platform' label (e.g. linux-x86_64) and a
    # 'PLATFORM' param (e.g. linux/x86_64). Other tasks stay null; do not guess.
    platform = _first(labels, ("build.appstudio.redhat.com/target-platform", "build.appstudio.redhat.com/platform", "appstudio.redhat.com/platform"))
    platform = platform or _value_from(resource.get("spec", {}).get("params", []), {"PLATFORM", "platform", "build-platform"})
    platform = platform or _value_from(status.get("results", []), {"platform", "build-platform"})
    platform = _normalize_platform(platform) if platform else None
    return {
        "cluster": cluster, "namespace": namespace, "resource_uid": metadata.get("uid"),
        "taskrun_name": metadata.get("name"), "pipelinerun_uid": owner.get("uid") or labels.get("tekton.dev/pipelineRunUID"),
        "pipelinerun_name": owner.get("name") or labels.get("tekton.dev/pipelineRun"),
        "pipeline_task": task_name, "platform": platform,
        "web_url": _first(annotations, ("pipelinesascode.tekton.dev/log-url", "appstudio.redhat.com/taskrun-url")),
        "condition_status": condition_status, "condition_reason": condition_reason,
        "observation_source": observation_source, "created_at": metadata.get("creationTimestamp"),
        "start_time": status.get("startTime"), "completion_time": status.get("completionTime"),
    }


class KonfluxExtractor:
    def __init__(self, cluster: str, namespace: str, archive_url: str | None = None, expected_server: str | None = None, label_selectors: list[str] | None = None):
        self.cluster = cluster
        self.namespace = namespace
        self.expected_server = expected_server.rstrip("/") if expected_server else None
        self.archive_url = (archive_url or os.getenv("KUBEARCHIVE_URL", "")).rstrip("/")
        self.archive_token = os.getenv("KUBEARCHIVE_TOKEN")
        self.api_version = os.getenv("TEKTON_API_VERSION", "v1")
        # Server-side selectors keep archive listings bounded; the KubeArchive
        # API ignores creationTimestamp field selectors during pagination, so
        # labels are the only reliable server-side restriction.
        self.label_selectors = list(label_selectors or [])

    def _query(self, limit: str, continuation: str | None) -> dict[str, str]:
        query = {"limit": limit}
        if continuation:
            query["continue"] = continuation
        if self.label_selectors:
            query["labelSelector"] = ",".join(self.label_selectors)
        return query

    def _live_page(self, resource: str, continuation: str | None) -> dict[str, Any]:
        if not shutil.which("oc"):
            raise SourceError("oc is required for live Konflux API access")
        if self.expected_server:
            try:
                current_server = subprocess.run(["oc", "whoami", "--show-server"], check=True, text=True, capture_output=True, timeout=10).stdout.strip().rstrip("/")
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                raise SourceError(f"Cannot inspect the current oc context: {error}") from error
            if current_server != self.expected_server:
                raise SourceError(f"Current oc server {current_server!r} does not match configured Konflux server {self.expected_server!r}")
        path = f"/apis/tekton.dev/{self.api_version}/namespaces/{urllib.parse.quote(self.namespace)}/{resource}?{urllib.parse.urlencode(self._query('500', continuation))}"
        try:
            output = subprocess.run(["oc", "get", "--raw", path], check=True, text=True, capture_output=True, timeout=45)
            return json.loads(output.stdout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
            raise SourceError(f"Live Kubernetes API request for {resource} failed: {error}") from error

    def _archive_page(self, resource: str, continuation: str | None) -> dict[str, Any]:
        if not self.archive_url:
            raise SourceError("KUBEARCHIVE_URL is not configured")
        # Smaller pages than the live API: archived taskrun responses are large
        # and the KubeArchive server can drop oversized chunked responses.
        endpoint = f"{self.archive_url}/apis/tekton.dev/{self.api_version}/namespaces/{urllib.parse.quote(self.namespace)}/{resource}?{urllib.parse.urlencode(self._query('100', continuation))}"
        headers = {"Authorization": f"Bearer {self.archive_token}"} if self.archive_token else {}
        result = _get_json(endpoint, headers)
        if not isinstance(result, dict) or "items" not in result:
            raise SourceError("KubeArchive response is not a Kubernetes resource list")
        return result

    @staticmethod
    def _list(fetch: Any, resource: str, since: datetime | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        continuation = None
        while True:
            page = fetch(resource, continuation)
            page_items = page.get("items", [])
            items.extend(page_items)
            continuation = page.get("metadata", {}).get("continue")
            # KubeArchive serves runs newest-first, so stop walking once a whole
            # page predates the window instead of paging the entire retained
            # history. Items with missing timestamps never trigger the stop.
            if since is not None and page_items:
                stamps = [parse_time(item.get("metadata", {}).get("creationTimestamp")) for item in page_items]
                if all(stamp is not None and stamp < since for stamp in stamps):
                    break
            if not continuation:
                return items
        # Windowed walk stopped early; the caller filters by window.
        return items

    def extract(self, since: datetime, until: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        results: list[tuple[str, list[dict[str, Any]]]] = []
        # CHRONOFLUX_KONFLUX_SOURCES=archive skips the live API, e.g. when
        # historical windows live entirely in KubeArchive and there is no
        # current oc login for the cluster.
        configured = os.getenv("CHRONOFLUX_KONFLUX_SOURCES", "live,archive").split(",")
        sources = [(name.strip(), fetch) for name, fetch in
                   (("live", self._live_page), ("archive", self._archive_page)) if name.strip() in configured]
        for source, fetch in sources:
            for resource in ("pipelineruns", "taskruns"):
                # Only the archive walk is bounded by the window; live listings
                # are small and their ordering is not guaranteed.
                raw = self._list(fetch, resource, since=since if source == "archive" else None)
                results.append((f"{source}:{resource}", [r for r in raw if in_window(r.get("metadata", {}).get("creationTimestamp"), since, until)]))
        mapped: dict[str, dict[str, Any]] = {}
        tasks: dict[str, dict[str, Any]] = {}
        for label, resources in results:
            source, kind = label.split(":", 1)
            destination = mapped if kind == "pipelineruns" else tasks
            mapper = map_pipelinerun if kind == "pipelineruns" else map_taskrun
            for resource in resources:
                resource_uid = resource.get("metadata", {}).get("uid")
                if not resource_uid:
                    raise SourceError(f"{kind} resource is missing metadata.uid; refusing an ambiguous upsert identity")
                row = mapper(resource, self.cluster, self.namespace, source)
                if resource_uid not in destination or (source == "live" and destination[resource_uid]["observation_source"] == "archive"):
                    destination[resource_uid] = row
        return list(mapped.values()), list(tasks.values())

    def check(self, source: str) -> None:
        # A single page per resource verifies authentication and response shape;
        # walking every page would make doctor impractically slow on KubeArchive.
        fetch = self._live_page if source == "live" else self._archive_page
        for resource in ("pipelineruns", "taskruns"):
            page = fetch(resource, None)
            if not isinstance(page.get("items"), list):
                raise SourceError(f"{source} {resource} response has no items list")
