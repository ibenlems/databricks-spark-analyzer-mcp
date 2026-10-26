from __future__ import annotations

import os
from typing import Any

from databricks.sdk import WorkspaceClient

import re
from concurrent.futures import ThreadPoolExecutor

from .cluster_logs import MAX_FILE_BYTES, parse_driver_log, parse_executor_log, parse_gc_log, scan_exceptions, to_lines
from .spark_metrics import SPARK_UI_PORT, from_rest, parse_event_log, read_text_lines


class DatabricksAnalyzerClient:
    """Simple Databricks API wrapper for job and run analysis."""

    def __init__(self, workspace_url: str | None = None, token: str | None = None):
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")

        if not self.workspace_url or not self.token:
            raise ValueError(
                "Databricks host and token must be provided via DATABRICKS_HOST and DATABRICKS_TOKEN or constructor arguments."
            )

        self.client = WorkspaceClient(host=self.workspace_url, token=self.token)

    def list_job_runs(self, job_id: int, limit: int = 20) -> list[dict[str, Any]]:
        runs = self.client.jobs.list_runs(job_id=job_id, limit=limit)
        return [run.as_dict() for run in runs]

    def get_run(self, run_id: int) -> dict[str, Any]:
        run = self.client.jobs.get_run(run_id)
        return run.as_dict()

    def get_run_tasks(self, run_id: int) -> list[dict[str, Any]]:
        run = self.client.jobs.get_run(run_id)
        return [task.as_dict() for task in (run.tasks or [])]

    def _cluster_ids_and_log_dests(self, run: dict[str, Any]) -> list[tuple[str, str | None]]:
        dests: dict[str, str] = {}
        for jc in run.get("job_clusters", []) or []:
            conf = (jc.get("new_cluster") or {}).get("cluster_log_conf") or {}
            dest = (conf.get("volumes") or conf.get("dbfs") or {}).get("destination")
            if dest:
                dests[jc.get("job_cluster_key")] = dest
        result: list[tuple[str, str | None]] = []
        for task in run.get("tasks", []) or []:
            cluster_id = (task.get("cluster_instance") or {}).get("cluster_id")
            if cluster_id and cluster_id not in [c for c, _ in result]:
                result.append((cluster_id, dests.get(task.get("job_cluster_key"))))
        return result

    def _spark_from_live_cluster(self, cluster_id: str) -> dict[str, Any]:
        base = f"/driver-proxy-api/o/0/{cluster_id}/{SPARK_UI_PORT}/api/v1"

        def api_get(path: str, query: dict[str, Any] | None = None) -> Any:
            return self.client.api_client.do("GET", path, query=query)

        return from_rest(api_get, base)

    def _spark_from_event_logs(self, cluster_id: str, log_dest: str) -> dict[str, Any]:
        root = f"{log_dest.rstrip('/')}/{cluster_id}/eventlog"
        paths: list[str] = []
        stack = [root]
        while stack:
            current = stack.pop()
            for entry in self.client.files.list_directory_contents(current):
                if entry.is_directory:
                    stack.append(entry.path)
                else:
                    paths.append(entry.path)
        lines: list[str] = []
        for path in sorted(paths):
            if path.rsplit("/", 1)[-1].startswith("eventlog"):
                raw = self.client.files.download(path).contents.read()
                lines.extend(read_text_lines(raw, path))
        if not lines:
            raise FileNotFoundError(f"No event log files under {root}")
        return parse_event_log(lines)

    def get_spark_evidence(self, run: dict[str, Any]) -> dict[str, Any]:
        """Collect Spark task metrics, stage plans and SQL text for the run's clusters.

        Tries the live Spark UI first (cluster running), then event logs from the
        configured cluster log destination. Reports per-source status in `sources`.
        """
        evidence: dict[str, Any] = {"tasks": [], "stage_plans": {}, "sql_queries": [], "executors": [], "stages": [], "spark_conf": {}, "sources": []}
        for cluster_id, log_dest in self._cluster_ids_and_log_dests(run):
            attempts: list[tuple[str, Any]] = [("live_spark_ui", lambda c=cluster_id: self._spark_from_live_cluster(c))]
            if log_dest:
                attempts.append(("event_log", lambda c=cluster_id, d=log_dest: self._spark_from_event_logs(c, d)))
            else:
                evidence["sources"].append({"cluster_id": cluster_id, "source": "event_log", "status": "skipped", "detail": "No cluster_log_conf destination configured."})
            for source, fetch in attempts:
                try:
                    data = fetch()
                except Exception as exc:
                    evidence["sources"].append({"cluster_id": cluster_id, "source": source, "status": "unavailable", "detail": str(exc)[:300]})
                    continue
                if not data["tasks"]:
                    evidence["sources"].append({"cluster_id": cluster_id, "source": source, "status": "empty", "detail": "No task metrics returned."})
                    continue
                evidence["tasks"].extend(data["tasks"])
                evidence["stage_plans"].update(data["stage_plans"])
                evidence["sql_queries"].extend(data["sql_queries"])
                evidence["executors"].extend(data.get("executors", []))
                evidence["stages"].extend(data.get("stages", []))
                evidence["spark_conf"].update(data.get("spark_conf", {}))
                evidence["sources"].append({"cluster_id": cluster_id, "source": source, "status": "ok", "task_count": len(data["tasks"])})
                break
        return evidence

    def _list_files(self, path: str, depth: int = 4) -> list[Any]:
        found: list[Any] = []
        try:
            entries = list(self.client.files.list_directory_contents(path))
        except Exception:
            return found
        for entry in entries:
            if entry.is_directory:
                if depth > 0:
                    found.extend(self._list_files(entry.path, depth - 1))
            else:
                found.append(entry)
        return found

    def _download(self, path: str) -> bytes:
        return self.client.files.download(path).contents.read(MAX_FILE_BYTES)

    @staticmethod
    def _ordered(entries: list[Any], base: str) -> list[Any]:
        """Rotated files first (chronological by name), the active file last."""
        picked = [e for e in entries if e.path.rsplit("/", 1)[-1].startswith(base)]
        return sorted(picked, key=lambda e: (e.path.rsplit("/", 1)[-1] in {base, f"{base}-active.log"}, e.path))

    def get_cluster_logs(
        self,
        cluster_id: str,
        log_dest: str,
        window: tuple[int, int] | None = None,
        max_executors: int = 48,
        workers: int = 8,
    ) -> dict[str, Any]:
        """Download and parse everything under <log_dest>/<cluster_id>: driver log4j/stdout/stderr,
        every executor's stdout/stderr (GC logs, task/file/shuffle activity) and init script logs."""
        root = f"{log_dest.rstrip('/')}/{cluster_id}"
        logs: dict[str, Any] = {"driver": {}, "executors": {}, "init_scripts": [], "sources": []}

        def read_all(entries: list[Any]) -> list[bytes]:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                return list(pool.map(lambda e: self._download(e.path), entries))

        def chain(chunks: list[bytes]):
            for raw in chunks:
                yield from to_lines(raw)

        driver_entries = self._list_files(f"{root}/driver")
        try:
            parts: dict[str, list[bytes]] = {}
            for base in ("log4j", "stdout", "stderr"):
                parts[base] = read_all(self._ordered(driver_entries, base))
            logs["driver"] = {
                "log": parse_driver_log(chain(parts["log4j"]), window, keep_tasks=True),
                "gc": parse_gc_log(chain(parts["stdout"])),
                "stderr": scan_exceptions(chain(parts["stderr"])),
            }
            logs["sources"].append({"component": "driver", "status": "ok", "files": len(driver_entries)})
        except Exception as exc:
            logs["sources"].append({"component": "driver", "status": "unavailable", "detail": str(exc)[:300]})

        exec_entries = self._list_files(f"{root}/executor")
        parsed = []
        for entry in exec_entries:
            seg = entry.path[len(root) + 1:].split("/")  # executor/<app>/<executor id>/<file>
            if len(seg) >= 4:
                parsed.append((seg[1], seg[2], entry))
        multi_app = len({app for app, _, _ in parsed}) > 1
        by_exec: dict[str, list[Any]] = {}
        for app, eid, entry in parsed:
            by_exec.setdefault(f"{app}:{eid}" if multi_app else eid, []).append(entry)
        keys = sorted(by_exec)[:max_executors]

        def load(key: str) -> tuple[str, dict[str, Any] | None, str | None]:
            try:
                files = by_exec[key]
                err = [self._download(e.path) for e in self._ordered(files, "stderr")]
                out = [self._download(e.path) for e in self._ordered(files, "stdout")]
                return key, {
                    "id": key,
                    "log": parse_executor_log(chain(err), window),
                    "gc": parse_gc_log(chain(out)),
                }, None
            except Exception as exc:
                return key, None, str(exc)[:200]

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for key, data, error in pool.map(load, keys):
                if data is not None:
                    logs["executors"][key] = data
                else:
                    logs["sources"].append({"component": f"executor {key}", "status": "unavailable", "detail": error})
        logs["sources"].append(
            {"component": "executors", "status": "ok" if logs["executors"] else "empty", "parsed": len(logs["executors"]), "found": len(by_exec), "truncated_to": max_executors if len(by_exec) > max_executors else None}
        )

        for entry in self._list_files(f"{root}/init_scripts"):
            try:
                text = list(to_lines(self._download(entry.path)))
                errors = [line.strip() for line in text if re.search(r"(?i)\b(error|failed|exception|not found)\b", line)][:5]
                logs["init_scripts"].append({"file": entry.path.rsplit("/", 1)[-1], "lines": len(text), "error_lines": errors})
            except Exception:
                continue
        return logs

    def _node_types(self) -> dict[str, dict[str, Any]]:
        if not hasattr(self, "_node_type_cache"):
            try:
                self._node_type_cache = {nt.node_type_id: nt.as_dict() for nt in (self.client.clusters.list_node_types().node_types or [])}
            except Exception:
                self._node_type_cache = {}
        return self._node_type_cache

    def get_cluster_specs(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect cluster definitions used by the run, resolved with node-type cores and memory."""
        raw: list[dict[str, Any]] = []
        for jc in run.get("job_clusters", []) or []:
            raw.append({"cluster_key": jc.get("job_cluster_key"), "new_cluster": jc.get("new_cluster") or {}})
        spec_cluster = (run.get("cluster_spec") or {}).get("new_cluster")
        if spec_cluster:
            raw.append({"cluster_key": "run_cluster_spec", "new_cluster": spec_cluster})
        for task in run.get("tasks", []) or []:
            if task.get("new_cluster"):
                raw.append({"cluster_key": task.get("task_key"), "new_cluster": task["new_cluster"]})
        if not raw:
            for cluster_id, _ in self._cluster_ids_and_log_dests(run):
                try:
                    raw.append({"cluster_id": cluster_id, "cluster_key": cluster_id, "new_cluster": self.client.clusters.get(cluster_id).as_dict()})
                except Exception:
                    continue
        node_types = self._node_types()
        for spec in raw:
            cluster = spec["new_cluster"]
            spec["node_type"] = node_types.get(cluster.get("node_type_id"), {})
            spec["driver_node_type"] = node_types.get(cluster.get("driver_node_type_id") or cluster.get("node_type_id"), {})
        return raw

    def build_stage_plans(self, tasks: list[dict[str, Any]] | None = None) -> dict[str, str]:
        stage_plans: dict[str, str] = {}
        for task in tasks or []:
            stage_id = task.get("stage_id") or task.get("stage") or task.get("stageId")
            if stage_id is None:
                continue
            for key in ("spark_plan", "physical_plan", "stage_plan", "sql_plan", "query_plan"):
                value = task.get(key)
                if isinstance(value, str) and value.strip():
                    stage_plans[str(stage_id)] = value
                    break
        return stage_plans

    def build_sql_queries(self, tasks: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        queries: list[dict[str, Any]] = []
        for task in tasks or []:
            stage_id = task.get("stage_id") or task.get("stage") or task.get("stageId")
            for key in ("sql_text", "query_text", "sql", "statement", "spark_sql"):
                value = task.get(key)
                if isinstance(value, str) and value.strip():
                    payload: dict[str, Any] = {"sql": value}
                    if stage_id is not None:
                        payload["stage_id"] = stage_id
                    queries.append(payload)
                    break
        return queries
