from __future__ import annotations

import gzip
import io
import json
from typing import Any, Iterable

from .units import parse_spark_time

SPARK_UI_PORT = 40001
SQL_START = "SparkListenerSQLExecutionStart"
CONF_PREFIXES = ("spark.sql.", "spark.executor.", "spark.driver.", "spark.default.", "spark.databricks.", "spark.dynamicAllocation.", "spark.shuffle.", "spark.memory.")


def _event_name(event: dict[str, Any]) -> str:
    return str(event.get("Event", "")).rsplit(".", 1)[-1]


def _filter_conf(items: Iterable[Any]) -> dict[str, str]:
    conf: dict[str, str] = {}
    for item in items:
        key, value = (item if isinstance(item, (list, tuple)) and len(item) == 2 else (None, None))
        if isinstance(key, str) and key.startswith(CONF_PREFIXES):
            conf[key] = str(value)
    return conf


def _task_row(stage_id: Any, attempt: Any, task_id: Any, metrics: dict[str, Any], duration: Any = None, executor_id: Any = None) -> dict[str, Any]:
    input_m = metrics.get("Input Metrics", {})
    output_m = metrics.get("Output Metrics", {})
    shuffle_r = metrics.get("Shuffle Read Metrics", {})
    shuffle_w = metrics.get("Shuffle Write Metrics", {})
    return {
        "stage_id": stage_id,
        "stage_attempt": attempt,
        "task_id": task_id,
        "executor_id": executor_id,
        "duration": duration,
        "executor_run_time": metrics.get("Executor Run Time", duration or 0),
        "executor_cpu_time_ns": metrics.get("Executor CPU Time", 0),
        "executor_deserialize_time": metrics.get("Executor Deserialize Time", 0),
        "peak_execution_memory": metrics.get("Peak Execution Memory", 0),
        "result_size": metrics.get("Result Size", 0),
        "records_read": input_m.get("Records Read", 0) + shuffle_r.get("Total Records Read", 0),
        "records_written": output_m.get("Records Written", 0),
        "input_bytes": input_m.get("Bytes Read", 0),
        "output_bytes": output_m.get("Bytes Written", 0),
        "shuffle_read_bytes": shuffle_r.get("Remote Bytes Read", 0) + shuffle_r.get("Local Bytes Read", 0),
        "shuffle_write_bytes": shuffle_w.get("Shuffle Bytes Written", 0),
        "shuffle_fetch_wait_ms": shuffle_r.get("Fetch Wait Time", 0),
        "memory_bytes_spilled": metrics.get("Memory Bytes Spilled", 0),
        "disk_bytes_spilled": metrics.get("Disk Bytes Spilled", 0),
        "gc_time": metrics.get("JVM GC Time", 0),
    }


def parse_event_log(lines: Iterable[str]) -> dict[str, Any]:
    """Extract task metrics, stage info, executors, Spark conf, stage-to-SQL mapping and plans from Spark event log lines."""
    tasks: list[dict[str, Any]] = []
    sql: dict[int, dict[str, Any]] = {}
    stage_to_sql: dict[int, int] = {}
    executors: dict[str, dict[str, Any]] = {}
    stages: list[dict[str, Any]] = []
    conf: dict[str, str] = {}

    for line in lines:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        name = _event_name(event)
        if name == "SparkListenerTaskEnd":
            info = event.get("Task Info", {})
            metrics = event.get("Task Metrics") or {}
            duration = (info.get("Finish Time", 0) or 0) - (info.get("Launch Time", 0) or 0)
            tasks.append(_task_row(event.get("Stage ID"), event.get("Stage Attempt ID"), info.get("Task ID"), metrics, duration, info.get("Executor ID")))
        elif name == SQL_START:
            sql[event["executionId"]] = {
                "execution_id": event["executionId"],
                "description": event.get("description"),
                "plan": event.get("physicalPlanDescription"),
            }
        elif name == "SparkListenerJobStart":
            exec_id = (event.get("Properties") or {}).get("spark.sql.execution.id")
            if exec_id is not None:
                for stage_id in event.get("Stage IDs", []):
                    stage_to_sql[int(stage_id)] = int(exec_id)
        elif name == "SparkListenerExecutorAdded":
            eid = str(event.get("Executor ID"))
            info = event.get("Executor Info", {})
            executors.setdefault(eid, {"id": eid}).update({"cores": info.get("Total Cores"), "host": info.get("Host"), "add_time": event.get("Timestamp")})
        elif name == "SparkListenerExecutorRemoved":
            executors.setdefault(str(event.get("Executor ID")), {"id": str(event.get("Executor ID"))})["remove_time"] = event.get("Timestamp")
        elif name == "SparkListenerBlockManagerAdded":
            eid = str((event.get("Block Manager ID") or {}).get("Executor ID"))
            executors.setdefault(eid, {"id": eid})["max_memory_bytes"] = event.get("Maximum Memory")
        elif name == "SparkListenerEnvironmentUpdate":
            conf.update(_filter_conf((event.get("Spark Properties") or {}).items()))
        elif name == "SparkListenerStageCompleted":
            info = event.get("Stage Info", {})
            stages.append(
                {
                    "stage_id": info.get("Stage ID"),
                    "attempt": info.get("Stage Attempt ID"),
                    "name": info.get("Stage Name"),
                    "num_tasks": info.get("Number of Tasks"),
                    "submission_time": info.get("Submission Time"),
                    "completion_time": info.get("Completion Time"),
                }
            )

    return _finalize(tasks, sql, stage_to_sql, list(executors.values()), stages, conf)


def _finalize(
    tasks: list[dict[str, Any]],
    sql: dict[int, dict[str, Any]],
    stage_to_sql: dict[int, int],
    executors: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    conf: dict[str, str] | None = None,
) -> dict[str, Any]:
    stage_plans: dict[str, str] = {}
    sql_queries: list[dict[str, Any]] = []
    for stage_id, exec_id in stage_to_sql.items():
        execution = sql.get(exec_id)
        if execution and execution.get("plan"):
            stage_plans[str(stage_id)] = execution["plan"]
    for execution in sql.values():
        sql_queries.append({k: v for k, v in execution.items() if v is not None})
    return {
        "tasks": tasks,
        "stage_plans": stage_plans,
        "sql_queries": sql_queries,
        "executors": executors or [],
        "stages": stages or [],
        "spark_conf": conf or {},
    }


def read_text_lines(raw: bytes, path: str) -> list[str]:
    if path.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return io.StringIO(raw.decode("utf-8", errors="replace")).readlines()


def _get(d: dict[str, Any] | None, key: str) -> Any:
    return (d or {}).get(key) or 0


def from_rest(api_get: Any, base: str) -> dict[str, Any]:
    """Build metrics from a live Spark UI REST API. api_get(path, query=None) returns parsed JSON."""
    apps = api_get(f"{base}/applications")
    if not apps:
        return {"tasks": [], "stage_plans": {}, "sql_queries": [], "executors": [], "stages": [], "spark_conf": {}}
    app_id = apps[0]["id"]
    root = f"{base}/applications/{app_id}"

    tasks: list[dict[str, Any]] = []
    stages: list[dict[str, Any]] = []
    for stage in api_get(f"{root}/stages", {"status": "complete"}) or []:
        sid, attempt = stage["stageId"], stage["attemptId"]
        stages.append(
            {
                "stage_id": sid,
                "attempt": attempt,
                "name": stage.get("name"),
                "num_tasks": stage.get("numTasks"),
                "submission_time": parse_spark_time(stage.get("submissionTime")),
                "completion_time": parse_spark_time(stage.get("completionTime")),
            }
        )
        offset, page = 0, 10000
        while True:
            task_list = api_get(f"{root}/stages/{sid}/{attempt}/taskList", {"offset": offset, "length": page}) or []
            for t in task_list:
                m = t.get("taskMetrics") or {}
                inp, out = m.get("inputMetrics") or {}, m.get("outputMetrics") or {}
                sr, sw = m.get("shuffleReadMetrics") or {}, m.get("shuffleWriteMetrics") or {}
                tasks.append(
                    {
                        "stage_id": sid,
                        "stage_attempt": attempt,
                        "task_id": t.get("taskId"),
                        "executor_id": t.get("executorId"),
                        "duration": t.get("duration"),
                        "executor_run_time": m.get("executorRunTime", t.get("duration", 0)),
                        "executor_cpu_time_ns": m.get("executorCpuTime", 0),
                        "executor_deserialize_time": m.get("executorDeserializeTime", 0),
                        "peak_execution_memory": m.get("peakExecutionMemory", 0),
                        "result_size": m.get("resultSize", 0),
                        "records_read": _get(inp, "recordsRead") + _get(sr, "recordsRead"),
                        "records_written": _get(out, "recordsWritten"),
                        "input_bytes": _get(inp, "bytesRead"),
                        "output_bytes": _get(out, "bytesWritten"),
                        "shuffle_read_bytes": _get(sr, "remoteBytesRead") + _get(sr, "localBytesRead"),
                        "shuffle_write_bytes": _get(sw, "bytesWritten"),
                        "shuffle_fetch_wait_ms": _get(sr, "fetchWaitTime"),
                        "memory_bytes_spilled": m.get("memoryBytesSpilled", 0),
                        "disk_bytes_spilled": m.get("diskBytesSpilled", 0),
                        "gc_time": m.get("jvmGcTime", 0),
                    }
                )
            if len(task_list) < page:
                break
            offset += page

    executors: list[dict[str, Any]] = []
    try:
        for ex in api_get(f"{root}/allexecutors") or []:
            executors.append(
                {
                    "id": ex.get("id"),
                    "cores": ex.get("totalCores"),
                    "host": ex.get("hostPort"),
                    "max_memory_bytes": ex.get("maxMemory"),
                    "add_time": parse_spark_time(ex.get("addTime")),
                    "remove_time": parse_spark_time(ex.get("removeTime")),
                    "total_gc_time_ms": ex.get("totalGCTime"),
                    "peak_jvm_heap_bytes": (ex.get("peakMemoryMetrics") or {}).get("JVMHeapMemory"),
                }
            )
    except Exception:
        pass

    conf: dict[str, str] = {}
    try:
        conf = _filter_conf((api_get(f"{root}/environment") or {}).get("sparkProperties", []))
    except Exception:
        pass

    sql: dict[int, dict[str, Any]] = {}
    job_to_sql: dict[int, int] = {}
    for execution in api_get(f"{root}/sql", {"details": "true", "planDescription": "true", "length": 1000}) or []:
        exec_id = execution["id"]
        sql[exec_id] = {
            "execution_id": exec_id,
            "description": execution.get("description"),
            "plan": execution.get("planDescription"),
            "nodes": execution.get("nodes"),
        }
        for key in ("successJobIds", "failedJobIds", "runningJobIds"):
            for job_id in execution.get(key, []):
                job_to_sql[int(job_id)] = int(exec_id)

    stage_to_sql: dict[int, int] = {}
    if sql:
        for job in api_get(f"{root}/jobs") or []:
            exec_id = job_to_sql.get(int(job["jobId"]))
            if exec_id is not None:
                for sid in job.get("stageIds", []):
                    stage_to_sql[int(sid)] = exec_id
    return _finalize(tasks, sql, stage_to_sql, executors, stages, conf)
