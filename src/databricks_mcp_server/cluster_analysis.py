from __future__ import annotations

from typing import Any

from .stage_metrics import group_by_stage, task_duration
from .units import GIB, MIB, fmt_bytes, fmt_ms, fmt_ratio

TARGET_PARTITION_BYTES = 128 * MIB


def _finding(kind: str, severity: str, message: str, evidence: str) -> dict[str, Any]:
    return {"type": kind, "severity": severity, "message": message, "evidence": evidence}


def _spark_conf_value(conf: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        if key in conf:
            return str(conf[key])
    return None


def describe_cluster_specs(cluster_specs: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Normalize job-cluster specs (new_cluster + resolved node type) into sizing facts."""
    out: list[dict[str, Any]] = []
    for spec in cluster_specs or []:
        cluster = spec.get("new_cluster") or spec.get("cluster") or {}
        node = spec.get("node_type") or {}
        driver_node = spec.get("driver_node_type") or {}
        autoscale = cluster.get("autoscale") or None
        fixed = cluster.get("num_workers")
        max_workers = autoscale.get("max_workers") if autoscale else fixed
        min_workers = autoscale.get("min_workers") if autoscale else fixed
        cores = node.get("num_cores")
        mem_gb = (node.get("memory_mb") or 0) / 1024 if node.get("memory_mb") else None
        engine = cluster.get("runtime_engine")
        out.append(
            {
                "cluster_key": spec.get("cluster_key"),
                "cluster_id": spec.get("cluster_id"),
                "spark_version": cluster.get("spark_version"),
                "runtime_engine": engine,
                "photon": bool(engine and "PHOTON" in str(engine).upper()) or "photon" in str(cluster.get("spark_version", "")).lower(),
                "node_type_id": cluster.get("node_type_id") or node.get("node_type_id"),
                "driver_node_type_id": cluster.get("driver_node_type_id") or driver_node.get("node_type_id"),
                "autoscale": bool(autoscale),
                "min_workers": min_workers,
                "max_workers": max_workers,
                "node_cores": cores,
                "node_memory_gb": round(mem_gb, 1) if mem_gb else None,
                "memory_per_core_gb": round(mem_gb / cores, 2) if mem_gb and cores else None,
                "provisioned_cores_max": cores * max_workers if cores and max_workers is not None else None,
                "provisioned_memory_gb_max": round(mem_gb * max_workers, 1) if mem_gb and max_workers is not None else None,
                "spark_conf": cluster.get("spark_conf") or {},
            }
        )
    return out


def _peak_concurrent(executors: list[dict[str, Any]], fallback: int) -> int:
    events: list[tuple[int, int]] = []
    for ex in executors:
        start = ex.get("add_time")
        if not isinstance(start, (int, float)):
            return fallback
        events.append((int(start), 1))
        end = ex.get("remove_time")
        if isinstance(end, (int, float)):
            events.append((int(end), -1))
    peak = current = 0
    for _, delta in sorted(events, key=lambda e: (e[0], e[1])):
        current += delta
        peak = max(peak, current)
    return peak or fallback


def _busy_wall_ms(stages_info: list[dict[str, Any]]) -> int | None:
    intervals = sorted(
        (s["submission_time"], s["completion_time"])
        for s in stages_info
        if isinstance(s.get("submission_time"), (int, float)) and isinstance(s.get("completion_time"), (int, float)) and s["completion_time"] >= s["submission_time"]
    )
    if not intervals:
        return None
    total = 0
    cur_start, cur_end = intervals[0]
    for start, end in intervals[1:]:
        if start > cur_end:
            total += cur_end - cur_start
            cur_start, cur_end = start, end
        else:
            cur_end = max(cur_end, end)
    return int(total + cur_end - cur_start)


def analyze_cluster(
    run: dict[str, Any],
    tasks: list[dict[str, Any]],
    stage_metrics: list[dict[str, Any]],
    executors: list[dict[str, Any]] | None = None,
    spark_conf: dict[str, Any] | None = None,
    cluster_specs: list[dict[str, Any]] | None = None,
    stages_info: list[dict[str, Any]] | None = None,
    log_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare cluster size (cores, memory, workers) with observed data volume, CPU use and memory pressure."""
    conf = spark_conf or {}
    specs = describe_cluster_specs(cluster_specs)
    workers = [e for e in (executors or []) if str(e.get("id")) != "driver"]
    findings: list[dict[str, Any]] = []

    cores_source = "executors"
    total_cores = sum(int(e.get("cores") or 0) for e in workers)
    if not total_cores:
        cores_source = "cluster_spec"
        total_cores = max((s["provisioned_cores_max"] or 0 for s in specs), default=0)
    if not total_cores:
        cores_source = "unknown"

    executor_count = len(workers) or max((s["max_workers"] or 0 for s in specs), default=0)
    peak_executors = _peak_concurrent(workers, executor_count) if workers else executor_count
    cores_per_exec = round(total_cores / len(workers), 1) if workers and total_cores else None
    storage_mem = [e.get("max_memory_bytes") for e in workers if e.get("max_memory_bytes")]
    storage_total = sum(storage_mem)
    node_mem_gb = next((s["node_memory_gb"] for s in specs if s["node_memory_gb"]), None)
    executor_memory_conf = _spark_conf_value(conf, "spark.executor.memory")

    wall_ms = _busy_wall_ms(stages_info or [])
    wall_source = "stage_intervals"
    if wall_ms is None:
        wall_ms = run.get("execution_duration") or run.get("run_duration")
        wall_source = "run_duration" if wall_ms else "unknown"

    total_run = sum(m["total_task_time_ms"] for m in stage_metrics)
    total_cpu = sum(m["cpu_time_ms"] for m in stage_metrics)
    input_bytes = sum(m["input_bytes"] for m in stage_metrics)
    shuffle_write = sum(m["shuffle_write_bytes"] for m in stage_metrics)
    spill_disk = sum(m["spill"]["disk_bytes"] for m in stage_metrics)
    spill_mem = sum(m["spill"]["memory_bytes"] for m in stage_metrics)
    gc_ms = sum(m["gc_time_ms"] for m in stage_metrics)
    gc_ratio = gc_ms / total_run if total_run else None

    slot_util = (total_run / (total_cores * wall_ms)) if total_cores and wall_ms and total_run else None
    cpu_util = (total_cpu / (total_cores * wall_ms)) if total_cores and wall_ms and total_cpu else None

    task_counts = [m["task_count"] for m in stage_metrics if m["total_task_time_ms"] > 0]
    weighted_fill = None
    if total_cores and stage_metrics:
        weight = sum(m["total_task_time_ms"] for m in stage_metrics)
        if weight:
            weighted_fill = sum(min(1.0, m["task_count"] / total_cores) * m["total_task_time_ms"] for m in stage_metrics) / weight

    per_exec: dict[Any, dict[str, float]] = {}
    for task in tasks:
        eid = task.get("executor_id")
        if eid is None:
            continue
        slot = per_exec.setdefault(eid, {"tasks": 0, "run_ms": 0.0})
        slot["tasks"] += 1
        slot["run_ms"] += task_duration(task)
    imbalance = None
    if len(per_exec) >= 4:
        loads = [v["run_ms"] for v in per_exec.values()]
        mean = sum(loads) / len(loads)
        imbalance = round(max(loads) / mean, 2) if mean else None

    ideal_tasks = [max(m["input_bytes"], m["shuffle_read_bytes"]) / TARGET_PARTITION_BYTES for m in stage_metrics]
    peak_ideal = int(max(ideal_tasks, default=0))
    max_stage_tasks = max(task_counts, default=0)
    p95_stage_tasks = int(sorted(task_counts)[int(0.95 * (len(task_counts) - 1))]) if task_counts else 0

    memory_per_core_gb = None
    if node_mem_gb and total_cores and executor_count:
        node_cores = next((s["node_cores"] for s in specs if s["node_cores"]), None)
        memory_per_core_gb = round(node_mem_gb / node_cores, 2) if node_cores else None
    elif storage_total and total_cores:
        memory_per_core_gb = round(storage_total / GIB / total_cores, 2)

    autoscale = any(s["autoscale"] for s in specs)
    photon = any(s["photon"] for s in specs)

    if slot_util is not None and wall_ms >= 60_000:
        if slot_util < 0.2 and total_cores >= 16 and p95_stage_tasks < total_cores:
            findings.append(
                _finding(
                    "cluster_overprovisioned",
                    "medium",
                    "The cluster is much larger than the work needs.",
                    f"{total_cores} cores but task slot utilization is {fmt_ratio(slot_util)}; 95% of stages have at most {p95_stage_tasks} tasks "
                    f"(max {max_stage_tasks}). Cores beyond ~{max(p95_stage_tasks, 1)} are mostly idle.",
                )
            )
        elif slot_util < 0.35:
            findings.append(
                _finding(
                    "cluster_underutilized",
                    "medium",
                    "Cores are mostly idle during the busy period.",
                    f"Slot utilization {fmt_ratio(slot_util)} over {fmt_ms(wall_ms)} on {total_cores} cores; "
                    f"{fmt_ratio(weighted_fill)} of stage time is spent in stages that can fill the cluster.",
                )
            )
        if slot_util < 0.5 and specs and not autoscale:
            findings.append(
                _finding(
                    "fixed_size_cluster",
                    "low",
                    "Fixed-size cluster with low utilization.",
                    f"{specs[0]['max_workers']} fixed workers, slot utilization {fmt_ratio(slot_util)}.",
                )
            )

    mem_pressure = spill_disk >= GIB or (gc_ratio is not None and gc_ratio >= 0.15)
    if mem_pressure:
        detail = f"disk spill {fmt_bytes(spill_disk)}, memory spill {fmt_bytes(spill_mem)}, GC {fmt_ratio(gc_ratio)} of task time"
        if memory_per_core_gb is not None:
            detail += f"; node memory per core {memory_per_core_gb} GB"
        findings.append(
            _finding(
                "cluster_memory_pressure",
                "high" if spill_disk >= 20 * GIB or (gc_ratio or 0) >= 0.25 else "medium",
                "Executors are short of memory for this workload.",
                detail,
            )
        )

    if imbalance is not None and imbalance >= 1.5:
        findings.append(_finding("executor_imbalance", "medium", "Executor load is uneven.", f"Busiest executor ran {imbalance}x the mean task time across {len(per_exec)} executors."))

    if specs and not photon:
        findings.append(_finding("photon_not_enabled", "low", "Photon is not enabled on the job cluster.", f"runtime_engine={specs[0]['runtime_engine']}, spark_version={specs[0]['spark_version']}"))

    shuffle_parts = _spark_conf_value(conf, "spark.sql.shuffle.partitions")
    aqe = (_spark_conf_value(conf, "spark.sql.adaptive.enabled") or "true").lower() == "true"
    if shuffle_parts and shuffle_parts.isdigit() and total_cores and not aqe and int(shuffle_parts) < total_cores:
        findings.append(_finding("shuffle_partitions_mismatch", "medium", "Fewer shuffle partitions than cores with AQE disabled.", f"spark.sql.shuffle.partitions={shuffle_parts} for {total_cores} cores."))

    recommendations: list[str] = []
    if peak_ideal and total_cores:
        recommendations.append(
            f"Largest stage handles {fmt_bytes(max(m['input_bytes'] for m in stage_metrics))} input / "
            f"{fmt_bytes(max(m['shuffle_read_bytes'] for m in stage_metrics))} shuffle read: ~{peak_ideal} tasks at 128 MiB each; "
            f"cluster has {total_cores} cores ({peak_ideal / total_cores:.1f} waves)."
        )
    if slot_util is not None and slot_util < 0.35 and total_cores:
        target = max(int(p95_stage_tasks), 8)
        recommendations.append(f"Right-size to roughly {target}-{max(target, int(max_stage_tasks * 0.5))} total cores (p95 stage parallelism is {p95_stage_tasks}).")
    if mem_pressure:
        recommendations.append("Move to memory-optimized nodes (more GB per core) or raise partition counts to shrink per-task memory.")

    jvm = (log_summary or {}).get("jvm") or {}
    heap = jvm.get("executor_heap_bytes")
    heap_info = {}
    if heap:
        node_cores = next((s["node_cores"] for s in specs if s["node_cores"]), None)
        heap_info = {
            "executor_heap": fmt_bytes(heap),
            "heap_per_core": fmt_bytes(heap / (cores_per_exec or node_cores)) if (cores_per_exec or node_cores) else None,
            "heap_share_of_node_memory": round(heap / GIB / node_mem_gb, 2) if node_mem_gb else None,
            "driver_heap": fmt_bytes(jvm.get("driver_heap_bytes")) if jvm.get("driver_heap_bytes") else None,
            "max_concurrent_queries": ((log_summary or {}).get("concurrency") or {}).get("max_concurrent_queries"),
        }
        per_core = heap / (cores_per_exec or node_cores) if (cores_per_exec or node_cores) else None
        if per_core is not None and per_core < 2 * GIB and mem_pressure:
            recommendations.append(f"Executor heap is only {fmt_bytes(per_core)} per core; move to memory-optimized nodes or fewer cores per executor.")

    return {
        "clusters": [{k: v for k, v in s.items() if k != "spark_conf"} for s in specs],
        "jvm": heap_info or None,
        "observed": {
            "source_of_cores": cores_source,
            "executor_count": executor_count,
            "peak_concurrent_executors": peak_executors,
            "total_cores": total_cores,
            "cores_per_executor": cores_per_exec,
            "executor_storage_memory_total": fmt_bytes(storage_total) if storage_total else None,
            "executor_memory_conf": executor_memory_conf,
            "node_memory_gb": node_mem_gb,
            "memory_per_core_gb": memory_per_core_gb,
            "photon": photon if specs else None,
            "autoscale": autoscale if specs else None,
        },
        "utilization": {
            "busy_wall_time": fmt_ms(wall_ms) if wall_ms else None,
            "wall_time_source": wall_source,
            "total_task_time": fmt_ms(total_run),
            "task_slot_utilization": round(slot_util, 4) if slot_util is not None else None,
            "cpu_utilization": round(cpu_util, 4) if cpu_util is not None else None,
            "cpu_efficiency": round(total_cpu / total_run, 4) if total_run and total_cpu else None,
            "stage_time_weighted_core_fill": round(weighted_fill, 4) if weighted_fill is not None else None,
            "gc_ratio": round(gc_ratio, 4) if gc_ratio is not None else None,
            "executor_load_imbalance": imbalance,
        },
        "data_vs_cluster": {
            "input_bytes": fmt_bytes(input_bytes),
            "shuffle_write_bytes": fmt_bytes(shuffle_write),
            "spill_disk_bytes": fmt_bytes(spill_disk),
            "input_per_core": fmt_bytes(input_bytes / total_cores) if total_cores else None,
            "shuffle_per_core": fmt_bytes(shuffle_write / total_cores) if total_cores else None,
            "input_to_executor_storage_memory": round(input_bytes / storage_total, 2) if storage_total else None,
            "stage_parallelism": {"p95_tasks_per_stage": p95_stage_tasks, "max_tasks_per_stage": max_stage_tasks, "ideal_tasks_largest_stage_at_128MiB": peak_ideal},
        },
        "findings": findings,
        "recommendations": recommendations,
    }
