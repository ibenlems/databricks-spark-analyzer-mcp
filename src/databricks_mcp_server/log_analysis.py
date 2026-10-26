from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .cluster_logs import summarize_file_reads
from .units import GIB, MIB, fmt_bytes, fmt_ms, percentile

BENIGN = [
    ("SQLConf", r"deprecated"),
    ("BlockManager", r"Asked to remove block .* does not exist"),
    ("ReplAwareSparkDataSourceListener", r"."),
    ("StreamingQueryListenerBus", r"Failed to capture UCS handle"),
    ("SparkConfUtils", r"Setting the same key twice"),
    ("DisabledDatabricksFileSystem", r"."),
    ("config", r"Weak cipher suite"),
    ("WsfsHttpClient", r"does not exist"),
    ("ThrottledLogger", r"REGION environment"),
    ("NativeMemoryWatchdog", r"."),
    ("JfrStreamingManager", r"."),
    ("FallbackEncryptionContextProvider", r"."),
    ("DriverDaemon", r"ShouldUseAutoscalingInfo"),
    ("AbfsClient", r"_encryption_meta"),
    ("TransportChannelHandler", r"Connection reset by peer"),
    ("DBCEventLoggingListener", r"."),
    ("TaskSchedulerImpl", r"Lost executor .*cluster termination"),
]
BENIGN_EXECUTOR_LOSS = ("cluster termination", "Driver commanded a shutdown", "Command exited with code 0")
_THROTTLE = re.compile(r"\b(429|503|ThrottlingException|ServerBusy|SlowDown|TooManyRequests)\b")


def fmt_ts(ms: int | None) -> str:
    if ms is None:
        return "n/a"
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S UTC")


def is_benign(event: dict[str, Any]) -> bool:
    return any(event["logger"].startswith(logger) and re.search(pattern, event["message"]) for logger, pattern in BENIGN)


def _finding(kind: str, severity: str, message: str, evidence: str) -> dict[str, Any]:
    return {"type": kind, "severity": severity, "message": message, "evidence": evidence}


def overlapping_stages(timestamps: list[int | None], stages_info: list[dict[str, Any]], top: int = 5) -> list[str]:
    windows = [
        (s["submission_time"], s["completion_time"], str(s["stage_id"]))
        for s in stages_info
        if isinstance(s.get("submission_time"), (int, float)) and isinstance(s.get("completion_time"), (int, float))
    ]
    counts: Counter[str] = Counter()
    for ts in timestamps:
        if ts is None:
            continue
        for start, end, sid in windows:
            if start - 1000 <= ts <= end + 1000:
                counts[sid] += 1
    return [sid for sid, _ in counts.most_common(top)]


def _merge_executors(executors: dict[str, dict[str, Any]]) -> dict[str, Any]:
    file_bytes: dict[str, int] = {}
    file_end: dict[str, int] = {}
    counts: Counter[str] = Counter()
    fetch_ms: list[int] = []
    fetch = {"events": 0, "blocks": 0, "bytes": 0, "remote_blocks": 0, "remote_bytes": 0, "slow": []}
    spill = {"count": 0, "rows": Counter(), "timestamps": []}
    events: dict[Any, dict[str, Any]] = {}
    totals = {"leaks": 0, "no_space": 0, "dropped": 0, "oom": 0, "task_exceptions": 0, "broadcast_ms": 0, "broadcast_reads": 0}
    oom_samples: list[str] = []
    for ex in executors.values():
        log = ex.get("log") or {}
        for path, size in log.get("file_bytes", {}).items():
            file_bytes[path] = file_bytes.get(path, 0) + size
            file_end[path] = max(file_end.get(path, 0), log["file_end"].get(path, 0))
        counts.update(log.get("file_read_counts", {}))
        f = log.get("fetch") or {}
        for k in ("events", "blocks", "bytes", "remote_blocks", "remote_bytes"):
            fetch[k] += f.get(k, 0)
        fetch_ms.extend(f.get("fetch_ms", []))
        fetch["slow"].extend(f.get("slow", []))
        s = log.get("spill_threshold") or {}
        spill["count"] += s.get("count", 0)
        spill["rows"].update(s.get("rows", {}))
        spill["timestamps"].extend(s.get("timestamps", []))
        for key, event in (log.get("events") or {}).items():
            merged = events.setdefault(key, {**event, "count": 0, "executors": set()})
            merged["count"] += event["count"]
            merged["first_ts"] = min(merged["first_ts"], event["first_ts"] or merged["first_ts"])
            merged["last_ts"] = max(merged["last_ts"], event["last_ts"] or merged["last_ts"])
            merged["executors"].add(ex.get("id"))
        totals["leaks"] += log.get("memory_leaks", 0)
        totals["no_space"] += (log.get("cache") or {}).get("no_space", 0)
        totals["dropped"] += (log.get("cache") or {}).get("dropped", 0)
        totals["oom"] += (log.get("oom") or {}).get("count", 0)
        oom_samples.extend((log.get("oom") or {}).get("samples", []))
        totals["task_exceptions"] += log.get("task_exceptions", 0)
        totals["broadcast_ms"] += sum(log.get("broadcast_read_ms", []))
        totals["broadcast_reads"] += len(log.get("broadcast_read_ms", []))
    return {
        "file_bytes": file_bytes, "file_end": file_end, "counts": counts, "fetch": fetch, "fetch_ms": fetch_ms,
        "spill": spill, "events": events, "totals": totals, "oom_samples": oom_samples,
        "files_truncated": any((ex.get("log") or {}).get("file_tracking_truncated") for ex in executors.values()),
    }


def _gc_brief(gc: dict[str, Any] | None) -> dict[str, Any] | None:
    if not gc or not gc.get("events"):
        return None
    return {
        "gc_events": gc["events"], "full_gc": gc["full_gc"], "full_gc_excluding_metadata": gc["full_gc_excluding_metadata"],
        "total_pause": fmt_ms(gc["total_pause_ms"]), "max_pause": fmt_ms(gc["max_pause_ms"]), "pause_fraction": gc["pause_fraction"],
        "heap_total": fmt_bytes(gc["heap_total_bytes"]), "heap_after_gc_peak": fmt_bytes(gc["heap_after_gc_peak_bytes"]),
        "heap_after_gc_peak_ratio": gc["heap_after_gc_peak_ratio"], "old_gen_after_full_gc_max_ratio": gc["old_gen_after_full_gc_max_ratio"],
        "collector": next((name for name, flag in (("ParallelGC", "UseParallelGC"), ("G1", "UseG1GC"), ("CMS", "UseConcMarkSweepGC")) if (gc.get("jvm_flags") or {}).get(flag) == "true"), None),
    }


def analyze_logs(
    logs: dict[str, Any] | None,
    stages_info: list[dict[str, Any]] | None = None,
    stage_plan_ids: set[str] | None = None,
    all_stage_ids: set[str] | None = None,
    total_cores: int | None = None,
) -> dict[str, Any] | None:
    """Turn parsed driver/executor cluster logs into a summary and findings (each tied to overlapping stages)."""
    if not logs or not (logs.get("driver") or logs.get("executors")):
        return None
    stages_info = stages_info or []
    driver = (logs.get("driver") or {}).get("log") or {}
    driver_gc = (logs.get("driver") or {}).get("gc")
    driver_err = (logs.get("driver") or {}).get("stderr") or {}
    executors = logs.get("executors") or {}
    merged = _merge_executors(executors)
    findings: list[tuple[dict[str, Any], list[str]]] = []
    notes: list[str] = []

    # --- tables read (from executor FileScanRDD lines) ---
    tables = summarize_file_reads(merged["file_bytes"], merged["file_end"], merged["counts"])
    small = [t for t in tables if t["data_files"] >= 100 and t.get("files_under_16MiB", 0) / t["data_files"] >= 0.8]
    if small:
        worst = max(t["files_under_16MiB"] for t in small)
        findings.append(
            (
                _finding(
                    "small_files",
                    "high" if worst >= 1000 else "medium",
                    "Tables are made of thousands of tiny files; every file costs an open/listing round trip and a scan task slot.",
                    "; ".join(
                        f"{t['table']}: {t['data_files']:,} files, median {fmt_bytes(t['file_size_p50'])}, "
                        f"{t['files_under_16MiB'] / t['data_files']:.0%} under 16 MiB, {fmt_bytes(t['unique_data_bytes'])} total"
                        for t in sorted(small, key=lambda t: -t["data_files"])[:6]
                    ),
                ),
                [],
            )
        )
    repeated = [t for t in tables if (t.get("read_amplification") or 0) >= 3 and t["data_bytes_read"] >= 5 * GIB]
    if repeated:
        worst = max(t["data_bytes_read"] for t in repeated)
        findings.append(
            (
                _finding(
                    "repeated_scans",
                    "high" if worst >= 100 * GIB else "medium",
                    "The same table files are scanned many times within the run.",
                    "; ".join(
                        f"{t['table']}: {fmt_bytes(t['unique_data_bytes'])} of data read {t['read_amplification']}x = {fmt_bytes(t['data_bytes_read'])} total"
                        for t in sorted(repeated, key=lambda t: -t["data_bytes_read"])[:5]
                    ),
                ),
                [],
            )
        )
    delta_heavy = [t for t in tables if t["delta_log_files"] and t["delta_log_bytes_read"] >= 256 * MIB]
    if delta_heavy:
        findings.append(
            (
                _finding(
                    "delta_log_overhead",
                    "low",
                    "Large Delta transaction-log reads.",
                    "; ".join(f"{t['table']}: {t['delta_log_files']} log files, {fmt_bytes(t['delta_log_bytes_read'])}" for t in delta_heavy[:5]),
                ),
                [],
            )
        )

    # --- window / single partition ---
    win = driver.get("window_single_partition") or {}
    if win.get("count"):
        ts = win["timestamps"]
        stages = overlapping_stages(ts, stages_info)
        findings.append(
            (
                _finding(
                    "window_no_partition",
                    "high",
                    "Spark logged that a window operation has no PARTITION BY and moves all data to a single partition.",
                    f"{win['count']} RunningWindowFunctionExec warnings between {fmt_ts(min(ts))} and {fmt_ts(max(ts))}; "
                    f"stages running then: {', '.join(stages) or 'unknown'}.",
                ),
                stages,
            )
        )

    # --- executors lost / task failures / OOM ---
    lost = [e for e in driver.get("lost_executors", []) if not any(b in e["reason"] for b in BENIGN_EXECUTOR_LOSS)]
    if lost:
        findings.append(
            (
                _finding(
                    "executor_lost",
                    "high",
                    "Executors were lost during the run (spot eviction, OOM kill or node failure).",
                    "; ".join(f"executor {e['executor']} on {e['host']} at {fmt_ts(e['ts'])}: {e['reason']}" for e in lost[:5]),
                ),
                overlapping_stages([e["ts"] for e in lost], stages_info),
            )
        )
    lt = driver.get("lost_tasks") or {}
    failures = lt.get("count", 0) + len(driver.get("task_set_failures", [])) + driver.get("fetch_failed", 0)
    if failures:
        top = ", ".join(f"{r} x{c}" for r, c in (lt.get("reasons") or Counter()).most_common(3))
        findings.append(
            (
                _finding(
                    "task_failures",
                    "high" if driver.get("task_set_failures") else "medium",
                    "Tasks failed and were retried.",
                    f"{lt.get('count', 0)} lost tasks, {len(driver.get('task_set_failures', []))} task-set failures, {driver.get('fetch_failed', 0)} FetchFailed. {top}",
                ),
                overlapping_stages(lt.get("timestamps", []), stages_info),
            )
        )
    oom = (driver.get("oom") or {}).get("count", 0) + merged["totals"]["oom"] + driver_err.get("oom_lines", 0)
    if oom:
        samples = ((driver.get("oom") or {}).get("samples", []) + merged["oom_samples"] + driver_err.get("oom_samples", []))[:3]
        findings.append((_finding("oom_errors", "high", "Out-of-memory errors appear in the logs.", f"{oom} lines. e.g. {' | '.join(samples)}"), overlapping_stages((driver.get("oom") or {}).get("timestamps", []), stages_info)))

    # --- event log completeness ---
    dropped = driver.get("event_log_dropped") or {}
    if dropped.get("count"):
        missing = sorted((all_stage_ids or set()) - (stage_plan_ids or set()), key=lambda s: int(s) if s.isdigit() else 0)
        events = ", ".join(f"{k} x{v}" for k, v in (dropped.get("listener_events") or {}).items())
        findings.append(
            (
                _finding(
                    "event_log_incomplete",
                    "medium",
                    "Some Spark events were too large for the event log and were dropped, so query plans are missing for part of the run.",
                    f"{dropped['count']} DBCEventLoggingListener write errors (events up to {fmt_bytes(dropped['max_bytes'])} vs the 2 MiB limit; {events}); "
                    f"{len(missing)} of {len(all_stage_ids or [])} stages have no query plan.",
                ),
                [],
            )
        )

    # --- GC / memory ---
    exec_gc = {eid: ex.get("gc") for eid, ex in executors.items() if ex.get("gc") and ex["gc"].get("events")}
    worst_eid, worst_gc = max(exec_gc.items(), key=lambda kv: kv[1].get("pause_fraction") or 0, default=(None, None))
    if worst_gc:
        frac = worst_gc.get("pause_fraction") or 0
        old = worst_gc.get("old_gen_after_full_gc_max_ratio") or 0
        total_full = sum(g["full_gc_excluding_metadata"] for g in exec_gc.values())
        if frac >= 0.1 or old >= 0.85:
            sev, kind = "medium", True
        elif total_full >= 20:
            sev, kind = "low", True
        else:
            sev, kind = "low", False
        if kind:
            findings.append(
                (
                    _finding(
                        "jvm_gc_pressure",
                        sev,
                        "Executor JVMs spend time in full garbage collections.",
                        f"{total_full} non-startup Full GCs across {len(exec_gc)} executors; worst executor {worst_eid}: pauses {fmt_ms(worst_gc['total_pause_ms'])} "
                        f"= {frac:.1%} of its lifetime (max pause {fmt_ms(worst_gc['max_pause_ms'])}), heap after GC peaked at {worst_gc['heap_after_gc_peak_ratio']:.0%} of "
                        f"{fmt_bytes(worst_gc['heap_total_bytes'])}, old gen after Full GC up to {old:.0%}.",
                    ),
                    [],
                )
            )
    if driver_gc and driver_gc.get("events"):
        frac = driver_gc.get("pause_fraction") or 0
        ratio = driver_gc.get("heap_after_gc_peak_ratio") or 0
        if frac >= 0.05 or ratio >= 0.85 or driver_gc["full_gc_excluding_metadata"] >= 5:
            findings.append(
                (
                    _finding(
                        "driver_memory_pressure",
                        "medium",
                        "The driver JVM is under memory pressure.",
                        f"GC pauses {frac:.1%} of driver lifetime, {driver_gc['full_gc_excluding_metadata']} Full GCs, heap after GC peaked at {ratio:.0%} of {fmt_bytes(driver_gc['heap_total_bytes'])}.",
                    ),
                    [],
                )
            )
    if merged["totals"]["leaks"]:
        findings.append((_finding("memory_leak", "medium", "Spark reported managed-memory leaks in tasks.", f"{merged['totals']['leaks']} 'Managed memory leak detected' warnings."), []))
    cache = driver.get("cache") or {}
    if cache.get("no_space") or cache.get("dropped") or merged["totals"]["no_space"] or merged["totals"]["dropped"]:
        findings.append(
            (
                _finding(
                    "cache_pressure",
                    "medium",
                    "Cached blocks did not fit in storage memory or were evicted.",
                    f"{cache.get('no_space', 0) + merged['totals']['no_space']} 'not enough space to cache', {cache.get('dropped', 0) + merged['totals']['dropped']} blocks dropped; "
                    f"min free storage memory {fmt_bytes(cache.get('min_free_bytes'))}.",
                ),
                [],
            )
        )

    # --- cached block loss (benign when it coincides with cluster termination) ---
    loss = driver.get("cache_block_loss") or {}
    term = [e["ts"] for e in driver.get("lost_executors", []) if "termination" in e["reason"]]
    if loss.get("count"):
        premature = [t for t in loss["timestamps"] if not term or t < min(term) - 60_000]
        if premature:
            findings.append((_finding("cached_data_lost", "medium", "Cached blocks were lost before the cluster terminated (evicted or executor loss).", f"{len(premature)} 'No more replicas available' warnings across {len(loss['rdds'])} cached RDDs."), overlapping_stages(premature, stages_info)))
        else:
            notes.append(f"{loss['count']} 'No more replicas available' warnings for {len(loss['rdds'])} cached RDDs all occurred at cluster termination (benign).")

    # --- executor row-buffer spill threshold ---
    spill = merged["spill"]
    if spill["count"]:
        threshold = ", ".join(f"{r:,} rows x{c}" for r, c in spill["rows"].most_common(2))
        stages = overlapping_stages(spill["timestamps"], stages_info)
        findings.append(
            (
                _finding(
                    "row_buffer_spill",
                    "medium" if spill["count"] >= 500 else "low",
                    "Window/join row buffers exceeded their in-memory threshold and switched to the spillable external sorter.",
                    f"{spill['count']} events ({threshold}) across {len(executors)} executors; stages running then: {', '.join(stages) or 'unknown'}.",
                ),
                stages,
            )
        )

    # --- shuffle fetch ---
    fetch, fetch_ms = merged["fetch"], merged["fetch_ms"]
    fetch_info = None
    if fetch["events"]:
        avg_block = fetch["bytes"] / fetch["blocks"] if fetch["blocks"] else 0
        fetch_info = {
            "fetch_events": fetch["events"], "blocks": fetch["blocks"], "bytes": fmt_bytes(fetch["bytes"]), "remote_bytes": fmt_bytes(fetch["remote_bytes"]),
            "avg_block_size": fmt_bytes(avg_block), "remote_fetch_ms_p50": round(percentile(fetch_ms, 50)), "remote_fetch_ms_p95": round(percentile(fetch_ms, 95)),
            "remote_fetch_ms_max": max(fetch_ms, default=0), "slow_fetches_over_1s": len(fetch["slow"]),
        }
        if fetch_info["remote_fetch_ms_p95"] >= 1000 or len(fetch["slow"]) >= 20:
            stages = overlapping_stages([t for t, _ in fetch["slow"]], stages_info)
            findings.append((_finding("slow_shuffle_fetch", "medium", "Remote shuffle fetches are slow.", f"p95 fetch {fetch_info['remote_fetch_ms_p95']} ms, max {fetch_info['remote_fetch_ms_max']} ms, {len(fetch['slow'])} fetches over 1 s."), stages))
        if fetch["blocks"] >= 50_000 and avg_block < 16 * 1024:
            findings.append(
                (
                    _finding(
                        "small_shuffle_blocks",
                        "low",
                        "Shuffle reads are made of very many tiny blocks (too many shuffle partitions for the data).",
                        f"{fetch['blocks']:,} blocks, average {fmt_bytes(avg_block)} ({fmt_bytes(fetch['bytes'])} total).",
                    ),
                    [],
                )
            )

    # --- large task binaries ---
    big = [b for b in driver.get("large_task_binary", []) if b >= 1_000_000]
    if big:
        findings.append((_finding("large_task_binary", "low", "Spark warned about very large serialized tasks.", f"{len(big)} task binaries of {fmt_bytes(min(big))}-{fmt_bytes(max(big))} (large closures or captured data)."), []))

    # --- concurrency ---
    queries = driver.get("active_queries") or []
    concurrency = None
    if queries:
        concurrency = {"max_concurrent_queries": max(queries), "mean_concurrent_queries": round(sum(queries) / len(queries), 2)}
        if total_cores and max(queries) >= max(4, total_cores):
            findings.append(
                (
                    _finding(
                        "query_concurrency",
                        "medium" if max(queries) >= 2 * total_cores else "low",
                        "Many queries ran at the same time on this cluster and compete for the same cores.",
                        f"Up to {max(queries)} concurrent queries (mean {concurrency['mean_concurrent_queries']}) on {total_cores} cores.",
                    ),
                    [],
                )
            )

    # --- storage errors & other errors ---
    events = list((driver.get("events") or {}).values()) + [e for e in merged["events"].values()]
    storage = [e for e in events if e["logger"].startswith("Abfs") and not is_benign(e)]
    throttle = [e for e in events if _THROTTLE.search(e["message"]) and e["level"] in ("WARN", "ERROR") and not is_benign(e)]
    if storage or throttle:
        bad = storage + throttle
        findings.append((_finding("storage_errors", "medium", "Cloud storage requests failed or were throttled.", "; ".join(f"{e['logger']} x{e['count']}: {e['message'][:140]}" for e in bad[:3])), overlapping_stages([e["first_ts"] for e in bad], stages_info)))
    other_errors = sorted((e for e in events if e["level"] in ("ERROR", "FATAL") and not is_benign(e) and e not in storage), key=lambda e: -e["count"])
    if other_errors:
        findings.append((_finding("log_errors", "medium", "ERROR-level messages were logged.", "; ".join(f"{e['logger']} x{e['count']}: {e['message'][:140]}" for e in other_errors[:4])), overlapping_stages([e["first_ts"] for e in other_errors], stages_info)))
    if driver_err.get("python_tracebacks"):
        findings.append((_finding("log_errors", "medium", "Python tracebacks were written to the driver stderr.", f"{driver_err['python_tracebacks']} tracebacks; {next(iter(driver_err.get('exception_lines', {})), '')}"), []))

    # --- init scripts ---
    init_errors = [s for s in logs.get("init_scripts", []) if s.get("error_lines")]
    if init_errors:
        findings.append((_finding("init_script_errors", "medium", "Init scripts logged errors.", "; ".join(f"{s['file']}: {s['error_lines'][0][:120]}" for s in init_errors[:3])), []))

    # --- summary ---
    jobs = driver.get("jobs") or {}
    by_callsite: dict[str, dict[str, Any]] = {}
    for job in jobs.values():
        site = by_callsite.setdefault(job["callsite"], {"jobs": 0, "seconds": 0.0, "max_seconds": 0.0})
        site["jobs"] += 1
        site["seconds"] += job.get("seconds") or 0.0
        site["max_seconds"] = max(site["max_seconds"], job.get("seconds") or 0.0)
    job_summary = {
        "count": len(jobs), "failed": sum(1 for j in jobs.values() if j.get("status") == "failed"),
        "by_callsite": sorted(({"callsite": k, **{a: round(b, 2) if isinstance(b, float) else b for a, b in v.items()}} for k, v in by_callsite.items()), key=lambda x: -x["seconds"])[:8],
        "slowest": sorted(({"callsite": j["callsite"], "seconds": j["seconds"]} for j in jobs.values() if j.get("seconds")), key=lambda x: -x["seconds"])[:5],
    }
    key_events = sorted((e for e in events if not is_benign(e)), key=lambda e: (e["level"] != "ERROR", -e["count"]))[:15]
    exec_heap = [ex["log"].get("xmx_bytes") for ex in executors.values() if (ex.get("log") or {}).get("xmx_bytes")]
    gc_flags = next(((g.get("jvm_flags") or {}) for g in exec_gc.values()), {})
    summary = {
        "coverage": {
            "driver_log_lines": driver.get("lines", 0), "executor_logs_parsed": len(executors),
            "time_range": f"{fmt_ts(driver.get('first_ts'))} - {fmt_ts(driver.get('last_ts'))}" if driver.get("first_ts") else None,
            "file_tracking_truncated": merged["files_truncated"],
            "sources": logs.get("sources", []),
        },
        "jvm": {
            "executor_heap_bytes": max(exec_heap) if exec_heap else (int(gc_flags["MaxHeapSize"]) if gc_flags.get("MaxHeapSize") else None),
            "executor_gc_threads": gc_flags.get("ParallelGCThreads"),
            "driver_heap_bytes": int((driver_gc or {}).get("jvm_flags", {}).get("MaxHeapSize", 0)) or None,
            "driver_gc": _gc_brief(driver_gc),
            "executor_gc_worst": {"executor": worst_eid, **(_gc_brief(worst_gc) or {})} if worst_gc else None,
            "executor_full_gcs_total": sum(g["full_gc_excluding_metadata"] for g in exec_gc.values()),
        },
        "jobs": job_summary,
        "concurrency": concurrency,
        "tables_read": [{k: v for k, v in t.items() if k != "table_path"} for t in tables[:12]],
        "shuffle_fetch": fetch_info,
        "cache": {**{k: v for k, v in cache.items() if k != "min_free_bytes"}, "min_free_storage_memory": fmt_bytes(cache.get("min_free_bytes")) if cache.get("min_free_bytes") is not None else None, "cached_in_memory": fmt_bytes(cache.get("mem_bytes", 0))},
        "codegen": {"compilations": (driver.get("codegen") or {}).get("count", 0), "total": fmt_ms((driver.get("codegen") or {}).get("total_ms", 0)), "max": fmt_ms((driver.get("codegen") or {}).get("max_ms", 0))},
        "scans": driver.get("scans"),
        "broadcast_reads": {"count": merged["totals"]["broadcast_reads"], "total": fmt_ms(merged["totals"]["broadcast_ms"])},
        "key_events": [
            {"level": e["level"], "logger": e["logger"], "count": e["count"], "message": e["message"][:200], "first": fmt_ts(e["first_ts"]), "last": fmt_ts(e["last_ts"])}
            for e in key_events
        ],
        "benign_notes": notes,
    }
    return {"summary": summary, "findings": findings}
