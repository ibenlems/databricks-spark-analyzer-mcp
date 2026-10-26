from __future__ import annotations

import io
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

from .units import percentile

LINE = re.compile(r"^(\d\d)/(\d\d)/(\d\d) (\d\d):(\d\d):(\d\d) (INFO|WARN|ERROR|DEBUG|TRACE|FATAL) ([\w.$\-]+): (.*)$")
SIZE = re.compile(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)")
_UNIT = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
_TS_CACHE: dict[str, int] = {}

MAX_FILE_BYTES = 256 * 1024 * 1024
MAX_TRACKED_FILES = 500_000
MAX_SAMPLES = 20


def parse_size(text: str | None) -> int:
    match = SIZE.search(text or "")
    return int(float(match.group(1)) * _UNIT[match.group(2)]) if match else 0


def line_ts(line: str) -> int | None:
    """Epoch ms for a 'yy/MM/dd HH:mm:ss' log prefix (cluster logs are UTC)."""
    key = line[:17]
    cached = _TS_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        dt = datetime(2000 + int(key[0:2]), int(key[3:5]), int(key[6:8]), int(key[9:11]), int(key[12:14]), int(key[15:17]), tzinfo=timezone.utc)
    except ValueError:
        return None
    _TS_CACHE[key] = int(dt.timestamp() * 1000)
    return _TS_CACHE[key]


def iso_ts(text: str) -> int | None:
    try:
        return int(datetime.strptime(text[:23], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return None


def to_lines(raw: bytes) -> Iterable[str]:
    import gzip

    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8", errors="replace")


_NORM_NUM = re.compile(r"\d+(?:\.\d+)?")
_NORM_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def normalize(message: str) -> str:
    return _NORM_NUM.sub("N", _NORM_ID.sub("ID", message))[:200]


def _in_window(ts: int | None, window: tuple[int, int] | None) -> bool:
    return window is None or ts is None or window[0] <= ts <= window[1]


def _sample(bucket: list[Any], value: Any) -> None:
    if len(bucket) < MAX_SAMPLES:
        bucket.append(value)


_LOST_EXEC = re.compile(r"Lost executor (\w+) on ([\d.]+): (.*)")
_JOB_GOT = re.compile(r"Got job (\d+) \((.*?)\) with (\d+) output partitions")
_JOB_DONE = re.compile(r"Job (\d+) (finished|failed): (.*?), took ([\d.]+) s")
_TASK_DONE = re.compile(r"Finished task ([\d.]+) in stage ([\d.]+) \(TID (\d+)\) in (\d+) ms on (\S+) \(executor (\w+)\)")
_CACHE_ADDED = re.compile(r"Added rdd_\d+_\d+ in (memory|disk) on [\S]+ \(size: ([\d.]+ \w+)(?:, free: ([\d.]+ \w+))?")
_ERR_KEYWORDS = ("OutOfMemoryError", "Container killed", "GC overhead limit", "Java heap space", "Direct buffer memory")


def parse_driver_log(lines: Iterable[str], window: tuple[int, int] | None = None, keep_tasks: bool = False) -> dict[str, Any]:
    """Parse driver log4j output into compact evidence (warnings, failures, jobs, cache, query concurrency)."""
    out: dict[str, Any] = {
        "lines": 0, "first_ts": None, "last_ts": None,
        "events": {}, "window_single_partition": {"count": 0, "timestamps": []},
        "cache_block_loss": {"count": 0, "timestamps": [], "rdds": set()},
        "large_task_binary": [], "lost_executors": [], "lost_tasks": {"count": 0, "reasons": Counter(), "timestamps": []},
        "task_set_failures": [], "oom": {"count": 0, "samples": [], "timestamps": []},
        "fetch_failed": 0, "event_log_dropped": {"count": 0, "max_bytes": 0, "listener_events": Counter()},
        "jobs": {}, "active_queries": [], "codegen": {"count": 0, "total_ms": 0.0, "max_ms": 0.0},
        "scans": {"count": 0, "without_pushed_filters": 0}, "bin_packing": None,
        "cache": {"mem_bytes": 0, "disk_bytes": 0, "min_free_bytes": None, "dropped": 0, "no_space": 0},
        "tasks": [], "exceptions": Counter(),
    }
    last_event: dict[str, Any] | None = None
    for raw in lines:
        out["lines"] += 1
        match = LINE.match(raw)
        if not match:
            stripped = raw.strip()
            if last_event is not None and stripped:
                if "Exceeded" in stripped and "bytes" in stripped:
                    m = re.search(r"Exceeded (\d+) bytes \(current = (\d+)\)", stripped)
                    if m:
                        out["event_log_dropped"]["max_bytes"] = max(out["event_log_dropped"]["max_bytes"], int(m.group(2)))
                    for ev in re.findall(r"(SparkListener\w+)\[", stripped):
                        out["event_log_dropped"]["listener_events"][ev] += 1
                if last_event.get("exception") is None and re.match(r"^[\w.$]+(Exception|Error)\b", stripped):
                    last_event["exception"] = stripped[:200]
                    out["exceptions"][normalize(stripped)] += 1
            continue
        ts = line_ts(raw)
        if not _in_window(ts, window):
            continue
        level, logger, msg = match.group(7), match.group(8), match.group(9)
        if out["first_ts"] is None:
            out["first_ts"] = ts
        out["last_ts"] = ts
        last_event = None

        if level in ("WARN", "ERROR", "FATAL"):
            key = (level, logger, normalize(msg))
            event = out["events"].get(key)
            if event is None:
                event = out["events"][key] = {"level": level, "logger": logger, "message": msg[:700], "count": 0, "first_ts": ts, "last_ts": ts}
            event["count"] += 1
            event["last_ts"] = ts
            last_event = event
            for kw in _ERR_KEYWORDS:
                if kw in msg:
                    out["oom"]["count"] += 1
                    out["oom"]["timestamps"].append(ts)
                    _sample(out["oom"]["samples"], msg[:200])
                    break

        if logger == "RunningWindowFunctionExec" and "No Partition Defined" in msg:
            out["window_single_partition"]["count"] += 1
            if len(out["window_single_partition"]["timestamps"]) < 200:
                out["window_single_partition"]["timestamps"].append(ts)
        elif logger == "BlockManagerMasterEndpoint" and "No more replicas" in msg:
            out["cache_block_loss"]["count"] += 1
            out["cache_block_loss"]["timestamps"].append(ts)
            rdd = re.search(r"rdd_(\d+)_", msg)
            if rdd:
                out["cache_block_loss"]["rdds"].add(rdd.group(1))
        elif logger == "DAGScheduler":
            if "large task binary" in msg:
                size = re.search(r"size (\d+)", msg)
                if size:
                    out["large_task_binary"].append(int(size.group(1)))
            elif msg.startswith("Got job"):
                m = _JOB_GOT.search(msg)
                if m:
                    out["jobs"][m.group(1)] = {"callsite": m.group(2), "partitions": int(m.group(3)), "start_ts": ts, "seconds": None, "status": "running"}
            elif msg.startswith("Job ") and (" finished:" in msg or " failed:" in msg):
                m = _JOB_DONE.search(msg)
                if m:
                    job = out["jobs"].setdefault(m.group(1), {"callsite": m.group(3), "partitions": None, "start_ts": ts})
                    job["seconds"] = float(m.group(4))
                    job["status"] = "failed" if m.group(2) == "failed" else "finished"
            elif "FetchFailed" in msg:
                out["fetch_failed"] += 1
        elif logger == "TaskSchedulerImpl":
            m = _LOST_EXEC.search(msg)
            if m:
                out["lost_executors"].append({"ts": ts, "executor": m.group(1), "host": m.group(2), "reason": m.group(3)[:200]})
        elif logger == "TaskSetManager":
            if msg.startswith("Lost task"):
                out["lost_tasks"]["count"] += 1
                out["lost_tasks"]["timestamps"].append(ts)
                out["lost_tasks"]["reasons"][normalize(msg.split("):", 1)[-1])[:120]] += 1
            elif keep_tasks and msg.startswith("Finished task"):
                m = _TASK_DONE.search(msg)
                if m:
                    out["tasks"].append(
                        {
                            "stage_id": int(float(m.group(2))), "stage_attempt": int(m.group(2).split(".")[1]) if "." in m.group(2) else 0,
                            "task_id": int(m.group(3)), "executor_id": m.group(6), "duration": int(m.group(4)), "executor_run_time": int(m.group(4)),
                        }
                    )
            elif level == "ERROR" and "failed" in msg and "times" in msg:
                _sample(out["task_set_failures"], msg[:250])
        elif logger == "DBCEventLoggingListener" and "Error in writing the event" in msg:
            out["event_log_dropped"]["count"] += 1
        elif logger == "ClusterLoadMonitor":
            m = re.search(r"Current active queries:(\d+)", msg)
            if m:
                out["active_queries"].append(int(m.group(1)))
        elif logger == "CodeGenerator":
            m = re.search(r"Code generated in ([\d.]+) ms", msg)
            if m:
                ms = float(m.group(1))
                out["codegen"]["count"] += 1
                out["codegen"]["total_ms"] += ms
                out["codegen"]["max_ms"] = max(out["codegen"]["max_ms"], ms)
        elif logger == "FileSourceStrategy" and msg.startswith("Pushed Filters:"):
            out["scans"]["count"] += 1
            if not msg[len("Pushed Filters:"):].strip():
                out["scans"]["without_pushed_filters"] += 1
        elif logger == "FileSourceScanExec" and out["bin_packing"] is None and "bin packing" in msg:
            m = re.search(r"max size: (\d+) bytes, open cost is considered as scanning (\d+) bytes", msg)
            if m:
                out["bin_packing"] = {"max_partition_bytes": int(m.group(1)), "open_cost_bytes": int(m.group(2))}
        elif logger == "BlockManagerInfo":
            m = _CACHE_ADDED.search(msg)
            if m:
                size = parse_size(m.group(2))
                out["cache"]["mem_bytes" if m.group(1) == "memory" else "disk_bytes"] += size
                if m.group(3):
                    free = parse_size(m.group(3))
                    current = out["cache"]["min_free_bytes"]
                    out["cache"]["min_free_bytes"] = free if current is None else min(current, free)
        elif logger == "MemoryStore":
            if "Not enough space to cache" in msg or "Will not store" in msg:
                out["cache"]["no_space"] += 1
            elif "dropped from memory" in msg:
                out["cache"]["dropped"] += 1
    return out


_FILE_READ = re.compile(r"Reading File path: (\S+), range: (\d+)-(\d+)")
_FETCH_GET = re.compile(r"Getting (\d+) \(([\d.]+ \w+)\) non-empty blocks")
_FETCH_REMOTE = re.compile(r"and (\d+) \(([\d.]+ \w+)\) remote blocks")
_FETCH_TIME = re.compile(r"Started (\d+) remote fetches in (\d+) ms")
_XMX = re.compile(r"-Xmx(\d+)([kKmMgG])")


def parse_executor_log(lines: Iterable[str], window: tuple[int, int] | None = None) -> dict[str, Any]:
    """Parse one executor's stderr (Spark executor log4j output)."""
    out: dict[str, Any] = {
        "lines": 0, "first_ts": None, "last_ts": None, "xmx_bytes": None, "tasks_run": 0,
        "file_reads": 0, "file_bytes": {}, "file_end": {}, "file_read_counts": Counter(), "file_tracking_truncated": False,
        "predicates": Counter(), "fetch": {"events": 0, "blocks": 0, "bytes": 0, "remote_blocks": 0, "remote_bytes": 0, "fetch_ms": [], "slow": []},
        "spill_threshold": {"count": 0, "rows": Counter(), "timestamps": []},
        "broadcast_read_ms": [], "events": {}, "memory_leaks": 0, "cache": {"no_space": 0, "dropped": 0},
        "oom": {"count": 0, "samples": []}, "exceptions": Counter(), "task_exceptions": 0,
    }
    last_event: dict[str, Any] | None = None
    for raw in lines:
        out["lines"] += 1
        if out["lines"] == 1 and "Spark Executor Command" in raw:
            m = _XMX.search(raw)
            if m:
                out["xmx_bytes"] = int(m.group(1)) * {"k": 1024, "m": 1024**2, "g": 1024**3}[m.group(2).lower()]
            continue
        match = LINE.match(raw)
        if not match:
            stripped = raw.strip()
            if last_event is not None and stripped and last_event.get("exception") is None and re.match(r"^[\w.$]+(Exception|Error)\b", stripped):
                last_event["exception"] = stripped[:200]
                out["exceptions"][normalize(stripped)] += 1
            continue
        ts = line_ts(raw)
        if not _in_window(ts, window):
            continue
        level, logger, msg = match.group(7), match.group(8), match.group(9)
        if out["first_ts"] is None:
            out["first_ts"] = ts
        out["last_ts"] = ts
        last_event = None

        if level in ("WARN", "ERROR", "FATAL"):
            key = (level, logger, normalize(msg))
            event = out["events"].get(key)
            if event is None:
                event = out["events"][key] = {"level": level, "logger": logger, "message": msg[:700], "count": 0, "first_ts": ts, "last_ts": ts}
            event["count"] += 1
            event["last_ts"] = ts
            last_event = event
            if any(kw in msg for kw in _ERR_KEYWORDS):
                out["oom"]["count"] += 1
                _sample(out["oom"]["samples"], msg[:200])

        if logger == "FileScanRDD":
            m = _FILE_READ.search(msg)
            if m:
                out["file_reads"] += 1
                path, end = m.group(1), int(m.group(3))
                size = end - int(m.group(2))
                if path in out["file_bytes"] or len(out["file_bytes"]) < MAX_TRACKED_FILES:
                    out["file_bytes"][path] = out["file_bytes"].get(path, 0) + size
                    out["file_end"][path] = max(out["file_end"].get(path, 0), end)
                    out["file_read_counts"][path] += 1
                else:
                    out["file_tracking_truncated"] = True
        elif logger == "FilterCompat" and msg.startswith("Filtering using predicate:"):
            out["predicates"][normalize(msg.split(":", 1)[1].strip())[:150]] += 1
        elif logger == "ShuffleBlockFetcherIterator":
            m = _FETCH_GET.search(msg)
            if m:
                fetch = out["fetch"]
                fetch["events"] += 1
                fetch["blocks"] += int(m.group(1))
                fetch["bytes"] += parse_size(m.group(2))
                r = _FETCH_REMOTE.search(msg)
                if r:
                    fetch["remote_blocks"] += int(r.group(1))
                    fetch["remote_bytes"] += parse_size(r.group(2))
            else:
                t = _FETCH_TIME.search(msg)
                if t:
                    ms = int(t.group(2))
                    out["fetch"]["fetch_ms"].append(ms)
                    if ms >= 1000 and len(out["fetch"]["slow"]) < 50:
                        out["fetch"]["slow"].append((ts, ms))
        elif logger == "ExternalAppendOnlyUnsafeRowArray" and "spill threshold" in msg:
            m = re.search(r"threshold of (\d+) rows", msg)
            out["spill_threshold"]["count"] += 1
            if m:
                out["spill_threshold"]["rows"][int(m.group(1))] += 1
            if len(out["spill_threshold"]["timestamps"]) < 500:
                out["spill_threshold"]["timestamps"].append(ts)
        elif logger == "Executor":
            if msg.startswith("Running task"):
                out["tasks_run"] += 1
            elif "Managed memory leak" in msg:
                out["memory_leaks"] += 1
            elif msg.startswith("Exception in task"):
                out["task_exceptions"] += 1
        elif logger == "TorrentBroadcast":
            m = re.search(r"Reading broadcast variable \d+ took (\d+) ms", msg)
            if m:
                out["broadcast_read_ms"].append(int(m.group(1)))
        elif logger == "MemoryStore":
            if "Not enough space to cache" in msg or "Will not store" in msg:
                out["cache"]["no_space"] += 1
            elif "dropped from memory" in msg:
                out["cache"]["dropped"] += 1
    return out


_GC_TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3})[+\-]\d{4}: (?:\d+\.\d+: )?\[(Full GC|GC)(?: \(([^)]*)\))?")
_GC_UNIFIED = re.compile(r"^\[(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3})[+\-]\d{4}\]\[\w+\s*\]\[gc[^\]]*\]\s*GC\(\d+\) Pause (\w+)?[^\n]*?(\d+)([KMG])->(\d+)([KMG])\((\d+)([KMG])\) ([\d.]+)ms")
_HEAP = re.compile(r"(\d+)([KMG])->(\d+)([KMG])\((\d+)([KMG])\)")
_UNITS = {"K": 1024, "M": 1024**2, "G": 1024**3}
_FLAG = re.compile(r"^\s*\w+\s+(\w+)\s+:?=\s+(\S+)")


def parse_gc_log(lines: Iterable[str]) -> dict[str, Any]:
    """Parse JVM GC logs (ParallelGC/G1 classic and unified formats) and PrintFlagsFinal output from a stdout file."""
    pauses: list[float] = []
    long: list[tuple[int | None, float]] = []
    causes: Counter[str] = Counter()
    full = full_non_meta = 0
    heap_total = heap_after_peak = 0
    old_ratio_max = 0.0
    first = last = None
    flags: dict[str, str] = {}
    for raw in lines:
        if raw[:1] == " " and "=" in raw[:70]:
            m = _FLAG.match(raw)
            if m and m.group(1) in {"MaxHeapSize", "InitialHeapSize", "ParallelGCThreads", "UseParallelGC", "UseG1GC", "UseConcMarkSweepGC", "ActiveProcessorCount"}:
                flags[m.group(1)] = m.group(2)
            continue
        ts = kind = cause = None
        pause_ms = None
        after = total = 0
        m = _GC_TS.match(raw)
        if m:
            ts, kind, cause = iso_ts(m.group(1)), m.group(2), (m.group(3) or "")
            body = re.sub(r"\[Metaspace:[^\]]*\]", "", raw)
            real = re.search(r"real=([\d.]+) secs", raw)
            secs = re.search(r", ([\d.]+) secs\]", raw)
            pause_ms = float(real.group(1) if real else (secs.group(1) if secs else 0)) * 1000
            heaps = _HEAP.findall(body)
            if heaps:
                h = heaps[-1]
                after, total = int(h[2]) * _UNITS[h[3]], int(h[4]) * _UNITS[h[5]]
            old = re.search(r"ParOldGen: (\d+)K->(\d+)K\((\d+)K\)", raw)
            if kind == "Full GC" and old and int(old.group(3)):
                old_ratio_max = max(old_ratio_max, int(old.group(2)) / int(old.group(3)))
        else:
            u = _GC_UNIFIED.match(raw)
            if not u:
                continue
            ts, kind = iso_ts(u.group(1)), "Full GC" if (u.group(2) or "") == "Full" else "GC"
            cause = u.group(2) or ""
            after, total, pause_ms = int(u.group(5)) * _UNITS[u.group(6)], int(u.group(7)) * _UNITS[u.group(8)], float(u.group(9))
            if kind == "Full GC" and total:
                old_ratio_max = max(old_ratio_max, after / total)
        pauses.append(pause_ms or 0.0)
        causes[f"{kind}:{cause}"] += 1
        if kind == "Full GC":
            full += 1
            if "Metadata" not in (cause or ""):
                full_non_meta += 1
        if pause_ms and pause_ms >= 1000:
            long.append((ts, pause_ms))
        heap_total = max(heap_total, total)
        heap_after_peak = max(heap_after_peak, after)
        first = ts if first is None or (ts and ts < first) else first
        last = ts if last is None or (ts and ts > last) else last
    total_pause = sum(pauses)
    span = (last - first) if first and last else 0
    return {
        "events": len(pauses), "full_gc": full, "full_gc_excluding_metadata": full_non_meta,
        "total_pause_ms": round(total_pause, 1), "max_pause_ms": round(max(pauses), 1) if pauses else 0.0,
        "p95_pause_ms": round(percentile(pauses, 95), 1), "span_ms": span,
        "pause_fraction": round(total_pause / span, 4) if span > 0 else None,
        "heap_total_bytes": heap_total, "heap_after_gc_peak_bytes": heap_after_peak,
        "heap_after_gc_peak_ratio": round(heap_after_peak / heap_total, 3) if heap_total else None,
        "old_gen_after_full_gc_max_ratio": round(old_ratio_max, 3) if full else None,
        "long_pauses": sorted(long, key=lambda x: -x[1])[:5], "causes": dict(causes.most_common(6)),
        "jvm_flags": flags, "first_ts": first, "last_ts": last,
    }


def scan_exceptions(lines: Iterable[str], limit: int = 10) -> dict[str, Any]:
    """Count exception classes, Python tracebacks and OOM markers in stderr-like files."""
    classes: Counter[str] = Counter()
    tracebacks = 0
    oom = 0
    samples: list[str] = []
    for raw in lines:
        if "Traceback (most recent call last)" in raw:
            tracebacks += 1
        if any(kw in raw for kw in _ERR_KEYWORDS):
            oom += 1
            _sample(samples, raw.strip()[:200])
        stripped = raw.strip()
        if re.match(r"^(Caused by: )?[\w.$]+(Exception|Error)\b", stripped):
            classes[normalize(stripped)[:140]] += 1
    return {"python_tracebacks": tracebacks, "oom_lines": oom, "oom_samples": samples, "exception_lines": dict(classes.most_common(limit))}


def table_key(path: str) -> tuple[str, bool]:
    """Group a data/log file path into its table root. Returns (root, is_delta_log)."""
    parts = path.split("?")[0].split("/")[:-1]
    if "_delta_log" in parts:
        return "/".join(parts[: parts.index("_delta_log")]), True
    while parts and "=" in parts[-1]:
        parts.pop()
    return "/".join(parts), False


def summarize_file_reads(file_bytes: dict[str, int], file_end: dict[str, int], read_counts: Counter[str]) -> list[dict[str, Any]]:
    """Per-table file count, size distribution and re-read amplification from FileScanRDD log lines.

    File size is the largest byte offset read (exact once the last range is read); read volume is total bytes read.
    """
    tables: dict[str, dict[str, Any]] = {}
    for path, read_bytes in file_bytes.items():
        root, is_log = table_key(path)
        t = tables.setdefault(root, {"data": [], "log": [], "read_bytes": 0, "log_read_bytes": 0, "reads": 0})
        size = file_end.get(path) or read_bytes
        if is_log:
            t["log"].append(size)
            t["log_read_bytes"] += read_bytes
        else:
            t["data"].append(size)
            t["read_bytes"] += read_bytes
            t["reads"] += read_counts.get(path, 1)
    out = []
    for root, t in tables.items():
        sizes = t["data"]
        entry: dict[str, Any] = {
            "table_path": root, "table": "/".join(root.rstrip("/").split("/")[-2:]),
            "data_files": len(sizes), "unique_data_bytes": sum(sizes), "data_bytes_read": t["read_bytes"],
            "delta_log_files": len(t["log"]), "delta_log_bytes_read": t["log_read_bytes"], "file_reads": t["reads"],
        }
        if sizes:
            p50 = percentile(sizes, 50)
            entry.update(
                {
                    "file_size_p50": int(p50), "file_size_p95": int(percentile(sizes, 95)), "file_size_max": max(sizes),
                    "files_under_16MiB": sum(1 for s in sizes if s < 16 * 1024 * 1024),
                    "read_amplification": round(t["read_bytes"] / sum(sizes), 2) if sum(sizes) else None,
                }
            )
        out.append(entry)
    return sorted(out, key=lambda e: -(e["data_bytes_read"] + e["delta_log_bytes_read"]))
