from __future__ import annotations

import hashlib
import re
from typing import Any

_EXPR_ID = re.compile(r"#\d+L?")
_BLOCK_HEADER = re.compile(r"^\((\d+)\)\s+(.*)$")
_TREE_LINE = re.compile(r"^([ :|]*(?:[+:]- )?)(?:\*\(\d+\)\s*)?(.*)$")
_NAME = re.compile(r"[A-Za-z][\w.]*")

PY_UDF_NODES = ("batchevalpython", "arrowevalpython", "flatmapgroupsinpandas", "mapinpandas", "aggregateinpandas", "flatmapcogroupsinpandas", "pythonmapinarrow")
DELTA_INTERNAL_MARKERS = ("filesizehistogramagg", "deletedrecordcountshistogramagg", "_delta_log", "_extract_deletionvector", "checkpointmetadata")
IGNORED_IN_TREE = {"project", "wholestagecodegen", "inputadapter", "columnartorow", "rowtocolumnar", "reusedexchange", "subqueryadaptivebroadcast"}


def _clean(text: str) -> str:
    return _EXPR_ID.sub("", text).strip()


def plan_id(plan: str) -> str:
    return hashlib.sha1(plan.encode("utf-8", errors="ignore")).hexdigest()[:10]


def _physical_section(plan: str) -> str:
    marker = "== Physical Plan =="
    if marker in plan:
        plan = plan.split(marker, 1)[1]
    plan = re.split(r"\n== (?!Physical)", plan, maxsplit=1)[0]
    return plan


def _split_top(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch in "<([":
            depth += 1
        elif ch in ">)]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


def parse_nodes(plan: str) -> list[dict[str, Any]]:
    """Parse a Spark physical plan (tree or formatted explain) into nodes with depth, name and text."""
    text = _physical_section(plan)
    lines = text.splitlines()
    nodes: list[dict[str, Any]] = []
    formatted = any(_BLOCK_HEADER.match(line.strip()) for line in lines if not line.startswith(" "))
    if formatted:
        current: dict[str, Any] | None = None
        for line in lines:
            header = _BLOCK_HEADER.match(line.strip()) if not line.startswith((" ", "+", ":", "*")) else None
            if header:
                if current:
                    nodes.append(current)
                name_match = _NAME.match(header.group(2))
                current = {"depth": 0, "name": name_match.group(0) if name_match else "", "text": header.group(2), "header": header.group(2)}
            elif current is not None and line.strip():
                current["text"] += " " + line.strip()
        if current:
            nodes.append(current)
        return nodes
    for line in lines:
        if not line.strip() or line.lstrip().startswith("=="):
            continue
        match = _TREE_LINE.match(line.rstrip())
        if not match:
            continue
        body = match.group(2).strip()
        name_match = _NAME.match(body)
        if not name_match:
            continue
        nodes.append({"depth": len(match.group(1)) // 3, "name": name_match.group(0), "text": body})
    return nodes


def _find(pattern: str, text: str, flags: int = 0) -> str | None:
    match = re.search(pattern, text, flags)
    return match.group(1) if match else None


def _scan_info(node: dict[str, Any]) -> dict[str, Any]:
    text = node["text"]
    head = re.match(r"(\w*Scan\w*)\s+(\w+)\s+([\w.`$\-]+)", node.get("header", text))
    location = _find(r"Location:\s*\w+\s*\[(.*?)\]", text) or _find(r"Location:\s*(\S+)", text)
    schema = _find(r"ReadSchema:\s*struct<(.*?)>(?:,\s*\w+:|\s*$|\)\s*$)", text) or _find(r"ReadSchema:\s*struct<(.*)>", text)
    columns = len(_split_top(schema)) if schema else None
    if columns is None:
        out = _find(r"\[(.*?)\]", text)
        columns = len(_split_top(out)) if out else None
    pushed = _find(r"PushedFilters:\s*\[(.*?)\]", text)
    partition = _find(r"PartitionFilters:\s*\[(.*?)\]", text)
    data_filters = _find(r"DataFilters:\s*\[(.*?)\]", text)
    fmt = (head.group(2) if head else _find(r"Format:\s*(\w+)", text)) or ""
    if not head:
        head_fmt = re.match(r"\w*Scan\w*\s+(\w+)", node.get("header", text))
        fmt = head_fmt.group(1) if head_fmt else fmt
    return {
        "operator": node["name"],
        "format": fmt.lower() or None,
        "table": head.group(3) if head else None,
        "location": location[:160] if location else None,
        "delta_internal": "_delta_log" in text,
        "columns_read": columns,
        "partition_filters": _clean(partition) if partition is not None else None,
        "pushed_filters": _clean(pushed) if pushed is not None else None,
        "data_filters": _clean(data_filters) if data_filters is not None else None,
    }


def _join_info(node: dict[str, Any]) -> dict[str, Any]:
    text = node["text"]
    keys = re.search(r"Join\w*\s*(?:\w+\s+)?\[(.*?)\],\s*\[(.*?)\]", text) or re.search(
        r"Left keys\s*(?:\[\d+\])?:\s*\[(.*?)\]\s*Right keys\s*(?:\[\d+\])?:\s*\[(.*?)\]", text
    )
    join_type = _find(r"Join type:\s*(\w+)", text) or _find(r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross|ExistenceJoin)\b", text)
    return {
        "operator": node["name"],
        "join_type": join_type,
        "left_keys": _clean(keys.group(1)) if keys else None,
        "right_keys": _clean(keys.group(2)) if keys else None,
        "skew_handled": "isskewjoin=true" in text.lower(),
        "build_side": _find(r"\b(BuildLeft|BuildRight)\b", text),
        "condition": _clean(text)[:200] if "nestedloop" in node["name"].lower() or "cartesian" in node["name"].lower() else None,
    }


def _exchange_info(node: dict[str, Any]) -> dict[str, Any]:
    text = node["text"]
    lowered = text.lower()
    if "singlepartition" in lowered:
        kind, keys, parts = "single", None, 1
    elif "roundrobin" in lowered:
        kind, keys = "round_robin", None
        parts = _find(r"RoundRobinPartitioning\((\d+)", text)
        parts = int(parts) if parts else None
    elif "rangepartitioning" in lowered:
        kind = "range"
        inner = _find(r"rangepartitioning\((.*)\)", text) or ""
        pieces = _split_top(inner)
        parts = int(pieces[-1]) if pieces and pieces[-1].isdigit() else None
        keys = _clean(", ".join(pieces[:-1])) if parts is not None else _clean(inner)
    else:
        kind = "hash" if "hashpartitioning" in lowered else "other"
        inner = _find(r"hashpartitioning\((.*?)\)(?:,|\s|$)", text) or ""
        pieces = _split_top(inner)
        parts = int(pieces[-1]) if pieces and pieces[-1].isdigit() else None
        keys = _clean(", ".join(pieces[:-1] if parts is not None else pieces))
    return {"kind": kind, "keys": keys or None, "partitions": parts, "operator": node["name"]}


def _aggregate_info(node: dict[str, Any]) -> dict[str, Any]:
    text = node["text"]
    keys = _find(r"keys=\[(.*?)\],\s*functions=", text) or _find(r"\bKeys\s*(?:\[\d+\])?:\s*\[(.*?)\]", text)
    funcs = _find(r"functions=\[(.*)\]", text) or _find(r"\bFunctions\s*(?:\[\d+\])?:\s*\[(.*?)\]\s*(?:Aggregate Attributes|Results|$)", text)
    return {
        "operator": node["name"],
        "mode": "partial" if "partial_" in text.lower() else "final/complete",
        "keys": _clean(keys) if keys else None,
        "functions": _clean(funcs)[:300] if funcs else None,
    }


def _finding(kind: str, severity: str, message: str, evidence: str) -> dict[str, Any]:
    return {"type": kind, "severity": severity, "message": message, "evidence": evidence}


def analyze_plan(plan: str | None) -> dict[str, Any] | None:
    """Parse a physical plan and return operators, per-category details, anti-pattern findings and a narrative."""
    if not plan or not plan.strip():
        return None
    nodes = parse_nodes(plan)
    lowered_all = plan.lower()

    scans: list[dict[str, Any]] = []
    joins: list[dict[str, Any]] = []
    exchanges: list[dict[str, Any]] = []
    aggregates: list[dict[str, Any]] = []
    sorts: list[dict[str, Any]] = []
    windows: list[dict[str, Any]] = []
    udfs: list[dict[str, Any]] = []
    generates: list[str] = []
    filters: list[str] = []
    operator_counts: dict[str, int] = {}
    photon = False
    broadcasts = 0

    for index, node in enumerate(nodes):
        name = node["name"]
        low = name.lower()
        operator_counts[name] = operator_counts.get(name, 0) + 1
        if low.startswith("photon"):
            photon = True
        text = node["text"]
        if low.endswith("scan") or "scan" in low and low not in {"inputadapter"}:
            if "existingrdd" not in text.lower():
                scans.append(_scan_info(node))
        elif "join" in low or "cartesianproduct" in low:
            joins.append(_join_info(node))
        elif "broadcastexchange" in low:
            broadcasts += 1
        elif "exchange" in low and "source" not in low and "reused" not in low:
            exchanges.append(_exchange_info(node) | {"_index": index})
        elif "aggregate" in low or "groupingagg" in low:
            aggregates.append(_aggregate_info(node))
        elif low == "sort" or low == "photonsort":
            sorts.append({"global": bool(re.search(r",\s*true,\s*\d+", text)), "text": _clean(text)[:160]})
        elif low.startswith("window") or low.endswith("window"):
            windows.append({"text": _clean(text)[:200], "_index": index, "no_partition": bool(re.search(r"\],\s*\[\],\s*\[", text))})
        elif low.startswith("generate"):
            generates.append(_clean(text)[:160])
        elif low == "filter" or low == "photonfilter":
            filters.append(_clean(text)[:200])
        if any(p in low for p in PY_UDF_NODES):
            udfs.append({"operator": name, "vectorized": "arrow" in low or "pandas" in low, "text": _clean(text)[:160]})
        elif "pythonudf" in text.lower() or re.search(r"\bUDF\(|scalaudf", text, re.IGNORECASE):
            udfs.append({"operator": name, "vectorized": False, "text": _clean(text)[:160]})

    internal = (bool(scans) and all(sc.get("delta_internal") for sc in scans)) or any(m in lowered_all for m in DELTA_INTERNAL_MARKERS)
    aqe_present = "adaptivesparkplan" in lowered_all or "aqeshuffleread" in lowered_all or "customshufflereader" in lowered_all
    aqe_final = "isfinalplan=true" in lowered_all
    findings: list[dict[str, Any]] = []

    cross = [j for j in joins if "cartesian" in j["operator"].lower() or "nestedloop" in j["operator"].lower() or j["join_type"] == "Cross"]
    if cross or "cross join" in lowered_all:
        detail = "; ".join(f"{j['operator']} {j['join_type'] or ''} {j['condition'] or ''}".strip() for j in cross) or "cross join in plan text"
        findings.append(_finding("cross_join", "high", "A cross / nested-loop join can explode row counts.", detail))

    smj = [j for j in joins if "sortmergejoin" in j["operator"].lower()]
    if smj:
        keys = "; ".join(f"[{j['left_keys'] or '?'}] = [{j['right_keys'] or '?'}] ({j['join_type'] or '?'})" for j in smj[:5])
        unhandled = [j for j in smj if not j["skew_handled"]]
        findings.append(
            _finding(
                "join_skew_risk",
                "medium" if unhandled else "low",
                f"{len(smj)} SortMergeJoin(s): both sides are shuffled and sorted, and hot keys produce stragglers.",
                f"Join keys: {keys}. Skew-join handling: {'enabled' if not unhandled else 'not in plan'}. Broadcast joins in plan: {broadcasts}.",
            )
        )

    hash_like = [e for e in exchanges if e["kind"] in {"hash", "range", "round_robin"}]
    if exchanges:
        shown = "; ".join(f"{e['kind']}({e['keys'] or '-'}, {e['partitions']})" for e in exchanges[:6])
        count = len(exchanges)
        findings.append(
            _finding(
                "shuffle_heavy",
                "medium" if count >= 4 else "low",
                f"The plan contains {count} shuffle exchange(s).",
                f"Exchanges: {shown}{' ...' if count > 6 else ''}.",
            )
        )
        if hash_like and all(e["partitions"] == 200 for e in hash_like) and not aqe_present:
            findings.append(
                _finding(
                    "default_shuffle_partitions",
                    "low",
                    "All shuffles use the default 200 partitions and the plan is not adaptive.",
                    f"{len(hash_like)} exchange(s) with 200 partitions.",
                )
            )
        if any(e["kind"] == "round_robin" for e in exchanges):
            findings.append(_finding("repartition_roundrobin", "low", "A round-robin repartition forces a full shuffle.", "RoundRobinPartitioning exchange present."))
        if any(e["kind"] == "single" for e in exchanges):
            under_window = any(
                w["no_partition"] or any(e["kind"] == "single" and 0 < e["_index"] - w["_index"] <= 2 for e in exchanges)
                for w in windows
            )
            if not under_window:
                findings.append(_finding("single_partition_exchange", "low", "Rows are shuffled into a single partition.", "Exchange SinglePartition present."))
        if not aqe_present and not photon and hash_like:
            findings.append(
                _finding(
                    "aqe_disabled",
                    "medium",
                    "The plan is not adaptive, so there is no skew-join handling or dynamic partition coalescing.",
                    "No AdaptiveSparkPlan / AQEShuffleRead nodes found.",
                )
            )

    for window in windows:
        near_single = any(e["kind"] == "single" and 0 < e["_index"] - window["_index"] <= 2 for e in exchanges)
        if window["no_partition"] or near_single:
            findings.append(_finding("window_no_partition", "high", "A window function has no PARTITION BY; all rows go through a single task.", window["text"]))
            break

    for agg in aggregates:
        text = ((agg["functions"] or "") + " " + (agg["keys"] or "")).lower()
        if "distinct" in text and "count" in text:
            findings.append(_finding("distinct_count", "medium", "count(distinct ...) forces an extra expand/aggregate pass.", f"{agg['operator']} functions: {agg['functions']}"))
            break
    if "distinct" in lowered_all and "count" in lowered_all and not any(f["type"] == "distinct_count" for f in findings) and "expand" in lowered_all:
        findings.append(_finding("distinct_count", "medium", "A DISTINCT+COUNT pattern is present, which may require large sort or hash operations.", "Expand + distinct aggregation in plan."))
    collect = [a for a in aggregates if re.search(r"collect_(list|set)", (a["functions"] or ""), re.IGNORECASE)]
    if collect:
        findings.append(_finding("collect_aggregate", "medium", "collect_list/collect_set buffers entire groups in memory.", collect[0]["functions"] or ""))

    if any(s["global"] for s in sorts):
        findings.append(_finding("global_sort", "medium", "A global sort requires a range shuffle and a full sort.", next(s["text"] for s in sorts if s["global"])))

    if generates:
        findings.append(_finding("explode_generate", "low", "explode/generate multiplies rows.", generates[0]))

    if udfs:
        batch = [u for u in udfs if not u["vectorized"]]
        findings.append(
            _finding(
                "udf_cost",
                "medium" if batch else "low",
                "UDF/Python execution detected; these block optimizations and add serialization overhead.",
                "; ".join(f"{u['operator']} ({'vectorized' if u['vectorized'] else 'row-at-a-time/opaque'})" for u in udfs[:5]),
            )
        )
    elif "udf" in lowered_all or "mapfunction" in lowered_all:
        findings.append(_finding("udf_cost", "medium", "A UDF or Python execution path is present.", "UDF-like operators were detected in the plan."))

    wide = [s for s in scans if s["columns_read"] and s["columns_read"] >= 30]
    if wide:
        findings.append(
            _finding(
                "select_star",
                "medium",
                "Scans read a very wide set of columns.",
                "; ".join(f"{s['table'] or s['operator']} reads {s['columns_read']} columns" for s in wide[:5]),
            )
        )
    elif "select *" in lowered_all or "project [*]" in lowered_all or "project[*]" in lowered_all:
        findings.append(_finding("select_star", "medium", "The query selects all columns, which can increase I/O and memory pressure on wide tables.", "SELECT * or Project [*] detected."))

    unfiltered = [
        s for s in scans
        if s["pushed_filters"] == "" and s["partition_filters"] in ("", None) and (s["data_filters"] in ("", None))
    ]
    if unfiltered:
        findings.append(
            _finding(
                "full_scan",
                "low",
                "Scans have no partition or pushed-down filters (full table reads).",
                "; ".join(f"{s['table'] or s['operator']} ({s['format']})" for s in unfiltered[:6]),
            )
        )
    nonc = [s for s in scans if s["format"] in {"csv", "json", "text"}]
    if nonc:
        findings.append(_finding("non_columnar_format", "medium", "Reading row-based text formats prevents column pruning and filter pushdown.", "; ".join(f"{s['table'] or s['operator']} ({s['format']})" for s in nonc[:5])))

    nonsarg = [f for f in filters if re.search(r"\b(lower|upper|substring|trim|date_format|year|month|to_date|unix_timestamp|regexp_\w+)\(\w+", f, re.IGNORECASE)]
    if nonsarg:
        findings.append(_finding("non_sargable_filter", "low", "Functions applied to filter columns block predicate pushdown.", nonsarg[0]))

    if internal:
        findings = []
    tree = [
        {"depth": n["depth"], "operator": n["name"], "detail": _clean(n["text"])[:160]}
        for n in nodes
        if n["name"].lower() not in IGNORED_IN_TREE
    ][:60]

    parts: list[str] = []
    if internal:
        parts.append("Delta metadata/maintenance plan (reads _delta_log only); anti-pattern checks skipped")
    if scans:
        parts.append(
            f"Scans {len(scans)} table(s): "
            + "; ".join(
                f"{s['table'] or s['location'] or s['operator']} [{s['format'] or '?'}, {s['columns_read'] if s['columns_read'] is not None else '?'} cols, "
                f"{'filters pushed' if s['pushed_filters'] or s['partition_filters'] else 'no filters'}]"
                for s in scans[:6]
            )
        )
    if joins:
        parts.append(f"{len(joins)} join(s): " + "; ".join(f"{j['operator']} {j['join_type'] or ''}".strip() for j in joins[:6]))
    if exchanges:
        parts.append(f"{len(exchanges)} shuffle(s) ({broadcasts} broadcast)")
    if aggregates:
        parts.append(f"{len(aggregates)} aggregate operator(s)")
    if windows:
        parts.append(f"{len(windows)} window operator(s)")
    if udfs:
        parts.append(f"{len(udfs)} UDF operator(s)")
    parts.append("adaptive (AQE) plan" if aqe_present else "non-adaptive plan")
    if photon:
        parts.append("Photon operators present")

    for e in exchanges:
        e.pop("_index", None)
    for w in windows:
        w.pop("_index", None)

    return {
        "plan_id": plan_id(plan),
        "node_count": len(nodes),
        "operator_counts": dict(sorted(operator_counts.items(), key=lambda kv: -kv[1])[:25]),
        "flags": {"aqe": aqe_present, "aqe_final_plan": aqe_final, "photon": photon, "delta_internal": internal},
        "scans": scans[:20],
        "joins": joins[:20],
        "exchanges": exchanges[:20],
        "aggregates": aggregates[:20],
        "sorts": sorts[:10],
        "windows": windows[:10],
        "udfs": udfs[:10],
        "plan_tree": tree,
        "findings": findings,
        "narrative": ". ".join(parts) + ".",
    }


_METRIC_INTEREST = (
    "spill", "output rows", "data size", "peak memory", "number of files", "size of files",
    "scan time", "sort time", "aggregate time", "shuffle bytes written", "records read", "fetch wait",
    "number of partitions", "time to build", "rows",
)


def summarize_operator_metrics(nodes: list[dict[str, Any]] | None, limit: int = 30) -> list[dict[str, Any]]:
    """Reduce Spark UI SQL node metrics to the operator-level numbers that matter (rows, bytes, spill, memory)."""
    out: list[dict[str, Any]] = []
    for node in nodes or []:
        metrics = {
            m.get("name"): m.get("value")
            for m in node.get("metrics", []) or []
            if m.get("name") and any(key in str(m["name"]).lower() for key in _METRIC_INTEREST)
        }
        if metrics:
            out.append({"node_id": node.get("nodeId"), "operator": node.get("nodeName"), "metrics": metrics})
    spilled = [n for n in out if any("spill" in k.lower() for k in n["metrics"])]
    rest = [n for n in out if n not in spilled]
    return (spilled + rest)[:limit]
