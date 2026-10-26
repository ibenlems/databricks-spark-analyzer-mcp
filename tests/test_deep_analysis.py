from databricks_mcp_server.analysis import analyze_job_run, analyze_stage_detail
from databricks_mcp_server.plan_analysis import analyze_plan
from databricks_mcp_server.spark_metrics import parse_event_log

GIB = 1024**3
MIB = 1024**2

PLAN = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[cust#1], functions=[count(distinct item#2), collect_list(x#3)])
   +- Exchange hashpartitioning(cust#1, 200), ENSURE_REQUIREMENTS
      +- *(5) SortMergeJoin [cust#1], [id#9], Inner
         :- *(2) Sort [cust#1 ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(cust#1, 200), ENSURE_REQUIREMENTS
         :     +- *(1) Filter (lower(name#4) = bob)
         :        +- FileScan parquet db.orders[a#1,b#2] Batched: true, DataFilters: [], Format: Parquet, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int,b:int>
         +- *(4) Sort [id#9 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(id#9, 200), ENSURE_REQUIREMENTS
               +- BatchEvalPython [my_udf(v#5)], [pythonUDF0#20]
                  +- FileScan csv db.cust[id#9] Batched: false, Format: CSV, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:int>
"""


def _tasks(stage, n, duration, **extra):
    return [{"stage_id": stage, "task_id": i, "executor_id": str(i % 4), "executor_run_time": duration, **extra} for i in range(n)]


def test_plan_analysis_extracts_operators_and_anti_patterns():
    analysis = analyze_plan(PLAN)
    kinds = {f["type"] for f in analysis["findings"]}
    assert {"join_skew_risk", "shuffle_heavy", "udf_cost", "non_columnar_format", "full_scan", "collect_aggregate", "non_sargable_filter"} <= kinds
    assert analysis["flags"]["aqe"] is True
    assert analysis["joins"][0]["left_keys"] == "cust"
    assert analysis["exchanges"][0]["partitions"] == 200
    assert {s["table"] for s in analysis["scans"]} == {"db.orders", "db.cust"}
    assert "narrative" in analysis and analysis["plan_tree"]


def test_stage_metrics_report_numbers_for_skew_shuffle_and_spill():
    tasks = _tasks(7, 99, 1000, shuffle_read_bytes=100 * MIB, shuffle_write_bytes=200 * MIB, gc_time=50)
    tasks.append(
        {
            "stage_id": 7, "task_id": 999, "executor_id": "0", "executor_run_time": 60000,
            "shuffle_read_bytes": 5 * GIB, "shuffle_write_bytes": 1 * GIB,
            "disk_bytes_spilled": 3 * GIB, "memory_bytes_spilled": 9 * GIB, "gc_time": 20000,
        }
    )
    run = {"run_id": 1, "job_id": 2, "state": {"result_state": "SUCCESS"}}
    result = analyze_job_run(run, tasks, stage_plans={"7": PLAN})

    stage = result["bad_stages"][0]
    m = stage["metrics"]
    assert m["skew_ratio"] == 60.0
    assert m["spill"]["disk_bytes"] == 3 * GIB
    assert m["shuffle_read_bytes"] > 14 * GIB
    assert m["readable"]["spill_disk"] == "3.00 GiB"
    types = {f["type"] for f in result["findings"]}
    assert {"task_skew", "data_skew", "disk_spill", "gc_pressure", "shuffle_heavy"} <= types
    skew = next(f for f in result["findings"] if f["type"] == "task_skew")
    assert "60.0x" in skew["evidence"][0] or "60x" in skew["evidence"][0] or "median" in skew["evidence"][0]
    assert skew["recommendation"]
    assert result["run_metrics"]["spill_disk_bytes"] == 3 * GIB
    assert result["query_analysis"][0]["stage_ids"] == ["7"]
    assert result["recommendations"]


def test_plan_findings_are_counted_once_per_plan_not_per_stage():
    tasks = []
    plans = {}
    for sid in range(20):
        tasks += _tasks(sid, 4, 100, shuffle_read_bytes=MIB)
        plans[str(sid)] = PLAN
    result = analyze_job_run({"run_id": 1, "state": {}}, tasks, stage_plans=plans)
    udf = next(f for f in result["findings"] if f["type"] == "udf_cost")
    assert udf["occurrences"] == 1
    assert udf["affected_stage_count"] == 20
    assert len(result["query_analysis"]) == 1
    assert result["bad_stages"] == []


def test_cluster_analysis_flags_overprovisioned_and_memory_pressure():
    tasks = _tasks(1, 8, 60000, executor_cpu_time_ns=20e9, disk_bytes_spilled=2 * GIB, gc_time=12000)
    executors = [{"id": str(i), "cores": 8, "max_memory_bytes": 10 * GIB, "add_time": 0} for i in range(8)]
    stages = [{"stage_id": 1, "submission_time": 0, "completion_time": 120_000}]
    specs = [
        {
            "cluster_key": "main",
            "new_cluster": {"node_type_id": "m5.2xlarge", "num_workers": 8, "spark_version": "14.3.x-scala2.12", "runtime_engine": "STANDARD"},
            "node_type": {"num_cores": 8, "memory_mb": 32768},
        }
    ]
    result = analyze_job_run({"run_id": 1, "state": {}}, tasks, executors=executors, stages=stages, cluster_specs=specs)
    cluster = result["cluster_analysis"]
    assert cluster["observed"]["total_cores"] == 64
    assert cluster["observed"]["memory_per_core_gb"] == 4.0
    assert cluster["utilization"]["task_slot_utilization"] == round(480000 / (64 * 120000), 4)
    kinds = {f["type"] for f in cluster["findings"]}
    assert {"cluster_overprovisioned", "cluster_memory_pressure", "photon_not_enabled", "fixed_size_cluster"} <= kinds


def test_event_log_captures_executors_conf_stages_and_cpu():
    import json

    lines = [
        json.dumps({"Event": "SparkListenerExecutorAdded", "Executor ID": "1", "Timestamp": 5, "Executor Info": {"Total Cores": 4, "Host": "h"}}),
        json.dumps({"Event": "SparkListenerBlockManagerAdded", "Block Manager ID": {"Executor ID": "1"}, "Maximum Memory": 1000}),
        json.dumps({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"spark.sql.shuffle.partitions": "64", "other": "x"}}),
        json.dumps({"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 3, "Stage Name": "n", "Number of Tasks": 2, "Submission Time": 10, "Completion Time": 90}}),
        json.dumps({"Event": "SparkListenerTaskEnd", "Stage ID": 3, "Stage Attempt ID": 0, "Task Info": {"Task ID": 1, "Executor ID": "1", "Launch Time": 0, "Finish Time": 10}, "Task Metrics": {"Executor Run Time": 8, "Executor CPU Time": 5_000_000, "Disk Bytes Spilled": 7}}),
    ]
    data = parse_event_log(lines)
    assert data["executors"][0]["cores"] == 4 and data["executors"][0]["max_memory_bytes"] == 1000
    assert data["spark_conf"] == {"spark.sql.shuffle.partitions": "64"}
    assert data["stages"][0]["completion_time"] == 90
    assert data["tasks"][0]["executor_cpu_time_ns"] == 5_000_000
    assert data["tasks"][0]["disk_bytes_spilled"] == 7


def test_analyze_stage_detail_returns_slowest_tasks_and_plan():
    tasks = _tasks(7, 10, 1000) + [{"stage_id": 7, "task_id": 77, "executor_run_time": 30000}]
    detail = analyze_stage_detail(7, tasks, stage_plans={"7": PLAN})
    assert detail["found"] and detail["slowest_tasks"][0]["task_id"] == 77
    assert detail["plan_analysis"]["joins"]
    assert any(i["type"] == "task_skew" for i in detail["issues"])
