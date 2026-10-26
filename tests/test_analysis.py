from databricks_mcp_server.analysis import analyze_job_run
from databricks_mcp_server import server


def test_fetch_job_run_context_returns_structured_error_on_failure(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")

    def boom(*args, **kwargs):
        raise RuntimeError("Databricks API failed")

    monkeypatch.setattr(server, "_collect", boom)

    result = server.fetch_job_run_context(123)

    assert result["success"] is False
    assert result["error"]["message"] == "Databricks API failed"
    assert result["summary"]["status"] == "error"


def test_analyze_job_run_detects_shuffle_and_skew():
    run = {
        "run_id": 101,
        "state": {"result_state": "TERMINATED", "life_cycle_state": "TERMINATED"},
        "start_time": 1720000000000,
        "end_time": 1720000200000,
        "job_id": 99,
    }

    tasks = [
        {"task_key": "t1", "attempt": 1, "executor_deserialize_time": 1000, "executor_run_time": 120000, "records_read": 5000000, "records_written": 4000000},
        {"task_key": "t2", "attempt": 1, "executor_deserialize_time": 800, "executor_run_time": 110000, "records_read": 4000000, "records_written": 3000000},
    ]

    plan = """
    == Physical Plan ==
    *(2) SortMergeJoin [customer_id]
    +- Exchange hashpartitioning(customer_id, 200)
    +- *(1) Filter (isnotnull(customer_id))
    +- *(1) Project [customer_id, total]
    +- *(1) Scan parquet
    """

    result = analyze_job_run(run, tasks, plan)

    assert result["run_id"] == 101
    assert any(item["type"] == "shuffle_heavy" for item in result["findings"])
    assert any(item["type"] == "join_skew_risk" for item in result["findings"])
    assert result["summary"]["severity"] in {"medium", "high"}


def test_analyze_job_run_handles_missing_plan_and_tasks():
    run = {"run_id": 202, "state": {"result_state": "FAILED", "life_cycle_state": "TERMINATED"}, "start_time": 1, "end_time": 2, "job_id": 42}

    result = analyze_job_run(run, [], None)

    assert result["run_id"] == 202
    assert result["summary"]["status"] == "FAILED"
    assert result["summary"]["severity"] in {"low", "medium", "high"}


def test_analyze_job_run_summarizes_bad_stages_for_specific_run():
    run = {
        "run_id": 777,
        "job_id": 12,
        "state": {"result_state": "TERMINATED", "life_cycle_state": "TERMINATED"},
    }

    tasks = [
        {"task_key": "stage_1_a", "stage_id": 1, "executor_run_time": 5000, "records_read": 200000, "records_written": 180000},
        {"task_key": "stage_1_b", "stage_id": 1, "executor_run_time": 6000, "records_read": 210000, "records_written": 185000},
        {"task_key": "stage_2_a", "stage_id": 2, "executor_run_time": 55000, "records_read": 5000000, "records_written": 4500000},
        {"task_key": "stage_2_b", "stage_id": 2, "executor_run_time": 70000, "records_read": 6000000, "records_written": 5000000},
        {"task_key": "stage_3_a", "stage_id": 3, "executor_run_time": 10000, "records_read": 9000000, "records_written": 1000000},
    ]

    stage_plans = {
        "2": "== Physical Plan ==\n*(2) SortMergeJoin [customer_id]\n+- Exchange hashpartitioning(customer_id, 200)",
        "3": "== Physical Plan ==\n*(3) Project [customer_id, total]\n+- *(2) Filter (isnotnull(customer_id))\n+- Scan parquet",
    }

    result = analyze_job_run(run, tasks, stage_plans=stage_plans)

    assert result["run_id"] == 777
    assert result["summary"]["status"] == "TERMINATED"
    assert "bad_stages" in result
    assert len(result["bad_stages"]) >= 1
    assert any(stage["stage_id"] == 2 for stage in result["bad_stages"])
    assert any(item["type"] == "shuffle_heavy" for item in result["findings"])
    assert "narrative_summary" in result
    assert "Stage 2" in result["narrative_summary"]
    assert "text_summary" in result
    assert "Stage 2" in result["text_summary"]


def test_analyze_job_run_detects_sql_anti_patterns_and_stage_details():
    run = {
        "run_id": 909,
        "job_id": 18,
        "state": {"result_state": "TERMINATED", "life_cycle_state": "TERMINATED"},
    }
    tasks = [
        {"task_key": "s1", "stage_id": 9, "executor_run_time": 20000, "records_read": 1000000, "records_written": 2000000},
        {"task_key": "s2", "stage_id": 9, "executor_run_time": 40000, "records_read": 1200000, "records_written": 2500000},
    ]
    stage_plans = {
        "9": "== Physical Plan ==\n*(3) SortMergeJoin [id]\n+- Exchange hashpartitioning(id, 200)\n+- *(2) Project [*]\n+- *(2) Filter (event_id > 0)\n+- Scan parquet"
    }

    result = analyze_job_run(run, tasks, stage_plans=stage_plans)

    assert any(item["type"] == "select_star" for item in result["findings"])
    assert any(item["type"] == "shuffle_heavy" for item in result["findings"])
    assert "stage_details" in result
    assert any(stage["stage_id"] == 9 for stage in result["stage_details"])
    assert any("query_findings" in stage for stage in result["stage_details"])
    assert "sql_issues" not in result
    assert "raw_context" in result
