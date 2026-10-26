import json

from databricks_mcp_server.spark_metrics import parse_event_log


def test_parse_event_log_extracts_tasks_and_plans():
    lines = [
        json.dumps({"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart", "executionId": 1, "description": "q", "physicalPlanDescription": "SortMergeJoin"}),
        json.dumps({"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": [3], "Properties": {"spark.sql.execution.id": "1"}}),
        json.dumps({"Event": "SparkListenerTaskEnd", "Stage ID": 3, "Stage Attempt ID": 0, "Task Info": {"Task ID": 9, "Launch Time": 10, "Finish Time": 50}, "Task Metrics": {"Executor Run Time": 30, "Shuffle Read Metrics": {"Remote Bytes Read": 5, "Local Bytes Read": 1, "Total Records Read": 2}}}),
        "not json",
    ]
    result = parse_event_log(lines)
    assert result["stage_plans"] == {"3": "SortMergeJoin"}
    assert result["tasks"][0]["stage_id"] == 3
    assert result["tasks"][0]["executor_run_time"] == 30
    assert result["tasks"][0]["shuffle_read_bytes"] == 6
