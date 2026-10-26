from collections import Counter

from databricks_mcp_server import cluster_logs as cl
from databricks_mcp_server.analysis import analyze_job_run
from databricks_mcp_server.log_analysis import analyze_logs

DRIVER = """26/10/23 13:00:11 INFO DriverDaemon$: Started Log4j2
26/10/23 13:04:53 INFO DAGScheduler: Got job 0 (executeCollect at X.scala:73) with 8 output partitions
26/10/23 13:05:07 INFO DAGScheduler: Job 0 finished: executeCollect at X.scala:73, took 14.164981 s
26/10/23 13:05:01 INFO TaskSetManager: Finished task 1.0 in stage 3.0 (TID 11) in 7735 ms on 10.0.0.2 (executor 1) (1/8)
26/10/23 13:05:02 INFO ClusterLoadMonitor: Added query with execution ID:0. Current active queries:9
26/10/23 13:15:51 WARN RunningWindowFunctionExec: MessageWithContext(No Partition Defined for Window operation! Moving all data to a single partition)
26/10/23 13:15:51 WARN RunningWindowFunctionExec: MessageWithContext(No Partition Defined for Window operation! Moving all data to a single partition)
26/10/23 13:14:14 WARN DAGScheduler: Broadcasting large task binary with size 1406573
26/10/23 13:10:00 WARN DBCEventLoggingListener: Error in writing the event to log
com.fasterxml.jackson.databind.JsonMappingException: Exceeded 2097152 bytes (current = 2104010) (through reference chain: org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart["sparkPlanInfo"])
26/10/23 13:16:11 WARN BlockManagerMasterEndpoint: No more replicas available for rdd_102_49!
26/10/23 13:16:11 ERROR TaskSchedulerImpl: Lost executor 0 on 10.0.0.1: cluster termination
26/10/23 13:08:00 ERROR TaskSchedulerImpl: Lost executor 1 on 10.0.0.2: Container killed by YARN for exceeding memory limits
""".splitlines()


def _ts(s):
    return cl.line_ts(s)


def test_driver_log_parsing_extracts_signals():
    d = cl.parse_driver_log(DRIVER, keep_tasks=True)
    assert d["window_single_partition"]["count"] == 2
    assert d["large_task_binary"] == [1406573]
    assert d["event_log_dropped"]["count"] == 1 and d["event_log_dropped"]["max_bytes"] == 2104010
    assert d["event_log_dropped"]["listener_events"]["SparkListenerSQLExecutionStart"] == 1
    assert d["jobs"]["0"]["seconds"] == 14.164981
    assert d["tasks"][0] == {"stage_id": 3, "stage_attempt": 0, "task_id": 11, "executor_id": "1", "duration": 7735, "executor_run_time": 7735}
    assert max(d["active_queries"]) == 9
    assert len(d["lost_executors"]) == 2


def test_executor_log_parsing_files_fetch_spill():
    lines = [
        "Spark Executor Command: java -Xmx7284M -cp x",
        "26/10/23 13:05:16 INFO FileScanRDD: Reading File path: abfss://c@a/tables/t1/part-0.parquet, range: 0-2048, partition values: [empty row], modificationTime: 1",
        "26/10/23 13:05:17 INFO FileScanRDD: Reading File path: abfss://c@a/tables/t1/part-1.parquet, range: 0-4096, partition values: [empty row], modificationTime: 1",
        "26/10/23 13:05:18 INFO FileScanRDD: Reading File path: abfss://c@a/tables/t1/part-1.parquet, range: 0-4096, partition values: [empty row], modificationTime: 1",
        "26/10/23 13:07:09 INFO ShuffleBlockFetcherIterator: Getting 40 (16.0 MiB) non-empty blocks including 20 (13.6 MiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 20 (2.4 MiB) remote blocks",
        "26/10/23 13:07:09 INFO ShuffleBlockFetcherIterator: Started 1 remote fetches in 2500 ms",
        "26/10/23 13:14:19 INFO ExternalAppendOnlyUnsafeRowArray: Reached spill threshold of 4096 rows, switching to org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter",
    ]
    e = cl.parse_executor_log(lines)
    assert e["xmx_bytes"] == 7284 * 1024**2
    assert e["file_reads"] == 3 and e["file_end"]["abfss://c@a/tables/t1/part-1.parquet"] == 4096
    assert e["fetch"]["blocks"] == 40 and e["fetch"]["remote_bytes"] == int(2.4 * 1024**2) and e["fetch"]["slow"][0][1] == 2500
    assert e["spill_threshold"]["rows"][4096] == 1
    tables = cl.summarize_file_reads(e["file_bytes"], e["file_end"], e["file_read_counts"])
    assert tables[0]["data_files"] == 2 and tables[0]["read_amplification"] == round((2048 + 8192) / (2048 + 4096), 2)


def test_gc_log_parsing_parallel_gc_and_flags():
    lines = [
        "     uintx MaxHeapSize                              := 7637827584                          {product}",
        "     bool UseParallelGC                             = true                                {product}",
        "2026-10-23T13:00:47.338+0000: [Full GC (Metadata GC Threshold) [PSYoungGen: 3748K->0K(734208K)] [ParOldGen: 17324K->15629K(94720K)] 21073K->15629K(828928K), [Metaspace: 20750K->20736K(1069056K)], 0.0391030 secs] [Times: user=0.11 sys=0.00, real=0.04 secs] ",
        "2026-10-23T13:15:27.477+0000: [Full GC (Ergonomics) [PSYoungGen: 100K->0K(1000K)] [ParOldGen: 8000K->9000K(10000K)] 8100K->9000K(11000K), 1.5 secs] [Times: user=1.0 sys=0.00, real=1.50 secs] ",
        "2026-10-23T13:15:42.533+0000: [GC (Allocation Failure) [PSYoungGen: 1764847K->56560K(1923072K)] 2198398K->491958K(6196736K), 0.0551709 secs] [Times: user=0.17 sys=0.00, real=0.05 secs] ",
    ]
    g = cl.parse_gc_log(lines)
    assert g["events"] == 3 and g["full_gc"] == 2 and g["full_gc_excluding_metadata"] == 1
    assert g["max_pause_ms"] == 1500.0 and g["long_pauses"][0][1] == 1500.0
    assert g["old_gen_after_full_gc_max_ratio"] == 0.9
    assert g["jvm_flags"]["MaxHeapSize"] == "7637827584" and g["jvm_flags"]["UseParallelGC"] == "true"


def test_log_analysis_findings_with_numbers_and_benign_filtering():
    driver = cl.parse_driver_log(DRIVER, keep_tasks=True)
    files = {f"abfss://c@a/tables/t1/part-{i}.parquet": 5000 for i in range(150)}
    ex = {"log": {
        "file_bytes": files, "file_end": dict(files), "file_read_counts": Counter({p: 1 for p in files}),
        "fetch": {"events": 1, "blocks": 60000, "bytes": 60000 * 1000, "remote_blocks": 0, "remote_bytes": 0, "fetch_ms": [], "slow": []},
        "spill_threshold": {"count": 3, "rows": Counter({4096: 3}), "timestamps": [_ts("26/10/23 13:15:51")]},
        "events": {}, "memory_leaks": 0, "cache": {}, "oom": {"count": 0, "samples": []}, "broadcast_read_ms": [],
    }, "gc": None, "id": "0"}
    stages = [{"stage_id": 7, "submission_time": _ts("26/10/23 13:15:50"), "completion_time": _ts("26/10/23 13:15:55")}]
    result = analyze_logs({"driver": {"log": driver, "gc": None, "stderr": {}}, "executors": {"0": ex}, "init_scripts": [], "sources": []}, stages, set(), {"7", "8"}, 8)
    by_type = {f["type"]: (f, s) for f, s in result["findings"]}
    assert by_type["window_no_partition"][1] == ["7"] and "2 RunningWindowFunctionExec" in by_type["window_no_partition"][0]["evidence"]
    assert "150 files" in by_type["small_files"][0]["evidence"]
    assert by_type["event_log_incomplete"][0]["evidence"].startswith("1 DBCEventLoggingListener")
    assert "2 of 2 stages have no query plan" in by_type["event_log_incomplete"][0]["evidence"]
    assert "Container killed" in by_type["executor_lost"][0]["evidence"] and "cluster termination" not in by_type["executor_lost"][0]["evidence"]
    assert "query_concurrency" in by_type and "large_task_binary" in by_type and "small_shuffle_blocks" in by_type
    assert any("termination" in n for n in result["summary"]["benign_notes"])
    assert "cached_data_lost" not in by_type


def test_analyze_job_run_merges_log_findings_and_summary():
    driver = cl.parse_driver_log(DRIVER)
    run = {"run_id": 1, "state": {"result_state": "SUCCESS"}}
    out = analyze_job_run(run, [{"stage_id": 7, "executor_run_time": 1000}], logs={"driver": {"log": driver, "gc": None, "stderr": {}}, "executors": {}, "sources": []})
    kinds = {f["type"]: f for f in out["findings"]}
    assert kinds["window_no_partition"]["category"] == "query_plan" and kinds["executor_lost"]["category"] == "logs"
    assert out["cluster_logs"]["coverage"]["driver_log_lines"] == len(DRIVER)
