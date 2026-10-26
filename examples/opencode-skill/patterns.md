# Detection Patterns

Report only when the threshold is met. Every report includes Stage ID and Task ID (fallback: Node ID).

## Core patterns

### 1. Missing Broadcast Join
- Flag when: one join side is <1 GB but strategy is SortMergeJoin or ShuffleHashJoin.
- Extract: left/right table or alias, estimated sizes, join type, strategy.
- Report: smaller-side table, both sizes, join type, actual strategy, why broadcast was missed (disabled/low autoBroadcastJoinThreshold, nested join, non-deterministic expression, multi-stage plan).

### 2. Skewed Partitions
- Flag when: in Exchange/Shuffle/AQEShuffleRead, a partition is >5x the average AND >5 GB, or >50% of total data.
- Report: operator, table/alias, partition size distribution (min, max, avg, count), likely cause (NULLs, dominant key).

### 3. Unwanted Coalesces
- Flag when: CoalescePartitions/CoalescedShufflePartitions yields max partition >10x advisory size (1.28 GB) or >2 GB, AND parallelism < executors x cores.
- Report: table/dataset, partition counts and sizes before/after, impact, recommendation (tune advisoryPartitionSize, limit coalesce, adjust skew thresholds).

### 4. High Spill
- Compute: totalSpill = memoryBytesSpilled + diskBytesSpilled.
- Flag when ANY: totalSpill >1 GB; totalSpill >20% of inputBytes; >=50% of tasks in the stage spilled.
- Severity: High = >5 GB or >50% tasks spilled; Medium = 1-5 GB; Low = <1 GB but repeated across stages.
- Report: operator, memory and disk spill (GB), input (GB), spill % vs input, tasks with spill / total, severity, recommendation (executor memory, AQE, shuffle partitions).

### 5. Missing Partition/Date Filters (Full Scan)
- Applies to: FileScan, BatchScan, DataSourceV2ScanExec, IcebergTableScan.
- Flag when: no partition filters and no pushed filters AND (rowsRead >1 B or input >500 GB).
- Also flag low data skipping: files read / files total >80% despite filters.
- Severity: High = >5 B rows or >2 TB; Medium = 1-5 B rows.
- Report: table, rows/size read, confirmed filter absence, severity, recommendation (partition filters, dynamic pruning, restrict date range).

### 6. Oversized Broadcast
- Applies to: BroadcastHashJoin, BroadcastNestedLoopJoin.
- Flag when: broadcast size >1 GB. Note multiple large broadcasts in the same stage.
- Report: table, size, join type, impact (driver memory, slow startup).

### 7. Data Explosion in Joins
- Flag when: outputRows > 2 x max(leftRows, rightRows). Ignore ratio <=2.
- Report: join type and condition, left/right table and rows, output rows, explosion ratio, likely cause (duplicate keys, missing distinct, wrong grain), recommendation (review keys, dedupe, pre-aggregate).

## Additional patterns

### 8. Small Files / Too Many Tasks
- Flag when: a scan stage has tens of thousands of tasks AND average file/partition size <16 MB.
- Report: table, task count, avg size, recommendation (compaction/OPTIMIZE, raise maxPartitionBytes).

### 9. Excessive Shuffle Volume
- Flag when: an Exchange writes >100 GB, or shuffle bytes > input bytes of the stage.
- Report: operator, shuffle write size, input size, avoidable shuffle opportunity (pre-filter, join order, pre-aggregate).

### 10. Too Few / Too Many Shuffle Partitions
- Flag when: average shuffle partition <1 MB (too many) or >1 GB (too few) and AQE did not correct it.
- Report: partition count, avg/max size, recommendation (spark.sql.shuffle.partitions, enable AQE).

### 11. Cartesian / Nested Loop Join
- Flag when: CartesianProduct or BroadcastNestedLoopJoin with non-equi or missing condition on large inputs.
- Report: join condition, input sizes/rows, recommendation (add equi-join key, rewrite condition).

### 12. Late Filter
- Flag when: a Filter after a Join/Exchange drops >90% of rows.
- Report: filter node, rows in/out, recommendation (push filter below join, check non-pushable predicates).

### 13. Python UDF / Row-based Execution
- Flag when: BatchEvalPython, ArrowEvalPython, or ColumnarToRow/RowToColumnar appears in a hot stage.
- Report: node, stage time share, recommendation (native functions, pandas/Arrow UDF, avoid Photon/codegen blockers).

### 14. Task Straggler / Duration Skew
- Flag when: max task duration >5x median in a stage, even if data sizes are balanced.
- Report: stage, median/max duration, input sizes of slowest tasks, likely cause (expensive keys, slow node, GC).

## Secondary patterns (report as Additional observations)

- 15 Repeated scans: same table scanned >=2 times; suggest CTE/cache.
- 16 Wide scans: ReadSchema has many columns / SELECT *; suggest column pruning.
- 17 Expensive Sort/Window: large spill or window without partition key.
- 18 Aggregation fallback: HashAggregate falls back to SortAggregate, or large spill.
- 19 AQE not applied: AdaptiveSparkPlan isFinalPlan=false or AQE disabled.
- 20 Failed/retried tasks: failed or speculative tasks, executor loss, fetch failures.
- 21 Driver bottleneck: collect, long BroadcastExchange build, gaps between jobs.
- Others: Photon fallback, Delta layout (Z-ORDER/liquid clustering), non-pushdown predicates (functions/casts on columns), Union with many branches, idle cluster time.
