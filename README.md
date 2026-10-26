# Databricks Spark Analyzer MCP

[![CI](https://github.com/<your-user>/databricks-spark-analyzer-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-user>/databricks-spark-analyzer-mcp/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A lightweight MCP server for Databricks that helps AI coding agents and local tools analyze Spark job runs, query plans, slow stages, and cluster behavior using real execution evidence.

This project is designed for:

- performance debugging of Databricks jobs
- Spark plan review and anti-pattern detection
- cluster sizing and skew analysis
- AI-agent friendly structured results for local analysis workflows

## Why this exists

When a Databricks job fails or runs too slowly, the usual signal is a large amount of fragmented evidence: Spark plans, stage metrics, executor data, cluster config, and sometimes logs. This server brings that context together into structured MCP tools that can be used by OpenCode, IDE agents, or your own automation.

## Features

- analyze a job run by ID from Databricks
- inspect a single Spark stage in detail
- fetch the raw job context for AI review
- analyze offline data supplied manually in JSON
- detect common bottlenecks like skew, spill, shuffle pressure, under-parallelism, and bad joins
- summarize cluster health and likely right-sizing issues
- parse Spark plans and identify suspicious patterns from the physical plan

## Supported tools

The server exposes tools such as:

- `analyze_run_by_id(run_id, job_id=None, stage_plans=None)`
- `analyze_stage(run_id, stage_id, job_id=None)`
- `fetch_job_run_context(run_id, job_id=None)`
- `analyze_run(...)`

## Quick start

### 1) Clone the repo

```bash
git clone https://github.com/<your-user>/databricks-spark-analyzer-mcp.git
cd databricks-spark-analyzer-mcp
```

### 2) Create the environment

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
```

If you use `uv`:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e '.[dev]'
```

### 3) Set Databricks credentials

```bash
export DATABRICKS_HOST="https://<your-workspace-host>"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

### 4) Run the MCP server

```bash
databricks-spark-analyzer-mcp
```

You can also run it as a module:

```bash
python -m databricks_mcp_server
```

## Example payload for offline analysis

```json
{
  "run": {
    "run_id": 101,
    "job_id": 99,
    "state": {"result_state": "TERMINATED", "life_cycle_state": "TERMINATED"}
  },
  "tasks": [
    {"task_key": "t1", "stage_id": 1, "executor_run_time": 120000},
    {"task_key": "t2", "stage_id": 2, "executor_run_time": 110000}
  ],
  "plan": "== Physical Plan ==\n*(2) SortMergeJoin [customer_id]\n+- Exchange hashpartitioning(customer_id, 200)",
  "stage_plans": {
    "2": "== Physical Plan ==\n*(2) SortMergeJoin [customer_id]\n+- Exchange hashpartitioning(customer_id, 200)"
  }
}
```

## Example: successful run analysis

Here is the kind of result the analyzer can produce when you ask it to summarize a successful run with evidence and recommended actions.

```text
Status was SUCCESS. The run had 102 stages and 3,996 tasks, with 77.5 min of total task time. Input was 64.6 GiB, shuffle write 4.67 GiB, memory spill 25.6 GiB and disk spill 1.47 GiB. GC was 6.2% of task time, and the cluster ran Spark 15.4 without Photon on 8 cores.

Limits of this analysis
- I used the MCP run-level analysis. I did not pull the single slowest SQL execution's final plan or node metrics.
- Task IDs are not available, so I cite stage IDs.
- 33 of 102 stages have no query plan, because 12 events exceeded the 2 MiB event-log limit. Join and plan-based patterns are incomplete for those stages.

Issues that meet the skill thresholds
1. High spill (Pattern 4), severity High
- Stage 157: 10 of 10 tasks spilled. Memory spill was 6.88 GiB and disk spill 371 MiB. Shuffle read was 580 MiB, about 58 MiB per task. Peak execution memory was 1.4 GB.
- Stage 161: 9 of 10 tasks spilled. Memory spill was 5.91 GiB and disk spill 382 MiB.
- Stages 150 and 153: also flagged for spill, but I don't have their numbers.
- Likely cause: only 10 shuffle partitions in the heavy stages, plus about 3.5 GB of memory per core.
- Recommendation:
- Raise the partition count for these stages, or lower the advisory partition size.
- Use memory-optimized workers, or fewer cores per executor.
2. Task straggler (Pattern 14), severity Medium
- Stage 2: median 124 ms, max 3.7 s (about 30x).
- Stage 21: median 114 ms, max 2.2 s (about 19x), with 209 tasks and 40% CPU efficiency.
- Likely cause: one dominant file or key. Stage 17 also has input skew: the largest task read 109 MiB against a 330 KiB median. That is 339x, but the absolute size is tiny.
- Impact: small in absolute terms, with stragglers adding 2–4 s per stage.
- Recommendation: enable AQE skew-join handling, and check hot keys with a groupBy(key).count().
3. Missing broadcast join (Pattern 1), probable
- Evidence: the plan has 3 SortMergeJoins and 0 broadcast joins, with no skew handling. Join keys are flight_uid, report_uid and algorithm_version, feature_name, algorithm_name, with semi, anti and inner joins.
- Table sizes: reports_flights is about 150 MiB, flights_info about 28 MiB and features_table_raw about 104 MiB, all well under 1 GB.
- Not available: the exact size of each join side. I can't confirm the strategy was chosen wrongly.
- Stages: 12–25 and 270–277.
- Recommendation: add a BROADCAST hint on the small side, or raise autoBroadcastJoinThreshold.

Additional observations
- Window with no PARTITION BY (Pattern 17): 10 RunningWindowFunctionExec warnings, in stages 294, 296, 297, 299 and 302. All rows go through a single task. This is the highest-priority fix in the MCP analysis. Add a partition key or rewrite the logic.
- Repeated scans (Pattern 15): reports_data (21.25 GiB) is read 3 times, 63.7 GiB in total. Read it once and cache it, or write an intermediate table.
- Single-partition exchanges: Exchange SinglePartition appears across stages 247–266. Check that the data is small.
- Row-buffer spill: 1,356 events in stages 189, 211 and 239. These suggest large window or join groups.
- Small files:
- Tables like reports_flights, reports_parameters and features_table_raw have 100–385 files each, with medians of 3–17 KiB.
- It is below the "tens of thousands of tasks" threshold, so I am not reporting it as Pattern 8.
- Running OPTIMIZE would still help.
- GC pressure: stage 143 spent 10.2% of its task time in GC (97.5 s). Stages 143 and 146 each scan about 3.17 B rows and about 21 GiB. I did not check them against the full-scan pattern, because I can't see their filters.
- Other:
- AQE coalescing would help the 15 stages with many tiny tasks (about 200 tasks, 9–114 ms median).
- Photon is not enabled.
- Up to 14 queries ran at once on 8 cores.
- 2 storage PathAlreadyExists (409) errors occurred in stages 268 and 269.

Dropped findings
- Skewed partitions (Pattern 2): the largest partition was 109 MiB, far below the 5 GB threshold.
- Excessive shuffle (Pattern 9): the 4.67 GiB shuffle write is under 100 GB.

Suggested priority
1. Fix the unpartitioned window.
2. Increase shuffle partitions or memory for stages 157 and 161 to remove the spill.
3. Broadcast the small join sides.
4. Cache or reuse the reports_data scan.
5. Compact the small tables and enable Photon.
```

## Custom patterns and skill thresholds

This project was designed to be pattern-driven. In practice, the analyzer can score and filter findings by a pattern name, severity, threshold, and a recommended action. That makes it easy to tune the result set for your own workload, team, or monitoring policy.

The idea is simple:

- define a pattern, for example `high_spill`, `broadcast_join_missing`, or `window_without_partition_by`
- attach a severity (`low`, `medium`, `high`, `critical`)
- define the rule: a metric threshold, plan match, or stage condition
- add a recommendation that the agent or user can apply immediately

Example pattern configuration:

```python
PATTERNS = [
    {
        "id": "high_spill",
        "name": "High spill",
        "severity": "high",
        "condition": lambda s: s.get("memory_spill_gib", 0) > 4.0 or s.get("disk_spill_gib", 0) > 0.5,
        "recommendation": "Increase shuffle partitions or use larger memory-optimized executors."
    },
    {
        "id": "window_without_partition",
        "name": "Window without PARTITION BY",
        "severity": "critical",
        "condition": lambda s: "RunningWindowFunctionExec" in (s.get("plan", "") or "") and "PARTITION BY" not in (s.get("plan", "") or ""),
        "recommendation": "Add a partition key or rewrite the window to avoid a single-task bottleneck."
    },
    {
        "id": "missing_broadcast_join",
        "name": "Missing broadcast join",
        "severity": "medium",
        "condition": lambda s: "SortMergeJoin" in (s.get("plan", "") or "") and "BroadcastHashJoin" not in (s.get("plan", "") or ""),
        "recommendation": "Consider a BROADCAST hint on the smaller side or increase the broadcast threshold."
    },
]
```

You can then filter the output like this:

```python
interesting = [
    p for p in PATTERNS
    if p["severity"] in {"high", "critical"}
]
```

This approach is especially useful when you want your analyzer to be opinionated about a specific workload. For example:

- data platform teams may care most about skew and spill
- ML pipelines may care about `small files`, `repeated scans`, and `window without partition`
- batch ETL teams may want stricter thresholds for `shuffle write`, `spill`, and `broadcast join` misses

You can add the exact patterns you want by editing your analyzer rules, then let OpenCode or your MCP agent summarize only the findings that cross your thresholds.

## OpenCode skill example

If you want to turn this into a reusable skill, you can tell OpenCode to prioritize specific patterns when analyzing a run. Example prompt:

```text
Use the Databricks Spark Analyzer MCP to review this run.
Focus on the following patterns only: high spill, window without PARTITION BY, missing broadcast join, repeated scans, and skewed tasks.
Treat high spill and window-without-partition as critical findings.
Return a short summary with root cause, affected stages, recommended fix, and priority order.
```

This is the same idea as a "skill": the agent is guided to use the analyzer with a focused set of known-good checks, instead of blindly summarizing all evidence.

## OpenCode integration example

OpenCode can connect to the server through its MCP config. The example below is the pattern to use in your local OpenCode config.

Example file: `examples/opencode.jsonc`

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "model": "github-copilot/claude-sonnet-5.5",
  "mcp": {
    "databricks-spark-analyzer": {
      "type": "local",
      "command": [
        "/absolute/path/to/databricks-spark-analyzer-mcp/.venv/bin/databricks-spark-analyzer-mcp"
      ],
      "enabled": true,
      "environment": {
        "DATABRICKS_HOST": "https://<your-workspace-host>",
        "DATABRICKS_TOKEN": "<your-token>"
      }
    }
  }
}
```

If you want to keep the token out of the repo file, set it in your shell and reference it from your environment loader instead of hardcoding it.

### OpenCode local config path

On Linux/macOS, a common location is:

```bash
~/.config/opencode/opencode.jsonc
```

Then restart OpenCode and the MCP server should appear as a connected tool provider.

## Example prompt for OpenCode

Once connected, you can ask things like:

```text
Use the Databricks Spark Analyzer MCP tool to analyze run 12345 in job 99.
Focus on skew, shuffle pressure, and cluster sizing.
Return only the most important findings and recommended actions.
```

Or:

```text
Inspect the slowest stage from this run and explain whether the issue looks like data skew, spill, or under-parallelism.
```

## Development

Run tests:

```bash
pytest -q
```

Or with `uv`:

```bash
uv run pytest -q
```

## Contributing

Contributions are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, testing, and PR guidance.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Publish to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin git@github.com:<your-user>/databricks-spark-analyzer-mcp.git
git push -u origin main
```

## Notes

This repo was created from the working Databricks MCP implementation and adapted for packaging and open-source publication. The core behavior remains the same: Spark evidence is retrieved from Databricks and translated into actionable diagnostics for conversion into AI agent workflows.
