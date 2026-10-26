# Databricks Spark Analyzer MCP

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)


A lightweight MCP server for Databricks that helps coding agents inspect Databricks Spark job runs, query plans, stage behavior, and cluster bottlenecks using real execution evidence.

This project is built as an MCP server first. It can be connected to any MCP-compatible coding agent, IDE, or local workflow. An example integration with [OpenCode](https://opencode.ai/) is provided below.

## Table of contents

- [What this project gives you](#what-this-project-gives-you)
- [Why integrate it](#why-integrate-it)
- [Quick start](#quick-start)
  - [1) Install](#1-install)
  - [2) Configure environment](#2-configure-environment)
  - [3) Run the server](#3-run-the-server)
- [MCP integration: use it with any coding agent](#mcp-integration-use-it-with-any-coding-agent)
- [OpenCode example (reference only)](#opencode-example-reference-only)
- [Tools exposed by the server](#tools-exposed-by-the-server)
- [Example prompt for an integrated agent](#example-prompt-for-an-integrated-agent)
- [Example result format](#example-result-format)
- [What patterns are available](#what-patterns-are-available)
- [Custom patterns and thresholds](#custom-patterns-and-thresholds)
- [Development and tests](#development-and-tests)
- [Contributing](#contributing)
- [License](#license)
- [Publish to GitHub](#publish-to-github)
- [Summary](#summary)

## What this project gives you

- Databricks run-level Spark analysis via MCP tools
- stage-level investigation and evidence collection
- query-plan review for skew, spill, joins, filters, window logic, and shuffle pressure
- structured output that can be consumed by any AI agent, not only OpenCode
- optional, user-defined pattern rules and thresholds

## Why integrate it

A Spark job often fails or slows down because the evidence is spread across many places: stage metrics, SQL plans, executor data, cluster config, and logs. This server brings that evidence into a single MCP interface that any agent can query consistently.

## Quick start

### 1) Install

```bash
git clone https://github.com/ibenlems/databricks-spark-analyzer-mcp.git
cd databricks-spark-analyzer-mcp

python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
```

If you prefer `uv`:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e '.[dev]'
```

### 2) Configure environment

```bash
export DATABRICKS_HOST="https://<your-workspace-host>"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

### 3) Run the server

```bash
databricks-spark-analyzer-mcp
```

Or:

```bash
python -m databricks_mcp_server
```

## MCP integration: use it with any coding agent

This project is designed to be generic. It is not tied to OpenCode. The same server can be plugged into any MCP client that supports local stdio or similar tool execution.

Typical MCP configuration pattern:

```json
{
  "mcpServers": {
    "databricks-spark-analyzer": {
      "command": "/absolute/path/to/databricks-spark-analyzer-mcp/.venv/bin/databricks-spark-analyzer-mcp",
      "env": {
        "DATABRICKS_HOST": "https://<your-workspace-host>",
        "DATABRICKS_TOKEN": "<your-token>"
      }
    }
  }
}
```

The exact JSON shape depends on the agent, but the important point is this: the server is reusable across many environments. OpenCode is just a concrete example, not a requirement.

## OpenCode example (reference only)

OpenCode is a convenient reference integration, but not the only supported use case.

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

Typical OpenCode config path:

```bash
~/.config/opencode/opencode.jsonc
```

Then restart OpenCode and the tools should appear as MCP capabilities.

## Tools exposed by the server

The server exposes tools such as:

- `analyze_run_by_id(run_id, job_id=None, stage_plans=None)`
- `analyze_stage(run_id, stage_id, job_id=None)`
- `fetch_job_run_context(run_id, job_id=None)`
- `analyze_run(...)`

These let an agent fetch Databricks run evidence and summarize Spark patterns, cluster characteristics, task skew, spill, joins, and slow stages.

## Example prompt for an integrated agent

```text
Use the Databricks Spark Analyzer MCP tool to inspect run 12345 in job 99.
Focus on spill, skew, shuffle pressure, missing broadcast joins, and cluster sizing.
Return only the highest-priority issues with evidence and remediation suggestions.
```

## Example result format

This is the kind of output the tool is designed to generate after a run-level analysis:

```text
Status was SUCCESS. The run had 102 stages and 3,996 tasks, with 77.5 min of total task time. Input was 64.6 GiB, shuffle write 4.67 GiB, memory spill 25.6 GiB and disk spill 1.47 GiB. GC was 6.2% of task time, and the cluster ran Spark 15.4 without Photon on 8 cores.

Limits of this analysis
- I used the MCP run-level analysis. I did not pull the single slowest SQL execution's final plan or node metrics.
- Task IDs are not available, so I cite stage IDs.
- 33 of 102 stages have no query plan, because 12 events exceeded the 2 MiB event-log limit.

Issues that meet the skill thresholds
1. High spill (Pattern 4), severity High
- Stage 157: 10 of 10 tasks spilled. Memory spill was 6.88 GiB and disk spill 371 MiB.
- Stage 161: 9 of 10 tasks spilled. Memory spill was 5.91 GiB and disk spill 382 MiB.
- Likely cause: only 10 shuffle partitions in the heavy stages and limited memory per core.
- Recommendation: raise shuffle partition count or use larger memory-optimized workers.

2. Task straggler (Pattern 14), severity Medium
- Stage 2: median 124 ms, max 3.7 s.
- Recommendation: check hot keys, enable AQE skew handling, and inspect data distribution.

3. Missing broadcast join (Pattern 1), probable
- Evidence: 3 SortMergeJoins and 0 broadcast joins in the plan.
- Recommendation: test a BROADCAST hint on the smaller join side or raise autoBroadcastJoinThreshold.

Additional observations
- Window with no PARTITION BY: this is the highest-priority fix.
- Repeated scans of the same large table are visible and should be cached or materialized once.
```

## What patterns are available

This project includes an opinionated set of Spark anti-pattern checks, with example pattern definitions under:

- [examples/opencode-skill/patterns.md](examples/opencode-skill/patterns.md)
- [examples/opencode-skill/SKILL.md](examples/opencode-skill/SKILL.md)

These include patterns such as:

- missing broadcast join
- skewed partitions
- high spill
- full scans without partition/date filters
- small files / too many tasks
- excessive shuffle volume
- missing partition keys in window logic
- task stragglers
- repeated scans and wide scans

You can add, remove, or tighten any of these rules depending on your workload.

## Custom patterns and thresholds

The analyzer can be customized to match your team’s risk profile. In practice, the signal is usually built from three things:

- pattern name
- severity
- threshold or condition

Example:

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
        "recommendation": "Add a partition key or rewrite the window logic."
    }
]
```

That gives you a simple way to create a stronger or more specific analysis policy without changing the MCP server itself.

## Development and tests

```bash
pytest -q
```

Or with `uv`:

```bash
uv run pytest -q
```

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, testing, and PR guidance.

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

## Summary

This repository is an MCP-first Databricks Spark analyzer that can be plugged into any coding agent. OpenCode is only one of many possible clients and is included as a working reference example, not as the main requirement.
