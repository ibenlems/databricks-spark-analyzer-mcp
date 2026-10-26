# Databricks Spark Analyzer MCP

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
