# Contributing

Thanks for considering a contribution to Databricks Spark Analyzer MCP.

## Development setup

```bash
git clone https://github.com/<your-user>/databricks-spark-analyzer-mcp.git
cd databricks-spark-analyzer-mcp
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
```

## Running tests

```bash
pytest -q
```

## Code style

- keep the project small and focused
- prefer clear, typed Python code
- add tests for bug fixes and new features
- keep logic and MCP payload formatting explicit

## Pull requests

1. Create a feature branch.
2. Add or update tests.
3. Run the test suite.
4. Open a pull request with a clear summary and motivation.

## Good contributions

Examples of helpful contributions include:

- new Spark anti-pattern checks
- better plan parsing helpers
- clearer result formatting
- improved OpenCode / MCP integration examples
- fixes for edge cases in Databricks event-log parsing

## Reporting issues

Please include:

- the Databricks runtime or cluster version
- a short description of the symptom
- relevant Spark plan or stage metrics when available
- the exact command or tool invocation used
