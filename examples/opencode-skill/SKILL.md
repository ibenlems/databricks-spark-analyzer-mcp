---
name: spark-sql-plan-analyzer
description: Use whenever the Databricks MCP server is called to analyze Spark application performance, the slowest SQL query, its final plan, stages, tasks or node metrics. Detects broadcast, skew, coalesce, spill, full-scan, join-explosion, shuffle, small-file, UDF and straggler issues.
---

# Spark SQL Plan Analyzer

Inspect the final plan of the slowest SQL operation of a Spark application and report only real, evidenced issues.

## Workflow
1. Using the Databricks MCP tools, fetch only the slowest running SQL execution for the application, then its final plan, node metrics and stage/task metrics.
2. Start with a concise checklist (3-7 conceptual bullets) of the analysis steps.
3. Evaluate every pattern in `patterns.md` against the plan.
4. Validate each finding: does the evidence meet the pattern threshold? If not, drop it or self-correct.
5. Report only patterns where an issue exists. Omit all others.
6. After the baseline patterns, you may add "Additional observations" supported by plan evidence. Keep them separate and clearly labeled.

## Constraints
- Advisory partition size = 128 MB (`spark.sql.adaptive.advisoryPartitionSizeInBytes`).
- Always cite stageId and taskId; if unavailable, use nodeId.
- Never guess missing metrics. State "not available" instead.
- Pattern 1 applies to non-broadcast joins; Pattern 6 applies to broadcast joins.
- Read-only analysis: do not modify tables, configs or jobs.

## Output (per issue)
- Pattern name and severity
- Stage ID / Task ID (or Node ID)
- Evidence (concrete numbers)
- Likely cause
- Recommendation
