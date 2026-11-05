# Assignment architecture

```text
Kaggle AI job postings ──┬─> clean jobs ─────────┐
                         └─> clean job skills ───┤
World Bank 2024 GDP ─────────> clean countries ───┴─> join ─> PostgreSQL
                                                               │
                                                               ├─> database analysis + chart
                                                               └─> cleaned CSV handoff
                                                                       │
                                              ┌────────────────────────┴────────────────────────┐
                                              │                                                 │
                                      Part 2 salary loop                           Part 3 budget agents
                                      model comparison                             role estimate + evaluation
```

The [Airflow DAG](../airflow/dags/ai_jobs_pipeline.py) schedules the source inspections and independent cleaning tasks, then waits for all inputs before joining. A single transactional load replaces the `ai_jobs` schema tables, so reruns do not duplicate rows. The analysis task reads from PostgreSQL and publishes the cleaned CSV. The schedule is Mondays at 09:00 UTC, with `catchup=False`, `LocalExecutor`, and `max_active_runs=1`.

Both modeling workflows read the same published CSV and use seed 42 with a 60/20/20 train, validation, and holdout split. [Part 2](../ai_jobs_loop/graph.py) adapts the class loop pattern to salary regression. [Part 3](../salary_budget_agents/graph.py) is a separate LangGraph with its own state, nodes, conditional routes, and one-retry recovery path. It does not invoke the Part 2 graph during execution; [the evaluation suite](../salary_budget_agents/evaluate.py) runs Part 2 only as a comparator.

The Docker [gateway](../services/gateway/app/main.py) exposes readiness, server-sent workflow events, and the evaluation result to the React dashboards. The UI draws Part 3 nodes and edges from the backend graph rather than a fixed list. The gateway mounts the cleaned dataset and agent package; the Airflow profile also runs PostgreSQL. Outputs under `artifacts/` and the published CSV are local runtime files and are ignored by Git.

The upstream classroom app and its supporting services remain in the repository because its demo routes and original loop are still available. The assignment's data sources, DAG, two agent graphs, and evaluations are separated in the paths above.
