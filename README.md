# AI Jobs Salary Pipeline

This Week 11 project uses an Airflow pipeline to join AI job postings with country data, then uses the cleaned table in two LangGraph salary workflows. The optional bonus is a hiring analytics brief for a new AI role. The repository started from the class AutoML Research Lab; the assignment workflows are separate from its demonstration graphs.

| Part | What runs | Main output |
| --- | --- | --- |
| 1 | Scheduled Airflow DAG | PostgreSQL tables, salary chart, cleaned CSV |
| 2 | Agent-based model loop | Validation trials and holdout MAE/R² |
| 3 | Adaptive six-agent graph | Role budget brief, retry path, evaluation suite |

## Data and Part 1 pipeline

The two independent sources are the [AI Jobs Market 2025–2026 Kaggle dataset](https://www.kaggle.com/datasets/alitaqishah/ai-jobs-market-2025-2026-salaries), used in my earlier project, and a [2024 World Bank GDP per capita snapshot](https://data.worldbank.org/indicator/NY.GDP.PCAP.CD). Their committed CSVs are in [`datasets/ai_jobs`](datasets/ai_jobs). The DAG also extracts a job-skill table from the postings. It cleans jobs, skills, and country observations in parallel; joins skill counts and GDP by country; loads four tables into PostgreSQL; then reads the joined table back for analysis. Global postings keep a null GDP value. The Monday 09:00 UTC schedule uses `LocalExecutor` and allows one load run at a time.

Start Docker Desktop, copy `.env.example` to `.env`, and run from the repository root:

```bash
cp .env.example .env
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow up --build -d airflow-db airflow-init airflow-scheduler airflow-webserver
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags unpause ai_jobs_salary_pipeline
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags trigger ai_jobs_salary_pipeline
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags list-runs -d ai_jobs_salary_pipeline
```

Airflow is at <http://localhost:8088> (`admin` / `admin` by default). The DAG writes its chart and JSON report under `artifacts/airflow/ai_jobs/` and publishes `datasets/ai_jobs/ai_jobs_clean_latest.csv` for the agent workflows. Two runs of the revised DAG succeeded on November 5, 2026. PostgreSQL held 1,500 joined jobs, 9,428 distinct job-skill pairs, and 13 country observations after the rerun. GDP matched 1,418 jobs; the remaining 82 were Global. Mean annual salary in these records was $194,892, with Architecture highest by category at $251,576.92. These are descriptive figures, not causal effects.

![Mean annual salary by category](docs/images/ai_jobs_salary_by_category.png)

Evidence: [successful Airflow Grid run](docs/images/ai_jobs_airflow_grid.png) · [DAG Graph](docs/images/ai_jobs_airflow_graph.png). [Part 1 methods and checks](docs/AI_JOBS_PART1.md).

## Part 2: salary model loop

The independent [`ai_jobs_loop`](ai_jobs_loop/graph.py) adapts the class exercise's agent pattern to salary regression. A Data Auditor checks the Airflow handoff, an Experiment Planner chooses the next regressor, a Model Trainer measures validation error, an Evaluation Judge may loop back, and a Holdout Reporter scores the selected model once. IDs and salary-derived fields are excluded. The split is 60% train, 20% validation, and 20% holdout with seed 42.

After running Part 1, start the web app with `make start-mac` or `make start-windows` (Git Bash) and open <http://127.0.0.1:5173/ai-jobs-loop>. Click **Run agent loop**. No API key is required. With three trials, Ridge had the lowest validation MAE ($44,479.50). Its holdout MAE was $43,857.32 and R² was 0.2681, compared with $53,456.47 MAE for a mean-prediction baseline. The modest R² leaves substantial salary variation unexplained.

Evidence: [live agent graph](docs/images/ai_jobs_agent_graph.png) · [model results](docs/images/ai_jobs_model_results.png).

## Part 3: adaptive salary budget brief

The bonus scenario is a hiring analytics team preparing a salary budget reference for a new AI role. The separate [`salary_budget_agents`](salary_budget_agents/graph.py) graph has a Data Steward, Strategy Planner, Model Trainer, Quality Reviewer, Recovery Agent, and Report Writer. The planner chooses job-only or GDP-enriched features from the data audit; the reviewer routes by measured validation gain; the recovery agent retries a failed training step once. The reporter opens the sealed holdout after model selection and gives an analyst-facing role estimate. This is a budget reference, not an individual offer recommendation.

Open <http://127.0.0.1:5173/salary-budget> after `make start-mac` or `make start-windows`. Enter the role details and click **Run workflow**. The dashboard shows the backend graph, its actual route, validation trials, the role estimate, and the evaluation suite. **Simulate one trainer failure** exercises the retry branch. Run the five-case suite in the dashboard or by CLI:

```bash
docker compose -f infra/docker-compose.yml --env-file .env exec gateway env PYTHONPATH=/:/app python -m salary_budget_agents.evaluate
```

The CLI writes `artifacts/salary_budget/evaluation.json`; each case records its input, expected behavior, scoring checks, and pass threshold. All **5/5 cases passed** locally. The default AI Engineering example received a $212,087.06 annual salary reference. On the same holdout, the fixed category-mean comparator had $51,735.16 MAE and 0.0718 R²; the Part 2 loop and Part 3 graph both had $43,857.32 MAE and 0.2681 R². Part 3 matched Part 2 on predictive error while adding conditional feature routing, bounded recovery, and explicit coordination checks.

Evidence: [adaptive graph view](docs/images/salary_budget_graph.png) · [evaluation and baseline comparison](docs/images/salary_budget_evaluation.png). [Part 3 design, test cases, and comparison limits](docs/AI_JOBS_PART3.md).

## Repository map

| Path | Purpose |
| --- | --- |
| [`airflow/dags/ai_jobs_pipeline.py`](airflow/dags/ai_jobs_pipeline.py) | Ingestion, cleaning, join, database load, analysis |
| [`ai_jobs_loop/`](ai_jobs_loop) | Part 2 salary model loop |
| [`salary_budget_agents/`](salary_budget_agents) | Part 3 graph and evaluation suite |
| [`frontend/src/pages/`](frontend/src/pages) | Live agent dashboards |
| [`services/gateway/app/main.py`](services/gateway/app/main.py) | Dashboard API and event streams |
| [`infra/`](infra) | Docker Compose and service images |
| [`docs/architecture.md`](docs/architecture.md) | Data flow and runtime boundaries |

Runtime outputs and `.env` are ignored by Git. The retained class-demo services and data support the original app routes and the Part 2 comparison; they are not sources for the AI jobs pipeline.
