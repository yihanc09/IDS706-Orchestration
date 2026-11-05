# Week 11 Mini-Assignment — Part 1: AI Jobs Airflow Pipeline

## Data and transformations

The first source is `datasets/ai_jobs/ai_jobs_market_2025_2026.csv` (1,500 AI job postings from my earlier AI Jobs project). Its original external provenance was not supplied. The second, independent source is a 2024 snapshot of the World Bank's [GDP per capita (current US$) indicator](https://data.worldbank.org/indicator/NY.GDP.PCAP.CD), stored at `datasets/ai_jobs/world_bank_gdp_per_capita_2024.csv`. I downloaded the observations for the 13 countries named in the jobs file from the [World Bank API](https://api.worldbank.org/v2/country/US;GB;SG;IN;JP;CN;CA;AE;NL;FR;AU;DE;CH/indicator/NY.GDP.PCAP.CD?date=2024&format=json&per_page=100) and committed the snapshot so runs are reproducible. Docker mounts both files at `/repo/datasets/ai_jobs/`.

Airflow creates three cleaned tables from the two sources:

| Table | Grain | Cleaning |
| --- | --- | --- |
| `ai_jobs.jobs` | One row per `job_id` | Trim text, convert numeric fields, remove duplicate IDs and rows with missing/invalid required values |
| `ai_jobs.job_skills` | One row per `(job_id, skill)` | Split `required_skills` on `|`, trim/lowercase, remove empty tokens and duplicate pairs |
| `ai_jobs.country_context` | One row per country | Check 2024 year, positive GDP per capita, unique ISO3 code, and coverage of all named job countries |

The DAG merges skill counts and a Python-skill flag with the jobs, then joins GDP per capita on ISO3 country code into `ai_jobs.jobs_enriched` (one row per job). The four tables live in a separate `ai_jobs` schema of the existing PostgreSQL container. The load replaces their contents in one transaction, so a retry does not duplicate rows. The 82 jobs labeled `Global` have no country-specific GDP observation; the left join keeps those jobs with a null GDP value.

The raw file has no blank fields or duplicate job IDs. Some skill lists repeat a token within a job; the skill table removes those duplicates. Some `annual_salary_usd` values are outside the source's `salary_min_usd`/`salary_max_usd` range. This pipeline uses the annual salary as reported and does not treat those ranges as validation rules.

## Graph, scheduling, and analysis

```text
inspect_jobs_source ─┬─> clean_jobs ───────────┐
                     └─> clean_skills ─────────┤
inspect_gdp_source ─────> clean_country_context ┴─> merge_tables ─> load_postgres ─> analyze_from_database
```

The DAG ID is `ai_jobs_salary_pipeline`. It runs Mondays at 09:00 UTC, with `catchup=False`. The two source inspections and three cleaning tasks can run in parallel under the configured `LocalExecutor`; the merge waits for all three cleaned inputs. `max_active_runs=1` prevents overlapping full-table loads.

The analysis task reads `ai_jobs.jobs_enriched` back from PostgreSQL, computes job counts and mean salary by category, and creates a bar chart. Per-run outputs are in `artifacts/airflow/ai_jobs/<run-key>/`: `jobs_clean.csv`, `skills_clean.csv`, `country_context_clean.csv`, `jobs_enriched.csv`, `salary_by_category.csv`, `salary_by_category.png`, and `analysis.json`. It also writes `datasets/ai_jobs/ai_jobs_clean_latest.csv` from the database as the Part 2 handoff.

![Mean annual salary by AI job category](images/ai_jobs_salary_by_category.png)

## Run

From the repository root, start Docker Desktop and run:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow up --build -d airflow-db airflow-init airflow-scheduler airflow-webserver
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags unpause ai_jobs_salary_pipeline
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags trigger ai_jobs_salary_pipeline
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-scheduler airflow dags list-runs -d ai_jobs_salary_pipeline
```

Airflow UI: <http://localhost:8088>. Open the DAG and its Graph and Grid tabs. The [Grid screenshot](images/ai_jobs_airflow_grid.png) shows the successful runs and eight tasks; the [Graph screenshot](images/ai_jobs_airflow_graph.png) shows the task dependencies. The default login is `admin` / `admin`, unless changed in `.env`.

Check the database and report:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile airflow exec airflow-db psql -U airflow -d airflow -c 'SELECT COUNT(*) FROM ai_jobs.jobs_enriched;'
ls artifacts/airflow/ai_jobs/*/analysis.json
ls artifacts/airflow/ai_jobs/*/salary_by_category.png
```

The published CSV has target `annual_salary_usd`, suitable for a regression workflow in Part 2. It contains `job_id` for traceability; exclude it from model features. The category analysis is descriptive and does not establish a causal salary effect.

## Verified local run (November 5, 2026)

The revised DAG completed twice successfully after two successful runs of the earlier version. After the second revised run, PostgreSQL still contained 1,500 jobs, 9,428 distinct job-skill links, 13 country observations, and 1,500 enriched records. GDP per capita matched 1,418 jobs, leaving only the 82 Global jobs unmatched. The mean annual salary was $194,892. Architecture had the highest category mean at $251,576.92. These are descriptive results from the supplied postings, not model evaluation metrics. The latest run report and chart were generated under `artifacts/airflow/ai_jobs/8e889f6769e78233/`.
