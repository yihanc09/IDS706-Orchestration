"""Scheduled AI jobs ETL: two cleaned tables, PostgreSQL, and salary analysis."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import hashlib
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.decorators import dag, task


ROOT = Path("/repo")
SOURCE = ROOT / "datasets/ai_jobs/ai_jobs_market_2025_2026.csv"
RUNS = ROOT / "artifacts/airflow/ai_jobs"
PUBLISH = ROOT / "datasets/ai_jobs/ai_jobs_clean_latest.csv"
REQUIRED = {"job_id", "job_category", "annual_salary_usd", "required_skills"}


def run_dir(context):
    # Airflow run IDs can contain slashes and colons; use a stable safe directory.
    key = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
    path = RUNS / key
    path.mkdir(parents=True, exist_ok=True)
    return path


def db_connection():
    return psycopg2.connect(
        host=os.environ.get("AI_JOBS_DB_HOST", "airflow-db"),
        dbname=os.environ.get("AI_JOBS_DB_NAME", "airflow"),
        user=os.environ.get("AI_JOBS_DB_USER", "airflow"),
        password=os.environ.get("AI_JOBS_DB_PASSWORD", "airflow"),
    )


@dag(
    dag_id="ai_jobs_salary_pipeline",
    description="AI job listings and skills -> PostgreSQL -> salary analysis",
    start_date=datetime(2026, 11, 1),
    schedule="0 9 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["assignment", "ai-jobs", "etl"],
)
def ai_jobs_salary_pipeline():
    @task
    def inspect_source():
        if not SOURCE.is_file():
            raise FileNotFoundError(SOURCE)
        columns = set(pd.read_csv(SOURCE, nrows=0).columns)
        missing = REQUIRED - columns
        if missing:
            raise ValueError(f"Missing source columns: {sorted(missing)}")
        return str(SOURCE)

    @task
    def clean_jobs(source_path: str, **context):
        """Clean one record per posting, keeping predictor columns for Part 2."""
        df = pd.read_csv(source_path)
        columns = [
            "job_id", "job_title", "job_category", "experience_level",
            "years_of_experience", "annual_salary_usd", "country", "remote_work",
            "industry", "demand_score", "posting_year", "posting_month", "is_llm_role",
        ]
        df = df[columns].copy()
        for column in ["job_id", "job_title", "job_category", "experience_level", "country", "remote_work", "industry"]:
            df[column] = df[column].astype("string").str.strip()
        for column in ["years_of_experience", "annual_salary_usd", "demand_score", "posting_year", "posting_month", "is_llm_role"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        before = len(df)
        df = df.drop_duplicates(subset="job_id", keep="first")
        df = df.dropna(subset=columns)
        df = df[(df.job_id != "") & (df.annual_salary_usd > 0)]
        df = df[(df.posting_month.between(1, 12)) & (df.is_llm_role.isin([0, 1]))]
        df = df[(df.years_of_experience >= 0) & (df.demand_score.between(0, 100))]
        out = run_dir(context) / "jobs_clean.csv"
        df.to_csv(out, index=False)
        return {"path": str(out), "rows": len(df), "removed": before - len(df)}

    @task
    def clean_skills(source_path: str, **context):
        """Explode pipe-separated skills and deduplicate each job-skill pair."""
        df = pd.read_csv(source_path, usecols=["job_id", "required_skills"])
        df["job_id"] = df["job_id"].astype("string").str.strip()
        df["required_skills"] = df["required_skills"].fillna("").astype(str)
        df["skill"] = df["required_skills"].str.split("|")
        df = df.explode("skill")
        df["skill"] = df["skill"].astype("string").str.strip().str.lower()
        df = df.dropna(subset=["job_id", "skill"])
        df = df[(df.job_id != "") & (df.skill != "")]
        df = df[["job_id", "skill"]].drop_duplicates()
        out = run_dir(context) / "skills_clean.csv"
        df.to_csv(out, index=False)
        return {"path": str(out), "rows": len(df)}

    @task
    def merge_tables(jobs_meta: dict, skills_meta: dict, **context):
        jobs = pd.read_csv(jobs_meta["path"])
        skills = pd.read_csv(skills_meta["path"])
        skills = skills[skills.job_id.isin(jobs.job_id)]
        features = skills.groupby("job_id").agg(
            skill_count=("skill", "count"),
            has_python=("skill", lambda values: int("python" in set(values))),
        ).reset_index()
        merged = jobs.merge(features, on="job_id", how="left", validate="one_to_one")
        merged["skill_count"] = merged["skill_count"].fillna(0).astype(int)
        merged["has_python"] = merged["has_python"].fillna(0).astype(int)
        if len(merged) != len(jobs):
            raise ValueError("Merge changed posting count")
        out_dir = run_dir(context)
        skills_out = out_dir / "skills_valid.csv"
        merged_out = out_dir / "jobs_enriched.csv"
        skills.to_csv(skills_out, index=False)
        merged.to_csv(merged_out, index=False)
        return {"jobs_path": jobs_meta["path"], "skills_path": str(skills_out), "merged_path": str(merged_out), "rows": len(merged), "skills": len(skills)}

    @task
    def load_postgres(meta: dict):
        """Replace all three business tables atomically so reruns are idempotent."""
        jobs = pd.read_csv(meta["jobs_path"])
        skills = pd.read_csv(meta["skills_path"])
        merged = pd.read_csv(meta["merged_path"])
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS ai_jobs")
                cur.execute("""CREATE TABLE IF NOT EXISTS ai_jobs.jobs (
                    job_id text PRIMARY KEY, job_title text, job_category text,
                    experience_level text, years_of_experience numeric,
                    annual_salary_usd numeric, country text, remote_work text,
                    industry text, demand_score numeric, posting_year integer,
                    posting_month integer, is_llm_role integer)""")
                cur.execute("""CREATE TABLE IF NOT EXISTS ai_jobs.job_skills (
                    job_id text REFERENCES ai_jobs.jobs(job_id), skill text,
                    PRIMARY KEY (job_id, skill))""")
                cur.execute("""CREATE TABLE IF NOT EXISTS ai_jobs.jobs_enriched (
                    job_id text PRIMARY KEY REFERENCES ai_jobs.jobs(job_id),
                    job_title text, job_category text, experience_level text,
                    years_of_experience numeric, annual_salary_usd numeric,
                    country text, remote_work text, industry text, demand_score numeric,
                    posting_year integer, posting_month integer, is_llm_role integer,
                    skill_count integer, has_python integer)""")
                cur.execute("TRUNCATE ai_jobs.job_skills, ai_jobs.jobs_enriched, ai_jobs.jobs")
                job_cols = list(jobs.columns)
                merged_cols = list(merged.columns)
                execute_values(cur, f"INSERT INTO ai_jobs.jobs ({','.join(job_cols)}) VALUES %s", list(jobs.itertuples(index=False, name=None)))
                execute_values(cur, "INSERT INTO ai_jobs.job_skills (job_id,skill) VALUES %s", list(skills.itertuples(index=False, name=None)))
                execute_values(cur, f"INSERT INTO ai_jobs.jobs_enriched ({','.join(merged_cols)}) VALUES %s", list(merged.itertuples(index=False, name=None)))
        return {"rows": len(merged), "skills": len(skills)}

    @task
    def analyze_from_database(load_meta: dict, **context):
        """Read back from PostgreSQL, save chart, metrics, and Part 2 handoff."""
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT job_id, job_category, annual_salary_usd, remote_work, years_of_experience, demand_score, is_llm_role, skill_count, has_python FROM ai_jobs.jobs_enriched ORDER BY job_id")
                columns = [col.name for col in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=columns)
        if len(df) != load_meta["rows"]:
            raise ValueError("Database read-back row count mismatch")
        df["annual_salary_usd"] = pd.to_numeric(df["annual_salary_usd"])
        summary = df.groupby("job_category").agg(
            jobs=("job_id", "count"), mean_salary_usd=("annual_salary_usd", "mean")
        ).sort_values("mean_salary_usd", ascending=False)
        out_dir = run_dir(context)
        summary.to_csv(out_dir / "salary_by_category.csv")
        plt.figure(figsize=(10, 6))
        summary.sort_values("mean_salary_usd")["mean_salary_usd"].plot.barh()
        plt.xlabel("Mean annual salary (USD)")
        plt.ylabel("Job category")
        plt.title("AI job salary by category")
        plt.tight_layout()
        plt.savefig(out_dir / "salary_by_category.png", dpi=150)
        plt.close()
        report = {
            "rows": len(df), "skill_links": load_meta["skills"],
            "overall_mean_salary_usd": round(float(df.annual_salary_usd.mean()), 2),
            "top_salary_category": str(summary.index[0]),
            "top_category_mean_salary_usd": round(float(summary.iloc[0].mean_salary_usd), 2),
            "chart": str(out_dir / "salary_by_category.png"),
        }
        (out_dir / "analysis.json").write_text(json.dumps(report, indent=2))
        PUBLISH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(PUBLISH, index=False)
        return report

    source = inspect_source()
    # Both transforms depend only on the source check, so LocalExecutor can run them in parallel.
    jobs = clean_jobs(source)
    skills = clean_skills(source)
    merged = merge_tables(jobs, skills)
    loaded = load_postgres(merged)
    analyze_from_database(loaded)


ai_jobs_salary_pipeline()
