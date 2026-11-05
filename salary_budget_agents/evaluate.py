"""Reproducible quality and coordination checks for the bonus workflow.

Run from the repository root: python -m salary_budget_agents.evaluate
The fixed and simple-agent comparators use the same cleaned CSV and seed.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd

from ai_jobs_loop.graph import run_loop as run_simple_agent
from salary_budget_agents.graph import DEFAULT_DATASET, run_workflow


OUTPUT = Path(__file__).resolve().parents[1] / "artifacts/salary_budget/evaluation.json"


def _case(name: str, input_spec: dict, expected: str, checks: dict[str, bool], threshold: float = 1.0) -> dict:
    score = round(sum(checks.values()) / len(checks), 3)
    return {
        "name": name, "input": input_spec, "expected_behavior": expected,
        "scoring_criteria": [{"criterion": key, "passed": value, "weight": round(1 / len(checks), 3)} for key, value in checks.items()],
        "score": score, "pass_threshold": threshold, "passed": score >= threshold,
    }


def _holdout_count(result: dict) -> int:
    return sum(step["node"] == "report_writer" and result["report"]["holdout_metrics"] is not None for step in result["steps"])


def evaluate(dataset_path: str | Path = DEFAULT_DATASET, *, output_path: str | Path | None = OUTPUT) -> dict:
    data = pd.read_csv(dataset_path)
    normal = run_workflow(dataset_path, seed=42, max_trials=3, target_gain=0.20)
    report = normal["report"]
    cases = [_case(
        "normal_budget_brief", {"dataset": str(dataset_path), "seed": 42, "max_trials": 3},
        "Use country data when eligible; select by validation MAE; beat the fixed category baseline; estimate the requested role; evaluate holdout once.",
        {
            "context_used": any(trial["use_context"] for trial in normal["history"]),
            "best_validation_mae": normal["best_trial"]["validation"]["mae"] == min(trial["validation"]["mae"] for trial in normal["history"]),
            "holdout_beats_category_baseline": report["holdout_metrics"]["mae"] < report["baseline_metrics"]["mae"],
            "role_reference_present": report["role_request"]["job_category"] == "AI Engineering" and report["role_salary_reference_usd"] > 0,
            "holdout_once": _holdout_count(normal) == 1,
        },
    )]

    with tempfile.TemporaryDirectory() as directory:
        missing_path = Path(directory) / "country_context_missing.csv"
        without_context = data.copy()
        without_context["gdp_per_capita_usd"] = float("nan")
        without_context.to_csv(missing_path, index=False)
        missing = run_workflow(missing_path, seed=42, max_trials=2, target_gain=0.99)
        cases.append(_case(
            "country_context_unavailable", {"GDP_missing_rate": 1.0, "max_trials": 2},
            "Route every trial to job-only features and still produce a reviewed holdout brief.",
            {
                "context_rejected": missing["profile"]["context_ready"] is False,
                "core_only": all(not trial["use_context"] for trial in missing["history"]),
                "two_trials": len(missing["history"]) == 2,
                "holdout_once": _holdout_count(missing) == 1,
            },
        ))

        leaked_path = Path(directory) / "salary_proxy_columns.csv"
        leaked = data.copy()
        leaked["salary_tier"] = leaked["annual_salary_usd"].round(-4)
        leaked["salary_min_usd"] = leaked["annual_salary_usd"] * 0.8
        leaked.to_csv(leaked_path, index=False)
        guarded = run_workflow(leaked_path, seed=42, max_trials=1)
        cases.append(_case(
            "salary_proxy_guard", {"extra_columns": ["salary_tier", "salary_min_usd"], "max_trials": 1},
            "Exclude target-derived columns from every training feature list and disclose the exclusions.",
            {
                "excluded_in_profile": {"salary_tier", "salary_min_usd"}.issubset(guarded["profile"]["excluded_columns"]),
                "not_in_features": all(not {"salary_tier", "salary_min_usd"}.intersection(trial["features"]) for trial in guarded["history"]),
                "holdout_once": _holdout_count(guarded) == 1,
            },
        ))

    recovered = run_workflow(dataset_path, seed=42, max_trials=1, simulate_failure=True)
    cases.append(_case(
        "transient_trainer_failure", {"simulate_failure": True, "max_trials": 1},
        "Send the failed step to Recovery Agent, retry it once, and finish with one holdout evaluation.",
        {
            "failure_recorded": any(step["node"] == "model_trainer" and step.get("status") == "failed" for step in recovered["steps"]),
            "one_recovery": sum(step["node"] == "recovery_agent" for step in recovered["steps"]) == 1,
            "trainer_retried": sum(step["node"] == "model_trainer" for step in recovered["steps"]) == 2,
            "holdout_once": _holdout_count(recovered) == 1,
        },
    ))

    exhausted = run_workflow(dataset_path, seed=42, max_trials=2, target_gain=0.99)
    cases.append(_case(
        "trial_budget_exhausted", {"max_trials": 2, "target_gain": 0.99},
        "Stop after two reviewed trials and open the holdout only after model selection.",
        {
            "two_trials": len(exhausted["history"]) == 2,
            "budget_reason": exhausted["verdict"]["reason"] == "trial budget used",
            "holdout_once": _holdout_count(exhausted) == 1,
        },
    ))

    simple = run_simple_agent(dataset_path, seed=42, max_cycles=3, target_mae=1)
    comparison = {
        "same_dataset_seed_and_60_20_20_split": True,
        "fixed_airflow_style_category_mean": {"holdout_mae": report["baseline_metrics"]["mae"], "holdout_r2": report["baseline_metrics"]["r2"], "model_trials": 0, "adaptive_routes": 0, "failure_recovery": False},
        "class_template_simple_agent_part2": {"holdout_mae": simple["holdout_metrics"]["mae"], "holdout_r2": simple["holdout_metrics"]["r2"], "model_trials": simple["cycles"], "adaptive_routes": sum(step["node"] == "evaluation_judge" for step in simple["steps"]), "failure_recovery": False},
        "adaptive_multi_agent_part3": {"holdout_mae": report["holdout_metrics"]["mae"], "holdout_r2": report["holdout_metrics"]["r2"], "model_trials": len(normal["history"]), "adaptive_routes": sum(step["node"] == "quality_reviewer" for step in normal["steps"]), "failure_recovery": True},
        "note": "The fixed comparator is a train-only category-mean predictor based on Part 1's category analysis. The simple comparator is the class-template loop adapted to this salary dataset; the original classroom fraud task has a different target and metric.",
    }
    output = {"scenario": "AI role salary budget reference for analyst review", "dataset_rows": len(data), "cases": cases, "passed": sum(case["passed"] for case in cases), "total": len(cases), "comparison": comparison}
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(output, indent=2) + "\n")
    return output


if __name__ == "__main__":
    result = evaluate()
    print(json.dumps({"passed": result["passed"], "total": result["total"], "comparison": result["comparison"]}, indent=2))
    raise SystemExit(0 if result["passed"] == result["total"] else 1)
