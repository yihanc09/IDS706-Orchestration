"""Independent LangGraph for an analyst-reviewed AI salary budget brief.

Agents choose a feature strategy, train candidates, review measured evidence,
recover from one failed training attempt, and report on a sealed holdout.
"""

from __future__ import annotations

import operator
import os
from pathlib import Path
from typing import Annotated, TypedDict

import numpy as np
import pandas as pd
from langgraph.graph import END, START, StateGraph
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


DEFAULT_DATASET = Path(os.getenv("AI_JOBS_CLEAN_DATASET", str(Path(__file__).resolve().parents[1] / "datasets/ai_jobs/ai_jobs_clean_latest.csv")))
TARGET = "annual_salary_usd"
CORE_NUMERIC = ["years_of_experience", "demand_score", "is_llm_role", "skill_count", "has_python"]
CONTEXT_NUMERIC = CORE_NUMERIC + ["gdp_per_capita_usd"]
CATEGORICAL = ["job_category", "remote_work"]
REQUIRED = ["job_id", TARGET, *CORE_NUMERIC, *CATEGORICAL]
CONTEXT_MISSING_LIMIT = 0.20
DEFAULT_ROLE = {"job_category": "AI Engineering", "years_of_experience": 5, "remote_work": "Fully Remote", "country_iso3": "USA", "is_llm_role": True, "has_python": True}


class BudgetState(TypedDict, total=False):
    dataset_path: str
    seed: int
    max_trials: int
    target_gain: float
    simulate_failure: bool
    role_request: dict
    failure_injected: bool
    profile: dict
    plan: dict
    training: dict
    history: list[dict]
    best_trial: dict
    retry_count: int
    verdict: dict
    report: dict
    steps: Annotated[list[dict], operator.add]


def _read_data(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Run the AI jobs Airflow DAG first: {path}")
    data = pd.read_csv(path)
    missing = set(REQUIRED) - set(data.columns)
    if missing:
        raise ValueError(f"Missing cleaned columns: {sorted(missing)}")
    if len(data) < 50 or data.job_id.isna().any() or data.job_id.duplicated().any():
        raise ValueError("Expected at least 50 jobs with unique, nonempty IDs")
    data[TARGET] = pd.to_numeric(data[TARGET], errors="coerce")
    if data[TARGET].isna().any() or (data[TARGET] <= 0).any():
        raise ValueError("Salary target must be positive and complete")
    if "gdp_per_capita_usd" in data:
        data["gdp_per_capita_usd"] = pd.to_numeric(data["gdp_per_capita_usd"], errors="coerce")
    return data


def _split(data: pd.DataFrame, seed: int):
    indices = np.arange(len(data))
    development, holdout = train_test_split(indices, test_size=0.2, random_state=seed)
    train, validation = train_test_split(development, test_size=0.25, random_state=seed)
    return train, validation, development, holdout


def _category_baseline(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    means = train.groupby("job_category")[TARGET].mean()
    return test.job_category.map(means).fillna(train[TARGET].mean()).to_numpy()


def _model(family: str, use_context: bool, seed: int) -> tuple[Pipeline, list[str]]:
    numeric = CONTEXT_NUMERIC if use_context else CORE_NUMERIC
    features = [*numeric, *CATEGORICAL]
    preprocess = ColumnTransformer([
        ("numeric", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), numeric),
        ("categorical", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False))]), CATEGORICAL),
    ])
    estimators = {
        "ridge": Ridge(alpha=10.0),
        "random_forest": RandomForestRegressor(n_estimators=120, min_samples_leaf=3, random_state=seed, n_jobs=1),
        "extra_trees": ExtraTreesRegressor(n_estimators=120, min_samples_leaf=3, random_state=seed, n_jobs=1),
    }
    return Pipeline([("features", preprocess), ("model", estimators[family])]), features


def _metrics(actual, predicted) -> dict:
    return {"mae": round(float(mean_absolute_error(actual, predicted)), 2), "r2": round(float(r2_score(actual, predicted)), 4), "n": int(len(actual))}


def data_steward(state: BudgetState) -> dict:
    data = _read_data(state["dataset_path"])
    role = state["role_request"]
    if role["job_category"] not in set(data.job_category) or role["remote_work"] not in set(data.remote_work):
        raise ValueError("Requested job category or work arrangement is absent from the cleaned data")
    if not 0 <= float(role["years_of_experience"]) <= 40:
        raise ValueError("Requested experience must be between 0 and 40 years")
    if "country_iso3" in data and role["country_iso3"] not in set(data.country_iso3.dropna()):
        raise ValueError("Requested country is absent from the cleaned data")
    missing_rate = float(data["gdp_per_capita_usd"].isna().mean()) if "gdp_per_capita_usd" in data else 1.0
    context_ready = missing_rate <= CONTEXT_MISSING_LIMIT and data.get("gdp_per_capita_usd", pd.Series(dtype=float)).notna().any()
    allowed = {TARGET, "job_id", *CONTEXT_NUMERIC, *CATEGORICAL}
    profile = {
        "rows": len(data), "context_missing_rate": round(missing_rate, 4),
        "context_ready": bool(context_ready), "excluded_columns": sorted(set(data) - allowed),
        "holdout_rows": len(_split(data, int(state["seed"]))[3]), "role_request": role,
    }
    return {"profile": profile, "steps": [{"node": "data_steward", "note": f"Checked {len(data)} jobs; country context missing {missing_rate:.1%}", "output": profile}]}


def strategy_planner(state: BudgetState) -> dict:
    history = state.get("history", [])
    context_ready = state["profile"]["context_ready"]
    candidates = [("ridge", False), ("ridge", True), ("random_forest", True)] if context_ready else [("ridge", False), ("random_forest", False), ("extra_trees", False)]
    tried = {(trial["family"], trial["use_context"]) for trial in history}
    family, use_context = next((item for item in candidates if item not in tried), (None, None))
    if family is None:
        raise ValueError("No candidate remains within the trial budget")
    plan = {"family": family, "use_context": use_context, "number": len(history) + 1}
    label = "jobs + country context" if use_context else "job fields only"
    return {"plan": plan, "retry_count": 0, "steps": [{"node": "strategy_planner", "note": f"Trial {plan['number']}: {family} using {label}", "output": plan}]}


def model_trainer(state: BudgetState) -> dict:
    plan = state["plan"]
    if state.get("simulate_failure") and not state.get("failure_injected"):
        return {"failure_injected": True, "training": {"error": "Simulated temporary training failure"}, "steps": [{"node": "model_trainer", "note": "Temporary training failure recorded for recovery test", "status": "failed"}]}
    try:
        data = _read_data(state["dataset_path"])
        train, validation, _, _ = _split(data, int(state["seed"]))
        model, features = _model(plan["family"], plan["use_context"], int(state["seed"]))
        model.fit(data.iloc[train][features], data.iloc[train][TARGET])
        metrics = _metrics(data.iloc[validation][TARGET], model.predict(data.iloc[validation][features]))
        baseline = _metrics(data.iloc[validation][TARGET], _category_baseline(data.iloc[train], data.iloc[validation]))
        trial = {**plan, "features": features, "validation": metrics, "baseline_validation": baseline}
        return {"training": {"trial": trial}, "steps": [{"node": "model_trainer", "note": f"{plan['family']} validation MAE ${metrics['mae']:,.2f}", "output": trial}]}
    except (ValueError, TypeError, KeyError) as exc:
        return {"training": {"error": str(exc)}, "steps": [{"node": "model_trainer", "note": f"Training failed: {exc}", "status": "failed"}]}


def quality_reviewer(state: BudgetState) -> dict:
    trial = state["training"]["trial"]
    history = [*state.get("history", []), trial]
    previous = state.get("best_trial")
    best = trial if previous is None or trial["validation"]["mae"] < previous["validation"]["mae"] else previous
    baseline_mae = trial["baseline_validation"]["mae"]
    gain = (baseline_mae - best["validation"]["mae"]) / baseline_mae
    if gain >= state["target_gain"]:
        action, reason = "report", "validation goal reached"
    elif len(history) >= state["max_trials"]:
        action, reason = "report", "trial budget used"
    else:
        action, reason = "revise", "validation gain below target"
    verdict = {"action": action, "reason": reason, "gain": round(gain, 4)}
    return {"history": history, "best_trial": best, "verdict": verdict, "steps": [{"node": "quality_reviewer", "note": f"{reason}; best gain {gain:.1%} vs category baseline", "output": verdict}]}


def recovery_agent(state: BudgetState) -> dict:
    retries = state.get("retry_count", 0) + 1
    if retries > 1:
        verdict = {"action": "report", "reason": "training failed after one retry", "gain": None}
        return {"retry_count": retries, "verdict": verdict, "steps": [{"node": "recovery_agent", "note": verdict["reason"], "output": verdict}]}
    return {"retry_count": retries, "steps": [{"node": "recovery_agent", "note": "Retry the planned training step once", "output": {"retry_count": retries}}]}


def report_writer(state: BudgetState) -> dict:
    best = state.get("best_trial")
    if best is None:
        report = {"status": "failed", "reason": state["verdict"]["reason"], "holdout_metrics": None, "baseline_metrics": None}
    else:
        data = _read_data(state["dataset_path"])
        _, _, development, holdout = _split(data, int(state["seed"]))
        model, features = _model(best["family"], best["use_context"], int(state["seed"]))
        model.fit(data.iloc[development][features], data.iloc[development][TARGET])
        actual = data.iloc[holdout][TARGET]
        holdout_metrics = _metrics(actual, model.predict(data.iloc[holdout][features]))
        baseline_metrics = _metrics(actual, _category_baseline(data.iloc[development], data.iloc[holdout]))
        role = state["role_request"]
        development_data = data.iloc[development]
        country_gdp = development_data.loc[development_data.country_iso3 == role["country_iso3"], "gdp_per_capita_usd"].median() if "country_iso3" in development_data and "gdp_per_capita_usd" in development_data else float("nan")
        role_features = {
            "years_of_experience": float(role["years_of_experience"]),
            "demand_score": float(development_data.demand_score.median()),
            "is_llm_role": int(role["is_llm_role"]),
            "skill_count": float(development_data.skill_count.median()),
            "has_python": int(role["has_python"]),
            "gdp_per_capita_usd": float(country_gdp),
            "job_category": role["job_category"], "remote_work": role["remote_work"],
        }
        estimate = round(float(model.predict(pd.DataFrame([role_features])[features])[0]), 2)
        report = {
            "status": "ready_for_analyst_review", "purpose": "Salary budget reference for a new AI role",
            "best_family": best["family"], "use_context": best["use_context"],
            "selected_validation_mae": best["validation"]["mae"],
            "holdout_metrics": holdout_metrics, "baseline_metrics": baseline_metrics,
            "role_request": role, "role_salary_reference_usd": estimate,
            "role_assumptions": "Demand score and skill count use development-set medians; country GDP uses the 2024 World Bank snapshot when selected.",
            "selection_reason": state["verdict"]["reason"],
            "limitations": "Dataset associations are not causal; this estimate is not an individual offer recommendation.",
        }
    return {"report": report, "steps": [{"node": "report_writer", "note": f"Brief {report['status']}; holdout evaluated {1 if best else 0} time(s)", "output": report}]}


def _after_trainer(state: BudgetState) -> str:
    return "recover" if "error" in state["training"] else "review"


def _after_recovery(state: BudgetState) -> str:
    return "retry" if state["retry_count"] <= 1 else "report"


def _after_review(state: BudgetState) -> str:
    return state["verdict"]["action"]


def build_graph():
    graph = StateGraph(BudgetState)
    for name, agent in [
        ("data_steward", data_steward), ("strategy_planner", strategy_planner),
        ("model_trainer", model_trainer), ("quality_reviewer", quality_reviewer),
        ("recovery_agent", recovery_agent), ("report_writer", report_writer),
    ]:
        graph.add_node(name, agent)
    graph.add_edge(START, "data_steward")
    graph.add_edge("data_steward", "strategy_planner")
    graph.add_edge("strategy_planner", "model_trainer")
    graph.add_conditional_edges("model_trainer", _after_trainer, {"review": "quality_reviewer", "recover": "recovery_agent"})
    graph.add_conditional_edges("recovery_agent", _after_recovery, {"retry": "model_trainer", "report": "report_writer"})
    graph.add_conditional_edges("quality_reviewer", _after_review, {"revise": "strategy_planner", "report": "report_writer"})
    graph.add_edge("report_writer", END)
    return graph.compile()


def initial_state(dataset_path: str | Path = DEFAULT_DATASET, *, seed: int = 42, max_trials: int = 3, target_gain: float = 0.20, simulate_failure: bool = False, role_request: dict | None = None) -> BudgetState:
    if not 1 <= max_trials <= 3 or not 0 <= target_gain <= 1:
        raise ValueError("max_trials must be 1–3 and target_gain must be 0–1")
    return {"dataset_path": str(dataset_path), "seed": seed, "max_trials": max_trials, "target_gain": target_gain, "simulate_failure": simulate_failure, "role_request": role_request or DEFAULT_ROLE.copy(), "failure_injected": False, "history": [], "retry_count": 0, "steps": []}


def result_from_state(state: BudgetState) -> dict:
    return {"profile": state.get("profile"), "history": state.get("history", []), "best_trial": state.get("best_trial"), "verdict": state.get("verdict"), "report": state.get("report"), "steps": state.get("steps", [])}


def run_workflow(dataset_path: str | Path = DEFAULT_DATASET, **options) -> dict:
    final = build_graph().invoke(initial_state(dataset_path, **options), config={"recursion_limit": 30})
    return result_from_state(final)


def stream_workflow(dataset_path: str | Path = DEFAULT_DATASET, **options):
    state = initial_state(dataset_path, **options)
    yield {"type": "run_started"}
    for update in build_graph().stream(state, stream_mode="updates", config={"recursion_limit": 30}):
        for name, patch in update.items():
            state.update({key: value for key, value in patch.items() if key != "steps"})
            state["steps"] = [*state["steps"], *patch.get("steps", [])]
            yield {"type": "node_completed", "node": name, "step": patch.get("steps", [{}])[-1]}
    yield {"type": "run_completed", "result": result_from_state(state)}


def status(dataset_path: str | Path = DEFAULT_DATASET) -> dict:
    graph = build_graph().get_graph()
    try:
        data = _read_data(dataset_path)
        error, rows = None, len(data)
    except (ValueError, FileNotFoundError) as exc:
        error, rows = str(exc), 0
    options = {"categories": sorted(data.job_category.dropna().unique().tolist()), "remote_options": sorted(data.remote_work.dropna().unique().tolist()), "countries": sorted(data.country_iso3.dropna().unique().tolist())} if error is None else {"categories": [], "remote_options": [], "countries": []}
    return {"ready": error is None, "error": error, "rows": rows, "dataset_path": str(dataset_path), "role_options": options,
            "nodes": [node for node in graph.nodes if node not in {"__start__", "__end__"}],
            "edges": [{"source": edge.source, "target": edge.target} for edge in graph.edges if edge.source != "__start__" and edge.target != "__end__"]}
