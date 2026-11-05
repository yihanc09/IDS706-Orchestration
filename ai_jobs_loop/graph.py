"""Agent workflow for salary regression on the Airflow-cleaned AI jobs table.

The planner proposes a model, the trainer measures validation error, and the
judge either asks for another trial or sends the best model to a sealed holdout.
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
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import ExtraTreesRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


TARGET = "annual_salary_usd"
NUMERIC = ["years_of_experience", "demand_score", "is_llm_role", "skill_count", "has_python"]
CATEGORICAL = ["job_category", "remote_work"]
FEATURES = NUMERIC + CATEGORICAL
FAMILIES = ("ridge", "random_forest", "extra_trees")
MEAN_BASELINE_GAIN_THRESHOLD = 0.10
DEFAULT_DATASET = Path(__file__).resolve().parents[1] / "datasets/ai_jobs/ai_jobs_clean_latest.csv"


class SalaryState(TypedDict, total=False):
    dataset_path: str
    target_mae: float
    max_cycles: int
    seed: int
    cycle: int
    profile: dict
    plan: dict
    trial: dict
    history: list[dict]
    best_trial: dict
    best_val_mae: float
    verdict: dict
    holdout_metrics: dict
    baseline_metrics: dict
    steps: Annotated[list[dict], operator.add]


def _read_data(state: SalaryState) -> pd.DataFrame:
    path = Path(state["dataset_path"])
    if not path.is_file():
        raise FileNotFoundError(f"Run the Airflow DAG first: {path}")
    df = pd.read_csv(path)
    missing = set([TARGET, *FEATURES]) - set(df.columns)
    if missing:
        raise ValueError(f"Cleaned dataset is missing columns: {sorted(missing)}")
    if len(df) < 50:
        raise ValueError("At least 50 cleaned job records are required")
    if "job_id" in df and df.job_id.duplicated().any():
        raise ValueError("Duplicate job IDs in the cleaned dataset")
    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce")
    if df[TARGET].isna().any() or (df[TARGET] <= 0).any():
        raise ValueError("Annual salary must be positive and non-missing")
    return df


def _split(df: pd.DataFrame, seed: int):
    indices = np.arange(len(df))
    dev, holdout = train_test_split(indices, test_size=0.2, random_state=seed)
    train, validation = train_test_split(dev, test_size=0.25, random_state=seed)
    return train, validation, dev, holdout


def _model(family: str, seed: int) -> Pipeline:
    if family not in FAMILIES:
        raise ValueError(f"Unknown model family: {family}")
    numeric = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    categorical = Pipeline([
        ("impute", SimpleImputer(strategy="most_frequent")),
        ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    prep = ColumnTransformer([("numeric", numeric, NUMERIC), ("categorical", categorical, CATEGORICAL)])
    estimator = {
        "ridge": Ridge(alpha=10.0),
        "random_forest": RandomForestRegressor(n_estimators=120, min_samples_leaf=3, random_state=seed, n_jobs=1),
        "extra_trees": ExtraTreesRegressor(n_estimators=120, min_samples_leaf=3, random_state=seed, n_jobs=1),
    }[family]
    return Pipeline([("features", prep), ("model", estimator)])


def _metrics(actual, predicted) -> dict:
    return {
        "mae": round(float(mean_absolute_error(actual, predicted)), 2),
        "rmse": round(float(np.sqrt(mean_squared_error(actual, predicted))), 2),
        "r2": round(float(r2_score(actual, predicted)), 4),
        "n": int(len(actual)),
    }


def data_auditor(state: SalaryState) -> dict:
    """Check the Airflow handoff and identify the exact feature set."""
    df = _read_data(state)
    profile = {
        "rows": len(df), "target": TARGET, "features": FEATURES,
        "mean_salary_usd": round(float(df[TARGET].mean()), 2),
        "excluded_columns": [c for c in df if c not in [TARGET, *FEATURES]],
    }
    return {"profile": profile, "steps": [{"node": "data_auditor", "note": f"Validated {len(df)} cleaned jobs; excluded job_id from model features", "profile": profile}]}


def experiment_planner(state: SalaryState) -> dict:
    """Choose a model family not yet tried, using validation feedback."""
    history = state.get("history") or []
    tried = {trial["family"] for trial in history}
    remaining = [family for family in FAMILIES if family not in tried]
    if not remaining:
        raise ValueError("No untried model family remains")
    if not history:
        family = "ridge"
        reason = "Start with a regularized linear model"
    elif len(remaining) == 2 and history[0]["family"] == "ridge":
        ridge = history[0]
        baseline_mae = ridge["validation_baseline_mae"]
        gain = (baseline_mae - ridge["validation"]["mae"]) / baseline_mae if baseline_mae > 0 else 0.0
        family = "random_forest" if gain < MEAN_BASELINE_GAIN_THRESHOLD else "extra_trees"
        reason = f"Ridge improved on the validation mean baseline by {gain:.1%}"
    else:
        family = remaining[0]
        reason = "Try the remaining model after reviewing validation results"
    used_llm = False
    # Optional LLM choice is constrained to untried families; experiments still
    # run without an API key and the judge always uses measured error.
    if os.getenv("AI_JOBS_USE_LLM", "false").lower() in {"1", "true", "yes"}:
        from loop_runtime import extract_json_object, openrouter_chat

        content, _ = openrouter_chat(
            "Choose one regression model family. Return only JSON: {\"family\":\"...\"}.",
            f"Allowed={remaining}; prior_validation={history}; dataset_profile={state.get('profile')}",
            max_tokens=100,
            temperature=0,
        )
        candidate = (extract_json_object(content or "") or {}).get("family")
        if candidate in remaining:
            family = candidate
            used_llm = True
            reason = "OpenRouter selected this model from the untried families"
    plan = {"family": family, "trial_number": len(history) + 1, "reason": reason}
    return {"plan": plan, "steps": [{"node": "experiment_planner", "note": f"Try {family} next: {reason}", "plan": plan, "used_llm": used_llm}]}


def model_trainer(state: SalaryState) -> dict:
    """Train on 60% of rows and score the untouched 20% validation split."""
    df = _read_data(state)
    seed = int(state.get("seed", 42))
    train, validation, _, _ = _split(df, seed)
    family = state["plan"]["family"]
    model = _model(family, seed)
    model.fit(df.iloc[train][FEATURES], df.iloc[train][TARGET])
    metrics = _metrics(df.iloc[validation][TARGET], model.predict(df.iloc[validation][FEATURES]))
    baseline = DummyRegressor(strategy="mean")
    baseline.fit(df.iloc[train][FEATURES], df.iloc[train][TARGET])
    baseline_mae = float(mean_absolute_error(df.iloc[validation][TARGET], baseline.predict(df.iloc[validation][FEATURES])))
    trial = {"family": family, "validation": metrics, "validation_baseline_mae": round(baseline_mae, 2)}
    patch = {
        "trial": trial,
        "cycle": int(state.get("cycle", 0)) + 1,
        "steps": [{"node": "model_trainer", "note": f"{family}: validation MAE ${metrics['mae']:,.2f}", "trial": trial}],
    }
    if metrics["mae"] < float(state.get("best_val_mae", float("inf"))):
        patch.update(best_trial=trial, best_val_mae=metrics["mae"])
    return patch


def evaluation_judge(state: SalaryState) -> dict:
    """Compare measured validation MAE with the goal and trial budget."""
    history = [*(state.get("history") or []), state["trial"]]
    best = state["best_trial"]
    target = float(state.get("target_mae", 30000))
    cycle = int(state["cycle"])
    max_cycles = min(int(state.get("max_cycles", 3)), len(FAMILIES))
    action = "finalize" if best["validation"]["mae"] <= target or cycle >= max_cycles else "revise"
    verdict = {"action": action, "best_family": best["family"], "best_val_mae": best["validation"]["mae"]}
    return {"history": history, "verdict": verdict, "steps": [{"node": "evaluation_judge", "note": f"{action}; best validation MAE ${verdict['best_val_mae']:,.2f}", "verdict": verdict}]}


def route_after_judge(state: SalaryState) -> str:
    return "revise" if state["verdict"]["action"] == "revise" else "holdout"


def holdout_reporter(state: SalaryState) -> dict:
    """Refit the chosen family on development data; evaluate holdout once."""
    df = _read_data(state)
    seed = int(state.get("seed", 42))
    _, _, dev, holdout = _split(df, seed)
    model = _model(state["best_trial"]["family"], seed)
    model.fit(df.iloc[dev][FEATURES], df.iloc[dev][TARGET])
    actual = df.iloc[holdout][TARGET]
    metrics = _metrics(actual, model.predict(df.iloc[holdout][FEATURES]))
    baseline = DummyRegressor(strategy="mean")
    baseline.fit(df.iloc[dev][FEATURES], df.iloc[dev][TARGET])
    baseline_metrics = _metrics(actual, baseline.predict(df.iloc[holdout][FEATURES]))
    return {
        "holdout_metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "steps": [{"node": "holdout_reporter", "note": f"Holdout MAE ${metrics['mae']:,.2f}; R² {metrics['r2']:.3f}", "holdout": metrics, "baseline": baseline_metrics}],
    }


def build_graph():
    graph = StateGraph(SalaryState)
    graph.add_node("data_auditor", data_auditor)
    graph.add_node("experiment_planner", experiment_planner)
    graph.add_node("model_trainer", model_trainer)
    graph.add_node("evaluation_judge", evaluation_judge)
    graph.add_node("holdout_reporter", holdout_reporter)
    graph.add_edge(START, "data_auditor")
    graph.add_edge("data_auditor", "experiment_planner")
    graph.add_edge("experiment_planner", "model_trainer")
    graph.add_edge("model_trainer", "evaluation_judge")
    graph.add_conditional_edges("evaluation_judge", route_after_judge, {"revise": "experiment_planner", "holdout": "holdout_reporter"})
    graph.add_edge("holdout_reporter", END)
    return graph.compile()


def status(dataset_path: str | Path = DEFAULT_DATASET) -> dict:
    path = Path(dataset_path)
    app = build_graph()
    graph = app.get_graph()
    error = None
    try:
        _read_data({"dataset_path": str(path)})
    except (FileNotFoundError, ValueError) as exc:
        error = str(exc)
    return {
        "ready": error is None,
        "dataset_path": str(path),
        "nodes": [name for name in graph.nodes if name not in {"__start__", "__end__"}],
        "edges": [{"source": edge.source, "target": edge.target} for edge in graph.edges],
        "error": error,
    }


def initial_state(dataset_path: str | Path = DEFAULT_DATASET, *, max_cycles: int = 3, target_mae: float = 30000, seed: int = 42) -> SalaryState:
    if not 1 <= max_cycles <= len(FAMILIES):
        raise ValueError(f"max_cycles must be between 1 and {len(FAMILIES)}")
    if target_mae <= 0:
        raise ValueError("target_mae must be positive")
    return {"dataset_path": str(dataset_path), "target_mae": target_mae, "max_cycles": max_cycles, "seed": seed, "cycle": 0, "history": [], "best_val_mae": 1e12, "steps": []}


def result_from_state(state: SalaryState) -> dict:
    return {
        "dataset_path": state["dataset_path"], "target_column": TARGET,
        "cycles": state.get("cycle", 0), "history": state.get("history", []),
        "best_trial": state.get("best_trial"), "best_val_mae": state.get("best_val_mae"),
        "holdout_metrics": state.get("holdout_metrics"), "baseline_metrics": state.get("baseline_metrics"),
        "steps": state.get("steps", []),
    }


def run_loop(dataset_path: str | Path = DEFAULT_DATASET, *, max_cycles: int = 3, target_mae: float = 30000, seed: int = 42) -> dict:
    final = build_graph().invoke(initial_state(dataset_path, max_cycles=max_cycles, target_mae=target_mae, seed=seed), config={"recursion_limit": 20})
    return result_from_state(final)


def stream_loop(dataset_path: str | Path = DEFAULT_DATASET, *, max_cycles: int = 3, target_mae: float = 30000, seed: int = 42):
    state = initial_state(dataset_path, max_cycles=max_cycles, target_mae=target_mae, seed=seed)
    yield {"type": "run_started", "dataset_path": str(dataset_path), "target_column": TARGET}
    for update in build_graph().stream(state, stream_mode="updates", config={"recursion_limit": 20}):
        for name, patch in update.items():
            yield {"type": "node_started", "node": name}
            state.update({key: value for key, value in patch.items() if key != "steps"})
            state["steps"] = [*state.get("steps", []), *patch.get("steps", [])]
            yield {"type": "node_completed", "node": name, "step": patch.get("steps", [{}])[-1], "cycle": state.get("cycle"), "best_val_mae": state.get("best_val_mae")}
    yield {"type": "run_completed", "result": result_from_state(state)}
