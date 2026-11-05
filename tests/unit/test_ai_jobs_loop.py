"""Regression-loop checks independent of the student's Airflow output."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ai_jobs_loop.graph import FEATURES, run_loop


class SalaryLoopTest(unittest.TestCase):
    def test_three_trials_select_by_validation_and_score_holdout_once(self):
        rows = []
        for index in range(120):
            years = index % 12
            rows.append({
                "job_id": f"JOB{index:04d}",
                "annual_salary_usd": 90000 + years * 9500 + (index % 5) * 1100,
                "years_of_experience": years,
                "demand_score": 50 + index % 40,
                "is_llm_role": index % 2,
                "skill_count": 3 + index % 6,
                "has_python": index % 3 == 0,
                "job_category": "Engineering" if index % 2 else "Research",
                "remote_work": "Remote" if index % 3 else "On-site",
            })
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cleaned.csv"
            pd.DataFrame(rows).to_csv(path, index=False)
            result = run_loop(path, max_cycles=3, target_mae=1)
            early_stop = run_loop(path, max_cycles=3, target_mae=1e9)

        self.assertEqual(result["cycles"], 3)
        self.assertEqual(len(result["history"]), 3)
        self.assertEqual(result["holdout_metrics"]["n"], 24)
        self.assertEqual(result["best_val_mae"], min(t["validation"]["mae"] for t in result["history"]))
        self.assertIn(result["best_trial"], result["history"])
        self.assertNotIn("job_id", FEATURES)
        self.assertEqual(sum(s["node"] == "holdout_reporter" for s in result["steps"]), 1)
        self.assertEqual(early_stop["cycles"], 1)


if __name__ == "__main__":
    unittest.main()
