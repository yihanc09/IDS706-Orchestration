"""Run or inspect the AI jobs salary agent graph."""

from __future__ import annotations

import argparse
import json

from ai_jobs_loop.graph import DEFAULT_DATASET, run_loop, status


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET))
    parser.add_argument("--max-cycles", type=int, default=3)
    parser.add_argument("--target-mae", type=float, default=30000)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    result = status(args.dataset) if args.status else run_loop(args.dataset, max_cycles=args.max_cycles, target_mae=args.target_mae)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
