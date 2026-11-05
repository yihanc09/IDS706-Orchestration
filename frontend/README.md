# Frontend

This React, TypeScript, and Vite app displays the live workflows. The browser sends API requests through Vite's /api proxy to the local gateway on port 8000.

## Assignment pages

- /ai-jobs-loop shows the Part 2 salary regression graph, validation trials, and holdout metrics.
- /salary-budget shows the Part 3 adaptive graph, role budget brief, simulated trainer failure route, and evaluation suite.

The earlier classroom pages remain available as references. The assignment pages use their own backend graphs and the cleaned AI jobs output from the Airflow DAG.

## Run locally

Run the Airflow DAG first to create datasets/ai_jobs/ai_jobs_clean_latest.csv. From the repository root, copy .env.example to .env, then start the service stack and Vite with make start-mac (or make start-windows in Git Bash). Open http://127.0.0.1:5173.

For a frontend-only build check:

~~~bash
cd frontend
npm install
npm run build
~~~

See the [project README](../README.md) for the complete pipeline, agent workflows, results, and screenshots.
