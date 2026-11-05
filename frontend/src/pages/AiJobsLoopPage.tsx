import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { AppNav } from '../components/AppNav'
import { LiveAiJobsGraph, type AgentStatus } from '../components/LiveAiJobsGraph'

type Metrics = { mae: number; rmse: number; r2: number; n: number }
type Trial = { family: string; validation: Metrics }
type Result = {
  cycles: number
  history: Trial[]
  best_trial: Trial
  best_val_mae: number
  holdout_metrics: Metrics
  baseline_metrics: Metrics
  steps: { node: string; note?: string }[]
}
type Readiness = { ready: boolean; error?: string | null; dataset_path: string; nodes: string[] }
type Event = { type: string; node?: string; step?: { note?: string }; cycle?: number; result?: Result; message?: string }

const money = (value?: number) => value == null ? '—' : `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}`

export function AiJobsLoopPage() {
  const [readiness, setReadiness] = useState<Readiness | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [maxCycles, setMaxCycles] = useState(3)
  const [targetMae, setTargetMae] = useState(30000)
  const [status, setStatus] = useState<Record<string, AgentStatus>>({})
  const [activity, setActivity] = useState<string[]>([])
  const [result, setResult] = useState<Result | null>(null)
  const sourceRef = useRef<EventSource | null>(null)

  async function refreshStatus() {
    try {
      const response = await fetch('/api/ai-jobs-loop/status')
      if (!response.ok) throw new Error(await response.text())
      setReadiness(await response.json() as Readiness)
    } catch (cause) {
      setError(String(cause))
    }
  }

  useEffect(() => {
    void refreshStatus()
    return () => sourceRef.current?.close()
  }, [])

  function start() {
    sourceRef.current?.close()
    setError(null)
    setResult(null)
    setStatus({})
    setActivity([])
    setBusy(true)
    const params = new URLSearchParams({ max_cycles: String(maxCycles), target_mae: String(targetMae) })
    const source = new EventSource(`/api/ai-jobs-loop/stream?${params}`)
    sourceRef.current = source

    source.addEventListener('node_started', (message) => {
      const event = JSON.parse((message as MessageEvent).data) as Event
      if (event.node) setStatus((prev) => ({ ...prev, [event.node!]: 'running' }))
    })
    source.addEventListener('node_completed', (message) => {
      const event = JSON.parse((message as MessageEvent).data) as Event
      if (event.node) setStatus((prev) => ({ ...prev, [event.node!]: 'done' }))
      if (event.step?.note) setActivity((prev) => [...prev, event.step!.note!])
    })
    source.addEventListener('run_completed', (message) => {
      const event = JSON.parse((message as MessageEvent).data) as Event
      setResult(event.result ?? null)
      if (event.result) {
        setStatus(Object.fromEntries(
          ['data_auditor', 'experiment_planner', 'model_trainer', 'evaluation_judge', 'holdout_reporter'].map((node) => [node, 'done' as AgentStatus]),
        ))
        setActivity(event.result.steps.map((step) => step.note).filter((note): note is string => Boolean(note)))
      }
      setBusy(false)
      source.close()
    })
    source.addEventListener('error', (message) => {
      let detail = 'The run stopped before returning metrics.'
      if ((message as MessageEvent).data) {
        try { detail = (JSON.parse((message as MessageEvent).data) as Event).message ?? detail } catch { /* connection error */ }
      }
      setError(detail)
      setBusy(false)
      source.close()
    })
  }

  return (
    <div className="lab-grid min-h-screen">
      <AppNav />
      <main className="mx-auto max-w-[1500px] px-5 py-8">
        <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="font-mono text-xs uppercase tracking-[0.2em] text-teal">Week 11 · Part 2</p>
            <h1 className="font-display text-4xl font-semibold">AI Jobs Salary Loop</h1>
            <p className="mt-2 max-w-3xl text-sm text-mist">LangGraph agents train and compare regressors on the table produced by Airflow. Lower mean absolute error (MAE) is better.</p>
          </div>
          <Link to="/student-loop" className="text-sm text-teal hover:underline">Classroom build loop ↗</Link>
        </div>

        <div className="mb-5 flex flex-wrap items-end gap-4 border border-line bg-panel p-4">
          <div className="min-w-[18rem] flex-1">
            <p className="font-mono text-[10px] uppercase tracking-widest text-mist">Airflow handoff</p>
            <p className="mt-1 break-all font-mono text-xs">{readiness?.dataset_path ?? 'Checking dataset…'}</p>
            <p className={`mt-1 text-xs ${readiness?.ready ? 'text-ok' : 'text-amber'}`}>
              {readiness?.ready ? 'Cleaned data is ready' : readiness?.error ?? 'Checking…'}
            </p>
          </div>
          <label className="text-xs text-mist">Trial budget
            <select className="mt-1 block border border-line bg-ink-2 px-3 py-2 text-sm text-paper" value={maxCycles} onChange={(e) => setMaxCycles(Number(e.target.value))} disabled={busy}>
              <option value={1}>1 model</option><option value={2}>2 models</option><option value={3}>3 models</option>
            </select>
          </label>
          <label className="text-xs text-mist">Target validation MAE ($)
            <input className="mt-1 block w-36 border border-line bg-ink-2 px-3 py-2 text-sm text-paper" type="number" min={1} step={1000} value={targetMae} onChange={(e) => setTargetMae(Number(e.target.value))} disabled={busy} />
          </label>
          <button type="button" onClick={start} disabled={!readiness?.ready || busy || targetMae <= 0} className="border border-teal bg-teal px-5 py-2 text-sm font-medium text-white disabled:opacity-50">
            {busy ? 'Running…' : 'Run agent loop'}
          </button>
          <button type="button" onClick={() => void refreshStatus()} disabled={busy} className="border border-line px-4 py-2 text-sm text-paper disabled:opacity-50">Refresh data</button>
        </div>

        {error && <p className="mb-5 border border-danger bg-red-50 p-3 text-sm text-danger">{error}</p>}
        <LiveAiJobsGraph status={status} />

        <div className="mt-6 grid gap-5 lg:grid-cols-[1fr_1fr]">
          <section className="border border-line bg-panel p-5">
            <h2 className="font-display text-xl font-semibold">Experiments</h2>
            <p className="mt-1 text-xs text-mist">The same validation split is used for each model. The holdout is opened after selection.</p>
            <table className="mt-4 w-full text-left text-sm">
              <thead className="border-b border-line text-xs text-mist"><tr><th className="py-2">Trial</th><th>Model</th><th>Validation MAE</th><th>R²</th></tr></thead>
              <tbody>{(result?.history ?? []).map((trial, index) => <tr key={trial.family} className="border-b border-line/60"><td className="py-2">{index + 1}</td><td>{trial.family.replaceAll('_', ' ')}</td><td>{money(trial.validation.mae)}</td><td>{trial.validation.r2.toFixed(3)}</td></tr>)}</tbody>
            </table>
            {!result && <p className="mt-4 text-sm text-mist">Run the graph to compare models.</p>}
          </section>
          <section className="border border-line bg-panel p-5">
            <h2 className="font-display text-xl font-semibold">Final evaluation</h2>
            <p className="mt-1 text-xs text-mist">Chosen by validation MAE, then refit on development data and tested on 300 holdout jobs.</p>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <Metric label="Best model" value={result?.best_trial.family.replaceAll('_', ' ') ?? '—'} />
              <Metric label="Validation MAE" value={money(result?.best_val_mae)} />
              <Metric label="Holdout MAE" value={money(result?.holdout_metrics.mae)} />
              <Metric label="Holdout R²" value={result ? result.holdout_metrics.r2.toFixed(3) : '—'} />
              <Metric label="Mean baseline MAE" value={money(result?.baseline_metrics.mae)} />
              <Metric label="Holdout rows" value={result?.holdout_metrics.n.toString() ?? '—'} />
            </div>
          </section>
        </div>

        <section className="mt-5 border border-line bg-panel p-5">
          <h2 className="font-display text-xl font-semibold">Agent activity</h2>
          <ol className="mt-3 space-y-1 font-mono text-xs text-mist">{activity.map((note, index) => <li key={`${index}-${note}`}>{index + 1}. {note}</li>)}</ol>
          {activity.length === 0 && <p className="mt-2 text-sm text-mist">Agent decisions will appear here during a run.</p>}
        </section>
      </main>
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="border border-line bg-ink-2 p-3"><p className="font-mono text-[10px] uppercase tracking-wider text-mist">{label}</p><p className="mt-1 text-lg font-semibold">{value}</p></div>
}
