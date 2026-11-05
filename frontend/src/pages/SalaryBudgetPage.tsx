import { useEffect, useRef, useState } from 'react'
import { AppNav } from '../components/AppNav'
import { LiveBudgetGraph } from '../components/LiveBudgetGraph'

type Metrics = { mae: number; r2: number; n: number }
type Trial = { family: string; use_context: boolean; validation: Metrics; baseline_validation: Metrics }
type Step = { node: string; note: string; status?: string }
type RoleRequest = { job_category: string; years_of_experience: number; remote_work: string; country_iso3: string; is_llm_role: boolean; has_python: boolean }
type Result = { profile: { rows: number; context_missing_rate: number; excluded_columns: string[] }; history: Trial[]; best_trial: Trial; verdict: { reason: string; gain: number }; report: { status: string; best_family: string; use_context: boolean; holdout_metrics: Metrics; baseline_metrics: Metrics; role_request: RoleRequest; role_salary_reference_usd: number; role_assumptions: string; limitations: string }; steps: Step[] }
type Readiness = { ready: boolean; error?: string; rows: number; dataset_path: string; nodes: string[]; edges: { source: string; target: string }[]; role_options: { categories: string[]; remote_options: string[]; countries: string[] } }
type Case = { name: string; score: number; pass_threshold: number; passed: boolean }
type Evaluation = { passed: number; total: number; cases: Case[]; comparison: Record<string, { holdout_mae?: number; holdout_r2?: number; model_trials?: number; failure_recovery?: boolean } | boolean | string> }
type Event = { type: string; node?: string; step?: Step; result?: Result; message?: string }

const money = (value?: number) => value == null ? '—' : `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`

export function SalaryBudgetPage() {
  const [readiness, setReadiness] = useState<Readiness | null>(null)
  const [result, setResult] = useState<Result | null>(null)
  const [steps, setSteps] = useState<Step[]>([])
  const [evaluation, setEvaluation] = useState<Evaluation | null>(null)
  const [maxTrials, setMaxTrials] = useState(3)
  const [targetGain, setTargetGain] = useState(20)
  const [simulateFailure, setSimulateFailure] = useState(false)
  const [role, setRole] = useState<RoleRequest>({ job_category: 'AI Engineering', years_of_experience: 5, remote_work: 'Fully Remote', country_iso3: 'USA', is_llm_role: true, has_python: true })
  const [busy, setBusy] = useState(false)
  const [evalBusy, setEvalBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const streamRef = useRef<EventSource | null>(null)

  useEffect(() => {
    fetch('/api/salary-budget/status').then(async (response) => {
      if (!response.ok) throw new Error(await response.text())
      setReadiness(await response.json() as Readiness)
    }).catch((cause) => setError(String(cause)))
    return () => streamRef.current?.close()
  }, [])

  function run() {
    streamRef.current?.close()
    setBusy(true); setError(null); setResult(null); setSteps([])
    const params = new URLSearchParams({ max_trials: String(maxTrials), target_gain: String(targetGain / 100), simulate_failure: String(simulateFailure), job_category: role.job_category, years_of_experience: String(role.years_of_experience), remote_work: role.remote_work, country_iso3: role.country_iso3, is_llm_role: String(role.is_llm_role), has_python: String(role.has_python) })
    const source = new EventSource(`/api/salary-budget/stream?${params}`)
    streamRef.current = source
    source.addEventListener('node_completed', (message) => {
      const event = JSON.parse((message as MessageEvent).data) as Event
      if (event.step) setSteps((previous) => [...previous, event.step!])
    })
    source.addEventListener('run_completed', (message) => {
      const event = JSON.parse((message as MessageEvent).data) as Event
      setResult(event.result ?? null); setSteps(event.result?.steps ?? []); setBusy(false); source.close()
    })
    source.addEventListener('error', (message) => {
      let detail = 'The run stopped before a brief was produced.'
      if ((message as MessageEvent).data) {
        try { detail = (JSON.parse((message as MessageEvent).data) as Event).message ?? detail } catch { /* connection error */ }
      }
      setError(detail); setBusy(false); source.close()
    })
  }

  async function runEvaluation() {
    setEvalBusy(true); setError(null)
    try {
      const response = await fetch('/api/salary-budget/evaluation')
      if (!response.ok) throw new Error(await response.text())
      setEvaluation(await response.json() as Evaluation)
    } catch (cause) { setError(String(cause)) }
    finally { setEvalBusy(false) }
  }

  const comparisonKeys = ['fixed_airflow_style_category_mean', 'class_template_simple_agent_part2', 'adaptive_multi_agent_part3']
  const comparisonLabels = ['Fixed Airflow-style category mean', 'Class-template simple agent (Part 2)', 'Adaptive multi-agent (Part 3)']

  return <div className="lab-grid min-h-screen"><AppNav /><main className="mx-auto max-w-[1500px] px-5 py-8">
    <div className="mb-6"><p className="font-mono text-xs uppercase tracking-[0.2em] text-teal">Week 11 · Bonus</p><h1 className="font-display text-4xl font-semibold">AI Salary Budget Research</h1><p className="mt-2 max-w-4xl text-sm text-mist">A hiring analytics team prepares a salary budget reference for a new AI role. Agents audit the Airflow data, select and train models, review evidence, recover from a failed step, and produce a brief for an analyst to review.</p></div>
    <div className="mb-5 flex flex-wrap items-end gap-4 border border-line bg-panel p-4">
      <div className="min-w-[17rem] flex-1"><p className="font-mono text-[10px] uppercase tracking-widest text-mist">Airflow handoff</p><p className="mt-1 break-all font-mono text-xs">{readiness?.dataset_path ?? 'Checking…'}</p><p className={`mt-1 text-xs ${readiness?.ready ? 'text-ok' : 'text-amber'}`}>{readiness?.ready ? `${readiness.rows} cleaned jobs ready` : readiness?.error ?? 'Checking…'}</p></div>
      <label className="text-xs text-mist">Trial budget<select className="mt-1 block border border-line bg-ink-2 px-3 py-2 text-sm text-paper" value={maxTrials} onChange={(event) => setMaxTrials(Number(event.target.value))} disabled={busy}><option value={1}>1 trial</option><option value={2}>2 trials</option><option value={3}>3 trials</option></select></label>
      <label className="text-xs text-mist">Target validation gain (%)<input className="mt-1 block w-28 border border-line bg-ink-2 px-3 py-2 text-sm text-paper" type="number" min={0} max={100} value={targetGain} onChange={(event) => setTargetGain(Number(event.target.value))} disabled={busy} /></label>
      <label className="flex items-center gap-2 text-xs text-mist"><input type="checkbox" checked={simulateFailure} onChange={(event) => setSimulateFailure(event.target.checked)} disabled={busy} />Simulate one trainer failure</label>
      <button type="button" onClick={run} disabled={!readiness?.ready || busy || targetGain < 0 || targetGain > 100 || role.years_of_experience < 0 || role.years_of_experience > 40} className="border border-teal bg-teal px-5 py-2 text-sm font-medium text-white disabled:opacity-50">{busy ? 'Running…' : 'Run workflow'}</button>
    </div>
    <section className="mb-5 border border-line bg-panel p-4"><h2 className="font-display text-lg font-semibold">New role to budget</h2><p className="mt-1 text-xs text-mist">Example requisition. The selected model estimates annual salary in USD; demand score and skill count use development-data medians.</p><div className="mt-3 flex flex-wrap items-end gap-4">
      <label className="text-xs text-mist">Job category<select className="mt-1 block border border-line bg-ink-2 px-3 py-2 text-sm text-paper" value={role.job_category} onChange={(event) => setRole((previous) => ({ ...previous, job_category: event.target.value }))} disabled={busy}>{(readiness?.role_options.categories ?? [role.job_category]).map((value) => <option key={value}>{value}</option>)}</select></label>
      <label className="text-xs text-mist">Experience (years)<input className="mt-1 block w-28 border border-line bg-ink-2 px-3 py-2 text-sm text-paper" type="number" min={0} max={40} value={role.years_of_experience} onChange={(event) => setRole((previous) => ({ ...previous, years_of_experience: Number(event.target.value) }))} disabled={busy} /></label>
      <label className="text-xs text-mist">Work arrangement<select className="mt-1 block border border-line bg-ink-2 px-3 py-2 text-sm text-paper" value={role.remote_work} onChange={(event) => setRole((previous) => ({ ...previous, remote_work: event.target.value }))} disabled={busy}>{(readiness?.role_options.remote_options ?? [role.remote_work]).map((value) => <option key={value}>{value}</option>)}</select></label>
      <label className="text-xs text-mist">Country<select className="mt-1 block border border-line bg-ink-2 px-3 py-2 text-sm text-paper" value={role.country_iso3} onChange={(event) => setRole((previous) => ({ ...previous, country_iso3: event.target.value }))} disabled={busy}>{(readiness?.role_options.countries ?? [role.country_iso3]).map((value) => <option key={value}>{value}</option>)}</select></label>
      <label className="flex items-center gap-2 text-xs text-mist"><input type="checkbox" checked={role.is_llm_role} onChange={(event) => setRole((previous) => ({ ...previous, is_llm_role: event.target.checked }))} disabled={busy} />LLM role</label>
      <label className="flex items-center gap-2 text-xs text-mist"><input type="checkbox" checked={role.has_python} onChange={(event) => setRole((previous) => ({ ...previous, has_python: event.target.checked }))} disabled={busy} />Python required</label>
    </div></section>
    {error && <p className="mb-5 border border-danger bg-red-50 p-3 text-sm text-danger">{error}</p>}
    <LiveBudgetGraph graphNodes={readiness?.nodes ?? []} graphEdges={readiness?.edges ?? []} steps={steps} />
    <div className="mt-5 grid gap-5 lg:grid-cols-2">
      <section className="border border-line bg-panel p-5"><h2 className="font-display text-xl font-semibold">Validation trials</h2><p className="mt-1 text-xs text-mist">The planner adds country context only when the data audit allows it. The holdout stays sealed until selection.</p><table className="mt-4 w-full text-left text-sm"><thead className="border-b border-line text-xs text-mist"><tr><th className="py-2">Model</th><th>Features</th><th>Validation MAE</th></tr></thead><tbody>{(result?.history ?? []).map((trial, index) => <tr key={index} className="border-b border-line/60"><td className="py-2">{trial.family.replaceAll('_', ' ')}</td><td>{trial.use_context ? 'jobs + GDP' : 'jobs only'}</td><td>{money(trial.validation.mae)}</td></tr>)}</tbody></table>{!result && <p className="mt-4 text-sm text-mist">Run the workflow to see its decisions.</p>}</section>
      <section className="border border-line bg-panel p-5"><h2 className="font-display text-xl font-semibold">Analyst brief</h2><p className="mt-1 text-xs text-mist">{result?.report?.status === 'ready_for_analyst_review' ? result.report.limitations : 'The selected model is evaluated once on 300 holdout jobs.'}</p><div className="mt-4 grid grid-cols-2 gap-3"><Metric label="Role salary reference" value={money(result?.report?.role_salary_reference_usd)} /><Metric label="Selected model" value={result?.report?.best_family?.replaceAll('_', ' ') ?? '—'} /><Metric label="Holdout MAE" value={money(result?.report?.holdout_metrics?.mae)} /><Metric label="Holdout R²" value={result?.report?.holdout_metrics?.r2?.toFixed(3) ?? '—'} /><Metric label="Fixed category baseline MAE" value={money(result?.report?.baseline_metrics?.mae)} /></div><p className="mt-3 text-xs text-mist">{result?.report?.role_assumptions ?? 'Waiting for a model review.'} {result?.verdict?.reason}</p></section>
    </div>
    <section className="mt-5 border border-line bg-panel p-5"><h2 className="font-display text-xl font-semibold">Agent decisions</h2><ol className="mt-3 space-y-1 font-mono text-xs text-mist">{steps.map((step, index) => <li key={index}>{index + 1}. {step.node.replaceAll('_', ' ')}: {step.note}</li>)}</ol>{steps.length === 0 && <p className="mt-2 text-sm text-mist">The route and retry record will appear here.</p>}</section>
    <section className="mt-5 border border-line bg-panel p-5"><div className="flex flex-wrap items-center justify-between gap-3"><div><h2 className="font-display text-xl font-semibold">Evaluation suite and baselines</h2><p className="mt-1 text-xs text-mist">Five cases score data checks, routing, retry, holdout discipline, and final quality. Comparators use the same salary data and split.</p></div><button type="button" onClick={() => void runEvaluation()} disabled={!readiness?.ready || evalBusy || busy} className="border border-teal px-4 py-2 text-sm text-teal disabled:opacity-50">{evalBusy ? 'Evaluating…' : 'Run evaluation suite'}</button></div>
      {evaluation && <><p className="mt-4 font-semibold">{evaluation.passed}/{evaluation.total} cases passed</p><div className="mt-3 grid gap-2 md:grid-cols-5">{evaluation.cases.map((test) => <div key={test.name} className="border border-line bg-ink-2 p-3 text-xs"><p className="font-medium">{test.name.replaceAll('_', ' ')}</p><p className={test.passed ? 'mt-1 text-ok' : 'mt-1 text-danger'}>{test.score.toFixed(2)} / {test.pass_threshold.toFixed(2)} · {test.passed ? 'PASS' : 'FAIL'}</p></div>)}</div><table className="mt-5 w-full text-left text-sm"><thead className="border-b border-line text-xs text-mist"><tr><th className="py-2">Workflow</th><th>Holdout MAE</th><th>R²</th><th>Trials</th><th>Retry route</th></tr></thead><tbody>{comparisonKeys.map((key, index) => { const value = evaluation.comparison[key] as { holdout_mae: number; holdout_r2: number; model_trials: number; failure_recovery: boolean }; return <tr key={key} className="border-b border-line/60"><td className="py-2">{comparisonLabels[index]}</td><td>{money(value.holdout_mae)}</td><td>{value.holdout_r2.toFixed(3)}</td><td>{value.model_trials}</td><td>{value.failure_recovery ? 'Yes' : 'No'}</td></tr> })}</tbody></table><p className="mt-3 text-xs text-mist">The fixed comparator fits category means on development rows only. The original classroom fraud example uses a different target, so its F1 score is not compared to salary MAE.</p></>}
    </section>
  </main></div>
}

function Metric({ label, value }: { label: string; value: string }) { return <div className="border border-line bg-ink-2 p-3"><p className="font-mono text-[10px] uppercase tracking-wider text-mist">{label}</p><p className="mt-1 text-lg font-semibold">{value}</p></div> }
