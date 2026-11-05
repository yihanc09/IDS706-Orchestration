import { useMemo } from 'react'
import { Background, Controls, MarkerType, ReactFlow, type Edge, type Node } from '@xyflow/react'

type Step = { node: string }
type GraphEdge = { source: string; target: string }

const positions: Record<string, { x: number; y: number }> = {
  data_steward: { x: 0, y: 60 }, strategy_planner: { x: 250, y: 60 },
  model_trainer: { x: 500, y: 60 }, quality_reviewer: { x: 750, y: 60 },
  recovery_agent: { x: 500, y: 280 }, report_writer: { x: 1000, y: 60 },
}
const roles: Record<string, string> = {
  data_steward: 'Audit handoff and leakage', strategy_planner: 'Choose feature and model plan',
  model_trainer: 'Fit and validate', quality_reviewer: 'Review gain and route',
  recovery_agent: 'Retry a failed training step', report_writer: 'Open sealed holdout once',
}
const names: Record<string, string> = {
  data_steward: 'Data Steward', strategy_planner: 'Strategy Planner', model_trainer: 'Model Trainer',
  quality_reviewer: 'Quality Reviewer', recovery_agent: 'Recovery Agent', report_writer: 'Report Writer',
}
const labels: Record<string, string> = {
  'model_trainer-recovery_agent': 'failure', 'recovery_agent-model_trainer': 'retry',
  'quality_reviewer-strategy_planner': 'revise', 'quality_reviewer-report_writer': 'report',
}

export function LiveBudgetGraph({ graphNodes, graphEdges, steps }: { graphNodes: string[]; graphEdges: GraphEdge[]; steps: Step[] }) {
  const nodes: Node[] = useMemo(() => {
    const visited = new Set(steps.map((step) => step.node))
    return graphNodes.map((id) => ({
    id, position: positions[id] ?? { x: 0, y: 0 },
    data: { label: <div className="text-left"><div className="font-semibold">{names[id] ?? id}</div><div className="mt-1 text-xs opacity-75">{roles[id]}</div><div className="mt-2 font-mono text-[10px] uppercase">{visited.has(id) ? 'visited' : 'waiting'}</div></div> },
    style: { width: 210 },
    className: `rounded border-2 p-2 shadow-sm ${visited.has(id) ? 'border-ok bg-green-50 text-ok' : 'border-line bg-white text-paper'}`,
    }))
  }, [graphNodes, steps])
  const edges: Edge[] = useMemo(() => {
    const traversed = new Set(steps.slice(1).map((step, index) => `${steps[index].node}-${step.node}`))
    return graphEdges.map(({ source, target }) => ({
    id: `${source}-${target}`, source, target, type: 'smoothstep', label: labels[`${source}-${target}`],
    markerEnd: { type: MarkerType.ArrowClosed },
    style: { stroke: traversed.has(`${source}-${target}`) ? '#128a6b' : '#a5b4ac', strokeWidth: traversed.has(`${source}-${target}`) ? 3 : 2, strokeDasharray: labels[`${source}-${target}`] ? '6 4' : undefined },
    labelStyle: { fill: '#4f6c5e', fontSize: 12 },
    }))
  }, [graphEdges, steps])
  return <section className="border border-line bg-panel/80">
    <div className="border-b border-line px-4 py-3"><h2 className="font-display text-xl font-semibold">Adaptive agent graph</h2><p className="text-xs text-mist">{graphNodes.length} agents. Highlighted edges show the route taken in this run. The recovery branch is used only after a training failure.</p></div>
    <div className="h-[420px]">{graphNodes.length > 0 ? <ReactFlow key={steps.length} nodes={nodes} edges={edges} fitView fitViewOptions={{ padding: 0.18 }} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false}><Background color="#c8d5ce" gap={20} /><Controls showInteractive={false} /></ReactFlow> : <p className="p-5 text-sm text-mist">Loading the backend graph…</p>}</div>
  </section>
}
