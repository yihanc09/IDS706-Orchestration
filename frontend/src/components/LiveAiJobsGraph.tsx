import { Background, Controls, MarkerType, ReactFlow, type Edge, type Node } from '@xyflow/react'

export type AgentStatus = 'waiting' | 'running' | 'done' | 'failed'

const agents = [
  { id: 'data_auditor', title: 'Data Auditor', role: 'Check Airflow output', x: 0, y: 80 },
  { id: 'experiment_planner', title: 'Experiment Planner', role: 'Choose next regressor', x: 280, y: 80 },
  { id: 'model_trainer', title: 'Model Trainer', role: 'Fit and validate', x: 560, y: 80 },
  { id: 'evaluation_judge', title: 'Evaluation Judge', role: 'Compare MAE', x: 840, y: 80 },
  { id: 'holdout_reporter', title: 'Holdout Reporter', role: 'Final MAE and R²', x: 1120, y: 80 },
] as const

const links: Edge[] = [
  ['data_auditor', 'experiment_planner'],
  ['experiment_planner', 'model_trainer'],
  ['model_trainer', 'evaluation_judge'],
  ['evaluation_judge', 'holdout_reporter'],
].map(([source, target]) => ({
  id: `${source}-${target}`, source, target, type: 'smoothstep',
  markerEnd: { type: MarkerType.ArrowClosed },
  style: { stroke: '#5a7267', strokeWidth: 2 },
}))
links.push({
  id: 'revise', source: 'evaluation_judge', target: 'experiment_planner',
  type: 'smoothstep', label: 'revise',
  sourceHandle: undefined, targetHandle: undefined,
  markerEnd: { type: MarkerType.ArrowClosed },
  style: { stroke: '#9a6b12', strokeWidth: 2, strokeDasharray: '6 4' },
  labelStyle: { fill: '#9a6b12', fontSize: 12 },
})

export function LiveAiJobsGraph({ status }: { status: Record<string, AgentStatus> }) {
  const nodes: Node[] = agents.map((agent) => {
    const current = status[agent.id] ?? 'waiting'
    const colors = {
      waiting: 'border-line bg-white text-paper',
      running: 'border-teal bg-teal/10 text-teal',
      done: 'border-ok bg-green-50 text-ok',
      failed: 'border-danger bg-red-50 text-danger',
    }
    return {
      id: agent.id,
      position: { x: agent.x, y: agent.y },
      data: {
        label: (
          <div className="text-left">
            <div className="font-semibold">{agent.title}</div>
            <div className="mt-1 text-xs opacity-75">{agent.role}</div>
            <div className="mt-2 font-mono text-[10px] uppercase tracking-wide">{current}</div>
          </div>
        ),
      },
      style: { width: 205 },
      className: `rounded border-2 p-2 shadow-sm ${colors[current]}`,
    }
  })

  return (
    <section className="border border-line bg-panel/80">
      <div className="border-b border-line px-4 py-3">
        <h2 className="font-display text-xl font-semibold">Live agent graph</h2>
        <p className="text-xs text-mist">A validation result can send the planner back for another model. Holdout runs once at the end.</p>
      </div>
      <div className="h-[340px]">
        <ReactFlow nodes={nodes} edges={links} fitView fitViewOptions={{ padding: 0.15 }} nodesDraggable={false} nodesConnectable={false} elementsSelectable={false}>
          <Background color="#c8d5ce" gap={20} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </section>
  )
}
