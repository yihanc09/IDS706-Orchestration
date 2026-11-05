import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { GuidePage } from './pages/GuidePage'
import { ControlCenterPage } from './pages/ControlCenterPage'
import { ProjectDetailPage } from './pages/ProjectDetailPage'
import { ProjectsPage } from './pages/ProjectsPage'
import { SetupPage } from './pages/SetupPage'
import { SimpleLoopPage } from './pages/SimpleLoopPage'
import { StudentLoopPage } from './pages/StudentLoopPage'
import { AiJobsLoopPage } from './pages/AiJobsLoopPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<ProjectsPage />} />
        <Route path="/projects/:projectId" element={<ProjectDetailPage />} />
        <Route path="/projects/:projectId/new" element={<SetupPage />} />
        <Route path="/session/:sessionId" element={<ControlCenterPage />} />
        <Route path="/guide" element={<GuidePage />} />
        <Route path="/student-loop" element={<StudentLoopPage />} />
        <Route path="/ai-jobs-loop" element={<AiJobsLoopPage />} />
        <Route path="/simple-loop" element={<SimpleLoopPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
