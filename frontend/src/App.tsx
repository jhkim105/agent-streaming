import { useAgentStream } from './hooks/useAgentStream';
import { ChatInput } from './components/ChatInput';
import { ChatTimeline } from './components/ChatTimeline';
import { ReportViewer } from './components/ReportViewer';
import { A2UIRenderer } from './components/A2UIRenderer';
import { Bot, AlertTriangle, RefreshCw } from 'lucide-react';
import './App.css';

export function App() {
  const {
    sessionId,
    connectionStatus,
    statusLogs,
    reportMarkdown,
    a2uiData,
    isResearching,
    errorMsg,
    submitQuery,
    sendUserAction,
    reconnect
  } = useAgentStream();

  return (
    <div className="app-container">
      {/* Header Bar */}
      <header className="app-header glass-panel">
        <div className="brand-section">
          <div className="brand-logo">
            <Bot size={24} />
          </div>
          <div>
            <h1 className="brand-title">Real-time AI Researcher</h1>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
              {sessionId ? `Session: ${sessionId.slice(0, 8)}...` : 'Connecting session...'}
            </span>
          </div>
        </div>

        <div className="status-badge">
          <span className={`status-dot ${connectionStatus.toLowerCase()}`} />
          <span>
            {connectionStatus === 'CONNECTED' && '서버 연결됨 (SSE Active)'}
            {connectionStatus === 'CONNECTING' && '세션 연결 중...'}
            {connectionStatus === 'DISCONNECTED' && '연결 끊김'}
            {connectionStatus === 'ERROR' && '연결 오류'}
          </span>
          {connectionStatus === 'ERROR' && (
            <button onClick={reconnect} style={{ background: 'none', color: 'var(--accent-cyan)', marginLeft: '4px' }}>
              <RefreshCw size={14} />
            </button>
          )}
        </div>
      </header>

      {/* Error Alert Toast */}
      {errorMsg && (
        <div className="glass-panel" style={{ padding: '0.875rem 1.25rem', borderColor: 'rgba(239, 68, 68, 0.4)', background: 'rgba(239, 68, 68, 0.1)', color: '#fca5a5', display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.9rem' }}>
          <AlertTriangle size={18} />
          <span>{errorMsg}</span>
        </div>
      )}

      {/* Input Form Section */}
      <ChatInput onSubmit={submitQuery} disabled={isResearching || connectionStatus !== 'CONNECTED'} />

      {/* Grid Content Layout */}
      <main className="main-content-grid">
        {/* Left Column: Reasoning Timeline */}
        <section>
          <ChatTimeline logs={statusLogs} isResearching={isResearching} />
        </section>

        {/* Right Column: Markdown Report Stream Viewer & A2UI Dashboard */}
        <section style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <ReportViewer markdownText={reportMarkdown} isResearching={isResearching} />
          {a2uiData && (
            <A2UIRenderer
              data={a2uiData}
              onActionSelect={sendUserAction}
              disabled={isResearching}
            />
          )}
        </section>
      </main>
    </div>
  );
}

export default App;
