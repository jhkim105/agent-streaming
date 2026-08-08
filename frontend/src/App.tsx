import { useAgentStream } from './hooks/useAgentStream';
import { ChatInput } from './components/ChatInput';
import { ChatTimeline } from './components/ChatTimeline';
import { ReportViewer } from './components/ReportViewer';
import { A2UIRenderer } from './components/A2UIRenderer';
import { Bot, AlertTriangle, RefreshCw, PlusCircle, History, MessageSquare } from 'lucide-react';
import './App.css';

export function App() {
  const {
    sessionId,
    conversationId,
    connectionStatus,
    statusLogs,
    reportMarkdown,
    a2uiData,
    isResearching,
    errorMsg,
    conversations,
    submitQuery,
    sendUserAction,
    startNewConversation,
    selectConversation,
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
              {conversationId ? `Conv: ${conversationId}` : (sessionId ? `Session: ${sessionId.slice(0, 8)}...` : 'Connecting session...')}
            </span>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <button 
            className="new-chat-btn glass-panel"
            onClick={startNewConversation}
            title="새로운 리서치 대화 시작"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.35rem',
              padding: '0.4rem 0.8rem',
              background: 'rgba(56, 189, 248, 0.15)',
              border: '1px solid rgba(56, 189, 248, 0.4)',
              color: 'var(--accent-cyan)',
              borderRadius: '8px',
              cursor: 'pointer',
              fontSize: '0.85rem',
              fontWeight: 600
            }}
          >
            <PlusCircle size={16} />
            <span>새 질문</span>
          </button>

          <div className="status-badge">
            <span className={`status-dot ${connectionStatus.toLowerCase()}`} />
            <span>
              {connectionStatus === 'CONNECTED' && '서버 연결됨'}
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

      {/* Main Grid Content Layout */}
      <main className="main-content-grid" style={{ gridTemplateColumns: '260px 1fr 1fr' }}>
        {/* Column 1: History Sidebar */}
        <section className="glass-panel" style={{ padding: '1rem', display: 'flex', flexDirection: 'column', gap: '0.75rem', maxHeight: 'calc(100vh - 230px)', overflowY: 'auto' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--text-bright)', fontWeight: 600, fontSize: '0.95rem', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem' }}>
            <History size={18} style={{ color: 'var(--accent-cyan)' }} />
            <span>이전 대화 히스토리</span>
          </div>

          {conversations.length === 0 ? (
            <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', textAlign: 'center', padding: '1rem 0' }}>
              이전 리서치 대화 기록이 없습니다.
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
              {conversations.map((item) => {
                const isActive = item.conversationId === conversationId;
                return (
                  <button
                    key={item.conversationId}
                    onClick={() => selectConversation(item.conversationId)}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      textAlign: 'left',
                      padding: '0.5rem 0.75rem',
                      borderRadius: '6px',
                      border: isActive ? '1px solid var(--accent-cyan)' : '1px solid transparent',
                      background: isActive ? 'rgba(56, 189, 248, 0.15)' : 'rgba(255, 255, 255, 0.03)',
                      color: isActive ? 'var(--accent-cyan)' : 'var(--text-bright)',
                      cursor: 'pointer',
                      fontSize: '0.85rem',
                      width: '100%',
                      transition: 'all 0.2s ease'
                    }}
                  >
                    <MessageSquare size={14} style={{ flexShrink: 0 }} />
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {item.title}
                    </span>
                  </button>
                );
              })}
            </div>
          )}
        </section>

        {/* Column 2: Reasoning Timeline */}
        <section>
          <ChatTimeline logs={statusLogs} isResearching={isResearching} />
        </section>

        {/* Column 3: Markdown Report Stream Viewer & A2UI Dashboard */}
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
