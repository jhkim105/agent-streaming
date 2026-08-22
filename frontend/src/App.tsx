import { useState } from 'react';
import { useAgentStream } from './hooks/useAgentStream';
import { Sidebar } from './components/Sidebar';
import { ChatThreadWindow } from './components/ChatThreadWindow';
import { ChatInputBar } from './components/ChatInputBar';
import { DebugPacketInspector } from './components/DebugPacketInspector';
import { ChevronDown, AlertTriangle, RefreshCw } from 'lucide-react';
import './App.css';

export function App() {
  const {
    connectionId,
    conversationId,
    connectionStatus,
    statusLogs,
    reportMarkdown,
    a2uiData,
    isResearching,
    errorMsg,
    conversations,
    rawPacketLogs,
    clearRawPacketLogs,
    submitQuery,
    sendUserAction,
    startNewConversation,
    selectConversation,
    reconnect
  } = useAgentStream();

  const [showDebugInspector, setShowDebugInspector] = useState<boolean>(false);

  return (
    <div className="app-chat-layout">
      {/* 1. Left Sidebar (Light Theme) */}
      <Sidebar
        conversations={conversations}
        currentConversationId={conversationId}
        onSelectConversation={selectConversation}
        onNewChat={startNewConversation}
        onToggleDebug={() => setShowDebugInspector((prev) => !prev)}
        showDebugInspector={showDebugInspector}
      />

      {/* 2. Main Chat Area */}
      <main className="chat-main-area">
        {/* Header Bar */}
        <header className="chat-header-bar">
          <div className="chat-model-selector">
            <span>Agent Assistant</span>
            <ChevronDown size={16} />
          </div>

          <div className="connection-status-pill">
            <span className={`status-dot-sm ${connectionStatus.toLowerCase()}`} />
            <span>
              {connectionStatus === 'CONNECTED' && (connectionId ? `연결됨 (${connectionId.slice(0, 10)}...)` : '서버 연결됨')}
              {connectionStatus === 'CONNECTING' && '연결 중...'}
              {connectionStatus === 'DISCONNECTED' && '연결 끊김'}
              {connectionStatus === 'ERROR' && '연결 오류'}
            </span>
            {connectionStatus === 'ERROR' && (
              <button onClick={() => reconnect()} style={{ background: 'none', color: 'var(--accent-cyan)', border: 'none', marginLeft: '4px', cursor: 'pointer' }}>
                <RefreshCw size={12} />
              </button>
            )}
          </div>
        </header>

        {/* Error Alert Toast */}
        {errorMsg && (
          <div style={{ padding: '0.65rem 1rem', background: '#fef2f2', borderBottom: '1px solid #fca5a5', color: '#991b1b', display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.85rem' }}>
            <AlertTriangle size={16} />
            <span>{errorMsg}</span>
          </div>
        )}

        {/* Raw SSE Packet Terminal Inspector Component Toggle */}
        {showDebugInspector && (
          <div style={{ padding: '0.75rem 1rem', background: 'var(--bg-sidebar)', borderBottom: '1px solid var(--border-color)' }}>
            <DebugPacketInspector logs={rawPacketLogs} onClear={clearRawPacketLogs} />
          </div>
        )}

        {/* 3. Middle Main Chat Thread Stream Window */}
        <ChatThreadWindow
          statusLogs={statusLogs}
          reportMarkdown={reportMarkdown}
          a2uiData={a2uiData}
          isResearching={isResearching}
          onSelectPrompt={(prompt) => submitQuery(prompt)}
          onActionSelect={sendUserAction}
        />

        {/* 4. Bottom Fixed Floating Capsule Input Bar */}
        <ChatInputBar
          onSubmit={submitQuery}
          disabled={isResearching || connectionStatus !== 'CONNECTED'}
        />
      </main>
    </div>
  );
}

export default App;
