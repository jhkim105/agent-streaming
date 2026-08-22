import React, { useState, useRef, useEffect } from 'react';
import type { RawPacketLog } from '../types/agent';
import { Terminal, Trash2, ExternalLink } from 'lucide-react';
import './DebugPacketInspector.css';

interface DebugPacketInspectorProps {
  logs: RawPacketLog[];
  onClear: () => void;
}

export const DebugPacketInspector: React.FC<DebugPacketInspectorProps> = ({ logs, onClear }) => {
  const [activeFilter, setActiveFilter] = useState<string>('ALL');
  const [autoScroll, setAutoScroll] = useState<boolean>(true);
  const terminalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (autoScroll && terminalRef.current) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [logs, autoScroll]);

  const filteredLogs = logs.filter(pkt => activeFilter === 'ALL' || pkt.type === activeFilter);

  return (
    <div className="debug-inspector-panel glass-panel">
      <div className="debug-inspector-header">
        <div className="debug-inspector-title">
          <Terminal size={18} />
          <span>Raw SSE Packet Inspector (Live Dump)</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <a
            href="http://localhost:8080/sse-debug.html"
            target="_blank"
            rel="noreferrer"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.25rem',
              color: 'var(--accent-cyan)',
              fontSize: '0.78rem',
              textDecoration: 'none'
            }}
          >
            <span>백엔드 정적 웹페이지 열기</span>
            <ExternalLink size={12} />
          </a>
          <button
            onClick={onClear}
            style={{
              background: 'none',
              border: 'none',
              color: 'var(--text-dim)',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.2rem',
              fontSize: '0.8rem'
            }}
            title="로그 비우기"
          >
            <Trash2 size={14} />
          </button>
        </div>
      </div>

      {/* 필터 바 */}
      <div className="debug-filter-bar">
        <div style={{ display: 'flex', gap: '0.25rem' }}>
          {['ALL', 'INIT', 'STATUS', 'CHUNK', 'A2UI_RENDER', 'DONE', 'ERROR'].map((type) => (
            <button
              key={type}
              className={`debug-filter-btn ${activeFilter === type ? 'active' : ''}`}
              onClick={() => setActiveFilter(type)}
            >
              {type}
            </button>
          ))}
        </div>

        <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
          />
          Auto Scroll
        </label>
      </div>

      {/* 실시간 Raw 이벤트 스트림 터미널 */}
      <div className="debug-terminal" ref={terminalRef}>
        {filteredLogs.length === 0 ? (
          <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', textAlign: 'center', margin: 'auto' }}>
            수신된 raw SSE 수신 패킷이 없습니다. 질문을 입력하면 원문 이벤트를 실시간으로 덤프합니다.
          </div>
        ) : (
          filteredLogs.map((pkt) => (
            <div key={`${pkt.count}-${pkt.timestamp}`} className={`debug-card ${pkt.type}`}>
              <div className="debug-card-header">
                <span>#{pkt.count} | <strong>{pkt.type}</strong> | ID: {pkt.eventId}</span>
                <span>⏱️ {pkt.timestamp}</span>
              </div>
              <div className="debug-raw-text">{pkt.rawData}</div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
