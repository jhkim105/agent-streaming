import React, { useState } from 'react';
import type { StatusLog, A2UIData } from '../types/agent';
import { ReportViewer } from './ReportViewer';
import { A2UIRenderer } from './A2UIRenderer';
import { Brain, ChevronDown, ChevronRight, Image as ImageIcon, Edit3, Globe, Layers } from 'lucide-react';
import './ChatThreadWindow.css';

interface ChatThreadWindowProps {
  statusLogs: StatusLog[];
  reportMarkdown: string;
  a2uiData: A2UIData | null;
  isResearching: boolean;
  onSelectPrompt: (promptText: string) => void;
  onActionSelect: (actionId: string, payload: Record<string, any>) => void;
}

export const ChatThreadWindow: React.FC<ChatThreadWindowProps> = ({
  statusLogs,
  reportMarkdown,
  a2uiData,
  isResearching,
  onSelectPrompt,
  onActionSelect
}) => {
  const [isThinkingOpen, setIsThinkingOpen] = useState<boolean>(true);

  const hasContent = statusLogs.length > 0 || reportMarkdown.trim().length > 0 || a2uiData !== null;

  return (
    <div className="chat-thread-container">
      {!hasContent ? (
        /* Empty Hero Section (ChatGPT Home Look) */
        <div className="chat-empty-hero">
          <h2 className="hero-title">오늘은 무엇을 해볼까요?</h2>

          <div className="hero-cards-grid">
            <button className="hero-card" onClick={() => onSelectPrompt('AGUI 스트리밍 아키텍처 및 4대 식별자 구조 비교 설명해줘')}>
              <Layers size={18} style={{ color: '#38bdf8' }} />
              <span>AGUI 스트리밍 비교</span>
            </button>
            <button className="hero-card" onClick={() => onSelectPrompt('최근 AI 에이전트 동향 및 기술 트렌드 요약')}>
              <Globe size={18} style={{ color: '#4ade80' }} />
              <span>웹 기반 에이전트 동향</span>
            </button>
            <button className="hero-card" onClick={() => onSelectPrompt('마이크로 서비스 간 이벤트 디스패치 샘플 코드 알려줘')}>
              <Edit3 size={18} style={{ color: '#c084fc' }} />
              <span>샘플 구현 작성</span>
            </button>
            <button className="hero-card" onClick={() => onSelectPrompt('SSE 배압 조절 및 Coroutine Flow 개념 가이드')}>
              <ImageIcon size={18} style={{ color: '#facc15' }} />
              <span>SSE 배압 가이드</span>
            </button>
          </div>
        </div>
      ) : (
        /* Active Chat Message Stream */
        <div className="chat-message-list">
          <div className="chat-message-row agent">
            <div className="agent-response-block">
              {/* Agent Thinking Process Accordion */}
              {statusLogs.length > 0 && (
                <div className="thinking-accordion">
                  <div className="thinking-header" onClick={() => setIsThinkingOpen(!isThinkingOpen)}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                      <Brain size={15} style={{ color: '#38bdf8' }} />
                      <span>{isResearching ? '추론 진행 중...' : '사고과정 (Thinking Process)'}</span>
                    </div>
                    {isThinkingOpen ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
                  </div>

                  {isThinkingOpen && (
                    <div className="thinking-body">
                      {statusLogs.map((log) => (
                        <div key={log.id} className="thinking-log-item">
                          <span>▸ {log.content}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Streaming Markdown Answer */}
              {(reportMarkdown || isResearching) && (
                <ReportViewer markdownText={reportMarkdown} isResearching={isResearching} />
              )}

              {/* Inline Dynamic A2UI Dashboard */}
              {a2uiData && (
                <A2UIRenderer
                  data={a2uiData}
                  onActionSelect={onActionSelect}
                  disabled={isResearching}
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
