import React, { useState, useEffect, useRef } from 'react';
import type { ChatTurn } from '../types/agent';
import { ReportViewer } from './ReportViewer';
import { A2UIRenderer } from './A2UIRenderer';
import { Brain, ChevronDown, ChevronRight, User, Bot, Sparkles, Layers, Globe, Edit3, ImageIcon } from 'lucide-react';
import './ChatThreadWindow.css';

interface ChatThreadWindowProps {
  turns: ChatTurn[];
  isResearching: boolean;
  onSelectPrompt: (promptText: string) => void;
  onActionSelect: (actionId: string, payload: Record<string, any>) => void;
}

export const ChatThreadWindow: React.FC<ChatThreadWindowProps> = ({
  turns,
  isResearching,
  onSelectPrompt,
  onActionSelect
}) => {
  // 각 턴별 사고과정(Thinking) 아코디언 열림/닫힘 상태 관리
  const [openThinkingMap, setOpenThinkingMap] = useState<Record<string, boolean>>({});
  const bottomRef = useRef<HTMLDivElement>(null);

  const toggleThinking = (runId: string) => {
    setOpenThinkingMap((prev) => ({
      ...prev,
      [runId]: prev[runId] !== undefined ? !prev[runId] : false
    }));
  };

  // 새로운 메시지가 추가되거나 스트리밍 중일 때 부드럽게 하단 스크롤
  useEffect(() => {
    if (bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [turns]);

  const hasContent = turns.length > 0;

  return (
    <div className="chat-thread-container">
      {!hasContent ? (
        /* Empty Hero Section (ChatGPT Home Look) */
        <div className="chat-empty-hero">
          <div className="hero-badge">
            <Sparkles size={16} />
            <span>AI Agent Streaming & AGUI Protocol</span>
          </div>
          <h2 className="hero-title">오늘은 무엇을 도와드릴까요?</h2>

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
        /* Active Multi-Turn Chat List */
        <div className="chat-message-list">
          {turns.map((turn, index) => {
            const isThinkingOpen = openThinkingMap[turn.runId] !== undefined 
              ? openThinkingMap[turn.runId] 
              : turn.isStreaming; // 스트리밍 중일 때는 기본으로 열어둠

            return (
              <div key={turn.runId || index} className="chat-turn-block">
                {/* 1. 사용자 질문 말풍선 (우측) */}
                {turn.userPrompt && (
                  <div className="chat-message-row user">
                    <div className="user-bubble-wrapper">
                      <div className="user-bubble">
                        {turn.userPrompt}
                      </div>
                      <div className="avatar user-avatar">
                        <User size={15} />
                      </div>
                    </div>
                  </div>
                )}

                {/* 2. 에이전트 응답 블록 (좌측) */}
                <div className="chat-message-row agent">
                  <div className="agent-bubble-wrapper">
                    <div className="avatar agent-avatar">
                      <Bot size={16} />
                    </div>

                    <div className="agent-response-block">
                      {/* 사고과정 (Thinking Process) Accordion */}
                      {turn.statusLogs && turn.statusLogs.length > 0 && (
                        <div className="thinking-accordion">
                          <div className="thinking-header" onClick={() => toggleThinking(turn.runId)}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                              <Brain size={15} style={{ color: '#38bdf8' }} />
                              <span>{turn.isStreaming ? '사고과정 추론 중...' : '사고과정 (Thinking Process)'}</span>
                            </div>
                            {isThinkingOpen ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
                          </div>

                          {isThinkingOpen && (
                            <div className="thinking-body">
                              {turn.statusLogs.map((log) => (
                                <div key={log.id} className="thinking-log-item">
                                  <span>▸ {log.content}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}

                      {/* 실시간 마크다운 스트리밍 답변 */}
                      {(turn.reportMarkdown || turn.isStreaming) && (
                        <ReportViewer 
                          markdownText={turn.reportMarkdown} 
                          isResearching={turn.isStreaming} 
                        />
                      )}

                      {/* 선언적 AGUI 대시보드 렌더링 */}
                      {turn.a2uiData && (
                        <A2UIRenderer
                          data={turn.a2uiData}
                          onActionSelect={onActionSelect}
                          disabled={isResearching}
                        />
                      )}
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
          <div ref={bottomRef} style={{ height: '1px' }} />
        </div>
      )}
    </div>
  );
};
