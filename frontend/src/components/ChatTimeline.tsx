import React from 'react';
import type { StatusLog } from '../types/agent';
import { Brain, Search, FileText, CheckCircle, Loader2 } from 'lucide-react';
import './ChatTimeline.css';

interface ChatTimelineProps {
  logs: StatusLog[];
  isResearching: boolean;
}

export const ChatTimeline: React.FC<ChatTimelineProps> = ({ logs, isResearching }) => {
  const getStepIcon = (step: string) => {
    switch (step) {
      case 'query_analysis':
        return <Brain size={16} className="step-icon text-indigo" />;
      case 'web_search':
        return <Search size={16} className="step-icon text-cyan" />;
      case 'web_scraping':
        return <FileText size={16} className="step-icon text-emerald" />;
      default:
        return <CheckCircle size={16} className="step-icon text-muted" />;
    }
  };

  return (
    <div className="timeline-container glass-panel">
      <div className="timeline-header">
        <h3 className="timeline-title">
          <span>🧠</span> 에이전트 추론 타임라인
        </h3>
        {isResearching && (
          <span className="researching-spinner">
            <Loader2 size={16} className="spin" /> 실행 중
          </span>
        )}
      </div>

      <div className="timeline-body">
        {logs.length === 0 ? (
          <div className="empty-timeline">
            <Brain size={36} className="empty-icon" />
            <p>질문을 제출하면 에이전트의 실시간 추론 과정이 타임라인으로 기록됩니다.</p>
          </div>
        ) : (
          <ul className="timeline-list">
            {logs.map((log) => (
              <li key={log.id} className="timeline-item animate-slide-up">
                <div className="timeline-item-icon">
                  {getStepIcon(log.step)}
                </div>
                <div className="timeline-item-content">
                  <div className="item-text">{log.content}</div>
                  <div className="item-time">
                    {new Date(log.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};
