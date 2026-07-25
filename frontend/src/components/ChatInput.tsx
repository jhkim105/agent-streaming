import React, { useState } from 'react';
import { Send, Sparkles } from 'lucide-react';
import './ChatInput.css';

interface ChatInputProps {
  onSubmit: (query: string) => void;
  disabled: boolean;
}

export const ChatInput: React.FC<ChatInputProps> = ({ onSubmit, disabled }) => {
  const [query, setQuery] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim() || disabled) return;
    onSubmit(query.trim());
  };

  const handleQuickQuestion = (text: string) => {
    setQuery(text);
  };

  return (
    <div className="input-wrapper glass-panel">
      <form onSubmit={handleSubmit} className="input-form">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="리서치하고 싶은 분석 주제나 질문을 입력하세요... (예: 생성형 AI 최신동향)"
          disabled={disabled}
          className="chat-input"
        />
        <button
          type="submit"
          disabled={disabled || !query.trim()}
          className="send-button"
        >
          <Send size={18} />
          <span>분석 시작</span>
        </button>
      </form>

      {/* Quick Suggestion Pills */}
      <div className="quick-suggestions">
        <span className="suggestion-label"><Sparkles size={14} /> 추천 주제:</span>
        <button
          className="suggestion-chip"
          onClick={() => handleQuickQuestion('최근 발표된 생성형 AI 신기술 동향 요약 보고서 써줘')}
          disabled={disabled}
        >
          🤖 생성형 AI 신기술 동향
        </button>
        <button
          className="suggestion-chip"
          onClick={() => handleQuickQuestion('실시간 소켓 스트리밍 아키텍처 비교 분석해줘')}
          disabled={disabled}
        >
          ⚡ 실시간 스트리밍 아키텍처
        </button>
      </div>
    </div>
  );
};
