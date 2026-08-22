import React, { useState } from 'react';
import { ArrowUp, Plus, Brain, Mic } from 'lucide-react';
import './ChatInputBar.css';

interface ChatInputBarProps {
  onSubmit: (query: string) => void;
  disabled?: boolean;
}

export const ChatInputBar: React.FC<ChatInputBarProps> = ({ onSubmit, disabled }) => {
  const [inputText, setInputText] = useState<string>('');

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    if (inputText.trim() && !disabled) {
      onSubmit(inputText);
      setInputText('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  return (
    <div className="chat-input-container">
      <form onSubmit={handleSubmit} className="chat-input-capsule">
        <button type="button" className="action-btn-circle" title="첨부">
          <Plus size={18} />
        </button>

        <textarea
          className="chat-textarea"
          rows={1}
          placeholder="무엇이든 물어보세요"
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
        />

        <div className="chat-input-actions">
          <button type="button" className="action-btn-circle" title="Think 모드">
            <Brain size={18} />
          </button>
          <button type="button" className="action-btn-circle" title="음성 입력">
            <Mic size={18} />
          </button>
          <button
            type="submit"
            className={`action-btn-circle ${inputText.trim() ? 'send-active' : ''}`}
            disabled={!inputText.trim() || disabled}
          >
            <ArrowUp size={18} />
          </button>
        </div>
      </form>

      <div className="input-disclaimer">
        AI Assistant는 실수할 수 있습니다. 중요한 정보는 확인해 주세요.
      </div>
    </div>
  );
};
