import React from 'react';
import type { ConversationSummary } from '../types/agent';
import { Plus, MessageSquare, Terminal, Bot } from 'lucide-react';
import './Sidebar.css';

interface SidebarProps {
  conversations: ConversationSummary[];
  currentConversationId: string | null;
  onSelectConversation: (conversationId: string) => void;
  onNewChat: () => void;
  onToggleDebug: () => void;
  showDebugInspector: boolean;
}

export const Sidebar: React.FC<SidebarProps> = ({
  conversations,
  currentConversationId,
  onSelectConversation,
  onNewChat,
  onToggleDebug,
  showDebugInspector
}) => {
  return (
    <aside className="chat-sidebar">
      <div className="sidebar-header">
        <div className="brand-title-chat">
          <Bot size={20} style={{ color: 'var(--accent-cyan)' }} />
          <span>AI Assistant</span>
        </div>
      </div>

      <button className="new-chat-sidebar-btn" onClick={onNewChat}>
        <Plus size={16} />
        <span>새 채팅</span>
      </button>

      <div className="sidebar-section-title">최근 대화</div>

      <div className="sidebar-conv-list">
        {conversations.length === 0 ? (
          <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)', padding: '0.5rem', textAlign: 'center' }}>
            이전 대화 기록이 없습니다.
          </div>
        ) : (
          conversations.map((item) => {
            const isActive = item.conversationId === currentConversationId;
            return (
              <button
                key={item.conversationId}
                className={`sidebar-conv-item ${isActive ? 'active' : ''}`}
                onClick={() => onSelectConversation(item.conversationId)}
              >
                <MessageSquare size={14} style={{ flexShrink: 0 }} />
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {item.title}
                </span>
              </button>
            );
          })
        )}
      </div>

      <div className="sidebar-footer">
        <button
          className="sidebar-footer-item"
          onClick={onToggleDebug}
          style={{ color: showDebugInspector ? 'var(--accent-cyan)' : 'var(--text-muted)' }}
        >
          <Terminal size={16} />
          <span>{showDebugInspector ? '디버거 닫기' : 'Raw 패킷 디버거'}</span>
        </button>
      </div>
    </aside>
  );
};
