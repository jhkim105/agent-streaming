import React, { useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { FileCode, Sparkles } from 'lucide-react';
import './ReportViewer.css';

interface ReportViewerProps {
  markdownText: string;
  isResearching: boolean;
}

export const ReportViewer: React.FC<ReportViewerProps> = ({ markdownText, isResearching }) => {
  const scrollRef = useRef<HTMLDivElement>(null);

  // 새로운 마크다운 청크가 들어올 때 하단 자동 스크롤
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [markdownText]);

  return (
    <div className="report-container glass-panel">
      <div className="report-header">
        <h3 className="report-title">
          <Sparkles size={18} className="title-icon" />
          <span>실시간 리서치 보고서</span>
        </h3>
        {isResearching && <span className="typing-indicator">● 타자기 스트리밍 중...</span>}
      </div>

      <div className="report-content" ref={scrollRef}>
        {!markdownText ? (
          <div className="empty-report">
            <FileCode size={48} className="empty-icon" />
            <p>에이전트가 완성을 진행하면 이곳에 보고서 마크다운이 실시간 타자기 효과로 출력됩니다.</p>
          </div>
        ) : (
          <div className="markdown-body">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {markdownText}
            </ReactMarkdown>
            {isResearching && <span className="cursor-blink">|</span>}
          </div>
        )}
      </div>
    </div>
  );
};
