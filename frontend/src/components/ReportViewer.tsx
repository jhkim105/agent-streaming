import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './ReportViewer.css';

interface ReportViewerProps {
  markdownText: string;
  isResearching: boolean;
}

export const ReportViewer: React.FC<ReportViewerProps> = ({ markdownText, isResearching }) => {
  return (
    <div className="report-container">
      <div className="report-content">
        <div className="markdown-body">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {markdownText}
          </ReactMarkdown>
          {isResearching && <span className="cursor-blink">▌</span>}
        </div>
      </div>
    </div>
  );
};
