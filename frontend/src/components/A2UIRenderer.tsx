import React from 'react';
import type { A2UIData, A2UIActionOption } from '../types/agent';
import './A2UIRenderer.css';

interface A2UIRendererProps {
  data: A2UIData;
  onActionSelect: (actionId: string, payload: Record<string, any>) => void;
  disabled?: boolean;
}

export const A2UIRenderer: React.FC<A2UIRendererProps> = ({ data, onActionSelect, disabled }) => {
  if (!data) return null;

  return (
    <div className="a2ui-dashboard">
      <div className="a2ui-header">
        <div className="a2ui-title">
          <span>{data.title || '📊 리서치 데이터 대시보드'}</span>
        </div>
        <span className="a2ui-version-badge">A2UI v{data.version || '1.0'}</span>
      </div>

      {/* 1. 지표 카드 메트릭 세션 */}
      {data.metrics && data.metrics.length > 0 && (
        <div className="a2ui-metrics-grid">
          {data.metrics.map((metric) => (
            <div key={metric.id} className="a2ui-metric-card">
              <div className="a2ui-metric-label">{metric.label}</div>
              <div className="a2ui-metric-value">{metric.value}</div>
              {metric.change && <div className="a2ui-metric-change">{metric.change}</div>}
            </div>
          ))}
        </div>
      )}

      {/* 2. 에이전트 액션 선택 섹션 (Human-in-the-Loop) */}
      {data.action_section && (
        <div className="a2ui-action-section">
          <div className="a2ui-action-title">{data.action_section.title}</div>
          <div className="a2ui-action-desc">{data.action_section.description}</div>

          <div className="a2ui-options-grid">
            {data.action_section.options.map((opt: A2UIActionOption) => (
              <button
                key={opt.action_id}
                className="a2ui-option-btn"
                disabled={disabled}
                onClick={() => onActionSelect(opt.action_id, opt.payload || { label: opt.label })}
              >
                <span className="a2ui-option-label">{opt.label}</span>
                {opt.description && <span className="a2ui-option-subtext">{opt.description}</span>}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
