export type EventType = 'INIT' | 'STATUS' | 'CHUNK' | 'DONE' | 'ERROR' | 'A2UI_RENDER';

export interface EventMetadata {
  step?: string;
  timestamp?: number;
}

export interface AgentEvent {
  sessionId: string;
  hostId?: string;
  type: EventType;
  content: string;
  metadata?: EventMetadata;
}

export interface StatusLog {
  id: string;
  step: string;
  content: string;
  timestamp: number;
}

export type ConnectionStatus = 'DISCONNECTED' | 'CONNECTING' | 'CONNECTED' | 'ERROR';

export interface A2UIMetric {
  id: string;
  label: string;
  value: string;
  change?: string;
  status?: 'normal' | 'success' | 'warning';
}

export interface A2UIActionOption {
  action_id: string;
  label: string;
  description?: string;
  payload?: Record<string, any>;
}

export interface A2UIData {
  version: string;
  title: string;
  metrics: A2UIMetric[];
  action_section: {
    title: string;
    description: string;
    options: A2UIActionOption[];
  };
}
