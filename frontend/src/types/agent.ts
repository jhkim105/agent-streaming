export type EventType = 'INIT' | 'STATUS' | 'CHUNK' | 'A2UI_RENDER' | 'DONE' | 'ERROR';

export type ConnectionStatus = 'DISCONNECTED' | 'CONNECTING' | 'CONNECTED' | 'ERROR';

export interface EventMetadata {
  step?: string;
  title?: string;
  timestamp?: number;
  connectionId?: string;
}

export interface AgentEvent {
  eventId: string;
  commandId?: string;
  conversationId?: string;
  hostId?: string;
  type: EventType;
  content: string;
  metadata?: EventMetadata;
  timestamp?: number;
}

export interface AgentCommand {
  commandId?: string;
  conversationId?: string;
  connectionId?: string;
  type: 'RESEARCH' | 'ACTION' | 'CANCEL';
  payload: Record<string, any>;
}

export interface StatusLog {
  id: string;
  step: string;
  content: string;
  timestamp: number;
}

export interface RawPacketLog {
  count: number;
  timestamp: string;
  type: string;
  eventId: string;
  rawData: string;
}

export interface A2UIMetric {
  id: string;
  label: string;
  value: string;
  change?: string;
  status?: string;
}

export interface A2UIActionOption {
  id: string;
  action_id?: string;
  label: string;
  description?: string;
  action_type: string;
  payload: Record<string, any>;
}

export interface A2UIActionSection {
  title?: string;
  description?: string;
  options: A2UIActionOption[];
}

export interface A2UIData {
  surfaceId: string;
  layout: string;
  title?: string;
  version?: string;
  metrics?: A2UIMetric[];
  action_section?: A2UIActionSection;
  components?: Array<{
    type: string;
    id: string;
    props: Record<string, any>;
  }>;
}

// 히스토리 대화 요약 DTO
export interface ConversationSummary {
  conversationId: string;
  title: string;
  category: string;
  createdAt: number;
  updatedAt: number;
}

// 히스토리 대화 상세 DTO (새로고침 복원 & 히스토리 상세용)
export interface ConversationDetail {
  conversationId: string;
  title: string;
  category: string;
  createdAt: number;
  updatedAt: number;
  timelineEvents: AgentEvent[];
  fullReport: string;
  a2uiPayload?: string;
  isCompleted: boolean;
}
