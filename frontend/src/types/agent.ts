export type EventType = 'INIT' | 'STATUS' | 'CHUNK' | 'A2UI_RENDER' | 'DONE' | 'ERROR';

export type ConnectionStatus = 'DISCONNECTED' | 'CONNECTING' | 'CONNECTED' | 'ERROR';

export interface EventMetadata {
  step?: string;
  title?: string;
  timestamp?: number;
  connectionId?: string;
  replace?: boolean;
}

export interface AgentEvent {
  sseEventId: string;
  runId: string;
  messageId: string;
  conversationId?: string;
  hostId?: string;
  type: EventType;
  content: string;
  metadata?: EventMetadata;
  timestamp?: number;
}

export interface AgentRunRequest {
  runId?: string;
  conversationId?: string;
  connectionId?: string;
  type: 'RESEARCH' | 'ACTION' | 'CANCEL';
  payload: Record<string, any>;
}

export interface StatusLog {
  id: string;
  step: string;
  content: string;
  messageId?: string;
  timestamp: number;
}

export interface RawPacketLog {
  count: number;
  timestamp: string;
  type: string;
  sseEventId: string;
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
  action_type?: string;
  payload: Record<string, any>;
}

export interface A2UIActionSection {
  title?: string;
  description?: string;
  options: A2UIActionOption[];
}

export interface A2UIData {
  surfaceId?: string;
  messageId?: string;
  layout?: string;
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

// 1개 턴(사용자 질문 + 에이전트 응답 세트) 모델
export interface ChatTurn {
  runId: string;
  userPrompt: string;
  statusLogs: StatusLog[];
  reportMarkdown: string;
  a2uiData: A2UIData | null;
  isStreaming: boolean;
  createdAt: number;
}

// 히스토리 대화 요약 DTO
export interface ConversationSummary {
  conversationId: string;
  title: string;
  category: string;
  createdAt: number;
  updatedAt: number;
}

// 1개 턴(Run) 상세 DTO (백엔드 통신용)
export interface ConversationRunDetail {
  runId: string;
  userPrompt: string;
  timelineEvents: AgentEvent[];
  fullReport: string;
  a2uiPayload?: string;
  isCompleted: boolean;
  createdAt?: number;
}

// 대화 상세 복원 DTO
export interface ConversationDetail {
  conversationId: string;
  title: string;
  category: string;
  createdAt: number;
  updatedAt: number;
  runs?: ConversationRunDetail[];
  timelineEvents?: AgentEvent[];
  fullReport?: string;
  a2uiPayload?: string;
  isCompleted?: boolean;
}
