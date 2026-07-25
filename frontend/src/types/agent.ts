export type EventType = 'INIT' | 'STATUS' | 'CHUNK' | 'DONE' | 'ERROR';

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
