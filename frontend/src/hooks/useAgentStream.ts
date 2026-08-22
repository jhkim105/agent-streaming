import { useState, useEffect, useRef, useCallback } from 'react';
import type { AgentEvent, ConnectionStatus, StatusLog, A2UIData, ConversationSummary, ConversationDetail, RawPacketLog } from '../types/agent';

const CONVERSATIONS_URL = '/api/conversations';
const CONVERSATION_STORAGE_KEY = 'agent_streaming_current_conversation_id';
const LAST_EVENT_ID_KEY = 'agent_streaming_last_event_id';

export function useAgentStream() {
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [conversationId, setConversationId] = useState<string | null>(() => {
    return localStorage.getItem(CONVERSATION_STORAGE_KEY) || null;
  });
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('DISCONNECTED');
  const [statusLogs, setStatusLogs] = useState<StatusLog[]>([]);
  const [reportMarkdown, setReportMarkdown] = useState<string>('');
  const [a2uiData, setA2uiData] = useState<A2UIData | null>(null);
  const [isResearching, setIsResearching] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  
  // RAW SSE 패킷 수신 로그 스택 (디버깅용)
  const [rawPacketLogs, setRawPacketLogs] = useState<RawPacketLog[]>([]);

  // 히스토리 대화 목록 상태
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);

  // React Hook Rules 준수: 모든 useRef 선언을 useCallback 이전 최상단에 배치
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<any>(null);
  const researchSafetyTimeoutRef = useRef<any>(null);
  const lastEventIdRef = useRef<string | null>(localStorage.getItem(LAST_EVENT_ID_KEY));
  const connectionIdRef = useRef<string | null>(null);
  const packetCountRef = useRef<number>(0);

  // 안전 타임아웃 해제 헬퍼 (응답 지연 시 락 자동 해제)
  const resetSafetyTimeout = useCallback(() => {
    if (researchSafetyTimeoutRef.current) {
      clearTimeout(researchSafetyTimeoutRef.current);
      researchSafetyTimeoutRef.current = null;
    }
  }, []);

  const startSafetyTimeout = useCallback(() => {
    resetSafetyTimeout();
    researchSafetyTimeoutRef.current = setTimeout(() => {
      setIsResearching(false);
      setErrorMsg('에이전트 응답 시간이 초과되었습니다. 다시 시도해 주세요.');
    }, 60000);
  }, [resetSafetyTimeout]);

  // 이전 대화 요약 목록 조회 API 호출 (GET /api/conversations)
  const fetchConversations = useCallback(async () => {
    try {
      const res = await fetch(CONVERSATIONS_URL);
      if (res.ok) {
        const data: ConversationSummary[] = await res.json();
        setConversations(data);
      }
    } catch (err) {
      console.error('[Fetch Conversations Error]', err);
    }
  }, []);

  // 특정 대화 상세 복원 및 화면 전환 (GET /api/conversations/{id})
  const selectConversation = useCallback(async (targetConvId: string) => {
    try {
      setErrorMsg(null);
      resetSafetyTimeout();
      const res = await fetch(`${CONVERSATIONS_URL}/${targetConvId}`);
      if (res.ok) {
        const detail: ConversationDetail = await res.json();
        
        setConversationId(detail.conversationId);
        localStorage.setItem(CONVERSATION_STORAGE_KEY, detail.conversationId);

        const restoredLogs: StatusLog[] = detail.timelineEvents.map((evt) => ({
          id: evt.eventId || Math.random().toString(36).substring(2, 9),
          step: evt.metadata?.step || 'thinking',
          content: evt.content,
          timestamp: evt.metadata?.timestamp || Date.now()
        }));
        setStatusLogs(restoredLogs);

        setReportMarkdown(detail.fullReport || '');

        if (detail.a2uiPayload) {
          try {
            setA2uiData(JSON.parse(detail.a2uiPayload));
          } catch {
            setA2uiData(null);
          }
        } else {
          setA2uiData(null);
        }

        setIsResearching(!detail.isCompleted);
      }
    } catch (err: any) {
      console.error('[Select Conversation Error]', err);
      setErrorMsg(`대화 복원 실패: ${err.message}`);
    }
  }, [resetSafetyTimeout]);

  // 명시적 신규 대화 스레드 생성 (POST /api/conversations)
  const createNewConversation = useCallback(async (): Promise<string> => {
    try {
      const res = await fetch(CONVERSATIONS_URL, { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        const newConvId = data.conversationId;
        setConversationId(newConvId);
        localStorage.setItem(CONVERSATION_STORAGE_KEY, newConvId);
        return newConvId;
      }
    } catch (err) {
      console.error('[Create Conversation Error]', err);
    }
    const fallbackId = 'conv-' + Math.random().toString(36).substring(2, 10);
    setConversationId(fallbackId);
    localStorage.setItem(CONVERSATION_STORAGE_KEY, fallbackId);
    return fallbackId;
  }, []);

  // RAW 패킷 덤프 기록 헬퍼
  const pushRawPacket = useCallback((type: string, event: MessageEvent) => {
    packetCountRef.current += 1;
    const now = new Date();
    const timeStr = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');
    
    const newPacket: RawPacketLog = {
      count: packetCountRef.current,
      timestamp: timeStr,
      type: type,
      eventId: event.lastEventId || 'N/A',
      rawData: event.data || ''
    };
    setRawPacketLogs((prev) => [...prev, newPacket]);
  }, []);

  // SSE 커넥션 수립 함수 (GET /api/conversations/{id}/events)
  const connectSSE = useCallback(async (targetConvId?: string) => {
    if (eventSourceRef.current) return;

    let convId = targetConvId || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!convId) {
      convId = await createNewConversation();
    }

    setConnectionStatus('CONNECTING');

    const streamUrl = `${CONVERSATIONS_URL}/${encodeURIComponent(convId)}/events`;

    const es = new EventSource(streamUrl);
    eventSourceRef.current = es;

    es.onopen = () => {
      setConnectionStatus('CONNECTED');
      setErrorMsg(null);
    };

    es.addEventListener('INIT', (event: MessageEvent) => {
      pushRawPacket('INIT', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        const connId = data.metadata?.connectionId || null;
        if (connId) {
          setConnectionId(connId);
          connectionIdRef.current = connId;
        }
        if (data.conversationId) {
          setConversationId(data.conversationId);
          localStorage.setItem(CONVERSATION_STORAGE_KEY, data.conversationId);
        }
      } catch (err) {
        console.error('[SSE INIT ERROR]', err);
      }
    });

    es.addEventListener('STATUS', (event: MessageEvent) => {
      pushRawPacket('STATUS', event);
      startSafetyTimeout();
      try {
        if (event.lastEventId) {
          lastEventIdRef.current = event.lastEventId;
          localStorage.setItem(LAST_EVENT_ID_KEY, event.lastEventId);
        }
        const data: AgentEvent = JSON.parse(event.data);
        const newLog: StatusLog = {
          id: data.eventId || Math.random().toString(36).substring(2, 9),
          step: data.metadata?.step || 'thinking',
          content: data.content,
          timestamp: data.metadata?.timestamp || Date.now()
        };
        setStatusLogs((prev) => [...prev, newLog]);
      } catch (err) {
        console.error('[SSE STATUS ERROR]', err);
      }
    });

    es.addEventListener('CHUNK', (event: MessageEvent) => {
      pushRawPacket('CHUNK', event);
      startSafetyTimeout();
      try {
        if (event.lastEventId) {
          lastEventIdRef.current = event.lastEventId;
          localStorage.setItem(LAST_EVENT_ID_KEY, event.lastEventId);
        }
        const data: AgentEvent = JSON.parse(event.data);
        setReportMarkdown((prev) => prev + data.content);
      } catch (err) {
        console.error('[SSE CHUNK ERROR]', err);
      }
    });

    es.addEventListener('A2UI_RENDER', (event: MessageEvent) => {
      pushRawPacket('A2UI_RENDER', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        const parsedA2UI: A2UIData = JSON.parse(data.content);
        setA2uiData(parsedA2UI);
      } catch (err) {
        console.error('[SSE A2UI_RENDER ERROR]', err);
      }
    });

    es.addEventListener('DONE', (event: MessageEvent) => {
      pushRawPacket('DONE', event);
      resetSafetyTimeout();
      setIsResearching(false);
      fetchConversations();
    });

    es.addEventListener('ERROR', (event: MessageEvent) => {
      pushRawPacket('ERROR', event);
      resetSafetyTimeout();
      try {
        if (event.data) {
          const data: AgentEvent = JSON.parse(event.data);
          setErrorMsg(data.content || '에이전트 처리 중 오류가 발생했습니다.');
        }
      } catch {
        setErrorMsg('SSE 연결 오류가 발생했습니다.');
      }
      setIsResearching(false);
    });

    es.onerror = () => {
      setConnectionStatus('ERROR');
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      if (!reconnectTimeoutRef.current) {
        reconnectTimeoutRef.current = setTimeout(() => {
          reconnectTimeoutRef.current = null;
          connectSSE();
        }, 3000);
      }
    };
  }, [createNewConversation, fetchConversations, pushRawPacket, resetSafetyTimeout, startSafetyTimeout]);

  useEffect(() => {
    connectSSE();
    fetchConversations();

    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      resetSafetyTimeout();
    };
  }, [connectSSE, fetchConversations, resetSafetyTimeout]);

  const startNewConversation = async () => {
    localStorage.removeItem(CONVERSATION_STORAGE_KEY);
    localStorage.removeItem(LAST_EVENT_ID_KEY);
    lastEventIdRef.current = null;
    setStatusLogs([]);
    setReportMarkdown('');
    setA2uiData(null);
    setErrorMsg(null);
    setIsResearching(false);
    setRawPacketLogs([]);
    packetCountRef.current = 0;
    resetSafetyTimeout();

    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    const newConvId = await createNewConversation();
    connectSSE(newConvId);
  };

  const submitQuery = async (queryText: string) => {
    if (!queryText.trim()) return;
    
    let activeConvId = conversationId || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      activeConvId = await createNewConversation();
    }

    const currentConnId = connectionIdRef.current || connectionId;

    setStatusLogs([]);
    setReportMarkdown('');
    setA2uiData(null);
    setErrorMsg(null);
    setIsResearching(true);
    startSafetyTimeout();

    try {
      const response = await fetch(`${CONVERSATIONS_URL}/${encodeURIComponent(activeConvId)}/commands`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          connectionId: currentConnId || '',
          type: 'RESEARCH',
          payload: {
            query: queryText
          }
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const resData = await response.json();
      if (resData.conversationId) {
        setConversationId(resData.conversationId);
        localStorage.setItem(CONVERSATION_STORAGE_KEY, resData.conversationId);
      }

      fetchConversations();
    } catch (err: any) {
      console.error('[Submit Query Error]', err);
      setErrorMsg(`질문 요청 실패: ${err.message}`);
      setIsResearching(false);
      resetSafetyTimeout();
    }
  };

  const sendUserAction = async (actionId: string, payload: Record<string, any>) => {
    const activeConvId = conversationId || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      setErrorMsg('대화가 시작되지 않았습니다.');
      return;
    }

    const currentConnId = connectionIdRef.current || connectionId;

    setIsResearching(true);
    setErrorMsg(null);
    startSafetyTimeout();

    try {
      const response = await fetch(`${CONVERSATIONS_URL}/${encodeURIComponent(activeConvId)}/commands`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          connectionId: currentConnId || '',
          type: 'ACTION',
          payload: {
            actionId,
            ...payload
          }
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }
    } catch (err: any) {
      console.error('[Send User Action Error]', err);
      setErrorMsg(`UI 액션 전송 실패: ${err.message}`);
      setIsResearching(false);
      resetSafetyTimeout();
    }
  };

  const clearRawPacketLogs = () => {
    setRawPacketLogs([]);
    packetCountRef.current = 0;
  };

  return {
    connectionId,
    conversationId,
    connectionStatus,
    statusLogs,
    reportMarkdown,
    a2uiData,
    isResearching,
    errorMsg,
    conversations,
    rawPacketLogs,
    clearRawPacketLogs,
    submitQuery,
    sendUserAction,
    startNewConversation,
    selectConversation,
    refreshConversations: fetchConversations,
    reconnect: connectSSE
  };
}
