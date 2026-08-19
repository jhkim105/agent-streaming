import { useState, useEffect, useRef, useCallback } from 'react';
import type { AgentEvent, ConnectionStatus, StatusLog, A2UIData, ConversationSummary, ConversationDetail } from '../types/agent';

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

  // 히스토리 대화 목록 상태
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);

  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<any>(null);
  const lastEventIdRef = useRef<string | null>(localStorage.getItem(LAST_EVENT_ID_KEY));
  const connectionIdRef = useRef<string | null>(null);

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
      const res = await fetch(`${CONVERSATIONS_URL}/${targetConvId}`);
      if (res.ok) {
        const detail: ConversationDetail = await res.json();
        
        // 대화 상태 복원
        setConversationId(detail.conversationId);
        localStorage.setItem(CONVERSATION_STORAGE_KEY, detail.conversationId);

        // 타임라인 상태 로그 복원
        const restoredLogs: StatusLog[] = detail.timelineEvents.map((evt) => ({
          id: evt.eventId || Math.random().toString(36).substring(2, 9),
          step: evt.metadata?.step || 'thinking',
          content: evt.content,
          timestamp: evt.metadata?.timestamp || Date.now()
        }));
        setStatusLogs(restoredLogs);

        // 완성된 마크다운 보고서 복원
        setReportMarkdown(detail.fullReport || '');

        // A2UI 대시보드 데이터 복원
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
        console.log('[Conversation Restored]', detail.conversationId);
      }
    } catch (err: any) {
      console.error('[Select Conversation Error]', err);
      setErrorMsg(`대화 복원 실패: ${err.message}`);
    }
  }, []);

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

    // 1. INIT 이벤트 (connectionId 및 conversationId 수신)
    es.addEventListener('INIT', (event) => {
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
        console.log('[SSE INIT] ConnectionId:', connId, 'Conv:', data.conversationId);
      } catch (err) {
        console.error('[SSE INIT ERROR]', err);
      }
    });

    // 2. STATUS 이벤트 (에이전트 추론 단계 로깅)
    es.addEventListener('STATUS', (event) => {
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

    // 3. CHUNK 이벤트 (마크다운 타자기 토큰 누적)
    es.addEventListener('CHUNK', (event) => {
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

    // 4. A2UI_RENDER 이벤트 (선언적 A2UI 대시보드 구조 수신)
    es.addEventListener('A2UI_RENDER', (event) => {
      try {
        const data: AgentEvent = JSON.parse(event.data);
        const parsedA2UI: A2UIData = JSON.parse(data.content);
        setA2uiData(parsedA2UI);
        console.log('[SSE A2UI_RENDER] A2UI Dashboard Schema Received:', parsedA2UI);
      } catch (err) {
        console.error('[SSE A2UI_RENDER ERROR]', err);
      }
    });

    // 5. DONE 이벤트
    es.addEventListener('DONE', () => {
      setIsResearching(false);
      console.log('[SSE DONE] Research report stream completed');
      fetchConversations();
    });

    // 6. ERROR 이벤트
    es.addEventListener('ERROR', (event: MessageEvent) => {
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
          console.log('[SSE Auto Reconnecting...]');
          connectSSE();
        }, 3000);
      }
    };
  }, [createNewConversation, fetchConversations]);

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
    };
  }, [connectSSE, fetchConversations]);

  // 신규 대화 시작 함수
  const startNewConversation = async () => {
    localStorage.removeItem(CONVERSATION_STORAGE_KEY);
    localStorage.removeItem(LAST_EVENT_ID_KEY);
    lastEventIdRef.current = null;
    setStatusLogs([]);
    setReportMarkdown('');
    setA2uiData(null);
    setErrorMsg(null);
    setIsResearching(false);

    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    const newConvId = await createNewConversation();
    connectSSE(newConvId);
  };

  // AgentCommand 제출 함수 (POST /api/conversations/{id}/commands)
  const submitQuery = async (queryText: string) => {
    if (!queryText.trim()) return;
    
    let activeConvId = conversationId || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      activeConvId = await createNewConversation();
    }

    const currentConnId = connectionIdRef.current || connectionId;

    // 초기화
    setStatusLogs([]);
    setReportMarkdown('');
    setA2uiData(null);
    setErrorMsg(null);
    setIsResearching(true);

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
    }
  };

  // 사용자 A2UI 액션 커맨드 제출 함수 (POST /api/conversations/{id}/commands)
  const sendUserAction = async (actionId: string, payload: Record<string, any>) => {
    const activeConvId = conversationId || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      setErrorMsg('대화가 시작되지 않았습니다.');
      return;
    }

    const currentConnId = connectionIdRef.current || connectionId;

    setIsResearching(true);
    setErrorMsg(null);

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
    }
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
    submitQuery,
    sendUserAction,
    startNewConversation,
    selectConversation,
    refreshConversations: fetchConversations,
    reconnect: connectSSE
  };
}
