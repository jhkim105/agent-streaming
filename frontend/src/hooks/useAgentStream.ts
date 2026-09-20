import { useState, useEffect, useRef, useCallback } from 'react';
import type { 
  AgentEvent, 
  ConnectionStatus, 
  StatusLog, 
  A2UIData, 
  ConversationSummary, 
  ConversationDetail, 
  RawPacketLog, 
  AgentRunRequest,
  ChatTurn 
} from '../types/agent';

const CONVERSATIONS_URL = '/api/conversations';
const CONVERSATION_STORAGE_KEY = 'agent_streaming_current_conversation_id';
const LAST_EVENT_ID_KEY = 'agent_streaming_last_event_id';

export function useAgentStream() {
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [conversationId, setConversationId] = useState<string | null>(() => {
    return localStorage.getItem(CONVERSATION_STORAGE_KEY) || null;
  });
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('DISCONNECTED');
  
  // 멀티턴 대화 목록 상태 (사용자 질문 + 에이전트 답변들의 누적 리스트)
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [isResearching, setIsResearching] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  
  // RAW SSE 패킷 수신 로그 스택 (디버깅용)
  const [rawPacketLogs, setRawPacketLogs] = useState<RawPacketLog[]>([]);

  // 히스토리 대화 목록 상태
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);

  // React Hook Refs
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<any>(null);
  const researchSafetyTimeoutRef = useRef<any>(null);
  const lastEventIdRef = useRef<string | null>(localStorage.getItem(LAST_EVENT_ID_KEY));
  const connectionIdRef = useRef<string | null>(null);
  const activeConversationIdRef = useRef<string | null>(conversationId);
  const activeRunIdRef = useRef<string | null>(null);
  const packetCountRef = useRef<number>(0);

  useEffect(() => {
    activeConversationIdRef.current = conversationId;
  }, [conversationId]);

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
      setTurns((prev) => prev.map((turn) => ({ ...turn, isStreaming: false })));
      setErrorMsg('에이전트 응답 시간이 초과되었습니다. 다시 시도해 주세요.');
    }, 60000);
  }, [resetSafetyTimeout]);

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

  const createNewConversation = useCallback(async (): Promise<string> => {
    try {
      const res = await fetch(CONVERSATIONS_URL, { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        const newConvId = data.conversationId;
        setConversationId(newConvId);
        activeConversationIdRef.current = newConvId;
        localStorage.setItem(CONVERSATION_STORAGE_KEY, newConvId);
        return newConvId;
      }
    } catch (err) {
      console.error('[Create Conversation Error]', err);
    }
    const fallbackId = 'conv-' + Math.random().toString(36).substring(2, 10);
    setConversationId(fallbackId);
    activeConversationIdRef.current = fallbackId;
    localStorage.setItem(CONVERSATION_STORAGE_KEY, fallbackId);
    return fallbackId;
  }, []);

  const pushRawPacket = useCallback((type: string, event: MessageEvent) => {
    packetCountRef.current += 1;
    const now = new Date();
    const timeStr = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');
    
    const newPacket: RawPacketLog = {
      count: packetCountRef.current,
      timestamp: timeStr,
      type: type,
      sseEventId: event.lastEventId || 'N/A',
      rawData: event.data || ''
    };
    setRawPacketLogs((prev) => [...prev, newPacket]);
  }, []);

  // SSE 커넥션 수립 함수
  const connectSSE = useCallback(async (targetConvId?: string) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    let convId = targetConvId || activeConversationIdRef.current || localStorage.getItem(CONVERSATION_STORAGE_KEY);
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
        const connId = data.content || data.metadata?.connectionId || null;
        if (connId) {
          setConnectionId(connId);
          connectionIdRef.current = connId;
        }
        if (data.conversationId) {
          setConversationId(data.conversationId);
          activeConversationIdRef.current = data.conversationId;
          localStorage.setItem(CONVERSATION_STORAGE_KEY, data.conversationId);
        }
      } catch (err) {
        console.error('[SSE INIT ERROR]', err);
      }
    });

    es.addEventListener('STATUS', (event: MessageEvent) => {
      pushRawPacket('STATUS', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        if (data.conversationId && data.conversationId !== activeConversationIdRef.current) {
          return;
        }

        startSafetyTimeout();
        if (event.lastEventId) {
          lastEventIdRef.current = event.lastEventId;
          localStorage.setItem(LAST_EVENT_ID_KEY, event.lastEventId);
        }

        const newLog: StatusLog = {
          id: data.sseEventId || Math.random().toString(36).substring(2, 9),
          step: data.metadata?.step || 'thinking',
          messageId: data.messageId || 'msg-think-1',
          content: data.content,
          timestamp: data.metadata?.timestamp || Date.now()
        };

        const targetRunId = data.runId || activeRunIdRef.current;

        setTurns((prevTurns) => {
          return prevTurns.map((turn) => {
            if (turn.runId === targetRunId || (!targetRunId && turn.isStreaming)) {
              return {
                ...turn,
                statusLogs: [...turn.statusLogs, newLog]
              };
            }
            return turn;
          });
        });
      } catch (err) {
        console.error('[SSE STATUS ERROR]', err);
      }
    });

    es.addEventListener('CHUNK', (event: MessageEvent) => {
      pushRawPacket('CHUNK', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        if (data.conversationId && data.conversationId !== activeConversationIdRef.current) {
          return;
        }

        startSafetyTimeout();
        if (event.lastEventId) {
          lastEventIdRef.current = event.lastEventId;
          localStorage.setItem(LAST_EVENT_ID_KEY, event.lastEventId);
        }

        const targetRunId = data.runId || activeRunIdRef.current;

        setTurns((prevTurns) => {
          return prevTurns.map((turn) => {
            if (turn.runId === targetRunId || (!targetRunId && turn.isStreaming)) {
              return {
                ...turn,
                reportMarkdown: turn.reportMarkdown + data.content
              };
            }
            return turn;
          });
        });
      } catch (err) {
        console.error('[SSE CHUNK ERROR]', err);
      }
    });

    es.addEventListener('A2UI_RENDER', (event: MessageEvent) => {
      pushRawPacket('A2UI_RENDER', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        if (data.conversationId && data.conversationId !== activeConversationIdRef.current) {
          return;
        }

        const parsedA2UI: A2UIData = JSON.parse(data.content);
        parsedA2UI.messageId = data.messageId || 'msg-a2ui-1';

        const targetRunId = data.runId || activeRunIdRef.current;

        setTurns((prevTurns) => {
          return prevTurns.map((turn) => {
            if (turn.runId === targetRunId || (!targetRunId && turn.isStreaming)) {
              return {
                ...turn,
                a2uiData: parsedA2UI
              };
            }
            return turn;
          });
        });
      } catch (err) {
        console.error('[SSE A2UI_RENDER ERROR]', err);
      }
    });

    es.addEventListener('DONE', (event: MessageEvent) => {
      pushRawPacket('DONE', event);
      try {
        const data: AgentEvent = JSON.parse(event.data);
        if (data.conversationId && data.conversationId !== activeConversationIdRef.current) {
          return;
        }

        const targetRunId = data.runId || activeRunIdRef.current;

        setTurns((prevTurns) => {
          return prevTurns.map((turn) => {
            if (turn.runId === targetRunId || (!targetRunId && turn.isStreaming)) {
              return {
                ...turn,
                isStreaming: false
              };
            }
            return turn;
          });
        });
      } catch {}

      resetSafetyTimeout();
      setIsResearching(false);
      activeRunIdRef.current = null;
      fetchConversations();
    });

    es.addEventListener('ERROR', (event: MessageEvent) => {
      pushRawPacket('ERROR', event);
      resetSafetyTimeout();
      try {
        if (event.data) {
          const data: AgentEvent = JSON.parse(event.data);
          if (data.conversationId && data.conversationId !== activeConversationIdRef.current) {
            return;
          }
          setErrorMsg(data.content || '에이전트 처리 중 오류가 발생했습니다.');
        }
      } catch {
        setErrorMsg('SSE 연결 오류가 발생했습니다.');
      }
      setIsResearching(false);
      setTurns((prevTurns) => prevTurns.map((turn) => ({ ...turn, isStreaming: false })));
      activeRunIdRef.current = null;
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
          connectSSE(activeConversationIdRef.current || undefined);
        }, 3000);
      }
    };
  }, [createNewConversation, fetchConversations, pushRawPacket, resetSafetyTimeout, startSafetyTimeout]);

  // 특정 대화 상세 복원 (멀티턴 runs 목록 전체 복원)
  const selectConversation = useCallback(async (targetConvId: string) => {
    try {
      setErrorMsg(null);
      resetSafetyTimeout();
      
      setConversationId(targetConvId);
      activeConversationIdRef.current = targetConvId;
      localStorage.setItem(CONVERSATION_STORAGE_KEY, targetConvId);

      connectSSE(targetConvId);

      const res = await fetch(`${CONVERSATIONS_URL}/${targetConvId}`);
      if (res.ok) {
        const detail: ConversationDetail = await res.json();

        // 1. 백엔드에서 runs 목록이 반환된 경우 (멀티턴 복원)
        if (detail.runs && detail.runs.length > 0) {
          const restoredTurns: ChatTurn[] = detail.runs.map((r) => ({
            runId: r.runId,
            userPrompt: r.userPrompt || '',
            statusLogs: (r.timelineEvents || []).map((evt) => ({
              id: evt.sseEventId || Math.random().toString(36).substring(2, 9),
              step: evt.metadata?.step || 'thinking',
              messageId: evt.messageId || 'msg-think-1',
              content: evt.content,
              timestamp: evt.metadata?.timestamp || Date.now()
            })),
            reportMarkdown: r.fullReport || '',
            a2uiData: r.a2uiPayload ? JSON.parse(r.a2uiPayload) : null,
            isStreaming: !r.isCompleted,
            createdAt: r.createdAt || detail.createdAt
          }));
          setTurns(restoredTurns);
          setIsResearching(restoredTurns.some((t) => t.isStreaming));
        } else {
          // 2. 단일 레거시 DTO인 경우 1개 턴으로 래핑 복원
          const singleTurn: ChatTurn = {
            runId: 'run-initial',
            userPrompt: detail.title || '',
            statusLogs: (detail.timelineEvents || []).map((evt) => ({
              id: evt.sseEventId || Math.random().toString(36).substring(2, 9),
              step: evt.metadata?.step || 'thinking',
              messageId: evt.messageId || 'msg-think-1',
              content: evt.content,
              timestamp: evt.metadata?.timestamp || Date.now()
            })),
            reportMarkdown: detail.fullReport || '',
            a2uiData: detail.a2uiPayload ? JSON.parse(detail.a2uiPayload) : null,
            isStreaming: !detail.isCompleted,
            createdAt: detail.createdAt
          };
          setTurns([singleTurn]);
          setIsResearching(!detail.isCompleted);
        }
      }
    } catch (err: any) {
      console.error('[Select Conversation Error]', err);
      setErrorMsg(`대화 복원 실패: ${err.message}`);
    }
  }, [connectSSE, resetSafetyTimeout]);

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

  // [+ 새 채팅] 시작 시: 턴 목록 초기화
  const startNewConversation = async () => {
    localStorage.removeItem(CONVERSATION_STORAGE_KEY);
    localStorage.removeItem(LAST_EVENT_ID_KEY);
    lastEventIdRef.current = null;
    setTurns([]);
    setErrorMsg(null);
    setIsResearching(false);
    setRawPacketLogs([]);
    packetCountRef.current = 0;
    activeRunIdRef.current = null;
    resetSafetyTimeout();

    const newConvId = await createNewConversation();
    connectSSE(newConvId);
  };

  // 질문 전송 시: 이전 대화 턴들을 보존하고 새로운 턴(ChatTurn)을 Append!
  const submitQuery = async (queryText: string) => {
    if (!queryText.trim()) return;
    
    let activeConvId = conversationId || activeConversationIdRef.current || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      activeConvId = await createNewConversation();
    }

    const currentConnId = connectionIdRef.current || connectionId;
    const clientRunId = 'run-' + Math.random().toString(36).substring(2, 10);
    activeRunIdRef.current = clientRunId;

    // 🔥 이전 대화는 그대로 두고, 새로운 턴을 배열 끝에 추가
    const newTurn: ChatTurn = {
      runId: clientRunId,
      userPrompt: queryText,
      statusLogs: [],
      reportMarkdown: '',
      a2uiData: null,
      isStreaming: true,
      createdAt: Date.now()
    };
    setTurns((prevTurns) => [...prevTurns, newTurn]);

    setErrorMsg(null);
    setIsResearching(true);
    startSafetyTimeout();

    try {
      const runPayload: AgentRunRequest = {
        runId: clientRunId,
        connectionId: currentConnId || '',
        type: 'RESEARCH',
        payload: {
          query: queryText
        }
      };

      const response = await fetch(`${CONVERSATIONS_URL}/${encodeURIComponent(activeConvId)}/runs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(runPayload)
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const resData = await response.json();
      if (resData.conversationId) {
        setConversationId(resData.conversationId);
        activeConversationIdRef.current = resData.conversationId;
        localStorage.setItem(CONVERSATION_STORAGE_KEY, resData.conversationId);
      }

      fetchConversations();
    } catch (err: any) {
      console.error('[Submit Query Error]', err);
      setErrorMsg(`질문 요청 실패: ${err.message}`);
      setIsResearching(false);
      setTurns((prev) => prev.map((t) => (t.runId === clientRunId ? { ...t, isStreaming: false } : t)));
      resetSafetyTimeout();
    }
  };

  // AGUI 액션 버튼 클릭 시
  const sendUserAction = async (actionId: string, payload: Record<string, any>) => {
    const activeConvId = conversationId || activeConversationIdRef.current || localStorage.getItem(CONVERSATION_STORAGE_KEY);
    if (!activeConvId) {
      setErrorMsg('대화가 시작되지 않았습니다.');
      return;
    }

    const currentConnId = connectionIdRef.current || connectionId;
    const clientRunId = 'run-' + Math.random().toString(36).substring(2, 10);
    activeRunIdRef.current = clientRunId;

    const actionLabel = payload.label || actionId;
    const newTurn: ChatTurn = {
      runId: clientRunId,
      userPrompt: `👉 ${actionLabel}`,
      statusLogs: [],
      reportMarkdown: '',
      a2uiData: null,
      isStreaming: true,
      createdAt: Date.now()
    };
    setTurns((prevTurns) => [...prevTurns, newTurn]);

    setIsResearching(true);
    setErrorMsg(null);
    startSafetyTimeout();

    try {
      const runPayload: AgentRunRequest = {
        runId: clientRunId,
        connectionId: currentConnId || '',
        type: 'ACTION',
        payload: {
          actionId,
          ...payload
        }
      };

      const response = await fetch(`${CONVERSATIONS_URL}/${encodeURIComponent(activeConvId)}/runs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(runPayload)
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }
    } catch (err: any) {
      console.error('[Send User Action Error]', err);
      setErrorMsg(`UI 액션 전송 실패: ${err.message}`);
      setIsResearching(false);
      setTurns((prev) => prev.map((t) => (t.runId === clientRunId ? { ...t, isStreaming: false } : t)));
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
    turns,
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
