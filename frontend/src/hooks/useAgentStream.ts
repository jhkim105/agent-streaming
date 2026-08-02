import { useState, useEffect, useRef, useCallback } from 'react';
import type { AgentEvent, ConnectionStatus, StatusLog, A2UIData } from '../types/agent';

const STREAM_URL = '/api/chat/stream';
const MESSAGE_URL = '/api/chat/message';
const ACTION_URL = '/api/chat/action';

export function useAgentStream() {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('DISCONNECTED');
  const [statusLogs, setStatusLogs] = useState<StatusLog[]>([]);
  const [reportMarkdown, setReportMarkdown] = useState<string>('');
  const [a2uiData, setA2uiData] = useState<A2UIData | null>(null);
  const [isResearching, setIsResearching] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const eventSourceRef = useRef<EventSource | null>(null);

  // SSE 커넥션 수립 함수
  const connectSSE = useCallback(() => {
    if (eventSourceRef.current) return;

    setConnectionStatus('CONNECTING');
    const es = new EventSource(STREAM_URL);
    eventSourceRef.current = es;

    es.onopen = () => {
      setConnectionStatus('CONNECTED');
      setErrorMsg(null);
    };

    // 1. INIT 이벤트 (Session ID 수신)
    es.addEventListener('INIT', (event) => {
      try {
        const data: AgentEvent = JSON.parse(event.data);
        if (data.sessionId) {
          setSessionId(data.sessionId);
          console.log('[SSE INIT] Session ID assigned:', data.sessionId);
        }
      } catch (err) {
        console.error('[SSE INIT ERROR]', err);
      }
    });

    // 2. STATUS 이벤트 (에이전트 추론 단계 로깅)
    es.addEventListener('STATUS', (event) => {
      try {
        const data: AgentEvent = JSON.parse(event.data);
        const newLog: StatusLog = {
          id: Math.random().toString(36).substring(2, 9),
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

    // 5. DONE 이벤트 (리서치 완성)
    es.addEventListener('DONE', () => {
      setIsResearching(false);
      console.log('[SSE DONE] Research report stream completed');
    });

    // 6. ERROR 이벤트 (에러 발생)
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
      // 끊김 시 리소스 클리어 후 자동 재연결 시도 가능
      es.close();
      eventSourceRef.current = null;
    };
  }, []);

  useEffect(() => {
    connectSSE();
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, [connectSSE]);

  // 질문 전송 함수 (POST /api/chat/message)
  const submitQuery = async (queryText: string) => {
    if (!queryText.trim()) return;
    if (!sessionId) {
      setErrorMsg('SSE 세션이 아직 연결되지 않았습니다. 잠시 후 다시 시도해 주세요.');
      return;
    }

    // 초기화
    setStatusLogs([]);
    setReportMarkdown('');
    setA2uiData(null);
    setErrorMsg(null);
    setIsResearching(true);

    try {
      const response = await fetch(MESSAGE_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          sessionId: sessionId,
          query: queryText
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }
    } catch (err: any) {
      console.error('[Submit Query Error]', err);
      setErrorMsg(`질문 요청 실패: ${err.message}`);
      setIsResearching(false);
    }
  };

  // 사용자 A2UI 액션 버튼 클릭 전송 함수 (POST /api/chat/action)
  const sendUserAction = async (actionId: string, payload: Record<string, any>) => {
    if (!sessionId) {
      setErrorMsg('SSE 세션이 연결되어 있지 않습니다.');
      return;
    }

    setIsResearching(true);
    setErrorMsg(null);

    try {
      const response = await fetch(ACTION_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          sessionId,
          actionId,
          payload
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
    sessionId,
    connectionStatus,
    statusLogs,
    reportMarkdown,
    a2uiData,
    isResearching,
    errorMsg,
    submitQuery,
    sendUserAction,
    reconnect: connectSSE
  };
}
