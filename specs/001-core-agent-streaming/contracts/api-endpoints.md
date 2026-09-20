# API 인터페이스 규격서 (Contracts: HTTP & SSE)

**대상 스펙**: [spec.md](../spec.md)  
**작성일자**: 2026-09-19 (runId / messageId 규격 반영)

---

## 1. REST API 엔드포인트

### 1) 신규 대화 스레드 생성
- **URL**: `POST /api/conversations`
- **응답 (201 Created)**:
  ```json
  {
    "conversationId": "conv-29acc0af"
  }
  ```

### 2) 전체 대화 요약 목록 조회
- **URL**: `GET /api/conversations`
- **응답 (200 OK)**:
  ```json
  [
    {
      "conversationId": "conv-29acc0af",
      "title": "AGUI 스트리밍 아키텍처",
      "category": "tech",
      "createdAt": 1726743600000,
      "updatedAt": 1726743650000
    }
  ]
  ```

### 3) 특정 대화 상세 및 히스토리 조회
- **URL**: `GET /api/conversations/{conversationId}`
- **응답 (200 OK)**:
  ```json
  {
    "conversationId": "conv-29acc0af",
    "title": "AGUI 스트리밍 아키텍처",
    "category": "tech",
    "createdAt": 1726743600000,
    "updatedAt": 1726743650000,
    "runs": [
      {
        "runId": "run-80e29ebd-ad4d-421f",
        "userPrompt": "AGUI 스트리밍 알려줘",
        "timelineEvents": [
          {
            "sseEventId": "evt-001",
            "messageId": "msg-think-1",
            "type": "STATUS",
            "content": "🔍 웹 검색 수행 중...",
            "timestamp": 1726743601000
          }
        ],
        "fullReport": "# AGUI 스트리밍 아키텍처 분석\n...",
        "a2uiPayload": "{\"version\":\"1.0\", ...}",
        "isCompleted": true
      }
    ]
  }
  ```

### 4) 실시간 SSE 연결 수립
- **URL**: `GET /api/conversations/{conversationId}/events`
- **헤더**: `Accept: text/event-stream`
- **스트림 개시 이벤트 (`INIT`)**:
  ```text
  id: evt-init
  event: INIT
  data: {"sseEventId":"evt-init","conversationId":"conv-29acc0af","type":"INIT","content":"conn-3dc1ec63-a46f-49b2-9cb3-d67ce2044fb4"}
  ```

### 5) 에이전트 턴 실행 요청 (Run Request)
- **URL**: `POST /api/conversations/{conversationId}/runs`
- **요청 Body**:
  ```json
  {
    "connectionId": "conn-3dc1ec63-a46f-49b2-9cb3-d67ce2044fb4",
    "type": "RESEARCH",
    "payload": {
      "query": "스마트 타이틀 생성 방법 알려줘"
    }
  }
  ```
- **응답 (202 Accepted)**:
  ```json
  {
    "status": "ACCEPTED",
    "conversationId": "conv-29acc0af",
    "runId": "run-80e29ebd-ad4d-421f",
    "message": "Agent Run queued successfully"
  }
  ```
