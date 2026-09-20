# 🛠️ REST / SSE API 및 메시지 데이터 명세서 (API Specification)

본 문서는 **대화형 AI 에이전트(ChatGPT형 UX)** 시스템의 REST 엔드포인트, SSE 스트리밍 규격 및 `runId` / `messageId` 기반 메시지 프로토콜을 정의합니다.

---

## 📡 1. REST API 엔드포인트 명세

### 1. 명시적 대화 스레드 생성
* **URL**: `POST /api/conversations`
* **설명**: `[+ 새 채팅]` 클릭 시 호출하여 신규 `conversationId`를 즉시 발행합니다.
* **응답 (201 Created)**:
  ```json
  {
    "conversationId": "conv-29acc0af"
  }
  ```

### 2. 단방향 SSE 연결 수립
* **URL**: `GET /api/conversations/{conversationId}/events`
* **Header**: `Accept: text/event-stream`, `Last-Event-ID: evt-xxx` (선택적)
* **설명**: 대화 스레드에 대한 SSE 소켓 연결을 수립하고 `INIT` 이벤트로 `connectionId`를 전달받습니다.

### 3. Agent Run 턴 실행 요청
* **URL**: `POST /api/conversations/{conversationId}/runs`
* **요청 본문 (`AgentRunRequest`)**:
  ```json
  {
    "connectionId": "conn-3dc1ec63-a46f-49b2-9cb3-d67ce2044fb4",
    "type": "RESEARCH",
    "payload": {
      "query": "AGUI 스트리밍 아키텍처 알려줘"
    }
  }
  ```
* **응답 (202 Accepted)**:
  ```json
  {
    "status": "ACCEPTED",
    "conversationId": "conv-924821e7",
    "runId": "run-80e29ebd-ad4d-421f",
    "message": "Agent Run queued successfully"
  }
  ```

### 4. 이전 대화 히스토리 및 상세 복원
* **대화 목록 조회**: `GET /api/conversations` ➔ `List<ConversationSummaryDto>`
* **대화 상세 복원**: `GET /api/conversations/{conversationId}` ➔ 멀티턴 `runs` 목록 (타임라인, 마크다운 본문, A2UI 페이로드) 복원

---

## ⚡ 2. SSE 이벤트 규격 (`AgentEvent`)

| 이벤트 타입 (`type`) | 내용 (`content`) | 역할 및 `messageId` 동작 |
|---|---|---|
| **`INIT`** | `connectionId` 값 | `connectionId` 전달 및 SSE 소켓 세션 활성화 |
| **`STATUS`** | `🔍 웹 검색 수행 중...` | 사고과정 단계 누적 (`messageId: "msg-think-1"`) |
| **`CHUNK`** | `# 생성 결과\n...` | 마크다운 토큰 스트리밍 (`messageId: "msg-report-1"`) |
| **`A2UI_RENDER`** | `{"version":"1.0", ...}` | 선언적 A2UI 대시보드 렌더링 (**동일 `messageId` 시 In-place 갱신**) |
| **`DONE`** | `Stream Completed` | 해당 `runId` 1개 턴 스트리밍 완결 알림 |
| **`ERROR`** | `오류 메시지 내용` | 에러 정보 알림 |

---

## 🎨 3. A2UI (Agent-to-UI) JSON 스키마 명세

```json
{
  "version": "1.0",
  "messageId": "msg-a2ui-dashboard-1",
  "title": "📊 TECH 분야 실시간 데이터 대시보드",
  "metrics": [
    {
      "id": "metric_sources",
      "label": "수집된 웹 출처",
      "value": "3개 사이트",
      "change": "Real-time Scraped",
      "status": "normal"
    },
    {
      "id": "metric_confidence",
      "label": "분석 신뢰도",
      "value": "95%",
      "change": "Verified",
      "status": "success"
    }
  ],
  "action_section": {
    "title": "💡 에이전트 맞춤형 후속 탐색 (Human-in-the-Loop)",
    "description": "원하시는 항목을 선택하면 탐색을 이어갑니다.",
    "options": [
      {
        "action_id": "tech_code_example",
        "label": "💻 실제 구현 코드 및 연동 예제 요청",
        "description": "프로젝트 적용 샘플 코드를 생성합니다.",
        "payload": { "selected_option": "tech_code_example" }
      }
    ]
  }
}
```
