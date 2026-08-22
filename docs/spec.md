# 🛠️ REST / SSE API 및 메시지 데이터 명세서 (API Specification)

본 문서는 **대화형 AI 에이전트(ChatGPT형 UX)** 시스템의 REST 엔드포인트, SSE 이행 규격 및 데이터 모델 스키마를 정의합니다.

---

## 📡 1. REST API 엔드포인트 명세

### 1. 명시적 대화 스레드 생성
* **URL**: `POST /api/conversations`
* **설명**: `[+ 새 채팅]` 버튼 클릭 시 호출하여 신규 `conversationId`를 즉시 발행합니다.
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

### 3. AgentCommand 커맨드 제출
* **URL**: `POST /api/conversations/{conversationId}/commands`
* **요청 본문 (`AgentCommand`)**:
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
    "commandId": "cmd-80e29ebd-ad4d-421f",
    "message": "AgentCommand queued successfully"
  }
  ```

### 4. 이전 대화 히스토리 및 상세 복원
* **대화 목록 조회**: `GET /api/conversations` ➔ `List<ConversationSummaryDto>`
* **대화 상세 복원**: `GET /api/conversations/{conversationId}` ➔ 타임라인, 마크다운 대화 이력, A2UI 페이로드 복원

---

## ⚡ 2. SSE 이벤트 규격 (`AgentEvent`)

| 이벤트 타입 (`type`) | 내용 (`content`) | 역할 |
|---|---|---|
| **`INIT`** | `SSE Connection Established` | `connectionId` 및 초기 세션 정보 전달 |
| **`STATUS`** | `🔍 웹 검색 수행 중...` | 에이전트의 내부 추론 과정 (Thinking Accordion에 축적) |
| **`CHUNK`** | `# 생성 결과\n...` | 마크다운 토큰 텍스트 스트리밍 (말풍선 타자기 표출) |
| **`A2UI_RENDER`** | `{"version":"1.0", ...}` | 선언적 A2UI 대시보드/카드 JSON 스키마 전달 |
| **`DONE`** | `Stream Completed` | 해당 답변 스트리밍 완결 알림 |
| **`ERROR`** | `오류 메시지 내용` | 에러 정보 알림 |

---

## 🎨 3. A2UI (Agent-to-UI) JSON 스키마 명세

```json
{
  "version": "1.0",
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
