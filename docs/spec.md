# Real-time AI Researcher Agent - Technical Specification

이 문서는 클래스/데이터 모델 명세, 데이터베이스 스키마, Redis 키 구조 및 REST API 엔드포인트 명세를 기술합니다.

관련 문서: [요구사항 정의서](requirements.md) | [시스템 아키텍처](architecture.md) | [ADR 0003 노드별 스트림 라우팅](adr/0003-redis-streams-bucketing-and-dynamic-session-routing.md)

---

## 1. 도메인 및 식별자 모델 명세 (Domain & Identity Models)

### 1.1. 4대 핵심 식별자 명세

- **`conversationId` (String/UUID)**: 비즈니스 리서치 대화 스레드 식별자 (대화 전체).
- **`commandId` (String/UUID)**: 클라이언트 명령(`AgentCommand`) 실행 1건 식별자.
- **`connectionId` (String/UUID)**: 물리적 SSE 소켓 연결 식별자 (소켓 1개).
- **`eventId` (String/UUID)**: 에이전트 스트리밍 이벤트(`AgentEvent`) 1건 식별자 (W3C SSE `id`와 1:1 연동).

### 1.2. 핵심 DTO / Domain Class 명세

#### AgentCommand (클라이언트 ➔ 백엔드 ➔ Agent Worker)
```kotlin
data class AgentCommand(
    val commandId: String = UUID.randomUUID().toString(),
    val conversationId: String,
    val connectionId: String,
    val type: CommandType, // RESEARCH, ACTION, CANCEL
    val payload: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)

enum class CommandType {
    RESEARCH,  // 질문 전송
    ACTION,    // A2UI 액션 버튼 선택
    CANCEL     // 진행 중인 에이전트 작업 중단
}
```

#### AgentEvent (Agent Worker ➔ 백엔드 ➔ 클라이언트 SSE)
```kotlin
data class AgentEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val commandId: String,
    val conversationId: String,
    val type: EventType, // INIT, STATUS, CHUNK, A2UI_RENDER, DONE, ERROR
    val content: String = "",
    val metadata: Map<String, Any> = emptyMap(),
    val timestamp: Long = System.currentTimeMillis()
)

enum class EventType {
    INIT, STATUS, CHUNK, A2UI_RENDER, DONE, ERROR
}
```

---

### 1.3. Redis 데이터 구조 명세 (Redis Data Structures)

#### 1. 소켓 위치 저장소 (Connection Registry)
* **Key**: `connection:host:{connectionId}`
* **Value**: `kotlin-node-1` (해당 SSE 소켓이 바인딩된 서버 노드 ID)
* **TTL**: 3600초 (소켓 해제 시 삭제)

#### 2. 명령-연결 매핑 저장소 (Command Correlation Registry)
* **Key**: `command:connection:{commandId}`
* **Value**: `CONN-A` (해당 AgentCommand를 보낸 SSE connectionId)
* **TTL**: 3600초 (명령 완결 처리 후 정제)

#### 3. 노드별 레디스 스트림 (Redis Streams)
* **Key**: `stream:host:{hostId}` (예: `stream:host:kotlin-node-1`)
* **Value**: AgentEvent JSON 페이로드 (targetConnectionId 포함)

---

## 2. REST API 엔드포인트 명세 (API Specifications)

### 2.1. 새 대화 생성 (`POST /api/conversations`)
* **Description**: 새로운 대화 스레드를 명시적으로 생성하고 `conversationId`를 발급받습니다.
* **Response**: `201 Created`
  ```json
  {
    "conversationId": "conv-a1b2c3d4-5678-90ef-ghij-1234567890ab",
    "createdAt": 1786187000000
  }
  ```

### 2.2. SSE 스트림 연결 (`GET /api/conversations/{conversationId}/events`)
* **Description**: 특정 대화 스레드에 대해 실시간 SSE 연결을 수립하고 `connectionId`를 발급받아 Redis에 등록합니다.
* **Headers**: `Last-Event-ID` (Optional, String) — 끊김 재연결 시 마지막 수신 이벤트 ID (`eventId`).
* **Response**: `text/event-stream`
* **First Event (INIT)**:
  ```json
  event: INIT
  data: {
    "type": "INIT",
    "eventId": "evt-00000000-0000-0000-0000-000000000000",
    "conversationId": "conv-a1b2c3d4-5678-90ef-ghij-1234567890ab",
    "connectionId": "conn-7dfa6d53-53bc-414e-9c67-5852e76573e7",
    "content": "SSE Connection Established"
  }
  ```

### 2.3. AgentCommand 제출 (`POST /api/conversations/{conversationId}/commands`)
* **Description**: 특정 대화 스레드에 `AgentCommand`를 제출하고 `commandId`를 생성하여 백엔드 Kafka `agent-commands` 토픽으로 전송합니다.
* **Request Body**:
  ```json
  {
    "connectionId": "conn-7dfa6d53-53bc-414e-9c67-5852e76573e7",
    "type": "RESEARCH",
    "payload": {
      "query": "LiteLLM 프레임워크 조사해줘"
    }
  }
  ```
* **Response**: `202 Accepted`
  ```json
  {
    "conversationId": "conv-a1b2c3d4-5678-90ef-ghij-1234567890ab",
    "commandId": "cmd-98765432-10fe-dcba-9876-543210fedcba",
    "status": "ACCEPTED"
  }
  ```

### 2.4. 이전 대화 목록 조회 (`GET /api/conversations`)
* **Description**: 사용자가 진행한 과거 대화 스레드 요약 목록을 조회합니다.

### 2.5. 특정 대화 상세 및 이력 조회 (`GET /api/conversations/{conversationId}`)
* **Description**: 특정 대화의 전체 커맨드/이벤트 타임라인, 완성된 리포트 및 대시보드 데이터를 조회합니다.

---

## 3. 현행 코드베이스 리팩토링 계획 (Refactoring Plan)

| 항목 | 현행 코드 | 목표 설계 |
|---|---|---|
| **도메인 클래명** | `ChatMessageRequest`, `AgentResponseEvent` | **`AgentCommand`**, **`AgentEvent`** |
| **요청 식별자** | `sessionId` / `query` | **`commandId`** (명령 유니크 키) |
| **이벤트 식별자** | Redis Stream ID | **`eventId`** (`AgentEvent` 내부 ID & SSE `id`) |
| **Kafka Topic** | `agent-requests`, `agent-responses` | `agent-commands`, `agent-events` |
| **대화 생성 API** | 메시지 전송 시 암묵적 생성 | `POST /api/conversations` 명시적 대화 생성 분리 |
| **API 엔드포인트** | `/api/chat/message` | `/api/conversations/{id}/commands` |
| **라우팅 키** | `session:host:{sessionId}` | `connection:host:{connectionId}` & `command:connection:{commandId}` |
