# 📄 대화형 AI 에이전트 요구사항 정의서 (Requirements Specification)

본 문서는 **비동기 이벤트 구동 아키텍처(Kafka + Redis Streams)** 기반의 **대화형 AI 에이전트(ChatGPT형 UX)**, **실시간 SSE 스트리밍**, **AGUI/A2UI 동적 UI 렌더링** 시스템의 기능적 및 비기능적 요구사항을 정의합니다.

---

## 🎯 1. 시스템 목표 및 핵심 가치 (System Goals)

1. **ChatGPT형 사용자 경험 (Conversational Agent UX)**:
   - 좌측 사이드바 기반의 대화 스레드 관리 (`[+ 새 채팅]`, 대화 목록 조회 및 스레드 전환, 멀티턴 대화 유지)
   - 메인 대화창 내 사용자 입력(우측 말풍선)과 에이전트 실시간 응답(좌측 마크다운 토큰 스트리밍) 제공
2. **에이전트 사고과정(Thinking Process) 시각화**:
   - 에이전트의 내부 추론 단계(`STATUS`)를 접기/펴기(Accordion) 형태의 'Think' 블록으로 제공하여 투명성 제공
3. **AGUI (Agent Guided UI) & A2UI (Agent-to-UI) 동적 인터랙션**:
   - LLM이 답변 메시지 내에 동적 메트릭 수치 카드 및 후속 행동 버튼(Human-in-the-Loop)을 선언적 JSON 구조로 전달하여 화면에 즉시 인라인 렌더링
4. **고성능 분산 SSE 스트리밍 & 4대 식별자 라우팅**:
   - 다중 노드 스케일아웃 환경에서도 `conversationId`, `commandId`, `connectionId`, `eventId` 체계를 통해 특정 브라우저 소켓으로 이벤트를 100% 무유실 직통 배달

---

## 🛠️ 2. 기능적 요구사항 (Functional Requirements)

### F-1. 대화 스레드 및 세션 관리 (Conversation Lifecycle)
- **F-1.1**: 사용자는 `[+ 새 채팅]` 버튼을 눌러 명시적으로 새로운 대화 스레드(`conversationId`)를 시작할 수 있어야 한다.
- **F-1.2**: 사용자는 좌측 사이드바에서 이전 대화 목록을 확인하고 특정 대화를 선택하여 이전 대화 타임라인, 마크다운 답변, A2UI 대시보드 상태를 복원할 수 있어야 한다.
- **F-1.3**: 멀티턴 대화 중 추가 질문 입력 시, 동일한 `conversationId` 스레드 맥락 내에서 에이전트 응답이 이어져야 한다.

### F-2. 실시간 SSE 스트리밍 및 메시지 발행 (Streaming & Messaging)
- **F-2.1**: 브라우저는 `GET /api/conversations/{conversationId}/events`를 통해 서버와 단방향 SSE 연결을 맺고 `connectionId`를 수신받아야 한다.
- **F-2.2**: 질문 커맨드(`AgentCommand`) 제출 시 `POST /api/conversations/{conversationId}/commands`를 호출하며, 서버는 202 Accepted와 함께 `commandId`를 반환해야 한다.
- **F-2.3**: 에이전트는 추론 상태(`STATUS`), 생성된 텍스트 토큰(`CHUNK`), A2UI 스키마(`A2UI_RENDER`), 완결(`DONE`), 에러(`ERROR`) 이벤트를 SSE로 실시간 스트리밍해야 한다.

### F-3. ChatGPT 스타일 메인 챗 UI 및 A2UI 렌더링 (UI/UX)
- **F-3.1**: 사용자 질문은 우측 말풍선, 에이전트 응답은 좌측 타자기 효과 마크다운 말풍선으로 표출되어야 한다.
- **F-3.2**: 에이전트의 내부 추론 과정(`STATUS`)은 답변 상단의 접기/펴기 가능한 'Think' 블록 내에 시간순으로 정리 표출되어야 한다.
- **F-3.3**: 수신된 `A2UI_RENDER` 스키마는 에이전트 답변 하단에 지표 수치 카드 및 후속 질문 버튼으로 인라인 렌더링되어야 한다.
- **F-3.4**: 사용자가 A2UI 액션 버튼을 클릭하면 `AgentCommand` (`type: "ACTION"`)가 전송되어 에이전트의 연관 후속 답변이 이어져야 한다.

---

## ⚡ 3. 비기능적 요구사항 (Non-Functional Requirements)

- **N-1. 배압(Backpressure) 보장**:
  - 클라이언트 네트워크 지연 시 이벤트가 유실되지 않도록 코루틴 suspending `send()` 기반의 배압을 보장해야 한다.
- **N-2. 분산 라우팅 무유실성**:
  - 에이전트 응답이 어떤 서버 노드에서 처리되더라도 Redis Connection Registry 및 Redis Stream XADD/XREAD 릴레이를 통해 클라이언트 소켓 노드로 올바르게 배달되어야 한다.
- **N-3. 동시 처리성**:
  - 에이전트 워커 프로세스는 멀티스레드 비동기 루프를 통해 복수의 질문 커맨드를 카프카 컨슈머 블로킹 없이 동시 수용할 수 있어야 한다.
