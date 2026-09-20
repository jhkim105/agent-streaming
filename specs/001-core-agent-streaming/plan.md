# 구현 계획서 (Implementation Plan): 핵심 에이전트 스트리밍 & AGUI 프로토콜

**브랜치**: `001-core-agent-streaming` | **작성일자**: 2026-09-19 | **스펙**: [spec.md](./spec.md)  
**입력 정보**: [specs/001-core-agent-streaming/spec.md](./spec.md)

---

## 📋 1. 요약 (Summary)

대화 세션 관리, Kafka/Redis 기반 비동기 SSE 스트리밍 파이프라인, 에이전트 사고과정(Thinking Accordion) 표출, 선언적 AGUI/A2UI 인터랙티브 컴포넌트 렌더링, 이전 대화 목록 조회 및 멀티턴 대화 연속성 지원을 위한 전체 시스템 기술 구현 계획입니다.
특히 **`runId` (1턴 실행 단위)**와 **`messageId` (동일 영역 In-place 갱신 단위)**, **`sseEventId` (네트워크 패킷 단위)**를 명확히 분리하여 프론트엔드 렌더링 안정성을 확보합니다.

---

## 🛠️ 2. 기술 컨텍스트 (Technical Context)

- **언어 및 버전**: Kotlin 1.9+ / Java 21 (게이트웨이), Python 3.11+ (에이전트 런타임), TypeScript 5+ (프론트엔드)
- **주요 프레임워크 및 라이브러리**:
  - `agent-server`: Spring Boot 3.x, Spring WebFlux, Kotlin Coroutines, `kotlin-logging`, `kotest`
  - `agent-runtime`: FastAPI, LangGraph, LangChain, `uv`
  - `frontend`: React 18, Vite, Tailwind CSS, Lucide Icons, Fetch EventSource
- **메시지 버스 및 저장소**: Apache Kafka (`agent-runs`, `agent-events`), Redis (Connection Registry & Stream Relay), In-Memory / RDB 스토어
- **테스트 프레임워크**: Kotest (`BehaviorSpec`, `DescribeSpec`), Pytest, Vitest

---

## 🔑 3. 턴 및 렌더링 식별자 계층 아키텍처

1. **`conversationId`**: 전체 대화 스레드 단위 (복수 턴의 누적 컨텍스트)
2. **`runId`**: 1개 턴 단위 (사용자 질문 1회 + 에이전트 응답 1회)
3. **`messageId`**: 응답 내 특정 UI/텍스트 블록 단위 ➔ **동일 `messageId` 수신 시 In-place 교체(Replace)**
4. **`sseEventId`**: 단일 SSE 네트워크 전송 패킷 순서 식별자

---

## 📜 4. 헌장 검증 (Constitution Check)

- [x] **비동기 이벤트 구동**: HTTP POST Run 요청은 202 즉시 반환, 비동기 Kafka 큐 처리 준수
- [x] **실시간 스트리밍**: SSE 단방향 스트림(`STATUS`, `CHUNK`, `A2UI_RENDER`, `DONE`) 준수
- [x] **AGUI / A2UI 선언적 UI**: JSON Schema v1.0 기반 UI 카드 및 Human-in-the-Loop 버튼 준수
- [x] **동일 영역 In-place 갱신**: `messageId` 기반 UI 상태 갱신 메커니즘 수립
- [x] **기술 스택 거버넌스**: Spring Boot 3.x, Kotlin Coroutines, Python uv, 한글 상세 주석 준수

---

## 📂 5. 프로젝트 구조 및 파일 레이아웃 (Project Structure)

### 문서 및 명세 (Specification Artifacts)
```text
specs/001-core-agent-streaming/
├── spec.md                  # 기능 요구사항 명세서 (한글)
├── plan.md                  # 구현 계획서 (본 파일)
├── research.md              # 아키텍처 및 기술 의사결정 기록
├── data-model.md            # 계층형 식별자 모델, DTO, A2UI JSON 스키마
├── quickstart.md            # E2E 검증 및 기동 가이드
├── contracts/
│   └── api-endpoints.md     # REST API 및 SSE 프로토콜 규격
└── tasks.md                 # 세부 구현 태스크 (.gitignore)
```

### 소스 코드 구조 (Repository Layout)
```text
agent-streaming/
├── agent-server/            # Kotlin WebFlux 게이트웨이 & SSE 라우터
│   └── src/main/kotlin/com/agent/stream/
│       ├── controller/      # REST & SSE API 컨트롤러 (Run 엔드포인트)
│       ├── service/         # HistoryStore, RoutingService, StreamService
│       ├── session/         # SessionRegistry, RedisConnectionRegistry
│       ├── listener/        # KafkaEventListener (AgentEvent 소비)
│       └── dto/             # AgentRunRequest, AgentEvent, ConversationDto
├── agent-runtime/           # Python LangGraph 에이전트 런타임
│   ├── app/                 # FastAPI & LangGraph 워커 로직
│   └── main.py              # Kafka Consumer(Run)/Producer(Event) 루프
└── frontend/                # React Vite 대화형 UI
    └── src/
        ├── components/      # Sidebar, ChatWindow, ThinkingAccordion, A2UIDashboard
        ├── hooks/           # useAgentStream, useConversation
        └── types/           # SSE Event & A2UI 타입 정의 (runId, messageId 대응)
```
