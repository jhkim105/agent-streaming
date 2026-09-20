# 작업 목록 (Tasks): 핵심 에이전트 스트리밍 & AGUI 프로토콜

**브랜치**: `001-core-agent-streaming` | **스펙**: [spec.md](./spec.md) | **계획서**: [plan.md](./plan.md)  
**식별자 체계**: `conversationId` > `runId` (1턴) > `messageId` (동일 영역 In-place 갱신) > `sseEventId` (SSE 패킷 번호)

---

## Phase 1: 기반 설정 및 인프라 (Setup & Infrastructure)

**목적**: Docker Compose 기반 분산 브로커(Kafka `agent-runs`/`agent-events`, Redis) 및 전체 프로젝트 기본 환경 점검

- [X] T001 [P] Docker Compose 설정 검증 및 `agent-runs`, `agent-events` 카프카 토픽 설정 in `docker-compose.yml`
- [X] T002 [P] 백엔드 게이트웨이 `application.yml`의 Kafka/Redis 토픽 및 설정 점검 in `agent-server/src/main/resources/application.yml`
- [X] T003 [P] Python 가상환경 및 LangGraph 의존성 패키지 점검 in `agent-runtime/pyproject.toml`
- [X] T004 [P] 프론트엔드 빌드 환경 및 Tailwind/Lucide 패키지 점검 in `frontend/package.json`

---

## Phase 2: 핵심 기반 구조 (Foundational Core)

**목적**: `runId` / `messageId` / `sseEventId` 체계가 적용된 백엔드 DTO, Redis 세션 레지스트리 및 이벤트 모델 정의

- [X] T005 [P] `AgentRunRequest` 및 `AgentEvent` DTO 정의 (runId, messageId, sseEventId 필드 포함) in `agent-server/src/main/kotlin/com/agent/stream/dto/AgentRunRequest.kt`
- [X] T006 [P] `RedisConnectionRegistry`에 `run:{runId}:conn` 및 `conn:{connectionId}:host` 매핑 구현 in `agent-server/src/main/kotlin/com/agent/stream/session/RedisConnectionRegistry.kt`
- [X] T007 [P] `ConversationHistoryStore`에 턴 단위(`runId`) 히스토리 축적 및 스마트 타이틀 갱신 로직 구현 in `agent-server/src/main/kotlin/com/agent/stream/service/ConversationHistoryStore.kt`
- [X] T008 [P] `KafkaEventListener`의 `agent-runs` 및 `agent-events` 컨슈밍 및 라우팅 로직 연동 in `agent-server/src/main/kotlin/com/agent/stream/listener/KafkaEventListener.kt`

---

## Phase 3: 사용자 스토리 1 - 실시간 대화 스레드 및 SSE 스트리밍 (Priority: P1) 🎯 MVP

**목표**: 신규 대화 생성, SSE 소켓 연결, 턴 요청(`POST /runs`) ➔ `STATUS`/`CHUNK`/`DONE` 실시간 스트리밍 완성

### 백엔드 게이트웨이 구현 (US1)
- [X] T009 [P] [US1] Kotest 기반 `ConversationController` REST/SSE 엔드포인트 테스트 작성 in `agent-server/src/test/kotlin/com/agent/stream/session/SseBackpressureIntegrationTest.kt`
- [X] T010 [US1] `POST /api/conversations`, `GET /api/conversations/{id}/events`, `POST /api/conversations/{id}/runs` 엔드포인트 구현 in `agent-server/src/main/kotlin/com/agent/stream/controller/ConversationController.kt`
- [X] T011 [US1] 코루틴 기반 SSE Emitter 및 배압(Backpressure) 처리 연동 in `agent-server/src/main/kotlin/com/agent/stream/service/StreamService.kt`

### Python 에이전트 런타임 구현 (US1)
- [X] T012 [P] [US1] `agent-runs` 토픽 메시지를 소비하여 `STATUS` 및 `CHUNK` 스트리밍을 발행하는 LangGraph 워커 구현 (한글 라인별 주석 필수) in `agent-runtime/src/main.py`
- [X] T013 [US1] 에이전트 실행 완료 시 `DONE` 이벤트 발행 및 스마트 타이틀 메타데이터 전달 in `agent-runtime/src/agent_graph.py`

### 프론트엔드 대화창 및 스트리밍 렌더러 (US1)
- [X] T014 [P] [US1] SSE 이벤트 구독 및 상태 관리를 담당하는 `useAgentStream` 커스텀 훅 구현 (`runId`, `sseEventId` 처리) in `frontend/src/hooks/useAgentStream.ts`
- [X] T015 [P] [US1] 사용자 질문 말풍선 및 타자기 효과 마크다운 답변 렌더러 컴포넌트 구현 in `frontend/src/components/ChatThreadWindow.tsx`
- [X] T016 [US1] 실시간 `STATUS` 이벤트를 접기/펴기로 표출하는 Thinking Accordion 컴포넌트 구현 in `frontend/src/components/ChatTimeline.tsx`

**체크포인트**: 새 대화방에서 질문을 입력하면 Thinking 스텝과 마크다운 답변이 실시간으로 스트리밍 표출되는 MVP 완성

---

## Phase 4: 사용자 스토리 2 - AGUI / A2UI 선언적 대시보드 및 In-place 갱신 (Priority: P2)

**목표**: `messageId` 기반의 선언적 지표 카드 렌더링, In-place 교체(Replace) 및 추천 액션 버튼 클릭 연동

### 프론트엔드 AGUI 컴포넌트 (US2)
- [X] T017 [P] [US2] `A2UI Schema v1.0` JSON 파서 및 TypeScript 인터페이스 정의 in `frontend/src/types/agent.ts`
- [X] T018 [US2] `messageId`가 일치하는 경우 카드를 복제하지 않고 In-place 교체하는 `A2UIRenderer` 컴포넌트 구현 in `frontend/src/components/A2UIRenderer.tsx`
- [X] T019 [US2] 추천 후속 액션 버튼 클릭 시 `POST /api/conversations/{id}/runs` (`type: "ACTION"`) 호출 연동 in `frontend/src/hooks/useAgentStream.ts`

### Python 에이전트 AGUI 생성 (US2)
- [X] T020 [US2] 분석 완료 시 `A2UI_RENDER` 선언적 JSON 이벤트(`messageId: "msg-a2ui-1"`)를 생성하여 발행하는 LangGraph 및 A2UI 스키마 지원 in `agent-runtime/src/a2ui_schema.py`

---

## Phase 5: 사용자 스토리 3 - 대화 목록 조회 및 멀티턴 히스토리 복원 (Priority: P3)

**목표**: 좌측 사이드바 대화 목록 조회, 이전 대화 클릭 시 과거 타임라인/본문/AGUI 카드 복원 및 후속 대화 이어가기

- [X] T021 [P] [US3] `GET /api/conversations` (요약 목록) 및 `GET /api/conversations/{id}` (상세 멀티턴 runs) API 구현 in `agent-server/src/main/kotlin/com/agent/stream/controller/ConversationController.kt`
- [X] T022 [P] [US3] 좌측 대화 목록 사이드바 및 새 채팅 생성 버튼 컴포넌트 구현 in `frontend/src/components/Sidebar.tsx`
- [X] T023 [US3] 이전 대화 선택 시 과거 사고과정, 마크다운 본문, A2UI 대시보드를 완벽히 복원하고 새 질문을 이어갈 수 있는 상태 복원 로직 구현 in `frontend/src/hooks/useAgentStream.ts`
- [X] T024 [US3] Kotest 기반 멀티턴 대화 히스토리 영속성 및 복원 통합 테스트 작성 in `agent-server/src/test/kotlin/com/agent/stream/history/ConversationHistoryIntegrationTest.kt`

---

## Phase 6: 마무리 및 품질 검증 (Polish & Validation)

**목적**: 전반적인 통합 테스트 실행, 빠른 실행 가이드 검증 및 코드 정리

- [X] T025 [P] Multi-Node Redis Stream 분산 릴레이 라우팅 Kotest 통합 테스트 검증 in `agent-server/src/test/kotlin/com/agent/stream/routing/MultiNodeDynamicRoutingIntegrationTest.kt`
- [X] T026 `quickstart.md`에 정의된 6단계 E2E 전체 시나리오 실행 및 검증
- [X] T027 Python 코드 내 한글 주석 점검 및 Kotlin 관례(Kotlin Idioms) 준수 점검
