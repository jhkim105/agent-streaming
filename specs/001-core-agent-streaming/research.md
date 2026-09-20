# 기술 연구 및 의사결정 보고서: 핵심 에이전트 스트리밍 & AGUI 프로토콜

**대상 스펙**: [spec.md](./spec.md)  
**작성일자**: 2026-09-19 (runId, messageId, sseEventId 전면 적용)

---

## 1. 분산 실시간 스트리밍 프로토콜 및 라우팅 (Routing Architecture)

- **결정 (Decision)**: WebFlux Sinks (Coroutines) 기반 SSE + Redis Connection Registry + Redis Stream 릴레이 채택.
- **선정 이유 (Rationale)**:
  - WebSocket에 비해 HTTP/2 및 SSE(Server-Sent Events)는 단방향 텍스트/이벤트 스트리밍에 가볍고 표준 헤더(`text/event-stream`)로 프록시/방화벽 통과가 용이함.
  - 클러스터 다중 게이트웨이 노드 환경에서 `connectionId -> hostname` 매핑을 Redis에 보관하고, 타 노드로의 메시지 배달은 Redis Stream(XADD/XREAD)으로 무유실 분산 릴레이 가능.
- **검토된 대안 (Alternatives Considered)**:
  - *전체 WebSocket 양방향 소켓*: 오버헤드가 크고 메시지 Correlation 관리가 복잡해 단방향 스트림에 불필요.
  - *Kafka 토픽을 브라우저 직접 소비*: 브라우저에서 직접 Kafka 연결 불가, 보안 및 세션 제어 한계.

---

## 2. 에이전트 비동기 파이프라인 및 멀티턴 메모리 관리 (Agent Runtime)

- **결정 (Decision)**: Kafka `agent-runs` / `agent-events` 비동기 큐 + Python LangGraph Multi-Turn Engine.
- **선정 이유 (Rationale)**:
  - 사용자 턴 실행 요청(`POST /api/conversations/{id}/runs`) 시 `202 Accepted`를 즉시 반환하여 HTTP 스레드 고갈 방지.
  - LangGraph 워커가 턴 요청(`runId`)을 비동기 소비(Consume)하여 실시간 `STATUS`(추론 과정), `CHUNK`(토큰), `A2UI_RENDER`(UI 스키마) 이벤트를 발행.
  - 게이트웨이 `ConversationHistoryStore`가 `DONE` 완결 시점에 전체 턴 히스토리(마크다운 본문, 타임라인, UI 페이로드)를 `runId` 단위로 집계/보관하여 후속 턴의 맥락을 완벽히 복원.

---

## 3. AGUI / A2UI 선언적 대시보드 컴포넌트 표준

- **결정 (Decision)**: 선언적 JSON Schema v1.0 (지표 메트릭 카드 + 추천 후속 행동 버튼 세션) + `messageId` 기반 In-place 갱신.
- **선정 이유 (Rationale)**:
  - LLM이 HTML/JS 코드를 직접 생성하는 대신 정형화된 JSON을 반환하게 함으로써 XSS 보안 취약점을 차단하고 React 클라이언트가 안전하게 컴포넌트를 인라인 렌더링.
  - 에이전트가 탐색 과정에서 UI를 갱신할 때 동일한 `messageId`를 전달하면 프론트엔드가 카드를 복제하지 않고 기존 영역을 즉시 업데이트(In-place Replace).
  - 사용자 클릭 액션은 `AgentRunRequest`의 `type="ACTION"`으로 게이트웨이 및 에이전트에 동일한 파이프라인으로 전달(Human-in-the-Loop 완결).
