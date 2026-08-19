# Task Phase 6: A2UI (Agent-to-UI) 선언적 UI 생성 및 양방향 피드백 연동

* **목표**: 에이전트(LLM)가 단순 마크다운 텍스트를 넘어 선언적 UI 스키마(JSON Protocol)를 스트리밍 생성하고, 프론트엔드가 이를 인터랙티브 대시보드로 렌더링하며, 사용자의 UI 액션 선택을 백엔드 및 에이전트로 피드백하는 A2UI 시스템을 구현합니다.
* **관련 문서**: [기술 명세서](../spec.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 6.1: A2UI 데이터 스키마 및 REST API 명세 확장**
  * `docs/spec.md` 내 SSE 이벤트 타입에 `A2UI_RENDER` 추가
  * 사용자 A2UI 액션 제출용 `POST /api/chat/action` REST 엔드포인트 규격 정의
  * Kafka 요청 토픽(`agent-requests`)에 `actionId` 및 `payload` 스키마 확장

- [x] **Task 6.2: Kotlin Agent Stream Server 내 A2UI 역방향 라우팅 추가**
  * `AgentActionRequest` (sessionId, actionId, payload) DTO 개발 (`ChatMessageRequest.kt`)
  * `StreamService.kt` 내 `sendUserAction` 메서드 개발하여 Kafka로 액션 프로듀스
  * `ChatController.kt` 내 `POST /api/chat/action` 컨트롤러 구현 및 단위 테스트 통과 (`BUILD SUCCESSFUL`)

- [x] **Task 6.3: Python Agent Worker 내 A2UI Pydantic 빌더 및 LangGraph 노드 추가**
  * `a2ui_schema.py` 개발: 지표 메트릭 카드, 후속 탐색 옵션 버튼, 데이터 신뢰도 A2UI 구조 생성 헬퍼 작성
  * `agent_graph.py` 내 `a2ui_generation` 노드 추가 및 `A2UI_RENDER` Kafka 이벤트 발행
  * `main.py` 내 사용자 UI 버튼 클릭(`A2UI_ACTION`) 수신 시 연동 리서치 지속 추론 로직 구현

- [x] **Task 6.4: React SPA Frontend 내 A2UI Render Engine 구현**
  * `types/agent.ts` 내 `A2UIData`, `A2UIMetric`, `A2UIActionOption` 인터페이스 정의
  * 글래스모피즘 & 미세 애니메이션 기반 반응형 `A2UIRenderer.tsx` 및 `A2UIRenderer.css` 개발
  * `useAgentStream.ts` 내 `A2UI_RENDER` SSE 이벤트 수신 및 `sendUserAction` 피드백 함수 연동
  * `App.tsx` 내 리포트 뷰어 하단에 A2UI 대시보드 컴포넌트 탑재

- [x] **Task 6.5: 브랜치 및 스냅샷 관리 (Git Tagging & Branching)**
  * `v1.0.0-base` Git Tag 생성으로 기본 비동기 리서처 시스템 버전을 안전하게 기록
  * `feature/a2ui-integration` 독립 기능 개발 브랜치에서 모듈별 빌드 및 검증 완료 (`built in 405ms`)
