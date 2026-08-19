# Project Task Dashboard & Checklist

이 문서는 실시간 AI 리서처 에이전트(Real-time AI Researcher Agent) 개발 작업을 Phase별 세부 Task 단위로 나눈 작업 트래킹 대시보드입니다.

관련 문서: [요구사항 정의서](../requirements.md) | [시스템 아키텍처](../architecture.md) | [기술 명세서](../spec.md) | [구현 계획서](../plan.md)

---

## 📊 전체 진행 현황 (Overall Progress)

- [x] **Phase 1: 로컬 인프라 구성 (Docker/Kafka/Redis)** `[4/4]`
- [x] **Phase 2: Python 에이전트 & Kafka 연동 개발** `[7/7]`
- [x] **Phase 3: Kotlin Agent Stream Server 개발** `[7/7]`
- [x] **Phase 4: Frontend Web Application 개발 (Vite+React)** `[6/6]`
- [x] **Phase 5: E2E 통합 및 분산 세션 라우팅 검증** `[5/5]`
- [x] **Phase 6: A2UI 선언적 UI 및 양방향 피드백 연동** `[5/5]`

---

## 📝 Phase별 세부 태스크 문서 링크

각 Phase를 클릭하면 상세 작업 내용 및 구현 가이드를 확인할 수 있습니다.

### [Phase 1: 인프라 구성 (docker-compose)](phase1-infrastructure.md)
- [x] Task 1.1: `docker-compose.yml` 서비스 정의 (Kafka, Redis, Kafka UI, Kafka Init)
- [x] Task 1.2: Kafka UI 모니터링 도구 추가 (포트 `8989`)
- [x] Task 1.3: Kafka 토픽(`agent-requests`, `agent-responses`) 생성
- [x] Task 1.4: 로컬 인프라 헬스체크 및 핑 검증 완료

### [Phase 2: Python 에이전트 개발 (LangGraph + uv)](phase2-python-agent.md)
- [x] Task 2.1: `uv` 패키지 관리자 기반 환경 구축 및 `pyproject.toml` 초기화
- [x] Task 2.2: Kafka Producer/Consumer 래퍼 모듈 작성 (`kafka_client.py`)
- [x] Task 2.3: `duckduckgo-search` 기반 웹 검색 도구 구현 (`search_tool.py`)
- [x] Task 2.4: BeautifulSoup 기반 웹 페이지 스크래퍼 도구 구현 (`scraper_tool.py`)
- [x] Task 2.5: LangGraph 제어 그래프 작성 (`agent_graph.py`)
- [x] Task 2.6: 추론 단계별 이벤트 (`STATUS`, `CHUNK`, `ERROR`, `DONE`) 카프카 전송
- [x] Task 2.7: 라인 단위 한글 주석 작성 및 단독 모듈 실행 검증 완료

### [Phase 3: Kotlin Agent Stream Server 개발 (Spring WebFlux)](phase3-kotlin-gateway.md)
- [x] Task 3.1: Spring Boot 3.3.2 + Gradle (Kotlin DSL) 초기화 (`agent-stream-server/`)
- [x] Task 3.2: `kotlin-logging` (7.0.0) 설정 및 환경 구성
- [x] Task 3.3: `Host ID` (UUID) 발급 및 `SendChannel` 세션 레지스트리 개발
- [x] Task 3.4: `GET /api/chat/stream` Coroutine Flow SSE 컨트롤러 구현 (`awaitClose` 자원 정리)
- [x] Task 3.5: `POST /api/chat/message` 엔드포인트 및 Kafka Producer 구현
- [x] Task 3.6: Kafka `agent-responses` Reactive Listener 구현
- [x] Task 3.7: **`kotest`** BDD 스타일 단위 테스트 통과 완료

### [Phase 4: React Frontend 개발 (Vite + React)](phase4-react-frontend.md)
- [x] Task 4.1: `Vite + React + TypeScript` 프론트엔드 프로젝트 초기화 (`frontend/`)
- [x] Task 4.2: Vanilla CSS 기반 모던 디자인 시스템 및 글로벌 스타일 작성 (`index.css`, `App.css`)
- [x] Task 4.3: `useAgentStream` SSE 수신 & `sessionId` 관리 커스텀 훅 개발 (`useAgentStream.ts`)
- [x] Task 4.4: 에이전트 추론 단계 타임라인 컴포넌트(`ChatTimeline.tsx`) 구현
- [x] Task 4.5: `react-markdown` 기반 리포트 타자기 스트리밍 컴포넌트(`ReportViewer.tsx`) 구현
- [x] Task 4.6: 에러 안내 토스트 및 자동 재연결 UI 구현 (`App.tsx`)

### [Phase 5: E2E 통합 및 분산 세션 라우팅 검증](phase5-e2e-routing.md)
- [x] Task 5.1: Kotlin Stream Server 내 `ReactiveRedisTemplate` 연동 및 `host:{hostId}` 리스너 구현 (`RedisConfig.kt`)
- [x] Task 5.2: `hostId` 불일치 시 `Redis.publish()` 분산 라우팅 중계 로직 작성 (`RedisRoutingService.kt`)
- [x] Task 5.3: 로컬 다중 포트 (Port 8080, 8081) 기동 스크립트 작성 (`scripts/start-all.sh`)
- [x] Task 5.4: React FE ↔ 다중 Stream Server ↔ Kafka ↔ Python Agent ↔ Redis E2E 검증 통과
- [x] Task 5.5: 첫 단어 응답시간(TTFT) 프로파일링 및 튜닝 (1.2초 달성)

### [Phase 6: A2UI 선언적 UI 및 양방향 피드백 연동](phase6-a2ui-integration.md)
- [x] Task 6.1: A2UI 이벤트 데이터 스키마 및 REST API 명세 확장 (`spec.md`)
- [x] Task 6.2: Kotlin Stream Server 내 `POST /api/chat/action` 및 Kafka 프로듀싱 연동
- [x] Task 6.3: Python Worker 내 `a2ui_schema.py` Pydantic 빌더 및 `a2ui_generation` 노드 개발
- [x] Task 6.4: React FE 내 `A2UIRenderer.tsx` 대시보드 컴포넌트 및 `sendUserAction` 피드백 구현
- [x] Task 6.5: `v1.0.0-base` Git Tagging 및 `feature/a2ui-integration` 브랜치 E2E 검증 완료
