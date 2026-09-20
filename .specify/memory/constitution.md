# 📜 프로젝트 헌장 (Project Constitution): Agent Streaming System

## 1. 핵심 아키텍처 원칙 (Core Architectural Principles)
- **이벤트 구동 및 완전 비동기성 (Event-Driven & Asynchronous)**: 모든 무거운 AI 에이전트 추론 작업은 분산 이벤트 버스(Kafka / Redis Streams)를 통해 비동기로 실행되어야 합니다. 클라이언트의 커맨드 제출 HTTP 요청은 즉시 `202 Accepted`로 응답해야 합니다.
- **실시간 스트리밍 우선주의 (Real-Time Streaming First)**: 에이전트의 내부 사고 과정(Thinking Steps), 텍스트 토큰(Markdown Chunks), UI 선언 스키마는 Server-Sent Events(SSE)를 통해 클라이언트로 실시간 점진적 스트리밍되어야 합니다.
- **AGUI / A2UI 선언적 UI 표준 (Declarative UI Protocol)**: 에이전트가 생성하는 동적 컴포넌트(지표 카드, 메트릭, 후속 행동 버튼 등)는 표준 AGUI / A2UI 선언적 JSON 프로토콜을 철저히 준수해야 합니다.
- **고가용성 및 무유실 분산 라우팅 (Resilience & Scale-Out)**: 4대 핵심 식별자(`conversationId`, `commandId`, `connectionId`, `eventId`) 체계를 기반으로 다중 서버 노드 환경에서도 100% 무유실 직통 배달을 보장해야 합니다.

## 2. 기술 스택 및 개발 거버넌스 (Tech Stack & Governance)
- **백엔드 게이트웨이**: Kotlin + Spring Boot 3.x (WebFlux, Coroutines, `kotlin-logging`, `kotest` 기반 BDD 테스트).
- **AI 에이전트 런타임**: Python + FastAPI + LangGraph / LangChain. 패키지 관리는 초고속 `uv`를 필수 사용하며, 초보자도 쉽게 이해할 수 있도록 상세한 한글 주석을 작성합니다.
- **프론트엔드 SPA**: React + TypeScript + Tailwind CSS + Lucide Icons (SSE 이벤트 파서 및 반응형 레이아웃).
