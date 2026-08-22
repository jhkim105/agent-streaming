# Conversational AI Agent System (Agent Server Architecture)

> **비동기 이벤트 구동 아키텍처(Kafka + Redis Streams) 기반의 대화형 AI 에이전트, 실시간 SSE 스트리밍 & AGUI/A2UI 동적 UI 렌더링 시스템**

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9+-7F52FF.svg?style=flat&logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x_(WebFlux)-6DB33F.svg?style=flat&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agent-FF6F61.svg?style=flat)](https://python.langchain.com/docs/langgraph)
[![React](https://img.shields.io/badge/React-18+(Vite)-61DAFB.svg?style=flat&logo=react&logoColor=black)](https://react.dev/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Broker-231F20.svg?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-Streams-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)

---

## 📌 1. 프로젝트 개요 (Overview)

본 프로젝트는 대화형 인터페이스(새 채팅, 이전 대화 목록, 스레드 전환 및 멀티턴 대화)를 제공하며, 백엔드 AI 에이전트가 실시간 LLM 추론(Ollama Qwen2.5-7B) 및 스마트 조건부 웹 조회를 수행하면서 **에이전트 사고과정(Thinking Accordion)**, **자연어 답변 토큰 스트리밍(SSE)**, **인라인 A2UI 동적 대시보드 및 후속 버튼**을 제공하는 고성능 에이전트 시스템입니다.

* **동시성 & 실시간 소켓 중계 (Kotlin Agent Server)**: JVM 논블로킹 리액티브 서버(Spring WebFlux)로 클라이언트와의 실시간 SSE 스트리밍 소켓을 코루틴 Flow 기반으로 배압(Backpressure)을 존중하여 안전하게 유지합니다.
* **AI 추론 엔진 (Python Agent Runtime Engine)**: 멀티스레드 비동기 그래프 및 조건부 웹 탐색 도구를 통해 대화 맥락을 이해하고 토큰 및 A2UI 스키마를 생성합니다.
* **CQRS & 이벤트 구동형 라우팅 (Kafka & Redis Streams)**: 
  * `AgentCommand` (클라이언트 ➔ 서버 ➔ Agent) / `AgentEvent` (Agent ➔ 서버 ➔ 클라이언트)로 관심사를 분리.
  * 다중 노드 스케일아웃 환경에서도 `commandId` ➔ `connectionId` ➔ `hostId` 동적 매핑 조회를 통해 요청을 전송한 특정 디바이스의 SSE 연결로 100% 무유실 릴레이 배달합니다.

---

## 📂 2. 프로젝트 디렉터리 구조 (Directory Layout)

```text
agent-streaming/
├── docker-compose.yml              # 로컬 인프라 (KRaft Kafka, Redis, Kafka UI)
├── start.sh                        # 원터치 통합 시스템 실행 스크립트
├── README.md                       # 프로젝트 메인 안내서
│
├── agent-server/                   # [Kotlin] Spring Boot 3.x WebFlux 게이트웨이 백엔드 서버
│   ├── src/main/kotlin/            # ConversationController, RedisConnectionRegistry, StreamService
│   └── build.gradle.kts            # Kotlin DSL 빌드 설정
│
├── agent-runtime/                  # [Python] Agent Runtime 추론 파이프라인 프로세스
│   ├── pyproject.toml              # uv 패키지 관리자 설정
│   └── src/                        # agent_graph.py, kafka_client.py, main.py
│
├── frontend/                       # [React] Vite + React + TS 독립 SPA 웹 앱
│   ├── src/                        # 라이트 테마 UI (Sidebar, ChatThreadWindow, ChatInputBar)
│   └── package.json
│
├── docs/                           # 현행화된 설계 및 명세 문서
│   ├── requirements.md             # 요구사항 정의서
│   ├── architecture.md             # 시스템 아키텍처 명세서
│   └── spec.md                     # REST/SSE API 명세서
│
└── work/                           # 작업 내역 및 기술 계획서
    └── 20260822-chat-agent-redesign.md
```
