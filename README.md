# Real-time AI Researcher Agent

> **비동기 이벤트 구동 아키텍처(Kafka + Redis Streams) 기반의 실시간 LLM 스트리밍(SSE) 리서처 에이전트 시스템**

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9+-7F52FF.svg?style=flat&logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x_(WebFlux)-6DB33F.svg?style=flat&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agent-FF6F61.svg?style=flat)](https://python.langchain.com/docs/langgraph)
[![React](https://img.shields.io/badge/React-18+(Vite)-61DAFB.svg?style=flat&logo=react&logoColor=black)](https://react.dev/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Broker-231F20.svg?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-Streams-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)

---

## 📌 1. 프로젝트 개요 (Overview)

본 프로젝트는 사용자가 리서치 질문이나 명령(`AgentCommand`)을 제출하면, 백엔드 AI 에이전트가 실시간 웹 탐색(DuckDuckGo, Web Scraper) 및 LLM 추론(Ollama Qwen2.5-7B)을 수행하고, 지연 없이 실시간 타자기 마크다운 보고서 및 A2UI 대시보드 이벤트(`AgentEvent`)를 스트리밍(SSE)으로 제공하는 시스템입니다.

* **동시성 & 실시간 소켓 중계 (Kotlin Agent Stream Server)**: JVM 논블로킹 리액티브 서버(Spring WebFlux)로 클라이언트와의 실시간 SSE 스트리밍 소켓을 코루틴 Flow 기반으로 배압(Backpressure)을 존중하여 안전하게 유지합니다.
* **AI 추론 엔진 (Python LangGraph Worker)**: 멀티 스텝 추론 그래프 및 웹 검색 도구를 통해 마크다운 분석 리포트를 동적으로 토큰 생성합니다.
* **CQRS & 이벤트 구동형 라우팅 (Kafka & Redis Streams)**: 
  * `AgentCommand` (클라이언트 ➔ 서버 ➔ Agent) / `AgentEvent` (Agent ➔ 서버 ➔ 클라이언트)로 관심사를 분리.
  * 다중 노드 스케일아웃 환경에서도 `commandId` ➔ `connectionId` ➔ `hostId` 동적 매핑 조회를 통해 요청을 전송한 특정 디바이스의 SSE 연결로 100% 무유실 릴레이 배달합니다.

---

## 🏗️ 2. 시스템 아키텍처 (Architecture)

```
 [ Client Browser ]
  (Vite + React SPA)
         │
         │ (1) POST /api/conversations ──► 새 대화 생성 (conversationId 발급)
         │ (2) GET /api/conversations/{id}/events ──► SSE 커넥션 수립 (connectionId 발급)
         │ (3) POST /api/conversations/{id}/commands ──► AgentCommand 전송 (commandId 발급)
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Agent Stream Server Cluster (Kotlin WebFlux)                                    │
│                                                                                 │
│  ┌─────────────────────────────┐               ┌─────────────────────────────┐  │
│  │ Stream Server Node 1        │               │ Stream Server Node 2        │  │
│  │ (Host ID: kotlin-node-1)    │               │ (Host ID: kotlin-node-2)    │  │
│  │  - Local SSE Map (CONN-A)   │               │  - Receives AgentCommand    │  │
│  │  - Connection Registry      │               │  - ConversationHistoryStore │  │
│  └──────────────▲──────────────┘               └──────────────┬──────────────┘  │
└─────────────────┼─────────────────────────────────────────────┼─────────────────┘
                  │                                             │ (4) Produce AgentCommand
                  │                                             ▼
                  │                                 ┌───────────────────────┐
                  │                                 │ Kafka Broker          │
                  │                                 │ Topic: agent-commands │
                  │                                 └───────────┬───────────┘
                  │                                             │ (5) Consume AgentCommand
                  │                                             ▼
                  │                    ┌──────────────────────────────────────────┐
                  │                    │ Python Agent Worker (LangGraph Engine)   │
                  │                    │  - DuckDuckGo -> Scraper -> Ollama LLM   │
                  │                    └────────────────────────┬─────────────────┘
                  │                                             │ (6) Produce AgentEvent
                  │                                             ▼
                  │                                 ┌───────────────────────┐
                  │                                 │ Kafka Broker          │
                  │                                 │ Topic: agent-events   │
                  │                                 └───────────┬───────────┘
                  │ (8) Node 1 Stream 읽기 (XREAD)              │ (7) Consume AgentEvent & Redis 동적 조회
                  │     (stream:host:kotlin-node-1)             │     commandId ➔ connectionId ➔ node-1
                  │                                             ▼
      ┌───────────┴─────────────────┐               ┌───────────────────────────┐
      │ Redis Streams               │ ◄── (XADD) ── │ Stream Server Consumer    │
      │ Key: stream:host:node-1     │               │ Node 2 (또는 인근 노드)   │
      └─────────────────────────────┘               └───────────────────────────┘
                  │ (9) SSE AgentEvent Direct Delivery
                  ▼
          [ Client Browser ] (CONN-A 소켓 타깃 배달)
```

---

## 🔑 3. 4대 핵심 식별자 체계 (Identity Model)

| 식별자 | 명칭 | 역할 및 범위 |
|---|---|---|
| **`conversationId`** | 대화 스레드 ID | 대화 전체 생명주기 (이력 복원 및 저장 기준) |
| **`commandId`** | AgentCommand ID | 명령 1건 식별 및 이벤트 Correlation 기준 (`requestId` 대체) |
| **`connectionId`** | SSE 연결 소켓 ID | 물리적 SSE 연결 식별 (요청 디바이스 타깃팅 기준) |
| **`eventId`** | AgentEvent ID | 이벤트 1건 식별 (W3C SSE `id` 및 Stream 복원 기준) |

---

## 📂 4. 프로젝트 모듈 및 디렉터리 구조 (Directory Layout)

```text
agent-streaming/
├── docker-compose.yml              # 로컬 인프라 (KRaft Kafka, Redis, Kafka UI)
├── start.sh                        # 원터치 통합 시스템 실행 스크립트
├── README.md                       # 프로젝트 메인 안내서
│
├── agent-stream-server/            # [Kotlin] Spring Boot 3.x WebFlux 스트리밍 서버
│   ├── src/main/kotlin/            # ConversationController, RedisConnectionRegistry, StreamService
│   └── build.gradle.kts            # Kotlin DSL 빌드 설정
│
├── agent-worker/                   # [Python] LangGraph 에이전트 프로세스
│   ├── pyproject.toml              # uv 패키지 관리자 설정
│   └── src/                        # agent_graph.py, kafka_client.py, ollama_client.py
│
├── frontend/                       # [React] Vite + React + TS 독립 SPA 웹 앱
│   ├── src/                        # ChatTimeline, ReportViewer, A2UIRenderer, useAgentStream
│   └── package.json
│
├── docs/                           # 현행화된 설계 및 명세 문서
│   ├── requirements.md             # 요구사항 정의서 (What 중심)
│   ├── architecture.md             # 상세 아키텍처 및 시퀀스 다이어그램
│   ├── spec.md                     # REST/SSE API 명세 및 데이터 스키마
│   ├── adr/                        # 아키텍처 의사결정 기록 (ADR)
│   │   ├── 0001-multi-node-session-routing.md
│   │   ├── 0002-kotlin-sse-coroutine-flow.md
│   │   └── 0003-redis-streams-bucketing-and-dynamic-session-routing.md
│   └── tasks/                      # 단계별 구현 태스크 목록
│
└── work/                           # 작업 내역 및 개편 이력 문서
    └── 20260819-architecture-재정의.md
```

---

## 🚀 5. 빠른 실행 가이드 (Quick Start)

### Option A: 원터치 통합 실행 (추천 ⚡)
모든 인프라(Docker Kafka/Redis), Python Worker, Kotlin Server, React SPA를 원클릭으로 일괄 기동하고 관리합니다.
```bash
# 프로젝트 루트에서 통합 실행 스크립트 구동
./start.sh

# 웹 앱 접속: http://localhost:5173
# (종료 시 터미널에서 Ctrl+C를 누르면 모든 관련 프로세스가 자동 정지됩니다)
```

---

### Option B: 서비스별 수동 개별 실행

#### Step 1: 인프라 실행 (Kafka & Redis)
```bash
# Docker Compose로 인프라 서비스 일괄 기동
docker compose up -d

# Kafka UI 대시보드 접속 확인 (URL: http://localhost:8989 - admin/admin)
```

#### Step 2: Python 에이전트 워커 실행
```bash
cd agent-worker

# uv 가상환경 구축 및 의존성 동기화
uv sync

# 에이전트 프로세스 기동
uv run python -m src.main
```

#### Step 3: Kotlin Agent Stream Server 실행
```bash
cd agent-stream-server

# Gradle 빌드 및 Spring Boot 실행 (Port: 8080)
./gradlew bootRun
```

#### Step 4: React 프론트엔드 웹 앱 실행
```bash
cd frontend

# 개발 서버 구동 (Port: 5173)
npm run dev
```

---

## 📚 6. 프로젝트 관련 문서 (Documentation)

* 📄 [요구사항 정의서 (requirements.md)](docs/requirements.md)
* 📐 [시스템 아키텍처 및 시퀀스 플로우 (architecture.md)](docs/architecture.md)
* 🛠️ [REST/SSE API 명세 및 기술 규격 (spec.md)](docs/spec.md)
* 📝 [아키텍처 재정의 및 작업 내역 (work/20260819-architecture-재정의.md)](work/20260819-architecture-재정의.md)
* 🏛️ [ADR 0003: 노드별 Redis Streams 라우팅 (adr/0003-redis-streams-bucketing-and-dynamic-session-routing.md)](docs/adr/0003-redis-streams-bucketing-and-dynamic-session-routing.md)
