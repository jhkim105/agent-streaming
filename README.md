# Real-time AI Researcher Agent

> **비동기 이벤트 구동 아키텍처(Kafka + Redis) 기반의 실시간 LLM 스트리밍(SSE) 리서치 에이전트 시스템**

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9+-7F52FF.svg?style=flat&logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x_(WebFlux)-6DB33F.svg?style=flat&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agent-FF6F61.svg?style=flat)](https://python.langchain.com/docs/langgraph)
[![React](https://img.shields.io/badge/React-18+(Vite)-61DAFB.svg?style=flat&logo=react&logoColor=black)](https://react.dev/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Broker-231F20.svg?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-Pub%2FSub-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)

---

## 📌 1. 프로젝트 개요 (Overview)

본 프로젝트는 사용자가 복잡한 분석이나 시장 조사 질문을 입력하면, 에이전트가 백그라운드에서 웹 검색 및 본문 파싱, 정보 종합을 수행하는 **실시간 AI 리서처(AI Researcher)** 시스템입니다.

* **동시성 & 실시간 소켓 중계 (Kotlin Agent Stream Server)**: JVM 논블로킹 리액티브 서버로 클라이언트와의 실시간 SSE(Server-Sent Events) 스트리밍 소켓을 코루틴 Flow 기반으로 효율적으로 유지합니다.
* **AI 추론 & 생태계 (Python LangGraph Worker)**: 다단계 추론 엔진과 웹 도구(DuckDuckGo Search, Web Scraper)를 통해 리서치 보고서를 동적으로 작성합니다.
* **마이크로서비스 결합도 격리 (Kafka & Redis)**: Kafka 메시지 버퍼로 서버와 AI 워커 간 부하를 격리하고, 스트리밍 서버가 다중 노드로 확장(Scale-out)되어도 Redis Pub/Sub 유니캐스트 라우팅을 통해 올바른 사용자 소켓으로 이벤트를 배달합니다.

---

## 🏗️ 2. 시스템 아키텍처 (Architecture)

```
 [ Client Browser ]
  (Vite + React SPA)
         │
         │ (1) GET /api/chat/stream  ──► SSE 커넥션 수립 & Session ID (UUID) 생성
         │ (2) POST /api/chat/message ─► 질문 등록 (Session ID 포함)
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Agent Stream Server Cluster (Kotlin WebFlux)                                    │
│                                                                                 │
│  ┌─────────────────────────────┐               ┌─────────────────────────────┐  │
│  │ Stream Server Node 1        │               │ Stream Server Node 2        │  │
│  │ (Host ID: kotlin-node-1)    │               │ (Host ID: kotlin-node-2)    │  │
│  │                             │               │                             │  │
│  │  - SSE Session Map (Local)  │               │  - SSE Session Map (Local)  │  │
│  │  - Kafka Producer/Consumer  │               │  - Kafka Producer/Consumer  │  │
│  └──────────────┬──────────────┘               └──────────────▲──────────────┘  │
└─────────────────┼─────────────────────────────────────────────┼─────────────────┘
                  │ (3) Produce Request                         │ (6) Consume Response
                  ▼                                             │
      ┌───────────────────────┐                     ┌───────────┴───────────┐
      │ Kafka Broker          │                     │ Kafka Broker          │
      │ Topic: agent-requests │                     │ Topic: agent-responses│
      └───────────┬───────────┘                     └───────────▲───────────┘
                  │                                             │
                  │ (4) Consume Request                         │ (5) Produce Response
                  ▼                                             │
      ┌─────────────────────────────────────────────────────────┴─────────────────┐
      │ Python Agent Worker (LangGraph Multi-Step Engine)                        │
      │  - Query Analysis -> DuckDuckGo Search -> Web Scraper -> Summarize/Stream │
      └───────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ (7) Host ID 불일치 시 Redis Pub/Sub 라우팅
                                        ▼
                         ┌─────────────────────────────┐
                         │ Redis Pub/Sub               │
                         │ Channel: host:kotlin-node-1 │
                         └──────────────┬──────────────┘
                                        │ (8) Publish Message
                                        ▼
                         [ Stream Server Node 1 ]
                                        │ (9) SSE Stream Response
                                        ▼
                                [ Client Browser ]
```

---

## 📂 3. 프로젝트 모듈 및 디렉터리 구조 (Directory Layout)

본 프로젝트는 각 컴포넌트의 역할이 완벽히 분리된 마이크로서비스 및 독립 프론트엔드 구조로 구성되어 있습니다.

```text
agent-streaming/
├── docker-compose.yml              # 로컬 인프라 (KRaft Kafka, Redis, Kafka UI)
├── README.md                       # 프로젝트 메인 안내서
│
├── agent-stream-server/            # [Kotlin] Spring Boot 3.x WebFlux 스트리밍 서버
│   ├── src/main/kotlin/            # SSE 세션 레지스트리, Kafka/Redis 라우터
│   └── build.gradle.kts            # Kotlin DSL 빌드 설정
│
├── agent-worker/                   # [Python] LangGraph 에이전트 프로세스
│   ├── pyproject.toml              # uv 패키지 관리자 설정
│   └── src/                        # LangGraph 그래프 및 Kafka 프로듀서/컨슈머
│
├── frontend/                       # [React] Vite + React + TS 독립 SPA 웹 앱
│   ├── src/                        # ChatTimeline, ReportViewer 스트리밍 UI
│   └── package.json
│
└── docs/                           # 기술 설계 및 프로젝트 문서
    ├── 1.requirements.md           # 요구사항 정의서
    ├── 2.architecture.md           # 상세 시스템 아키텍처 및 시퀀스 다이어그램
    ├── 3.spec.md                   # REST/SSE API 명세 및 데이터 스키마
    ├── 4.plan.md                   # 5단계 개발 로드맵 및 가이드라인
    ├── adr/                        # 아키텍처 의사결정 기록 (ADR)
    │   ├── 0001-multi-node-session-routing.md
    │   └── 0002-kotlin-sse-coroutine-flow.md
    └── tasks/                      # 단계별 세부 구현 태스크 목록 및 대시보드
        ├── README.md               # 전체 태스크 트래킹 대시보드
        ├── phase1-infrastructure.md
        ├── phase2-python-agent.md
        ├── phase3-kotlin-gateway.md
        ├── phase4-react-frontend.md
        └── phase5-e2e-routing.md
```

---

## 🚀 4. 빠른 실행 가이드 (Quick Start)

### Step 1: 인프라 실행 (Kafka & Redis)
로컬 인프라는 KRaft 기반 Kafka와 Redis를 구동합니다.
```bash
# Docker Compose로 인프라 서비스 일괄 기동
docker compose up -d

# Kafka UI 대시보드 접속 확인
# URL: http://localhost:8989 (ID: admin / PW: admin)
```

### Step 2: Python 에이전트 워커 실행
초고속 패키지 관리자 **`uv`**를 사용하여 환경 구축 및 워커를 실행합니다.
```bash
cd agent-worker

# uv 가상환경 구축 및 의존성 동기화
uv sync

# 에이전트 프로세스 기동 (추천 단독 명령어 또는 모듈 실행)
uv run agent-worker
# 또는: uv run python -m src.main
```

### Step 3: Kotlin Agent Stream Server 실행
```bash
cd agent-stream-server

# Gradle 빌드 및 Spring Boot 실행
./gradlew bootRun
```

### Step 4: React 프론트엔드 웹 앱 실행
```bash
cd frontend

# 패키지 설치 및 개발 서버 구동
npm install
npm run dev
# 접속 URL: http://localhost:5173
```

---

## 📚 5. 프로젝트 관련 문서 (Documentation)

* 📄 [요구사항 정의서 (1.requirements.md)](docs/1.requirements.md)
* 📐 [시스템 아키텍처 및 시퀀스 플로우 (2.architecture.md)](docs/2.architecture.md)
* 🛠️ [REST/SSE API 명세 및 기술 규격 (3.spec.md)](docs/3.spec.md)
* 🚀 [구현 로드맵 (4.plan.md)](docs/4.plan.md)
* 🏛️ [ADR 0001: 다중 노드 세션 라우팅 (adr/0001-multi-node-session-routing.md)](docs/adr/0001-multi-node-session-routing.md)
* 🏛️ [ADR 0002: Coroutine Flow SSE 스트리밍 (adr/0002-kotlin-sse-coroutine-flow.md)](docs/adr/0002-kotlin-sse-coroutine-flow.md)
* 📊 [구현 태스크 트래킹 대시보드 (tasks/README.md)](docs/tasks/README.md)
