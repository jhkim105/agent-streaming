# Real-time AI Researcher Agent - System Architecture & Sequence Flow

이 문서는 프로젝트의 전체 시스템 구조, 컴포넌트 간 통신 아키텍처, 데이터 플로우, 도메인 모델(`AgentCommand`, `AgentEvent`) 및 4대 식별자 체계(`conversationId`, `commandId`, `connectionId`, `eventId`), Redis 분산 라우팅 아키텍처 명세입니다.

관련 문서: [요구사항 정의서](requirements.md) | [기술 명세서](spec.md) | [ADR 0003 노드별 스트림 라우팅](adr/0003-redis-streams-bucketing-and-dynamic-session-routing.md)

---

## 1. 도메인 모델 및 4대 핵심 식별자

본 시스템은 **이벤트 구동형 아키텍처(Event-Driven Architecture)**와 **CQRS 패턴**을 기반으로 클라이언트의 요청 명령인 **`AgentCommand`**와 에이전트가 발행하는 스트리밍 이벤트인 **`AgentEvent`**를 명확히 분리하여 처리합니다.

```text
               AgentCommand (CommandId)
Client (UI) ─────────────────────────────► Kotlin Stream Server
                                                 │
                                                 ▼ Kafka 'agent-commands'
                                            Python Worker
                                                 │
                                                 ▼ Kafka 'agent-events'
Client (UI) ◄───────────────────────────── Kotlin Stream Server
                 AgentEvent (EventId)
```

### 🔑 4대 핵심 식별자 모델 (Identity Model)

| 식별자 (ID) | 도메인 용도 | 발급 시점 | 범위 & 역할 |
|---|---|---|---|
| **`conversationId`** | 대화 스레드 전체 식별자 | 새 대화 생성 시 (`POST /api/conversations`) | 대화 생명주기 전체 (이력 조회 및 복원 기준) |
| **`commandId`** | 클라이언트 명령(`AgentCommand`) 1건 식별자 | 명령 제출 시 (`POST /api/conversations/{id}/commands`) | 커맨드 1건과 발생 이벤트 간 Correlation 기준 |
| **`connectionId`** | 물리적 SSE 소켓 연결 식별자 | SSE 연결 수립 시 (`GET /api/conversations/{id}/events`) | 연결 생명주기 (명령 전송 디바이스 타깃팅 기준) |
| **`eventId`** | 에이전트 이벤트(`AgentEvent`) 1건 식별자 | Agent 이벤트 발행 시 (Python Worker 생성) | W3C `Last-Event-ID` 및 이벤트 순서 보장 기준 |

#### 계층 및 포함 관계
```text
Conversation C100
│
├── Connection CONN-A (PC 브라우저 탭 1)
│     ├── AgentCommand CMD-001 ("삼성전자 분석해줘")
│     │     ├── AgentEvent EVT-001 (AGENT_STARTED)
│     │     ├── AgentEvent EVT-002 (STATUS)
│     │     ├── AgentEvent EVT-003 (TOKEN)
│     │     └── AgentEvent EVT-004 (DONE)
│     │
│     └── AgentCommand CMD-003 ("추가 실적 알려줘")
│           └── AgentEvent EVT-008...
│
└── Connection CONN-B (모바일 앱 또는 탭 2)
      └── AgentCommand CMD-002 ("SK하이닉스 분석해줘")
            └── AgentEvent EVT-005...
```

---

## 2. 분산 연결 라우팅 모델 (Routing Model)

다중 서버 인스턴스(Scale-out) 환경에서 L4/L7 로드밸런서에 의해 HTTP 명령 요청과 SSE 연결이 서로 다른 서버 노드로 유입될 수 있습니다.

```text
commandId ➔ connectionId ➔ hostId ➔ Node-based Redis Stream ➔ Local SSE Connection
```

### Redis 라우팅 키 구조
1. **`connection:host:{connectionId}` (Key-Value)**: `kotlin-node-1` (해당 SSE 소켓 연결이 존재하는 서버 노드 ID)
2. **`command:connection:{commandId}` (Key-Value)**: `CONN-A` (해당 명령을 보낸 SSE connectionId)
3. **`stream:host:{hostId}` (Redis Streams)**: 서버 노드별 전용 무유실 릴레이 스트림

---

## 3. 시스템 아키텍처 토폴로지 (System Topology)

### Mermaid 토폴로지 다이어그램

```mermaid
flowchart TB
    subgraph ClientLayer ["🌐 Client Layer (Web / Mobile)"]
        ClientA["Client PC (Conn: CONN-A)"]
        ClientB["Client Mobile (Conn: CONN-B)"]
    end

    subgraph ServerCluster ["🚀 Kotlin Agent Stream Server Cluster"]
        Node1["Stream Server Node 1<br/>(Host ID: kotlin-node-1)<br/>- Local SSE Map (CONN-A)<br/>- Connection Manager"]
        Node2["Stream Server Node 2<br/>(Host ID: kotlin-node-2)<br/>- Receives AgentCommand (CMD-001)<br/>- History Store"]
    end

    subgraph DataBrokerLayer ["⚡ Broker & Distributed Memory Layer"]
        RedisConn[("Redis Connection Registry<br/>connection:host:CONN-A = node-1<br/>command:connection:CMD-001 = CONN-A")]
        RedisStream[("Redis Streams<br/>stream:host:kotlin-node-1")]
        KafkaReq[("Kafka Broker<br/>Topic: agent-commands")]
        KafkaResp[("Kafka Broker<br/>Topic: agent-events")]
    end

    subgraph AgentWorkerLayer ["🧠 AI Agent Worker Layer"]
        PyWorker["Python Agent Worker<br/>(LangGraph Engine)<br/>- DuckDuckGo / Scraper<br/>- Ollama LLM"]
    end

    %% Connection Flows
    ClientA -- "(1) GET /conversations/C100/events<br/>(SSE 연결)" --> Node1
    Node1 -- "(2) SET connection:host:CONN-A 'node-1'" --> RedisConn

    ClientA -- "(3) POST /conversations/C100/commands<br/>(AgentCommand 제출, connectionId=CONN-A)" --> Node2
    Node2 -- "(4) SET command:connection:CMD-001 'CONN-A'" --> RedisConn
    Node2 -- "(5) Produce AgentCommand (CMD-001, C100)" --> KafkaReq

    KafkaReq -- "(6) Consume AgentCommand" --> PyWorker
    PyWorker -- "(7) Produce AgentEvent (CMD-001, EVT-001, TOKEN)" --> KafkaResp

    KafkaResp -- "(8) Consume AgentEvent (CMD-001)" --> Node2
    Node2 -. "(9) Lookup connectionId for CMD-001 (CONN-A)<br/>Lookup hostId for CONN-A ('node-1')" .-> RedisConn

    Node2 -- "(10) XADD stream:host:node-1<br/>(Target: CONN-A, Event: EVT-001)" --> RedisStream
    RedisStream -- "(11) XREAD stream:host:node-1" --> Node1
    Node1 -- "(12) SSE AgentEvent Delivery to CONN-A<br/>(Client A로만 배달)" --> ClientA

    %% Styling
    style ClientA fill:#2b2d42,stroke:#8d99ae,color:#fff
    style ClientB fill:#2b2d42,stroke:#8d99ae,color:#fff
    style Node1 fill:#1d3557,stroke:#457b9d,color:#fff
    style Node2 fill:#1d3557,stroke:#457b9d,color:#fff
    style RedisConn fill:#d90429,stroke:#ef233c,color:#fff
    style RedisStream fill:#d90429,stroke:#ef233c,color:#fff
    style KafkaReq fill:#e07a5f,stroke:#f4a261,color:#fff
    style KafkaResp fill:#e07a5f,stroke:#f4a261,color:#fff
    style PyWorker fill:#2a9d8f,stroke:#e9c46a,color:#fff
```

---

## 4. 대표 시퀀스 플로우 (Sequence Diagram)

### 4.1. 대화 생성, SSE 연결 수립 및 AgentCommand 처리 플로우

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client (PC Browser)
    participant Server1 as Stream Server (node-1)
    participant Server2 as Stream Server (node-2)
    participant Redis as Redis Registry
    participant Kafka as Kafka Broker
    participant Agent as Python Worker

    %% 1. Conversation Creation
    Client->>Server1: POST /api/conversations
    Server1-->>Client: 201 Created { conversationId: "C100" }

    %% 2. SSE Connection
    Client->>Server1: GET /api/conversations/C100/events
    Note over Server1: connectionId = CONN-A 생성
    Server1->>Redis: SET connection:host:CONN-A "node-1" EX 3600
    Server1-->>Client: event: INIT { conversationId: "C100", connectionId: "CONN-A" }

    %% 3. AgentCommand Submission
    Client->>Server2: POST /api/conversations/C100/commands<br/>{ connectionId: "CONN-A", type: "RESEARCH", payload: { query: "삼성전자 분석해줘" } }
    Note over Server2: commandId = CMD-001 생성
    Server2->>Redis: SET command:connection:CMD-001 "CONN-A" EX 3600
    Server2-->>Client: 202 Accepted { conversationId: "C100", commandId: "CMD-001" }
    Server2->>Kafka: Produce AgentCommand to 'agent-commands' { conversationId: "C100", commandId: "CMD-001", ... }

    %% 4. Agent Event Processing & Streaming
    Kafka->>Agent: Consume AgentCommand
    loop Streaming AgentEvents (EVT-001, EVT-002...)
        Agent->>Kafka: Produce AgentEvent to 'agent-events' { commandId: "CMD-001", eventId: "EVT-001", type: "TOKEN", content: "삼성전자는..." }
        Kafka->>Server2: Consume AgentEvent (CMD-001)
        Server2->>Redis: GET command:connection:CMD-001 ➔ CONN-A<br/>GET connection:host:CONN-A ➔ "node-1"
        Server2->>Server1: Redis Streams XADD stream:host:node-1 (Target: CONN-A)
        Server1-->>Client: SSE AgentEvent (CONN-A 타깃 배달)
    end
```

### 4.2. 새로고침 및 `Last-Event-ID` 복원 플로우

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client (Browser)
    participant Server as Stream Server (node-1)
    participant RedisStream as Redis Streams
    participant History as Conversation History Store

    Note over Client: 사용자가 새로고침 발생!
    Client->>Server: GET /api/conversations/C100/events<br/>(Header: Last-Event-ID: EVT-005)
    Note over Server: 신규 connectionId = CONN-B 생성 및 Redis 업데이트
    Server->>RedisStream: XREAD stream:host:node-1 (Last-Event-ID: EVT-005 이후 미수신 이벤트 조회)
    RedisStream-->>Server: 미수신 AgentEvent 목록 반환
    Server-->>Client: event: TOKEN (EVT-006, EVT-007... 끊긴 시점부터 복원 스트리밍)

    %% 이전 대화 이력 복원
    Client->>Server: GET /api/conversations/C100
    Server->>History: conversationId C100 이력 조회
    History-->>Server: 대화 요약, 커맨드 목록, 완성된 마크다운 보고서 반환
    Server-->>Client: 200 OK (화면 완전 복원)
```

---

## 5. 핵심 REST API 사양 요약

- **`POST /api/conversations`**: 새 대화 스레드 생성 (`conversationId` 발급)
- **`GET /api/conversations/{conversationId}/events`**: 특정 대화의 SSE 스트림 연결 수립 (`connectionId` 발급 및 소켓 등록)
- **`POST /api/conversations/{conversationId}/commands`**: 해당 대화에 `AgentCommand` 제출 (`commandId` 발급, `connectionId` 매핑)
- **`GET /api/conversations`**: 이전 대화 목록 요약 조회
- **`GET /api/conversations/{conversationId}`**: 특정 대화의 전체 이력 및 완결 리포트 상세 조회
