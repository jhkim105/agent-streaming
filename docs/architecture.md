# 📐 시스템 아키텍처 및 라우팅 명세 (System Architecture & Routing)

본 문서는 **대화형 AI 에이전트(ChatGPT형 UX)** 시스템의 구성 요소, 4대 식별자 모델, 분산 SSE 메시지 라우팅 흐름 및 데이터 플로우를 정의합니다.

---

## 🏗️ 1. 전체 시스템 토폴로지 (System Topology)

```text
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ ChatGPT Style Frontend (React SPA)                                      │
 │                                                                         │
 │ ┌───────────────────┐ ┌───────────────────────────────────────────────┐ │
 │ │ Left Sidebar      │ │ Right Main Chat Window                        │ │
 │ │ - [+ 새 채팅]     │ │ - User Message (오른쪽 말풍선)                │ │
 │ │ - 이전 대화 목록  │ │ - Agent Response (좌측 마크다운 토큰 스트림)  │ │
 │ │ - 세션 스레드 전환│ │ - Agent Thinking Accordion (사고과정)         │ │
 │ │                   │ │ - Inline A2UI Dashboard (동적 카드/버튼)      │ │
 │ └───────────────────┘ └───────────────────────────────────────────────┘ │
 └────────────────────────────────────┬────────────────────────────────────┘
                                      │ (1) POST /api/conversations (conversationId)
                                      │ (2) GET /api/conversations/{id}/events (connectionId)
                                      │ (3) POST /api/conversations/{id}/commands (commandId)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Agent Stream Server Cluster (Kotlin WebFlux + Coroutines)                       │
│  - SessionRegistry & RedisConnectionRegistry (다단계 라우팅)                     │
│  - ConversationHistoryStore (멀티턴 챗 타임라인 & A2UI 페이로드 영속성)          │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │ (4) Kafka agent-commands
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Python Agent Worker (LangGraph Multi-Turn Engine)                               │
│  - Query Processing -> LLM Streaming -> STATUS / CHUNK / A2UI_RENDER / DONE    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 2. 4대 핵심 식별자 모델 (Identity Model)

| 식별자 | 명칭 | 역할 및 범위 |
|---|---|---|
| **`conversationId`** | 대화 스레드 ID | 멀티턴 대화 전체 생명주기 (이력 복원 및 저장 기준) |
| **`commandId`** | AgentCommand ID | 1회 사용자 커맨드 식별 및 이벤트 Correlation 기준 |
| **`connectionId`** | SSE 연결 소켓 ID | 물리적 SSE 연결 식별 (특정 브라우저 소켓 타깃팅 기준) |
| **`eventId`** | AgentEvent ID | 개별 스트리밍 이벤트 식별 (W3C SSE `id` 및 복원 기준) |

---

## 🔄 3. 메시지 라우팅 및 시퀀스 다이어그램 (Sequence Flow)

```text
Client (React)          Server Node 1 (Host-1)     Redis Registry     Kafka Topic           Python Agent
  │                           │                       │                │                       │
  │── (1) POST /conversations ───────────────────────►│                │                       │
  │◄── 202 Created (conversationId="conv-123") ───────│                │                       │
  │                           │                       │                │                       │
  │── (2) GET /conversations/conv-123/events ────────►│                │                       │
  │    (SSE Connection Established)                   │                │                       │
  │◄── INIT (connectionId="conn-abc") ────────────────│                │                       │
  │                           │── registerConnectionHost("conn-abc", "Host-1") ───────────────►│
  │                           │                       │                │                       │
  │── (3) POST /conversations/conv-123/commands ────►│                │                       │
  │    {connectionId:"conn-abc", query:"AGUI 알려줘"} │                │                       │
  │◄── 202 Accepted (commandId="cmd-999") ────────────│                │                       │
  │                           │── registerCommandConnection("cmd-999", "conn-abc") ──────────►│
  │                           │── Produce AgentCommand ("cmd-999") ───►│                       │
  │                           │                       │                │── Consume Command ───►│
  │                           │                       │                │                       │ (LLM 추론)
  │                           │                       │                │◄── Produce AgentEvent │
  │                           │◄── Consume AgentEvent ("cmd-999") ─────│    (STATUS, CHUNK...) │
  │                           │── getConnectionByCommand("cmd-999") ──►│                       │
  │                           │◄── return "conn-abc" ──────────────────│                       │
  │                           │── getConnectionHost("conn-abc") ──────►│                       │
  │                           │◄── return "Host-1" (Local Match) ──────│                       │
  │◄── SSE Direct Delivery ───│                       │                │                       │
  │    (STATUS, CHUNK, A2UI)  │                       │                │                       │
```

---

## 🏛️ 4. 다중 노드 분산 릴레이 메커니즘 (Multi-Node Routing)

타깃 `connectionId`의 소켓이 본인 노드가 아닌 타 노드(`Host-2`)에 위치할 경우, `Redis Streams` (`stream:host:Host-2`)로 `XADD` 릴레이하며, `Host-2` 노드가 `XREAD`로 수신받아 해당 클라이언트 소켓으로 직통 배달합니다.
