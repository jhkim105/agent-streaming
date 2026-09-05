# 🔬 GET SSE (CQRS 분리형) vs POST SSE (단일 요청형) 아키텍처 비교 분석

> **문서 번호**: RESEARCH-0003  
> **작성일**: 2026-08-29  
> **상태**: 승인됨 (Approved)  
> **주제**: AI 에이전트 스트리밍 환경에서 GET SSE(CQRS 분리 롱 세션) 방식과 POST SSE(단일 요청 동기 스트림) 방식의 아키텍처 관점 비교

---

## 📌 1. 개요 및 비교 배경

생성형 AI 및 AI 에이전트 인터페이스에서 실시간 텍스트 토큰 및 상태를 스트리밍하기 위해 Server-Sent Events (SSE)를 주로 활용합니다. 이때 SSE를 구현하는 통신 아키텍처는 크게 두 가지 접근 방식으로 나뉩니다:

1. **GET SSE + CQRS 분리형 (현재 프로젝트 적용 방식)**:
   - 클라이언트는 대화방 입장 시 `GET`으로 단방향 롱 세션 SSE 채널을 수립하고, 사용자 명령(질문, 액션)은 별도 `POST` API로 제출하는 비동기 이벤트 구동 모델.
2. **POST SSE 단일 파이프 방식 (전통적 LLM 챗 방식 / ChatGPT API 기본 모델)**:
   - 클라이언트의 `POST` 요청에 대해 서버가 즉시 응답을 닫지 않고 `Content-Type: text/event-stream`의 응답 바디(Response Body)로 토큰을 흘려보낸 뒤 연결을 종료하는 모델.

본 문서는 두 방식의 시스템 토폴로지, 시퀀스, 장단점 및 기술 선택 기준을 분석하여 정리합니다.

---

## 🏗️ 2. 아키텍처 및 시퀀스 비교

### 1) GET SSE + CQRS 분리형 (현재 프로젝트 구조)

> **"채널 수립(GET)과 명령 전송(POST)을 분리하여 비동기 분산 파이프라인을 수용하는 구조"**

```text
[Client (EventSource / React)]        [Gateway (Spring WebFlux)]              [Broker / Agent Runtime]
        │                                         │                                      │
        │─── (1) GET /conversations/{id}/events ─►│ (connectionId 발급 & 소켓 등록)       │
        │◄── (2) INIT Event (conn-abc) ───────────│                                      │
        │                                         │                                      │
        │─── (3) POST /commands {conn-abc, query}►│─── (4) Kafka Produce (AgentCommand) ─►│
        │◄── (5) 202 Accepted {commandId} ────────│                                      │ (비동기 다단계 추론)
        │                                         │                                      │ (STATUS, CHUNK, A2UI)
        │                                         │◄── (6) Kafka Consume (AgentEvent) ───│
        │◄── (7) SSE Direct Push (STATUS, CHUNK) ─│ (Redis Registry / Local Session)     │
        │◄── (8) SSE DONE Event ──────────────────│                                      │
```

* **특징**:
  * 클라이언트와 게이트웨이 간 물리적 SSE 연결(`connectionId`)이 대화 세션 내내 상시 유지(Long-lived)됩니다.
  * 요청 제출과 응답 수신이 완전히 비동기(Asynchronous)로 분리되어 있습니다.

---

### 2) POST SSE 단일 파이프 방식 (전통적 단순 챗 구조)

> **"질문 전송(Request)과 스트리밍 응답(Response)을 1:1 단일 HTTP 트랜잭션으로 묶는 구조"**

```text
[Client (fetch + ReadableStream)]        [API Gateway / Server]                      [LLM Engine]
        │                                         │                                      │
        │─── (1) POST /api/chat {query} ─────────►│                                      │
        │        (Header: Accept: text/event-stream)                                     │
        │                                         │─── (2) LLM Stream Request ──────────►│
        │                                         │◄── (3) Token Stream ─────────────────│
        │◄── (4) POST Response Stream (Chunk) ────│                                      │
        │◄── (5) POST Response Close (DONE) ──────│ (HTTP Connection Close)              │
```

* **특징**:
  * 한 번의 질문마다 별도의 HTTP 커넥션이 열리고, 응답 생성이 완료되면 커넥션이 즉시 종료(Short-lived)됩니다.
  * 웹 표준 `EventSource` API가 POST 메서드를 지원하지 않으므로, 브라우저의 `fetch` + `ReadableStream` API로 커스텀 파싱해야 합니다.

---

## ⚖️ 3. 상세 항목별 비교 분석

| 비교 항목 | GET SSE (현재 구조: CQRS 분리형) | POST SSE (단일 요청 스트리밍) |
| :--- | :--- | :--- |
| **HTTP 표준 준수** | **W3C 표준 준수** (브라우저 기본 `EventSource` API 사용 가능) | **비표준 SSE** (`EventSource` POST 미지원 ➔ `fetch` + `ReadableStream` 필수) |
| **커넥션 수명 주기** | **Long-lived (롱 세션)**: 대화방 진입 시 1회 수립 및 상시 유지 | **Short-lived (단기 세션)**: 질문 시 연결 ➔ 응답 종료 시 소멸 |
| **관심사 분리 (CQRS)** | **완전 분리**: 커맨드(POST)와 이벤트 스트림(GET)이 독립적으로 동작 | **결합**: 쓰기 요청의 반환 스트림으로 읽기 채널이 종속됨 |
| **비동기 분산 확장성** | **최적**: Kafka, Redis 등 비동기 메시지 브로커와 결합 용이 | **제약**: HTTP 연결을 잡고 있는 API 서버가 LLM 응답 완료까지 스레드/소켓을 점유해야 함 |
| **서버 주도형 푸시** | **지원**: 클라이언트 요청 없이도 서버/에이전트가 상태나 알림 푸시 가능 | **불가**: 클라이언트의 POST 요청이 열려 있는 동안에만 데이터 전달 가능 |
| **페이로드 전달 크기** | **대용량 지원**: 질문/첨부파일은 별도 POST 본문으로 전송 | **대용량 지원**: POST 본문으로 요청 데이터 전달 |
| **인프라 복잡도** | **높음**: `connectionId` 매핑, Redis 라우팅 레지스트리, 분산 세션 관리 필요 | **낮음**: 단순 API 게이트웨이 프록시 수준으로 구현 가능 |
| **네트워크 배압 (Backpressure)** | 코루틴 `Channel.send()` 및 리액티브 Flow 기반으로 완전 제어 가능 | TCP/HTTP 계층의 기본 Flow Control에 의존 |

---

## 🎯 4. 아키텍처 선택 가이드 및 결론

### 💡 POST SSE 방식을 선택해야 하는 경우
1. **단순 LLM 1:1 질의응답 시스템**:
   - 도구 호출, 외부 웹 검색, 다단계 비동기 워크플로우 없이 "질문 ➔ LLM 즉시 답변 ➔ 완료"로 끝나는 단순 챗봇.
2. **인프라 단순화가 최우선인 환경**:
   - Kafka, Redis Streams 같은 메시지 브로커나 분산 세션 레지스트리를 운영하기 부담스러운 소규모 서비스.

### 🚀 GET SSE + CQRS (현재 프로젝트 구조)를 선택해야 하는 경우
1. **다단계 AI 에이전트 (LangGraph, Multi-Agent Runtime)**:
   - 의도 분석, 웹 검색, 스크래핑, 툴 실행 등 수십 초 이상 걸리는 백엔드 비동기 작업 과정을 `STATUS`, `LOG`, `A2UI` 등의 이벤트로 안정적으로 실시간 전송해야 할 때.
2. **분산 스케일아웃 환경 (Multi-Node Gateway)**:
   - 요청을 수신한 게이트웨이 노드와 실제 에이전트 작업을 수행하는 런타임이 분리되어 있어도, Redis 라우팅 체계(`commandId` ➔ `connectionId` ➔ `hostId`)를 통해 특정 클라이언트 브라우저로 100% 무유실 배달이 필요할 때.
3. **인터랙티브 UI (A2UI / Human-in-the-Loop)**:
   - 에이전트가 스트리밍 도중 사용자에게 선택지나 확인 요청(Action Card)을 보내고, 사용자의 추가 액션(`POST /commands`)에 따라 기존 스트림 맥락을 이어가야 할 때.

---

### 📌 종합 결론
본 프로젝트(`agent-streaming`)에 구축된 **GET SSE (CQRS 분리형) 아키텍처**는 단순 텍스트 프록시를 넘어 **비동기 이벤트 구동(Kafka/Redis), 분산 노드 라우팅, 멀티스텝 에이전트(Agent Runtime) 및 인터랙티브 UI(A2UI)**를 완벽하게 수용하기 위한 **엔터프라이즈급 AI 에이전트 아키텍처의 표준 설계**입니다.
