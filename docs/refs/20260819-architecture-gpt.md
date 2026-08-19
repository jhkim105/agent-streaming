# Stock Agent Architecture

## 1. 목적

주식 에이전트 서비스의 대화, 메시지 요청, Agent Runtime 실행, Kafka, SSE streaming 및 연결 라우팅 구조를 정의한다.

구성 요소:

- Client (Web / Mobile)
- Kotlin API Server
- Kafka
- Agent Runtime
- SSE Connection Manager
- Conversation / Message 저장소
- 필요 시 Redis (분산 SSE routing)

핵심 원칙:

- `conversationId`는 대화를 생성할 때 발급한다.
- `requestId`는 사용자가 메시지를 보낼 때마다 새로 발급한다.
- `connectionId`는 실제 SSE 연결마다 발급한다.
- `eventId`는 Agent가 생성하는 streaming event를 식별한다.
- HTTP 메시지 요청과 SSE 연결은 분리한다.

---

## 2. 핵심 ID 모델

| ID | 의미 | 발급 시점 | 범위 |
|---|---|---|---|
| `conversationId` | 대화 자체 | 새 대화 생성 시 1회 | 대화 전체 |
| `requestId` | 사용자 메시지/Agent 실행 1건 | 메시지 요청마다 | 요청 1건 |
| `connectionId` | SSE 연결 1개 | SSE 연결 시마다 | 연결 1개 |
| `eventId` | Agent streaming event 1개 | Agent event 발생 시 | request 1건 내 |

관계:

```text
Conversation C100
│
├── Request R001
│     ├── Event E001
│     ├── Event E002
│     └── Event E003
│
├── Request R002
│     ├── Event E004
│     └── Event E005
│
└── Request R003
      └── Event E006


Conversation C100
│
├── Connection CONN-A → PC
└── Connection CONN-B → Mobile
```

중요한 점:

- 하나의 conversation에는 여러 request가 존재한다.
- 하나의 conversation에는 여러 SSE connection이 존재할 수 있다.
- 하나의 request는 특정 connection에서 발생한다.
- 하나의 request에서는 여러 event가 발생한다.

---

# 3. 전체 아키텍처

```text
                              Client
                         Web / Mobile
                              │
              ┌───────────────┴────────────────┐
              │                                │
              │  Conversation API              │
              │  Message API                   │
              │                                │
              ▼                                ▼
     POST /v1/conversations          GET /v1/conversations/
             │                       {conversationId}/events
             │                                │
             ▼                                ▼
     ┌────────────────────────────────────────────────────┐
     │                Kotlin API Server                   │
     │                                                    │
     │  Conversation API                                  │
     │  Message API                                       │
     │  SSE Connection Manager                            │
     │  Kafka Producer                                    │
     │  Kafka Consumer                                    │
     └──────────────┬───────────────────┬─────────────────┘
                    │                   │
                    │ Kafka Request     │ Kafka Response
                    ▼                   ▲
                 ┌────────────────────────┐
                 │         Kafka          │
                 └────────────┬───────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │   Agent Runtime   │
                    │                   │
                    │ Agent / LLM /     │
                    │ Tool / State      │
                    └───────────────────┘
```

---

# 4. Conversation lifecycle

`conversationId`는 최초 메시지 요청 때 암묵적으로 생성하기보다 별도의 Conversation 생성 API에서 명시적으로 발급하는 방식을 권장한다.

## 4.1 새 대화 생성

```http
POST /v1/conversations
```

Kotlin Server:

```text
Conversation 생성
      │
      └─ conversationId = C100 발급
```

응답:

```json
{
  "conversationId": "C100"
}
```

이후 Client는 C100을 사용한다.

---

# 5. 새 대화의 최초 메시지 흐름

Conversation을 만든 다음 SSE 연결을 먼저 생성하고 메시지를 전송한다.

```text
Client
  │
  │ 1. POST /v1/conversations
  ▼
Kotlin Server
  │
  └─ conversationId = C100
  │
  ▼
Client
  │
  │ 2. GET /v1/conversations/C100/events
  ▼
Kotlin Server
  │
  └─ connectionId = CONN-A
  │
  ▼
SSE Connection 유지
  │
  │
  │ 3. POST /v1/conversations/C100/messages
  ▼
Kotlin Server
  │
  ├─ requestId = R001 생성
  │
  └─ Kafka Request
          │
          ▼
     Agent Runtime
          │
          ▼
        Kafka
          │
          ▼
     Kotlin Server
          │
          ▼
      CONN-A SSE
          │
          ▼
        Client
```

SSE를 메시지 요청보다 먼저 연결하는 이유는 최초 Agent response가 발생하기 전에 수신 채널을 확보하기 위해서다.

---

# 6. 기존 대화에서 메시지 요청

기존 conversation에서는 새로운 conversationId를 만들지 않는다.

```text
conversationId = C100
```

메시지마다 새로운 requestId를 만든다.

```text
Client
  │
  │ POST /v1/conversations/C100/messages
  ▼
Kotlin Server
  │
  ├─ requestId = R002
  │
  └─ Kafka
       │
       ▼
  Agent Runtime
       │
       ▼
     Kafka
       │
       ▼
  Kotlin Server
       │
       ▼
  SSE Connection
       │
       ▼
     Client
```

예:

```text
C100
├── R001 "삼성전자 분석해줘"
├── R002 "SK하이닉스도 분석해줘"
└── R003 "둘 중 어느 종목이 더 좋아?"
```

---

# 7. Message API

권장 API:

```http
POST /v1/conversations/{conversationId}/messages
```

Request:

```json
{
  "content": "삼성전자 최근 실적 분석해줘"
}
```

Kotlin Server:

```text
1. conversationId 검증
2. requestId 생성
3. connectionId 확인
4. Message 저장
5. Kafka Request 발행
6. requestId 반환
```

응답 예:

```http
202 Accepted
```

```json
{
  "conversationId": "C100",
  "requestId": "R001"
}
```

HTTP 요청은 Agent 완료를 기다리지 않는다.

---

# 8. SSE API

권장 API:

```http
GET /v1/conversations/{conversationId}/events
```

연결 시:

```text
conversationId = C100
connectionId = CONN-A
```

Connection Manager:

```text
CONN-A → SSE Connection
```

그리고 요청과 연결을 매핑한다.

```text
requestId → connectionId

R001 → CONN-A
R002 → CONN-A
```

---

# 9. 다중 디바이스 / 다중 탭

동일 conversation을 여러 디바이스에서 열 수 있다.

```text
Conversation C100
│
├── CONN-A → PC
└── CONN-B → Mobile
```

PC에서 메시지를 요청:

```text
requestId = R001
connectionId = CONN-A
```

Agent response:

```text
R001
 ↓
CONN-A
 ↓
PC
```

Mobile에는 전송하지 않는다.

따라서 단순히:

```text
conversationId → SSE
```

로 관리하지 않고:

```text
requestId → connectionId → SSE Connection
```

으로 라우팅한다.

---

# 10. Connection lifecycle

SSE 연결은 conversation lifecycle과 다르다.

예:

```text
C100
 │
 ├── CONN-A  ← PC에서 연결
 │
 │   연결 종료
 │
 └── CONN-C  ← PC 재연결
```

Conversation은 계속 C100이지만 실제 SSE connection은 변경될 수 있다.

따라서 `conversationId`와 `connectionId`를 분리한다.

---

# 11. Request와 Connection 매핑

메시지 요청이 들어왔을 때 현재 SSE connection을 식별한다.

예:

```text
Client
  │
  ├─ connectionId = CONN-A
  │
  └─ POST message
         │
         ▼
      requestId = R001
```

Kotlin Server는:

```text
R001 → CONN-A
```

매핑을 저장한다.

Agent response가 돌아오면:

```text
Kafka Response
      │
      ▼
requestId = R001
      │
      ▼
R001 → CONN-A
      │
      ▼
SSE Connection
      │
      ▼
Client
```

---

# 12. Kafka Request

예:

```json
{
  "conversationId": "C100",
  "requestId": "R001",
  "messageId": "M001",
  "type": "USER_MESSAGE",
  "content": "삼성전자 최근 실적 분석해줘"
}
```

`requestId`는 Kafka request/response correlation에 사용한다.

Kafka key는 conversation 단위 순서가 중요하다면 `conversationId`를 고려한다.

```text
Kafka key = conversationId
```

동일 conversation의 메시지가 동일 partition에 배치되도록 하여 conversation 단위 순서 보장을 쉽게 할 수 있다.

단, Agent Runtime의 stateful/stateless 구조에 따라 partition 전략은 별도 검토한다.

---

# 13. Kafka Response

Agent Runtime은 streaming event마다 correlation 정보를 포함한다.

```json
{
  "conversationId": "C100",
  "requestId": "R001",
  "eventId": "E001",
  "type": "TOKEN",
  "content": "삼성전자의"
}
```

다음 event:

```json
{
  "conversationId": "C100",
  "requestId": "R001",
  "eventId": "E002",
  "type": "TOKEN",
  "content": "최근 실적은"
}
```

완료:

```json
{
  "conversationId": "C100",
  "requestId": "R001",
  "eventId": "E010",
  "type": "DONE"
}
```

---

# 14. Agent Event 모델

`eventId`는 하나의 request에서 발생하는 이벤트를 식별한다.

예:

```text
R001
│
├── E001 AGENT_STARTED
├── E002 TOOL_CALL
├── E003 TOOL_RESULT
├── E004 TOKEN
├── E005 TOKEN
├── E006 TOKEN
└── E007 DONE
```

Event type 예:

```text
AGENT_STARTED
MESSAGE_STARTED
TOKEN
TOOL_CALL
TOOL_RESULT
THINKING_STATUS
ERROR
DONE
```

실제 event type은 Agent Runtime의 구현에 맞춰 정의한다.

---

# 15. SSE Event

SSE에는 `eventId`를 SSE `id`로 사용하는 것을 고려한다.

```text
id: E004
event: token
data: {"requestId":"R001","content":"삼성전자는"}

id: E005
event: token
data: {"requestId":"R001","content":"최근"}

id: E006
event: token
data: {"requestId":"R001","content":"실적에서"}
```

클라이언트가 E006까지 받은 뒤 연결이 끊어지면:

```text
Last-Event-ID: E006
```

을 사용하여 이후 event부터 재전송하는 구조를 고려할 수 있다.

Replay를 지원하려면 서버 또는 별도 event store에 event를 일정 기간 보관해야 한다.

---

# 16. Kotlin Server 내부 구성

권장 논리적 구성:

```text
Kotlin API Server
│
├── ConversationController
│     └── POST /conversations
│
├── MessageController
│     └── POST /conversations/{id}/messages
│
├── SseController
│     └── GET /conversations/{id}/events
│
├── ConversationService
│
├── MessageService
│
├── SseConnectionManager
│
├── KafkaProducer
│
└── KafkaConsumer
```

SSE Connection Manager의 역할:

```text
connectionId
      ↓
SSE Connection

requestId
      ↓
connectionId
```

---

# 17. 서버 Scale-out

Kotlin Server가 여러 인스턴스로 구성되면:

```text
                    Load Balancer
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
           Server-1   Server-2   Server-3
                         │
                    SSE Connection
```

Kafka response를 받은 서버와 SSE connection이 존재하는 서버가 다를 수 있다.

예:

```text
R001
 ↓
Server-1 Kafka Consumer
 ↓
SSE connection은 Server-2
```

따라서 분산 환경에서는:

```text
requestId
    ↓
serverInstanceId
    ↓
connectionId
    ↓
SSE Connection
```

routing이 필요하다.

Redis 등을 이용하여 routing 정보를 공유하는 방식을 고려한다.

예:

```text
Redis

R001 → Server-2
CONN-A → Server-2
```

Server-1이 R001을 수신하면 Server-2로 내부 전달하고 Server-2가 SSE로 전송한다.

Redis Pub/Sub, Redis Streams 또는 별도의 internal event routing 방식은 신뢰성 요구사항에 따라 선택한다.

---

# 18. 권장 초기 구현

MVP에서는 다음 정도로 시작한다.

```text
conversationId
requestId
connectionId
eventId
```

단일 Kotlin Server 기준:

```text
SseConnectionManager

connectionId → SSE Connection

requestId → connectionId
```

예:

```text
CONN-A → SSE Connection A
CONN-B → SSE Connection B

R001 → CONN-A
R002 → CONN-B
```

Kafka response:

```text
R001
 ↓
CONN-A
 ↓
SSE
```

---

# 19. 전체 대표 시나리오

## 19.1 새 대화

```text
1. Client
   POST /v1/conversations

2. Kotlin
   conversationId = C100 생성

3. Client
   GET /v1/conversations/C100/events

4. Kotlin
   connectionId = CONN-A 생성

5. Client
   POST /v1/conversations/C100/messages

6. Kotlin
   requestId = R001 생성

7. Kotlin
   R001 → CONN-A 매핑

8. Kotlin
   Kafka Request 발행

9. Agent Runtime
   Agent 실행

10. Agent Runtime
    Kafka Response E001, E002, ... 발행

11. Kotlin
    requestId R001로 CONN-A 조회

12. Kotlin
    SSE streaming

13. Client
    DONE 수신
```

## 19.2 기존 대화

```text
1. Client
   POST /v1/conversations/C100/messages

2. Kotlin
   requestId = R002 생성

3. Kotlin
   R002 → 현재 connectionId 매핑

4. Kafka Request

5. Agent Runtime 실행

6. Kafka Response

7. requestId R002 → connectionId 조회

8. SSE streaming
```

---

# 20. 최종 구조

```text
                         Client
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             │             │             │
       Create Conversation  SSE       Send Message
             │             │             │
             ▼             ▼             ▼
        conversationId   connectionId   requestId
             │             │             │
             └─────────────┼─────────────┘
                           │
                           ▼
                    Kotlin API Server
                           │
                    requestId mapping
                           │
                           ▼
                         Kafka
                           │
                           ▼
                    Agent Runtime
                           │
                           ▼
                         Kafka
                           │
                           ▼
                    Kotlin Consumer
                           │
                     requestId
                           │
                     connectionId
                           │
                           ▼
                         SSE
                           │
                           ▼
                        Client
```

## 21. 핵심 설계 원칙 요약

```text
conversationId
= 대화를 식별한다.

requestId
= 하나의 사용자 메시지/Agent 실행을 식별한다.

connectionId
= 하나의 실제 SSE 연결을 식별한다.

eventId
= 하나의 Agent streaming event를 식별한다.
```

그리고 전체 routing은:

```text
requestId
    ↓
connectionId
    ↓
SSE Connection
    ↓
Client
```

이다.

Conversation은 논리적인 개념이고 SSE connection은 물리적인 연결이므로 lifecycle을 분리한다.

새 대화에서는:

```text
Conversation 생성
    ↓
conversationId 발급
    ↓
SSE 연결
    ↓
메시지 요청
    ↓
requestId 발급
    ↓
Agent 실행
    ↓
SSE streaming
```

기존 대화에서는:

```text
기존 conversationId
    ↓
새 메시지
    ↓
새 requestId
    ↓
Agent 실행
    ↓
SSE streaming
```

을 따른다.
