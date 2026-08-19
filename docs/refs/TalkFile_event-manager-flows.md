# Event Manager — Flow 다이어그램

구현(v4.1) 기준 흐름도. GitLab/IDE에서 Mermaid로 렌더된다.
계약 상세는 [`event-manager-api-spec.md`](event-manager-api-spec.md) 참조.

---

## 1. 아키텍처 — 컴포넌트와 두 쌍(pair)

```mermaid
flowchart LR
  C["client (SSE)"]
  GW["API Gateway<br/>신원 헤더 주입"]
  subgraph EM["event-manager"]
    direction TB
    H8080["client HTTP :8080<br/>events / topics SSE"]
    H8081["internal HTTP :8081<br/>messages / done / topics push"]
    G9090["gRPC :9090<br/>고빈도 stream"]
  end
  BACK["agent-executor"]
  CHAT["conversation manager"]
  RS[("Redis<br/>Streams")]
  RP[("Redis<br/>Pub/Sub")]

  C -->|"POST /v1/events, GET .../stream, /cancel"| GW --> H8080
  C -->|"GET /v1/topics/:id/stream"| GW

  EM -->|"trigger, cancel 전달"| BACK
  BACK -->|"응답 push (REST 단건)"| H8081
  BACK -->|"응답 push (고빈도)"| G9090

  H8080 <-->|"XADD / XREAD"| RS
  H8081 -->|"XADD"| RS
  G9090 -->|"XADD"| RS
  H8080 <-->|"SUBSCRIBE"| RP
  H8081 -->|"PUBLISH"| RP

  EM -->|"발화·응답 저장 (REST)"| CHAT
```

- **events** 쌍: Redis **Stream**(무유실). client `/v1/events`·`.../stream`·`/cancel` ↔ EM이 agent-executor `POST /v1/execute` 호출 후 **응답 SSE 소비**(대안: internal push `/internal/events/*`·gRPC).
- **topics** 쌍: Redis **Pub/Sub**(best-effort). client `/v1/topics/:id/stream` ↔ agent-executor `/internal/topics/:id/messages`.

---

## 2. events — 정상 흐름 (발화 → 응답 → 저장)

두 파이프가 독립적인 것이 핵심: **XADD(파이프 A)** 와 **XREAD 중계(파이프 B)** 는 서로를 기다리지 않는다.

```mermaid
sequenceDiagram
  autonumber
  participant C as client
  participant EM as event-manager
  participant BK as agent-executor
  participant R as Redis Stream
  participant CH as conversation manager

  C->>EM: POST /v1/events (user-id, dialog_id, message)
  EM-->>CH: addMessage(user 발화) [best-effort, 비동기]
  EM->>R: XADD start  (파이프 A, +PEXPIRE 60m 원자적)
  EM->>R: XREAD 구독 (파이프 B, mux)
  EM-->>C: SSE start (id=엔트리ID)
  EM->>BK: POST /v1/execute (user-id, traceparent, 본문 전달)

  loop 응답 SSE 소비 (start·block.start → delta* → block.done)
    BK-->>EM: SSE event (block.*, ping은 저장 skip)
    EM->>R: XADD (파이프 A)
    R-->>EM: XREAD 전달 (mux)
    EM-->>C: SSE event (id=엔트리ID)
  end

  BK-->>EM: SSE done (또는 error/cancled)
  Note over EM,R: done 처리 (client 연결과 무관하게 완주)
  EM->>R: XADD done
  EM->>R: XRANGE 전체 → 재조립
  EM->>R: SET saved:dialog:block NX
  EM-->>CH: addMessage(assistant, 재조립본) [NX 성공 시 1회]
  EM->>R: EXPIRE 5s (완료 후 축소)
  EM-->>C: SSE done → 연결 종료
```

---

## 3. client 연결 종료 → 재연결 (무유실)

client가 끊겨도 **XADD·done·저장은 계속**된다. client는 커서로 이어본다.

```mermaid
sequenceDiagram
  autonumber
  participant C as client
  participant EM as event-manager
  participant BK as agent-executor
  participant R as Redis Stream
  participant CH as conversation manager

  Note over C,EM: 스트리밍 중 마지막 SSE id(cursor) 보관
  C--xEM: 연결 끊김
  EM-)EM: XREAD 중계만 중단 (파이프 B)

  rect rgb(230,245,230)
    Note over EM,R: 파이프 A는 계속
    BK->>EM: 남은 block.* + done push
    EM->>R: XADD 계속 → done 처리 → conversation 저장
  end

  C->>EM: GET /v1/events/:dialog/stream (Last-Event-ID: cursor)
  alt stream 존재 (TTL 내)
    EM->>R: XREAD (cursor 이후부터)
    R-->>EM: 미열람 이벤트 + done
    EM-->>C: SSE 이어보냄 → done
  else stream 없음 (완료 후 5s 경과 / TTL 만료)
    EM-->>C: 404 not_found
    C->>CH: 완성 응답 직접 조회
  end
```

---

## 4. topics — 구독 / 발행 (best-effort)

```mermaid
sequenceDiagram
  autonumber
  participant C as client
  participant EM as event-manager
  participant BK as agent-executor
  participant CH as conversation manager
  participant P as Redis Pub/Sub

  C->>EM: GET /v1/topics/:topic/stream (user-id)
  EM->>P: SUBSCRIBE notify:{bNN}  (bNN = crc32(user)%N, 버킷 워커가 공유)
  Note over C,EM: 지속 SSE (done 없음, : ping 유지)

  BK->>EM: POST /internal/topics/:topic/messages {dialog_id, metadata, message}
  EM->>CH: addMessage (conversation_id·dialog_id는 본문) [저장·내구성, 비동기]
  EM->>P: PUBLISH notify:{bNN}  { u, t, payload } 봉투
  alt 버킷 구독 중인 pod 있음
    P-->>EM: message (봉투)
    Note over EM: 봉투의 (u,t)로 로컬 구독자에게 in-process fan-out
    EM-->>C: SSE event: message (payload)
  else 구독자 없음
    Note over P: 라이브 유실 (정상). 메시지는 이미 conversation에 저장됨
  end
```

> **연결 상한**: 채널을 (user,topic)별이 아니라 **버킷 `notify:{bNN}`**(bNN =
> crc32(user)%N)로 두어, 한 pod가 여는 pub/sub 구독은 버킷 수(≤N)로 상한 고정된다.
> SSE 연결 10만 개라도 Redis 구독 커넥션은 pod×N 수준. 버킷당 워커 1개가 구독을
> 공유하고, 메시지 봉투 `{u,t,payload}`의 (user,topic)로 로컬 구독자에게 fan-out한다
> (events 멀티플렉서 §5와 동일한 커넥션 상한 원리, XREAD 대신 SUBSCRIBE). 유휴 워커는
> 은퇴해 구독을 회수한다. 클러스터에서 `{bNN}` 해시태그로 구독이 노드에 분산되며, 일반
> PUBLISH는 cluster bus로 전 노드에 전파된다(운영 Redis Cluster 6.2.16; 7.0+에선 sharded
> pub/sub로 bus 전파 제거 가능).

---

## 5. 멀티플렉서 — 버킷 해시태그로 커넥션 상한 고정

`stream:{bucket_N}:user:dialog`, `bucket_N = crc32(user_id) % N`. 같은 user는 같은 버킷(=슬롯).
버킷은 N개뿐 → **XREAD 워커·커넥션 수 = N 상한**(SSE 연결 수와 무관).

```mermaid
flowchart LR
  subgraph Conns["수천 개 SSE 연결"]
    c1["user A · conn1"]
    c2["user A · conn2 (멀티뷰)"]
    c3["user B · conn"]
    c4["user C · conn"]
  end

  c1 & c2 -->|"crc32(A)%N = b02"| W2
  c3 -->|"crc32(B)%N = b02"| W2
  c4 -->|"crc32(C)%N = b09"| W9

  subgraph MUX["멀티플렉서 (워커 ≤ N개)"]
    W2["worker b02<br/>XREAD 1개 · 커넥션 1개"]
    W9["worker b09<br/>XREAD 1개 · 커넥션 1개"]
  end

  W2 -->|"슬롯 b02 다중키 XREAD"| S2[("Redis slot(b02)")]
  W9 -->|"슬롯 b09 다중키 XREAD"| S9[("Redis slot(b09)")]

  W2 -.->|"미열람분만 fan-out"| c1 & c2 & c3
```

- 워커는 **최소 위치부터 읽고 각 구독자에게 미열람분만** 전달(순서·무중복). 느린 구독자는 drop → 재연결.
- 유휴 워커는 은퇴해 커넥션 회수.

---

## 6. done 처리기 — 재조립 & 1회 저장

```mermaid
flowchart TB
  D["SSE done/error/cancled/block.canceled 도달<br/>(또는 /cancel fallback)"] --> XADD["done/canceled XADD"]
  XADD --> RANGE["XRANGE 전체 읽기"]
  RANGE --> RE{"이벤트별 재조립<br/>(block_id 그룹)"}
  RE -->|"block.delta: text append / json list"| BLK["완결 block 목록"]
  RE -->|"block.message"| IGN["무시 (실시간 저장됨)"]
  RE -->|"history"| HIS["history 수집"]
  BLK --> NX{"SET saved:dialog:block NX"}
  NX -->|성공 1회| SAVE["conversation addMessage(assistant)"]
  NX -->|이미 저장됨| SKIP["skip (멀티뷰어·멀티pod 중복 방지)"]
  HIS --> SAVEH["conversation addMessage(history)"]
  SAVE --> EXP["EXPIRE stream 5s"]
  SAVEH --> EXP
  SKIP --> EXP
```

---

## 렌더 방법
- **GitLab**: md 파일 열면 ```mermaid``` 블록 자동 렌더.
- **VS Code / JetBrains**: Mermaid 미리보기 플러그인 또는 마크다운 프리뷰.
- **로컬 이미지로 추출**: `mmdc`(mermaid-cli) 등으로 svg/png 변환.
