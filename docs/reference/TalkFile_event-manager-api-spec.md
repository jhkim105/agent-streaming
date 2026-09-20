# Event Manager — API 명세서

client 대면 SSE API와 내부 연동 계약. OpenAI/Anthropic 스트리밍 관례를 따른다.
이 문서가 API 계약의 단일 출처이며, **현재 구현(Go)의 실제 동작을 반영한다.**

- 버전: 4.1 (구현 반영 — §15 확정 결정 포함)
- 코드: `internal/` 각 패키지, 엔트리 `cmd/event-manager`
- 흐름도: [`event-manager-flows.md`](event-manager-flows.md) (Mermaid — 아키텍처/events/재연결/topics/멀티플렉서/done)

---

## 1. 개요 — 두 쌍(pair) 구조

event-manager는 **두 개의 대칭 쌍**을 제공한다.

| 쌍 | 성격 | 전송 | client 연결 | agent-executor 연동 |
|---|---|---|---|---|
| **events** | 요청-응답 | Redis **Stream** (무유실) | `POST /v1/events`, `GET .../stream`, `POST .../cancel` | EM → `POST /v1/execute` 호출 후 **응답 SSE 소비**(주 경로). 대안: internal push `/internal/events/...`·gRPC |
| **topics** | 구독-푸시 | Redis **Pub/Sub** (라이브 best-effort) + conversation 저장 | `GET /v1/topics/{topic_id}/stream` | `POST /internal/topics/{topic_id}/messages` |

**events vs topics 선택 기준**
- 순서·무유실·집계가 필요한 **stream 응답**(LLM 글자 단위 등) → **events**.
- 완결된 **단건 알림**(스케줄·푸시 등) → **topics**. 라이브 전달은 유실 허용이지만 메시지는 conversation에 저장된다.
- LLM stream을 topics로 보내지 말 것.

**HTTP/gRPC 표면 (HTTP 1포트 + gRPC)**

| 리스너 | 기본 포트 | 노출 | 역할 |
|---|---|---|---|
| HTTP | `8080` | `/v1/*`만 API GW 등록(공개), `/internal/*`은 미등록(내부 전용) | events/topics SSE + probe + internal messages/done/topics 푸시 |
| gRPC | `9090` | 클러스터 내부 전용 | 고빈도 stream 수신(`EventIngest.PushEvents`) |

HTTP는 한 포트(8080)로 통일한다. 외부 노출은 **API Gateway 경로 등록**으로 제어 —
`/v1/events`·`/v1/topics`만 등록되어 외부 접근 가능, `/internal/*`은 미등록이라 외부 접근 불가
(클러스터 내부에서만 `:8080/internal/*` 호출). gRPC는 게이트웨이로 노출하지 않는다.

**인프라 전제**
- 운영(prd) Redis: **Redis Cluster 6.2.16**. events(Stream)·topics(Pub/Sub) 모두 이 클러스터를 사용한다.
- 6.2.x에는 **sharded pub/sub(`SSUBSCRIBE`/`SPUBLISH`)가 없다**(Redis 7.0 도입). 따라서 topics는
  일반 `PUBLISH`/`SUBSCRIBE`를 쓰며, 클러스터에서 `PUBLISH`는 cluster bus로 전 노드에 전파된다.
  채널의 `{bucket_N}` 해시태그는 구독을 노드별로 분산하고, 향후 Redis 7.0+ 업그레이드 시
  sharded pub/sub로 전환해 bus 전파를 제거할 수 있도록 코드에 미리 반영돼 있다.

---

## 2. 식별자 계층

| 식별자 | 의미 | 저장/채널 |
|---|---|---|
| `dialog_id` | events 요청/턴 1건. **Redis stream 단위** | key: `stream:{bucket_N}:user_id:dialog_id` |
| `block_id` | 개별 block(말풍선) | (dialog 내부) |
| `message_id` | block 안 개별 row. **중복 제거 키** | (dialog 내부) |
| `topic_id` | topics 구독 단위 | 채널: `notify:{bucket_N}` (user 버킷, key 아님). (user,topic) 라우팅은 메시지 봉투로 |

- client가 `dialog_id`, `topic_id`를 **UUIDv7**로 생성. event-manager는 형식을 검증한다.
- `message_id`: 요청/`block.done`에 유효 UUIDv7이 있으면 사용, 없으면 **EM이 생성**. 발화·응답 저장에 동일 값.
- SSE `id:`(cursor) = **Redis 엔트리 ID**. 재연결 시 `last-event-id`로 이어받기.

### 2.1 stream key 버킷팅 (확정: 버킷 해시태그)

stream key는

```
stream:{bucket_N}:user_id:dialog_id      # bucket_N = crc32(user_id) % N  (예: {b07})
```

이며 `{bucket_N}`이 Redis Cluster **해시태그**다. bucket을 user_id로 산출하므로 한
user의 모든 dialog는 같은 버킷(=같은 슬롯)에 co-locate된다. 멀티플렉서 워커 하나가 그
**버킷 전체를 커넥션 1개 + 블로킹 다중키 XREAD 1개**로 읽는다(CROSSSLOT 없음). 버킷은
정확히 **N개**뿐이라 user·SSE 연결 수와 무관하게 **XREAD 워커(=블로킹 Redis 커넥션)
수는 N으로 상한 고정**된다. 워커는 버킷 단위로 만들어지고 유휴 시 은퇴한다.

> N(`numBuckets`, 기본 16)은 Redis Cluster 노드 수와 무관한 **논리 버킷 수 = 최대 워커
> 수**다. bucket을 user_id로 산출해 재연결·멀티뷰어가 한 워커에 모이고, 다중키 XREAD가
> 동일 슬롯이라 안전하다.
>
> **topics도 동일 버킷팅**을 쓴다. 알림 채널은 (user,topic)별이 아니라 버킷
> `notify:{bucket_N}` 1개이고, pod당 버킷 워커 1개가 그 구독을 공유하며 메시지 봉투
> `{u,t,payload}`의 (user,topic)로 로컬 구독자에게 fan-out한다. 그래서 pub/sub 커넥션도
> SSE 연결 수와 무관하게 **pod×N 상한**으로 고정된다(events의 XREAD 상한과 동형). 클러스터
> 에서 `{bucket_N}` 해시태그로 구독이 노드에 분산되고, 일반 PUBLISH는 cluster bus로 전
> 노드에 전파된다(운영 Redis Cluster **6.2.16** 기준; 7.0+는 sharded pub/sub로 bus 전파
> 제거 가능, 코드에 해시태그로 대비됨).

---

## 3. 헤더

모든 요청 공통. **Gateway가 인증 후 주입**(client가 넣지 않음, 외부 유입 시 제거·재설정).
소문자 표기(HTTP/2·3 규격). Go는 대소문자 무관하게 읽는다.

| 헤더 | 필수 | 설명 |
|---|---|---|
| `user-id` | ✅ | 사용자 식별자. stream key의 해시태그. |
| `device-id` | ⬜ | 디바이스 식별자 |
| `persona-id` | ⬜ | 페르소나 식별자. **conversation manager 저장 시 필요**(§8.5). |
| `tenant-id` | ⬜ | 테넌트 식별자 |
| `traceparent` | ⬜ | 분산 추적(W3C Trace Context) |

event-manager는 이 신원을 하위(agent-executor·conversation manager) 호출로 **전파**한다.

SSE 공통 응답: `content-type: text/event-stream`, `cache-control: no-cache`,
`x-accel-buffering: no`.

---

## 4. events — client 대면

### 4.1 POST /v1/events — 발화 + 응답 스트리밍

```
content-type: application/json
accept: text/event-stream
user-id: <id>                # Gateway 주입
```
```json
{
  "dialog_id": "UUIDv7",
  "metadata": { "conversation_id": "room_98f2", "agent_id": "travel-agent" },
  "context":  { "client": { "ASR": { "version": "1.8" } } },
  "message": {
    "role": "user",
    "content": [
      { "type": "text", "text": "예약할게" },
      { "type": "json", "json": { "action": "book_flight.v1", "payload": { "seat": "window" } } }
    ]
  }
}
```
| root | 설명 |
|---|---|
| `dialog_id` | 요청 식별자(UUIDv7). Redis stream 단위. |
| `metadata` | 참고·추적 정보(conversation_id, agent_id, sub_agent_id 등). |
| `context` | 런타임 실행 환경(ASR 버전 등). 그대로 agent-executor로 전달. |
| `message` | 대화 메시지(role + content 블록). |

content 블록 타입: `text`, `json`(자사 커스텀 구조화 데이터). 모든 필드 snake_case.

`message_id`: `metadata.message_id`가 있고 **유효한 UUIDv7이면 그대로 사용**, 없거나 무효면
**EM이 생성**한다. 발화 저장·응답 저장에 동일 `message_id`를 쓴다.

**처리(EM)**: ① 발화를 conversation에 저장(best-effort, 비동기) → ② agent-executor
`POST {agentExecutorUrl}/v1/execute` 호출(신원 헤더 `user-id`+`traceparent` 필수, 본문 그대로 전달)
→ ③ **응답으로 돌아오는 SSE(`start`·`block.*`·`history`·`done`·`error`)를 EM이 소비**하며
각 이벤트를 **그대로** XADD(파이프 A) — 첫 이벤트 `start`의 XADD로 stream이 생성된다
→ ④ 멀티플렉서로 XREAD 중계(파이프 B). 응답: `200`, `text/event-stream` → 6장.
소비는 client 연결과 독립(백그라운드)이라 client가 끊겨도 완주한다.

### 4.2 GET /v1/events/{dialog_id}/stream — 재개 / 멀티뷰어

```
accept: text/event-stream
user-id: <id>
last-event-id: <cursor>      # 재개 시
```
쿼리 `from`: `start`(처음부터) | `now`(지금 이후). 기본 `start`. `last-event-id` 우선.

동작:
- stream 있음 → cursor/from 위치부터 XREAD. 종료 이벤트(done/canceled/error) 만나면 종료.
- stream 없음 → **404** `{ "error": { "type":"not_found" } }` → client가 conversation manager 직접 조회.

여러 연결이 동시에 붙을 수 있음(멀티뷰어). 각자 독립 XREAD로 같은 응답 수신.

### 4.3 POST /v1/events/{dialog_id}/cancel — 취소

진행 중인 turn을 취소한다. EM consumer는 client 연결과 **독립적으로** 돌기 때문에(연결을 끊어도
XADD·done이 완주) 연결 종료로는 중단되지 않아 **명시적 취소**가 필요하다 — OpenAI
Runs/Responses·Anthropic Batches의 `/cancel`과 같은 성격(단순 스트리밍의 "연결 abort"와 다름).

**① client → event-manager**
```
POST /v1/events/{dialog_id}/cancel
user-id: <id>            # Gateway 주입, 필수
traceparent: <...>       # 필수
(본문 없음 — 대상 turn은 경로의 dialog_id)
```
응답 `202 { "status": "cancelling" }` (진행 중 → 최종 취소).

**② event-manager → agent-executor (전파).** agent-executor는 `/v1/execute` **단일 엔드포인트**이므로,
발화와 같은 스키마에 최상위 `action` 디스크리미네이터를 실어 취소를 전파한다. `action`이 없으면
(또는 `"message"`) 평소 발화 실행, `"cancel"`이면 해당 `dialog_id` turn을 취소한다(`message`·`context` 없음).
신원 헤더(`user-id`+`traceparent`)는 그대로 전파.
```json
{
  "dialog_id": "0190f3aa-1c2d-7e00-8a1b-000000000001",
  "action": "cancel",
  "metadata": { "conversation_id": "room_98f2", "agent_id": "travel-agent" }
}
```
이 호출의 **응답은 plain `202`뿐**이다(SSE 스트림 아님). EM은 상태코드만 확인하고 body를 닫는다 —
취소 이벤트는 이 응답이 아니라 **원 turn 스트림**으로 온다(③).

**③ 취소 이벤트 흐름.** 전파를 받은 agent가 `block.canceled`(`reason:"user_cancel"`)를 발행 →
executor가 **원 turn(`POST /v1/execute` call #1) 스트림**에 relay → `done`으로 종결. EM은 그 스트림을
이미 소비 중이므로(pipe A) `canceled`를 자체 합성하지 않고 **그대로 중계·저장**한다(§6). client는
`/cancel` 응답이 아니라 자신의 **events SSE 스트림**(POST /v1/events 또는 GET .../stream, pipe B)에서
`… block.canceled … done` 순서로 받는다.

> **합의 상태 / fallback**: 이 전파 계약(`/v1/execute` + `action:"cancel"`)은 executor 팀과 **합의됨**
> — Executor API 규격 §6의 미정의 항목을 본 정의로 확정한다. 다만 전파 호출이 실패하거나 executor
> 배포 이전인 경우, EM은 **fallback으로 로컬 처리**한다 — `canceled` XADD + done 처리(중단 시점까지
> 저장, §9.3) + 진행 중 `/v1/execute` consumer 취소(업스트림 연결 종료).

---

## 4.4 OpenAI Chat Completions 호환 — live voice 연동

live voice(SGW/LVTF)는 LLM을 **OpenAI Chat Completions 규격**으로 호출한다. EM은 이 규격을
그대로 받는 **얇은 어댑터**를 제공해 live voice의 LLM vendor로 동작한다(skt-aicc처럼 **단일 모델 +
metadata 라우팅** vendor). 어댑터는 요청을 §4.1 `POST /v1/events`와 **동일한 내부 처리**(발화 저장 →
agent-executor consumer(pipe A) → Redis 중계(pipe B))로 넘기고, 응답만 OpenAI
`chat.completion.chunk`로 재인코딩한다. **기존 events 경로는 변경하지 않는다.**
구현: `internal/httpserver/openai.go`.

### 엔드포인트
- `POST /v1/chat/completions` — 스트리밍 채팅 완성(현재 `stream:true`만 지원).
- `GET /v1/models` — vendor 모델 목록(EM 단일 모델).

### 확정된 3가지 매핑 결정
1. **identity/라우팅** — OpenAI 요청엔 `user-id` 등이 없으므로 EM 필수 신원을 요청 `metadata`
   필드로 받는다(주). 없으면 신원 헤더 → `user` 필드 순으로 fallback.
2. **멀티턴 히스토리** — live voice는 매 요청 `messages[]`에 `dialog.historyCount`만큼 히스토리를
   싣는다. EM은 **마지막 user turn → `message`**, **`messages[]` 전체 → `context.openai`**로 실어
   agent-executor에 넘긴다(상태 비저장, `conversation_id` 불필요).
3. **barge-in** — chat/completions 스트림 연결이 **종료 이벤트 전에 끊기면** EM이 취소를
   전파한다(§4.3 경로 재사용). native events의 "연결 끊김 ≠ 취소"와 다른, 음성 barge-in 계약.

### 요청 예 (`POST /v1/chat/completions`)
```
content-type: application/json
accept: text/event-stream
authorization: Bearer <apiKey>   # requires-api-key vendor일 때 live voice가 전달(현재 EM 미검증)
```
```json
{
  "model": "helix-event-manager",
  "stream": true,
  "temperature": 0.7,
  "max_tokens": 1024,
  "metadata": {
    "user_id": "u1",
    "device_id": "d1",
    "persona_id": "p1",
    "tenant_id": "skt-va",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "conversation_id": "room_98f2",
    "agent_id": "travel-agent"
  },
  "messages": [
    { "role": "system",    "content": "당신은 친절한 여행 도우미입니다." },
    { "role": "user",      "content": "제주도 항공권 예약해줘" },
    { "role": "assistant", "content": "언제 출발하시나요?" },
    { "role": "user",      "content": "이번 주말" }
  ]
}
```

EM이 내부적으로 만드는 §4.1 `EventRequest`(agent-executor로 전달):
```json
{
  "dialog_id": "<EM이 생성한 UUIDv7>",
  "metadata": { "conversation_id": "room_98f2", "agent_id": "travel-agent" },
  "context": {
    "openai": {
      "source": "live-voice",
      "model": "helix-event-manager",
      "temperature": 0.7,
      "max_tokens": 1024,
      "system": "당신은 친절한 여행 도우미입니다.",
      "messages": [ /* 위 messages[] 전체 — agent가 히스토리로 사용 */ ]
    }
  },
  "message": { "role": "user", "content": [ { "type": "text", "text": "이번 주말" } ] }
}
```

### 응답 예 (스트리밍 `text/event-stream`)
EM SSE(`block.delta`/`block.message`의 text·`json`의 fallbackText) → OpenAI `chat.completion.chunk`.
첫 청크에 `delta.role:"assistant"`, `done`/`canceled` → `finish_reason:"stop"` 후 `[DONE]`.
```
data: {"id":"chatcmpl-0190f3aa-...","object":"chat.completion.chunk","created":1730000000,"model":"helix-event-manager","choices":[{"index":0,"delta":{"role":"assistant","content":"제주"},"finish_reason":null}]}

data: {"id":"chatcmpl-0190f3aa-...","object":"chat.completion.chunk","created":1730000000,"model":"helix-event-manager","choices":[{"index":0,"delta":{"content":" 항공권 89000원 예약되었습니다."},"finish_reason":null}]}

data: {"id":"chatcmpl-0190f3aa-...","object":"chat.completion.chunk","created":1730000001,"model":"helix-event-manager","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: [DONE]
```

오류(스트림 도중):
```
data: {"error":{"type":"internal_error","message":"AGENT_CONNECTION_LOST: ..."}}

data: [DONE]
```

요청 파싱/신원 누락 등 시작 전 오류는 OpenAI 오류 객체로 즉시 응답한다:
```json
{ "error": { "type": "invalid_request_error", "message": "missing user identity (metadata.user_id / user-id header / user field)", "code": null, "param": null } }
```

### 모델 목록 예 (`GET /v1/models`)
```json
{ "object": "list",
  "data": [ { "id": "helix-event-manager", "object": "model", "created": 0, "owned_by": "helix-event-manager" } ] }
```

### 이벤트 매핑 요약
| EM SSE | OpenAI chunk |
|---|---|
| `block.delta`(text) | `choices[0].delta.content` |
| `block.delta`(json) | `fallbackText`만 사용(구조화 json은 음성에서 생략) |
| `block.message` | content로 전달(음성 필러 발화 — **미확정**, live voice 확인 필요) |
| `start`/`block.start`/`block.done`/`block.canceled`/`history` | 무출력 |
| `done`/`canceled` | `finish_reason:"stop"` + `data: [DONE]` |
| `error` | `data: {"error":{...}}` + `data: [DONE]` |

### 미확정(구현 전 cross-team 확인)
- LVTF가 vendor 요청 `metadata`에 **실제로 싣는 키/구조**(→ `oaiMetadata` 확정).
- agent-executor가 `context.openai.messages`에서 히스토리를 읽도록 합의.
- `block.message` 필러를 TTS로 읽을지 여부.
- 비스트리밍(`stream:false`) 지원 여부(현재 400), apiKey 검증 여부(현재 미검증), 모델 목록 config화.

---

## 5. topics — client 대면

### 5.1 GET /v1/topics/{topic_id}/stream — 구독

```
accept: text/event-stream
user-id: <id>
```
- event-manager가 user 버킷 채널 `notify:{bucket_N}`(bucket_N = crc32(user_id)%N)를 SUBSCRIBE.
  버킷 워커 1개가 그 구독을 공유하고, 수신 봉투의 (user,topic)로 이 구독자에게 fan-out한다.
- 지속 SSE(done 없음). heartbeat `: ping` 주석(60초)으로 유지. 단일 연결 상한은
  `topicsSseTimeout`(기본 `0`=무제한)이며, 기본값에서는 client 끊김·서버 shutdown 시에만
  종료된다. 양수로 두면 그 시간 후 종료 → client 재연결.
- 여러 연결(멀티 기기/탭)이 같은 topic_id 구독 시 모두 수신.
- **라이브 전달은 best-effort**: 구독 없을 때 온 메시지는 라이브 스트림에서 유실(정상). 단
  메시지 자체는 EM이 발행 시 conversation에 저장하므로(§8.4) 이후 조회로 확인 가능하다.
- 알림은 `event: message` 이벤트로 payload를 그대로 전달한다.

`topic_id`는 client가 정하는 구독 키. user_id로 격리되어 같은 topic_id라도 user가 다르면
다른 채널.

---

## 6. SSE 이벤트 모델 (events)

| 이벤트 | 레벨 | 발생 주체 | 설명 |
|---|---|---|---|
| `start` | 스트림 | agent-executor | 시작. EM이 **그대로 중계**(첫 XADD가 stream 생성) |
| `block.start` | block | agent-executor | 말풍선 시작. **metadata를 여기 한 번 실음** |
| `block.message` | block | agent-executor | 실시간 저장 conversation("기다려" 등). **done 집계에서 무시**, 즉시 conversation 저장 |
| `block.delta` | block | agent-executor | 응답 증분(0..N). **증분만.** 순서·재개는 cursor로 |
| `block.done` | block | agent-executor | block 완료(message_id). done 처리기가 모아 conversation 저장 |
| `block.canceled` | block | agent-executor | block 단위 취소(`reason: user_cancel`). **종결 아님** — 스트림은 `done`으로 닫힘. EM이 relay·저장 |
| `history` | 스트림 | agent-executor | conversation 저장하되 사용자 미노출(agent-executor 메모리 용도) |
| `canceled` | 스트림 | event-manager | **fallback**: cancel 전파 실패·배포 이전에만 EM이 합성하는 스트림 종결(stop_reason: canceled). 정상 취소는 `block.canceled`+`done` (§4.3) |
| `done` | 스트림 | agent-executor | 정상 완료(종료). 주 경로는 EM이 **그대로 중계** / 대안 §8.2 경로에선 EM이 합성 |
| `error` | 스트림 | agent-executor/EM | 오류. `source`로 발생 주체 구분(agent 전파 vs executor/EM 자체 발행). §7.3 |
| `ping` | — | 양쪽 | SSE 주석 `: ping` (named 이벤트 아님). idle-timeout·중도 차단 방지용 keepalive |

- **주 경로**: `start`·`block.*`·`history`·`done`·`error`는 agent-executor의 `/v1/execute` **SSE 응답**을
  EM이 소비해 **그대로 XADD**한다. 따라서 client가 받는 `start`/`done`도 agent-executor가 발행한 값을 중계한 것이다.
- event-manager가 **합성**하는 이벤트는 (a) cancel 전파 실패·배포 이전 시 **fallback** `canceled`(§4.3 — 정상
  취소는 executor의 `block.canceled`+`done`을 relay), (b) 업스트림 연결 실패·조기 EOF 시 `error`(§7.3),
  (c) 대안 내부 푸시 경로(§8.2)의 `done`/`canceled`(agent-executor가 SSE 대신 REST `/done`을 호출하므로
  EM이 종료 이벤트를 `stop_reason`과 함께 합성)뿐이다.
- **ping**: keepalive는 named 이벤트가 아니라 **SSE 주석 `: ping`**이다(중도 네트워크 차단·
  idle-timeout 방지용). agent-executor가 보내는 `: ping`은 EM이 SSE 파싱 단계에서 **전부
  무시**한다(저장·중계 안 함). EM은 client에게 `: ping`을 **60초마다** 보내 연결을 유지한다.
  (방어적으로 named `event: ping`이 들어와도 저장하지 않고 건너뛴다.)
- agent-executor의 종료 이벤트 오타 `cancled`는 EM이 `canceled`로 정규화한다.

**시퀀스 예시**
```
event: start
id: 1730000000000-0
data: {"dialog_id":"...","metadata":{"conversation_id":"room_98f2"}}

event: block.start
id: 1730000001000-0
data: {"dialog_id":"...","metadata":{"block_id":"...","conversation_id":"room_98f2","agent_id":"travel-agent"}}

event: block.delta
id: 1730000001500-0
data: {"dialog_id":"...","metadata":{"block_id":"..."},"delta":{"content":[{"type":"text","text":"제주 항공권 89000원 - 예약되었"}]}}

event: block.done
id: 1730000002000-0
data: {"dialog_id":"...","metadata":{"block_id":"...","conversation_id":"room_98f2","agent_id":"travel-agent"}}

event: done
id: 1730000009000-0
data: {"dialog_id":"...","metadata":{"conversation_id":"room_98f2"}}
```

---

## 7. 응답 메시지 형식

### 7.1 완결 메시지 (block.message / block.done 저장 단위)
```json
{
  "dialog_id": "UUIDv7",
  "metadata": { "message_id": "UUIDv7", "block_id": "UUIDv7",
                "conversation_id": "room_98f2", "agent_id": "travel-agent" },
  "message": {
    "role": "assistant",
    "content": [
      { "type": "text", "text": "제주 항공권 89000원 - 예약되었습니다." },
      { "type": "json",
        "json": { "action": "rich_card.v1", "payload": { "title":"제주 항공권", "price":"89000" } },
        "fallbackText": "제주 항공권 89000원 - 예약되었습니다.",
        "chat": { "bubble": false, "llm": true } }
    ]
  }
}
```

### 7.2 증분 (block.delta)
```json
{
  "dialog_id": "UUIDv7",
  "metadata": { "block_id": "UUIDv7" },
  "delta": { "content": [ { "type": "text", "text": "제주 항공권 89000원 - 예약되었" } ] }
}
```

**done 처리기 재조립 규칙** — block_id별로 묶어:
- `text`: 순서대로 append하여 하나의 text 블록으로.
- `json`: list로 수집(순서 유지).
- `block.message`: 집계에서 무시(별도 실시간 저장됨).
- `history`: 별도 수집해 저장(사용자 미노출 의도).
- `chat.bubble`=사용자 노출 여부, `chat.llm`=LLM 컨텍스트 저장 여부(현재 conversation manager에
  이 플래그 저장 자리가 없어 §8.5의 손실 매핑 적용).

### 7.3 오류 (error)
```json
{
  "dialog_id": "UUIDv7",
  "metadata": { "conversation_id": "room_98f2", "agent_id": "agent-a" },
  "source": "executor",
  "error": { "code": "AGENT_CONNECTION_LOST", "message": "Agent-A와의 연결이 끊어졌습니다." }
}
```
- `source`: 오류 발생 주체. agent가 발행해 전파된 오류는 해당 `agent_id`(예: `agent-a`),
  event-manager/executor가 자체 감지·발행한 오류는 `executor`.
- `error.code` 예: `UNKNOWN_AGENT`(미등록 agent_id), `INVALID_REQUEST`(필수 헤더/필드 누락·
  본문 파싱 실패), `AGENT_CONNECTION_LOST`(agent 연결 끊김), `AGENT_INTERNAL_ERROR`(agent 처리 오류),
  `PARSE_ERROR`(agent 응답 파싱 실패), `AGENT_TIMEOUT`(SSE 조기 EOF, §8).
- 요청 body 파싱 실패처럼 식별자를 알 수 없으면 `dialog_id`/`metadata`가 생략될 수 있다.
  이때도 client 응답 스트림은 `start`로 열리고 `error` 알림 후 `done`으로 닫힌다(start…done 불변식).

---

## 8. 내부 연동

**주 경로 — agent-executor `/v1/execute` SSE 소비.** EM이 `POST {agentExecutorUrl}/v1/execute`를
호출하고 그 응답 SSE(start·block.*·history·done·error·`cancled`·`ping`)를 소비해 XADD한다(§4.1).
- 헤더 `user-id`, `traceparent` 필수. 본문은 client 요청(§4.1)을 그대로 전달(snake_case).
- `block.message`는 소비 즉시 conversation 저장, `block.done`은 EM이 message_id 정책 적용 후 병합 저장,
  `done`/`error`/`cancled(→canceled)`에서 done 처리(재조립·저장·TTL 축소)로 마감. `ping`은 저장 안 함.
- SSE가 EOF로 조기 종료되면 EM이 `error`(AGENT_TIMEOUT)를 합성해 마감한다.

**대안 경로 — internal push API(`:8080`의 `/internal/*` HTTP / `:9090` gRPC).** agent-executor가
응답을 EM으로 직접 밀어넣는 방식이 필요할 때 쓰는 대체 수단. `/internal/*`은 client와 같은 8080
포트에서 서빙되지만 API GW에 미등록이라 외부 접근 불가(클러스터 내부 전용). 아래 8.1~8.4는 이
대안 경로의 계약이다. 두 경로 모두 동일한 XADD 파이프(§9)로 수렴하며, 신원 헤더/필드(특히 `user-id`)를 실어야 한다.

### 8.1 events 응답 전달 (단건, REST · 대안 경로)
```
POST /internal/events/{dialog_id}/messages
user-id: <id>   (+ persona-id 등 신원 전파)
Content-Type: application/json

{ "event": "block.delta", "block_id": "UUIDv7", "data": { ...§7 payload... } }
```
- **이벤트 봉투** `{ event, data, block_id? }`. `event`는 `block.start` | `block.message` |
  `block.delta` | `block.done` | `history` | `error` 중 하나(그 외는 400).
  `block_id`가 비면 `data.metadata.block_id`에서 유추.
- event-manager가 `data`를 그대로 XADD(파이프 A) → XREAD로 client 중계.
- `block.message`는 XADD와 동시에 **즉시 conversation 저장**(§8.5).
- 응답 `202`.

### 8.2 events 완료 통지
```
POST /internal/events/{dialog_id}/done
user-id: <id>
{ "stop_reason": "end_turn" }   // 생략 시 end_turn; "canceled"면 canceled 이벤트
```
done 처리(§9): done/canceled XADD → XRANGE 재조립 → `SET saved:{dialog_id}:{block_id} NX
EX 3600` 성공 시에만 conversation 저장 → stream TTL을 5초로 축소. 응답 `200`.

### 8.3 고빈도 stream (gRPC)
- proto: `proto/event_ingest.proto`, 서비스 `EventIngest.PushEvents`(client-streaming).
- agent-executor가 `StreamChunk { user_id, dialog_id, event, data(bytes JSON), block_id }`를
  연속 push → event-manager가 각각 XADD(파이프 A와 동일 경로). 종료 시 `PushSummary{count}`.
- REST 단건(8.1)과 gRPC 고빈도가 동일 XADD 파이프로 수렴.

### 8.4 topics 푸시 전달
```
POST /internal/topics/{topic_id}/messages
user-id: <id>
{                                       // §7.1 CompletedMessage 형식
  "dialog_id": "uuid_v7",
  "metadata": { "conversation_id": "room_98f2", "agent_id": "agent-b" },
  "message": { "role": "assistant", "content": [{ "type": "text", "text": "..." }] }
}
```
- **저장 + 라이브 푸시(독립)**. 두 동작은 서로 무관하게 수행된다:
  1. **conversation 저장(내구성)**: 본문을 §8.5 매핑으로 conversation에 저장한다(백그라운드, best-effort).
     `dialog_id`·`conversation_id`는 **본문**에서 취한다(events처럼 URL이 아님). `message_id`가
     없으면 EM이 UUIDv7 생성(있으면 그 값 사용 → addMessage 멱등, 재시도 안전). role 없으면
     `assistant`. **`conversation_id`가 없으면 저장할 대화가 없어 저장을 건너뛰고 경고 로그를
     남긴다**(라이브 푸시는 그대로 진행).
  2. **라이브 푸시**: user 버킷 채널로 `PUBLISH notify:{bucket_N}` 하되 본문을 봉투
     `{ u, t, p:본문 }`으로 감싼다. 각 pod 버킷 워커가 풀어 (user,topic) 일치 구독자에게만
     원문을 `event: message`로 전달한다.
- 응답 `202 { "delivered": <버킷 채널을 구독 중인 pod 수> }`. 0이면 그 버킷을 듣는 pod가
  없다는 뜻(라이브 유실이며 정상 — 저장은 별개로 수행됨). 특정 (user,topic) 구독자 유무를
  나타내지 않는다.

### 8.5 conversation manager 저장 매핑 (확정)
event-manager는 발화·완결 응답을 사내 conversation-manager(REST)에 저장한다.
```
POST {conversation}/conversations/{conversation_id}/messages
Persona-Id: <id>   (+ User-Id)
{ "message_id": "UUIDv7", "dialog_id": "UUIDv7", "role": "user|assistant|system|tool",
  "channel": "web", "content": [ { "type": "text", "text": "..." } ] }
```
- 모든 필드 **snake_case**. `dialog_id`는 **필수**(conversation-manager 요구).
- `message_id` 정책: 요청의 `metadata.message_id`가 유효 UUIDv7이면 사용, 아니면 EM 생성(§4.1).
  발화·응답에 동일 값을 쓴다.
- **conversation 생성/조회는 client 소유.** event-manager는 `metadata.conversation_id`로
  addMessage만 수행(best-effort). conversation이 없으면 conversation manager가 404 → 로깅.
- `message_id` 기준 **멱등 upsert**. EM의 `SET NX`(§9)가 이중 방어.
- `channel` 기본값 `web`.
- conversation manager의 `ContentBlock`은 `{type,text}`만 저장 → **`json` 블록은 fallbackText
  (없으면 직렬화 JSON)로 평탄화**(손실 매핑). `chat.bubble/llm` 플래그는 저장 자리 없음.
- `history`는 "미노출" 플래그가 없어 그대로 저장(알려진 한계).

---

## 9. 처리 흐름 (events) — 두 파이프 독립

- **파이프 A (XADD)**: agent-executor `/v1/execute` SSE 응답을 event-manager가 소비해 XADD(대안
  경로 8.1/8.3도 동일 파이프). XADD와 진행중 TTL(60분) 갱신을 **MULTI/EXEC로 원자적**으로 수행
  (단일 key=단일 슬롯이라 클러스터 안전).
- **파이프 B (XREAD)**: client 연결이 멀티플렉서로 stream을 자기 cursor부터 중계.

두 파이프는 독립. **client가 끊겨도 XADD와 done 처리는 완주**한다. 취소는 `/cancel`로만.

### 9.1 정상 흐름
POST → 발화 저장 + agent-executor `/v1/execute` 호출 → **응답 SSE(`start`·block.*·`done`)를 EM이 소비**하며
**그대로** XADD(첫 `start`가 stream 생성) + client 중계 → SSE에 `done` 도달 → done 처리(아래) → client 종료.

### 9.2 client 연결 종료
client 끊김 → XREAD(중계) 중단. XADD·done 처리는 계속. client는 GET으로 이어받기.

### 9.3 done 처리 (event-manager 주체)
SSE `done`(또는 stop/error) 도달 시 → done(또는 canceled) XADD → 전체 stream XRANGE → 재조립(§7) → 완결 block마다
`SET saved:{dialog_id}:{block_id} NX EX 3600` 성공 시 conversation 저장(멀티뷰어·멀티pod에서 1회만)
→ history 저장 → stream `EXPIRE 5초`.

### 9.4 취소 (§4.3)
정상: `/cancel` → EM이 `action:"cancel"`을 `/v1/execute`로 전파 → agent `block.canceled` +
executor `done`을 EM이 relay·저장(`canceled` 자체 합성 안 함).
fallback(전파 실패·배포 이전): EM 로컬 처리 — `canceled` XADD + done 처리(중단 시점까지 저장)
+ 진행 중 `/v1/execute` consumer 취소(업스트림 종료).

---

## 10. 재연결 (events)
1. delta 수신 중 마지막 cursor(SSE `id`) 저장.
2. 끊김 — 그 사이 chunk는 Redis에 계속 XADD(보존).
3. `GET /v1/events/{dialog_id}/stream` + `last-event-id: <cursor>` (+ `user-id`).
4. stream 있으면 이어보냄(**어느 pod로든** TTL 내 재개) / 없으면 404 → conversation 조회.
5. 종료 이벤트 → 종료.

---

## 11. TTL / 타임아웃 (Redis TTL은 events만; SSE 연결 상한은 양쪽)

| 항목 | 값 | 설명 |
|---|---|---|
| Stream 진행중 | 60분 | **매 XADD마다 갱신**(XADD+PEXPIRE를 MULTI/EXEC로 원자적) |
| Stream 완료후 | 5초 | done 처리 시 축소(메모리 회수) |
| events SSE 연결 | 60분 | 절대 상한, 초과 시 종료 → Last-Event-ID로 무손실 재개 |
| topics SSE 연결 | 무제한 | `topicsSseTimeout=0`. client 끊김·서버 shutdown 시에만 종료 |
| saved 마커 | 60분 | `saved:{dialog}:{block}` NX 마커 수명 |
| heartbeat | 60초 | `: ping` (SSE 주석 keepalive, 양쪽) |

> topics(pub/sub)는 Redis key가 아니므로 TTL 없음. 구독 없으면 자동 소멸.

---

## 12. 중복 제거
- **message_id**: 건별 고유. client는 이미 받은 message_id 재수신 시 버림.
- **block.done 저장**: `SET saved:{dialog_id}:{block_id} NX`로 1회만 conversation 저장(멀티뷰어·
  멀티pod·재처리 대비). conversation manager의 `message_id` 멱등이 추가 방어.

---

## 13. 커넥션 폭발 방지 — 멀티플렉서
SSE 연결마다 XREAD를 열지 않는다. 버킷 해시태그(`{bucket_N}`, bucket=crc32(user_id)%N)별
워커 1개가 커넥션 1개 + 블로킹 다중키 XREAD 1개로 그 버킷의 모든 stream을 읽어 구독자별
인메모리 채널로 fan-out한다. 버킷은 N개뿐이라 **워커·커넥션 수는 N으로 상한 고정**.
한 stream에 여러 구독자(0부터/재개/멀티기기)가 붙으면 워커가 **최소 위치부터 읽고 각
구독자에게 미열람분만** 전달(순서 보장, 중복 없음). 느린 구독자는 drop+채널 close →
client가 `last-event-id`로 재연결. 워커는 유휴 시 은퇴해 커넥션을 회수한다.

---

## 14. 개발용 콘솔 (로컬 전용)
`DEV_CONSOLE=true`(또는 config `devConsole: true`)일 때 client 포트에 브라우저 테스트
콘솔을 제공한다.
- `GET /console` — 발화(POST), SSE 구독/재연결(cursor), 취소(cancel), topics 구독/발행을 UI로 테스트.
- 이때 두 리스너에 관대한 CORS를 적용(콘솔이 client 포트에서 internal 포트로 topics 발행).
  **프로덕션에서는 끈다.**

---

## 15. 확정 결정 (구 "미확정" 해소)
- **stream key 버킷팅**: `stream:{bucket_N}:user_id:dialog_id` — bucket_N=crc32(user_id)%N을
  Redis 해시태그로(§2.1). N=`numBuckets`(기본 16) = 최대 XREAD 워커 수.
- **진행중 TTL 갱신 원자성**: 매 XADD를 XADD+PEXPIRE MULTI/EXEC로 처리(§9, §11).
- **internal 메시지 계약**: 이벤트 봉투 `{event, data, block_id?}`(§8.1). start/done/canceled는
  EM이 합성.
- **conversation manager 저장 API**: 사내 conversation-manager REST에 맞춤(§8.5). 조회는 client가 직접.
- **gRPC proto**: `proto/event_ingest.proto`, `EventIngest.PushEvents`(§8.3).
- **신원 전파**: 소문자 `user-id` 등 헤더를 agent-executor·conversation manager로 전파(§3).

## 16. 남은 한계 / 향후
- conversation manager에 `chat.bubble/llm`·history "미노출" 플래그 저장 자리 없음(현재 손실 매핑).
- done 통지(`/done`) 실패 시 재시도 없음(진행중 60분 TTL로 자연 회수, 저장 누락 가능 — 로깅).
- topics 발행 시 대상 user_id는 헤더로 받는다(멀티캐스트/브로드캐스트는 미지원).
