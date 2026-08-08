# ADR 0002: Redis Streams 버킷팅 및 컨슈머 시점 동적 세션 라우팅 아키텍처 채택

* **상태 (Status)**: 승인됨 (Accepted)
* **날짜 (Date)**: 2026-08-08
* **작성자 (Author)**: Antigravity Agent & User
* **관련 문서**: [ADR 0001 세션 라우팅](0001-multi-node-multi-node-routing.md) | [요구사항 정의서](../1.requirements.md) | [시스템 아키텍처](../2.architecture.md)

---

## 1. 배경 및 문제 정의 (Context)

기존 멀티 노드 스트리밍 아키텍처(ADR 0001)는 **Kafka + Redis Pub/Sub** 조합을 사용하여 코틀린 인스턴스 간 세션 교차 라우팅을 수행했습니다. 그러나 실제 운영 환경을 고려했을 때 아래 **3가지 심각한 문제점 및 아키텍처 한계**가 발견되었습니다:

1. **Redis Pub/Sub의 메시지 유실 위험 (At-most-once Delivery)**:
   * Redis Pub/Sub은 영속성(Persistence)이 없는 Fire-and-Forget 방식입니다.
   * 서버 노드 배포/재시작이나 네트워크 미세 끊김이 발생하여 Redis 연결이 재수립되는 짧은 시간 동안 전송된 `CHUNK`나 `STATUS` 토큰 이벤트는 **Redis 채널에서 즉시 유실(Drop)**되는 문제가 존재합니다.
2. **L4/L7 라운드로빈 로드밸런서 환경에서의 세션 라우팅 실패**:
   * 클라이언트가 `GET /api/chat/stream`으로 **Node 1**에 SSE 소켓을 수립했더라도, `POST /api/chat/message` 질문 요청은 라운드로빈 로드밸런서에 의해 **Node 2**로 들어갈 수 있습니다.
   * 질문을 받은 Node 2가 질문 제출 시점(Producer)에 자코의 `hostId`(`node-2`)를 박아 Kafka로 송신할 경우, 파이썬 에이전트의 응답이 Node 2로 배달되지만 **실제 사용자 소켓은 Node 1에 맺어져 있어 100% 메시지가 유실**됩니다.
3. **대동시 접속 환경에서의 Redis 커넥션 폭발 위험 (Connection Explosion)**:
   * 대화방 또는 유저 수에 비례하여 1:1로 Redis Stream Key/Channel을 생성하고 `XREAD` 커넥션을 맺을 경우, 동시 대화 수가 10,000개로 늘어나면 **Redis 커넥션 및 폴링 스레드 수도 10,000개로 폭발**하여 레디스 인프라가 붕괴할 수 있습니다.

---

## 2. 의사결정 (Architectural Decisions)

위 문제들을 근본적으로 해결하기 위해 다음 **4가지 핵심 아키텍처 변경안**을 채택합니다:

### 💡 결정 1: Redis Session-Host Registry (세션 소켓 위치 동적 저장소) 도입
* 클라이언트가 `GET /api/chat/stream`으로 코틀린 노드에 접속하거나 새로고침 시, 해당 노드는 Redis 인메모리에 실시간 소켓 위치를 등록합니다.
  ```
  SET session:host:{sessionId} {localHostId} EX 3600
  ```
* 소켓 연결 해제 시 안전하게 해당 세션 위치를 제거합니다.

### 💡 결정 2: 컨슈머 시점 동적 세션 위치 조회 (Consumer-side Dynamic Lookup) 채택
* `POST /api/chat/message` 질문 전송 시(Producer 시점)에는 `hostId`를 메시지에 고정하지 않고 비동기 질문 큐잉만 수행합니다.
* 파이썬 워커 응답을 소비하는 코틀린 수신부(Consumer 시점)에서 **매 이벤트 전달 직전 순간 Redis에서 해당 `sessionId`를 보유한 최신 소켓 노드(`targetHostId`)를 동적으로 조회**하여 라우팅합니다.
* **효과**: 스트리밍 진행 도중 사용자가 새로고침하거나 L4 라운드로빈으로 소켓이 다른 노드로 이동하더라도, 최신 소켓 노드를 동적 추적하여 무중단 배달합니다.

### 💡 결정 3: Redis Streams 16개 버킷팅 아키텍처 (`stream:bucket:{00~15}`) 채택
* `conversationId`를 기반으로 16개의 논리 버킷으로 해시 분할합니다:
  ```
  bucket_id = abs(conversationId.hashCode()) % 16   (예: stream:bucket:07)
  ```
* **효과**: 동시 진행 대화방 수가 1,000개든 10만 개든 상관없이, 코틀린 서버 ➔ Redis 간 `XREAD` 다중키 블로킹 커넥션 및 폴링 스레드 수를 **물리적으로 16개 이하로 상한 고정(Capped)**합니다.

### 💡 결정 4: W3C `Last-Event-ID` 커서 기반 복구 및 원자적 완결 락
* **W3C `Last-Event-ID` 수신**: 새로고침/재연결 시 클라이언트가 전송한 `Last-Event-ID` (Redis Stream ID) 이후의 미열람 토큰부터 Redis Stream에서 `XREAD`로 조회하여 유실 없이 리플레이합니다.
* **원자적 1회 완결 저장 락**: `DONE` 이벤트 수신 시 `SET saved:conv:{conversationId} NX` 락을 획득한 1개 노드만 DB에 마크다운 보고서를 1회 정제 저장하고 Stream 5초 TTL 정리를 수행합니다.

---

## 3. 구조 다이어그램 (Architectural Flow)

```
[ Client SSE ] ──(1. SSE 연결)──► [ Node 1 ] ──► (2. Redis 세션 등록: session:host:sse-100 -> "node-1")
 
[ Client POST ] ──(3. 질문 전송)─► [ Node 2 ] ──► (4. Kafka agent-requests 발송 - hostId 미고정)
                                                           │
                                                           ▼
                                                [ Python Agent Worker ]
                                                           │
                                                           ▼
 [ Client SSE ] ◄──(7. SSE 배달)─── [ Node 1 ] ◄── (5. Kafka 응답 소비 & Redis 동적 위치 조회!)
                                                          "sse-100 소켓이 현시점 node-1에 있구나!"
                                                          (6. Redis Stream: stream:bucket:07 배달)
```

---

## 4. 파급 효과 및 결과 (Consequences)

### 긍정적 영향 (Positive Results)
1. **0% 유실률 달성**: Redis Streams(At-least-once) 도입으로 네트워크 끊김이나 서버 재시작 시에도 토큰 유실이 완전히 방지됩니다.
2. **L4/L7 라운드로빈 완벽 대응**: 질문 유입 노드와 소켓 맺은 노드가 달라도 컨슈머 동적 조회로 100% 소켓 노드를 찾아 배달됩니다.
3. **인프라 커넥션 폭발 완벽 차단**: 버킷팅 구조로 동시 대화 10만 개 발생 시에도 Redis XREAD 커넥션 수가 16개 이하로 통제됩니다.
4. **새로고침 무중단 이어받기**: `Last-Event-ID` 커서 기반으로 끊어진 시점부터 유실 없이 타자기 효과를 이어나갈 수 있습니다.

### 트레이드오프 및 고려사항 (Trade-offs & Considerations)
* 백엔드에 16개 버킷 Streams XADD/XREAD 멀티플렉서 리스너 관리 및 Redis 세션 매핑 서비스(`RedisSessionRegistry`) 구현 복잡도가 추가됩니다. (단, 이 복잡도는 백엔드 내부로만 격리됨).
