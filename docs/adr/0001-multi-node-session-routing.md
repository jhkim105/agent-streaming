# ADR 0001: Multi-Node Gateway Session Routing Strategy

* **Status**: Accepted (확정)
* **Date**: 2026-07-25
* **Authors**: Architecture & Engineering Team
* **Deciders**: Lead Architect & Developer

---

## 1. 배경 및 문제 정의 (Context)

본 프로젝트는 동시 연결 처리가 우수한 **Kotlin WebFlux 게이트웨이**와 비동기 AI 에이전트인 **Python LangGraph 워커**, 그리고 메시지 버퍼인 **Kafka 브로커**로 구성된 비동기 이벤트 구동 아키텍처(Event-Driven Architecture)를 채택하고 있습니다.

* **문제 상황**:
  1. 게이트웨이 서버가 고가용성 및 가용량 확장을 위해 다중 인스턴스(Multi-Node: `node-1`, `node-2` 등)로 스케일아웃될 때, 클라이언트와의 **SSE(Server-Sent Events) 소켓 커넥션은 특정 서버 노드의 인메모리에만 존재**합니다.
  2. 파이썬 에이전트가 처리를 완료하고 결과를 Kafka `agent-responses` 토픽으로 반환할 때, 해당 메시지를 소비(Consume)하는 게이트웨이 노드가 **클라이언트 소켓을 보유한 노드와 다를 수 있습니다.**
* **목표**: 비동기 카프카 메시지를 수신한 노드가 클라이언트 SSE 커넥션을 쥐고 있는 목적지 노드로 메시지를 세션 유실 없이 안전하게 전달하는 라우팅 메커니즘을 결정합니다.

---

## 2. 결정 고려 요인 (Decision Drivers)

* **네트워크 및 자원 효율성**: 노드 수가 늘어나도 무분별한 트래픽 복제/낭비($O(N)$)가 없어야 함 ($O(1)$ 대역폭 유지).
* **동적 확장성 (Scalability)**: 카프카 토픽의 파티션 개수 변경이나 리밸런싱(Rebalance)에 구애받지 않고 노드를 자유롭게 증설/축소할 수 있어야 함.
* **운영 단순성 & 결합도**: 미들웨어 의존성과 시스템 장애 전파 범위를 최소화해야 함.

---

## 3. 검토된 대안들 (Considered Options)

### 옵션 1: Host ID 기반 Redis Pub/Sub 유니캐스트 라우팅 (Selected)
* **원리**: 게이트웨이 각 노드가 부팅 시 유니크한 `Host ID`(예: `kotlin-node-1`)를 생성하고 Redis 채널 `host:kotlin-node-1`을 구독합니다. 파이썬 워커 및 카프카 메시지에 `hostId`를 유지시켜 반환하며, 메시지를 소비한 노드가 타겟 `hostId`에 해당하는 Redis 채널로 1:1 전송(Publish)합니다.
* **장점**: 목적지 노드로만 정확히 1번 메시지가 전파되므로 트래픽 낭비가 전혀 없음 ($O(1)$). 노드 동적 스케일아웃이 매우 자유로움.
* **단점**: Redis 미들웨어가 필수적이며 타겟 노드가 아닐 경우 1단계의 추가 홉(Hop) 발생 (+1~3ms Latency).

### 옵션 2: Kafka 파티션 키 지정 직접 라우팅 (Partition-aware Routing)
* **원리**: 코틀린 노드 수와 카프카 응답 토픽의 파티션 수를 1:1 매핑하고, 파이썬 워커가 파티션 키(Partition Key)를 지정하여 해당 노드로 메시지를 직통 배달합니다.
* **장점**: Redis 미들웨어 없이 카프카만으로 통신 가능하며 최단 Latency 확보.
* **단점**: 노드 수 증설 시 카프카 파티션 개수 변경 및 Consumer Rebalance에 종속되어 확장성이 떨어짐.

### 옵션 3: Redis 글로벌 브로드캐스트 (Global Broadcast)
* **원리**: 모든 코틀린 노드가 단일 글로벌 Redis 채널을 구독하고 메시지를 수신한 노드가 일단 전체 전파한 뒤 각자 `sessionId`를 비교하여 버리거나 전송합니다.
* **장점**: 구현이 극도로 단순함.
* **단점**: 노드가 N대일 경우 트래픽이 N배 복제 전파되어 CPU 및 대역폭 낭비가 심각함 ($O(N)$).

### 옵션 4: L7 Sticky Session + Direct Dynamic SSE URL
* **원리**: L7 로드밸런서(Nginx/ALB)의 Sticky Session 또는 동적 SSE URL을 부여하여 클라이언트를 특정 노드에 고정시킵니다.
* **단점**: 카프카 Consumer Group이 응답 메시지를 가져가는 노드가 여전히 임의의 노드가 될 수 있어 게이트웨이 내부 라우팅 문제는 해결되지 않음.

---

## 4. 최종 의사결정 (Decision Outcome)

**결정: 옵션 1 (Host ID 기반 Redis Pub/Sub 유니캐스트 라우팅) 채택**

### 선택 이유 (Rationale)
1. **확장성 및 유연성**: 카프카 파티션 개수나 리밸런싱 이슈에 영향을 받지 않고 코틀린 게이트웨이 컨테이너를 자유롭게 스케일아웃(Scale-out)할 수 있습니다.
2. **자원 효율성**: $O(1)$ 유니캐스트 방식으로 메시지가 타겟 서버에만 1회 배달되므로 브로드캐스트 대비 네트워크 대역폭과 memory/CPU 부하를 대폭 절감할 수 있습니다.
3. **인프라 정합성**: 실시간 AI 에이전트 서빙 환경에서 Redis는 캐싱 및 세션 상태 관리 목적으로 표준적으로 도입되므로 추가 인프라 부담이 적습니다.

---

## 5. 아키텍처에 미치는 영향 (Consequences)

### 긍정적 영향 (Positive)
* 게이트웨이 서버를 2대 이상 다중 포트로 띄우는 E2E 분산 환경 테스트 가능.
* 파이썬 에이전트는 게이트웨이 서버의 수나 구조를 몰라도 메시지에 포함된 `hostId`만 원본 그대로 돌려주면 되므로 완전한 마이크로서비스 결합도 격리 달성.

### 수반되는 작업 (Follow-ups)
* **Kotlin Gateway**:
  * 부팅 시 UUID 기반 `Host ID` 생성 로직 작성 (`kotlin-node-{UUID}`)
  * `ReactiveRedisTemplate`을 이용해 본인 `Host ID` 채널 구독(Subscribe) 로직 구현
  * Kafka 수신 시 `hostId` 비교 후 `ReactiveRedisTemplate.convertAndSend()` 호출 구현
* **Python Agent**:
  * 카프카 응답 메시지 세팅 시 요청 메타데이터의 `hostId` 및 `sessionId` 필드 유지 필수화
