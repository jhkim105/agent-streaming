# Redis Stream vs Kafka 성능 및 특성 비교

* **작성일 (Date)**: 2026-08-08
* **관련 문서**: [ADR 0003 버킷팅 라우팅](../adr/0003-redis-streams-bucketing-and-dynamic-session-routing.md) | [시스템 아키텍처](../2.architecture.md)

---

## 1. 한 줄 요약

| | Redis Stream | Kafka |
|---|---|---|
| **속도 (단일 메시지 지연)** | 🟢 **마이크로초 단위 (< 1ms)** | 🟡 밀리초 단위 (2~10ms+) |
| **처리량 (Throughput)** | 🟡 수십만 msg/s (메모리 제한) | 🟢 **수백만 msg/s (디스크 분산)** |
| **적합 규모** | 중소형 (인스턴스당 GB 단위) | 대형 (TB 단위 무제한) |
| **운영 복잡도** | 🟢 낮음 | 🔴 높음 |

---

## 2. 속도(지연, Latency) 비교

### Redis Stream이 Kafka보다 빠른 이유

Redis Stream은 **모든 데이터를 RAM에 저장**합니다. 반면 Kafka는 **디스크 기반 Append-only Log**를 사용합니다.

```
Redis Stream 쓰기 경로:
  XADD → 메모리 직접 기록 → 완료
  평균 지연: 0.1 ~ 0.5 ms

Kafka 쓰기 경로:
  Produce → 네트워크 → Broker → OS 페이지 캐시 → 디스크 fsync → ACK
  평균 지연: 2 ~ 10 ms (acks=all, 복제 완료 대기 시)
```

### Kafka 지연이 길어지는 주요 원인

| 원인 | 설명 |
|------|------|
| **복제 지연 (Replication)** | `acks=all` 설정 시 모든 ISR(In-Sync Replica)의 ACK를 기다림 |
| **배치 전송 (Batching)** | `linger.ms` 설정에 따라 메시지를 일정 시간 버퍼링 후 묶어 전송 |
| **네트워크 왕복 (RTT)** | Broker가 별도 JVM 프로세스로 분리되어 네트워크 hop 발생 |
| **Consumer Poll 주기** | Consumer가 `poll()` 간격마다 배치로 수신하므로 즉시 수신 보장 불가 |

### 지연 측정 참고 벤치마크 (공개 자료 기반)

```
환경: 동일 로컬 네트워크, 단일 브로커/노드, 메시지 크기 1KB

Redis Stream XADD p99:   0.3 ms
Redis Stream XREAD p99:  0.5 ms

Kafka Produce p99:        5 ms   (acks=1)
Kafka Produce p99:       15 ms   (acks=all, RF=3)
Kafka Consume p99:        8 ms   (poll.interval 기준)
```

> **결론**: 단일 메시지 **End-to-End 지연**은 Redis Stream이 Kafka보다 **약 10~30배 빠릅니다**.

---

## 3. 처리량(Throughput) 비교

### Kafka가 Redis Stream보다 처리량이 높은 이유

Kafka의 핵심 설계는 **순차 디스크 I/O + 파티션 수평 확장**입니다.

```
Kafka 처리량 확장 구조:
  Topic 파티션 수 ↑  →  Consumer 병렬 처리 수 ↑  →  Throughput ↑
  브로커 노드 수 ↑   →  디스크 I/O 분산        →  Throughput ↑

Redis Stream 처리량 제약:
  단일 Redis 노드 메모리: 수십 GB
  Redis Cluster 사용 시 확장 가능하나 Stream Key 샤딩이 복잡해짐
```

| 항목 | Redis Stream | Kafka |
|------|-------------|-------|
| 단일 노드 최대 처리량 | 수십만 msg/s | 수십만 ~ 백만 msg/s |
| 클러스터 최대 처리량 | 제한적 (메모리) | 수백만 msg/s (디스크 무제한 확장) |
| 메시지 보존 기간 | 메모리 한도 내 (MAXLEN 필요) | 설정에 따라 영구 보존 가능 |

---

## 4. 본 프로젝트 관점에서의 선택 근거

### 현재 아키텍처에서 두 기술이 모두 사용되는 이유

```
[ Python Agent ] ---(Kafka)---> [ Kotlin Server ] ---(Redis Stream)---> [ Client SSE ]
                  ↑ 왜 Kafka?                        ↑ 왜 Redis Stream?
```

### Kafka를 Agent ↔ Kotlin 구간에 사용하는 이유

| 이유 | 설명 |
|------|------|
| **독립적 확장성** | Python Worker와 Kotlin Server를 완전히 분리하여 독립 스케일링 가능 |
| **내구성 (Durability)** | AI 에이전트 응답은 유실 시 재실행 비용이 큼 → 디스크 기반 내구성 필요 |
| **Consumer Group 재처리** | 장애 시 오프셋 기반으로 정확한 재처리 가능 |
| **다양한 Consumer** | 향후 로깅, 분석 파이프라인 등 다수 Consumer 추가 용이 |

### Redis Stream을 Kotlin ↔ Client 구간에 사용하는 이유 (ADR 0003)

| 이유 | 설명 |
|------|------|
| **초저지연 필요** | SSE 토큰 스트리밍은 ms 단위 지연도 사용자에게 체감됨 |
| **커넥션 상한 고정** | 버킷팅으로 16개 커넥션으로 수만 대화 처리 (Kafka로는 불가) |
| **세션 라우팅 동반** | Redis Session Registry와 같은 인프라에서 원자적으로 처리 가능 |
| **TTL 기반 자동 정리** | 완결된 스트림의 자동 메모리 회수가 간단 |

---

## 5. 각 기술의 적합한 사용 시나리오

### Redis Stream이 유리한 경우 ✅

- **실시간 채팅, SSE 토큰 스트리밍** 처럼 **ms 이하 지연**이 중요할 때
- **중소규모** 동시 접속 (수만 세션 이하)
- Kafka 없이 단일 인프라로 메시지 큐 + 세션 관리를 함께 구현할 때
- 이미 Redis를 캐시/세션 저장소로 쓰고 있어 **인프라 추가 없이** 도입할 때

### Kafka가 유리한 경우 ✅

- **수백만 msg/s 이상** 대규모 이벤트 처리가 필요할 때
- **장기 메시지 보존** (며칠~수년)이 필요한 감사 로그, 이벤트 소싱
- **다수의 독립적인 Consumer**가 동일 이벤트를 각자 소비해야 할 때 (분석, 알림, 저장 등)
- **정확한 오프셋 기반 재처리**가 필요한 미션 크리티컬 데이터 파이프라인

---

## 6. 핵심 기술 특성 전체 비교표

| 비교 항목 | Redis Stream | Kafka |
|-----------|-------------|-------|
| **저장 매체** | 메모리 (RAM) | 디스크 (SSD/HDD) |
| **단일 메시지 지연** | < 1 ms | 2~15 ms |
| **최대 처리량** | 수십만 msg/s | 수백만 msg/s |
| **메시지 보존** | 메모리 한도 (MAXLEN) | 설정에 따라 영구 |
| **Consumer Group** | ✅ 지원 (XREADGROUP) | ✅ 지원 |
| **메시지 재처리** | ID 기반 XRANGE | 오프셋 기반 Seek |
| **At-least-once** | ✅ (ACK 기반 PEL) | ✅ (오프셋 커밋) |
| **Exactly-once** | ❌ (별도 구현 필요) | ✅ (트랜잭션 Producer) |
| **수평 확장** | Redis Cluster (복잡) | 파티션 추가 (간단) |
| **운영 복잡도** | 낮음 | 높음 (ZooKeeper/KRaft 등) |
| **순서 보장** | 단일 키 내 보장 | 파티션 내 보장 |
| **TTL 자동 정리** | ✅ EXPIRE 가능 | ❌ (수동 정책 필요) |
| **세션/캐시 통합** | ✅ (동일 Redis 인스턴스) | ❌ (별도 인프라) |
| **적합 규모** | 중소형 | 중대형 |

---

## 7. 결론 및 선택 가이드

```
Q: 단순히 "속도"만 본다면?
A: Redis Stream이 Kafka보다 약 10~30배 빠릅니다.
   (RAM vs 디스크라는 근본적 차이 때문)

Q: 그럼 항상 Redis Stream을 써야 하나?
A: 아닙니다. 대용량 내구성 파이프라인, 다중 Consumer 팬아웃,
   장기 보존이 필요하면 Kafka가 적합합니다.

Q: 본 프로젝트에서의 선택은?
A: Kafka(내구성, Agent-Server 분리) + Redis Stream(저지연, 세션 라우팅)의
   역할 분담 아키텍처가 최선입니다.
   두 기술은 경쟁 관계가 아닌 보완 관계입니다.
```
