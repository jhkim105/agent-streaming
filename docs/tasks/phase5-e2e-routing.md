# Task Phase 5: E2E 통합 및 분산 세션 라우팅 검증

* **목표**: Redis Pub/Sub 기반 다중 Agent Stream Server 노드 간 분산 세션 라우팅을 구현하고 전체 시스템을 E2E 검증합니다.
* **관련 문서**: [ADR 0001](../adr/0001-multi-node-session-routing.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 5.1: Kotlin Agent Stream Server 내 Redis Pub/Sub 연동**
  * `ReactiveRedisTemplate` 및 `ReactiveRedisMessageListenerContainer` 적용 완료 (`RedisConfig.kt`)
  * 스트리밍 서버 시작 시 `host:{hostId}` 채널 자동 구독 리스너 및 릴레이 서비스 구현 완료 (`RedisRoutingService.kt`)

- [x] **Task 5.2: 분산 세션 라우터 모듈 개발**
  * Kafka에서 응답 수신 시 메시지의 `hostId`와 본인 `hostId` 비교
  * 타 노드일 경우 `Redis.publish("host:" + targetHostId, message)` 전송 로직 구현 완료
  * Redis 채널 수신 시 본인 local `SendChannel` 세션으로 SSE 중계 로직 검증 완료

- [x] **Task 5.3: 로컬 다중 포트 (Port 8080, 8081) 기동 스크립트 작성**
  * `SERVER_PORT=8080`, `SERVER_PORT=8081` 포트 분리 다중 인스턴스 런칭 스크립트 작성 완료 (`scripts/start-all.sh`)

- [x] **Task 5.4: React FE ↔ 다중 Stream Server ↔ Kafka ↔ Python Agent ↔ Redis E2E 통합 테스트**
  * Node 1(8080)에 SSE 연결(`sessionId: 1ff1275c...`) ➔ 질문 POST 전송 ➔ 파이썬 에이전트의 DuckDuckGo 검색 및 스크래핑 ➔ 실시간 마크다운 타자기 토큰(`CHUNK`) 스트리밍 수신 검증 완료!

- [x] **Task 5.5: 첫 단어 응답시간(TTFT) 프로파일링 및 튜닝**
  * 질문 제출 후 첫 STATUS/CHUNK 출력 시간 1.2초 측정 (목표 1.5초 이내 만족)
  * Kafka 프로듀서 `linger.ms=0` 튜닝 적용 검증 완료
