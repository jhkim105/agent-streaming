# Task Phase 3: Kotlin Agent Stream Server 개발 (`agent-stream-server`)

* **목표**: Coroutines Flow 기반 비동기 SSE 스트림 중계 및 세션 관리를 수행하는 Spring WebFlux 서버를 개발합니다.
* **글로벌 개발 규칙 준수**:
  * Spring Boot 3.3.2 및 Kotlin Idioms 준수
  * Slf4j 대신 **`kotlin-logging`** (7.0.0) 로깅 표준 사용
  * JUnit 대신 **`kotest`** BDD 스타일 테스트 작성
* **관련 문서**: [기술 명세서](../3.spec.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 3.1: Spring Boot 3.3.2 프로젝트 초기화**
  * Spring Boot 3.3.2 + Gradle (Kotlin DSL) 설정 완료 (`agent-stream-server/build.gradle.kts`)
  * `spring-boot-starter-webflux`, `kotlinx-coroutines-flow`, `spring-kafka`, `kotlin-logging`, `kotest` 의존성 적용 완료

- [x] **Task 3.2: `kotlin-logging` 로깅 패키지 및 환경 설정**
  * `KLogger` 표준 인스턴스 구성 및 로그 포맷 설정 완료

- [x] **Task 3.3: `Host ID` 및 Local Coroutine SSE 세션 레지스트리 개발**
  * 게이트웨이 기동 시 난수 UUID `Host ID` 생성 (`kotlin-node-{UUID}`) (`AgentStreamConfig.kt`)
  * `ConcurrentHashMap<String, SendChannel<ServerSentEvent<String>>>` 세션 맵 작성 완료 (`SessionRegistry.kt`)

- [x] **Task 3.4: `GET /api/chat/stream` Coroutine Flow SSE 컨트롤러 구현**
  * `callbackFlow` 핫 스트림 기반 `Flow<ServerSentEvent<String>>` 엔드포인트 구현 완료 (`ChatController.kt`)
  * 접속 시 `sessionId` 생성, 세션 레지스트리 등록 및 `type: INIT` 전송 구현
  * `awaitClose` 블록을 활용한 클라이언트 커넥션 끊김 시 세션 자원 자동 정리 구현 완료

- [x] **Task 3.5: `POST /api/chat/message` 컨트롤러 & Kafka Producer 구현**
  * `sessionId` 및 `query` 수신 ➔ 카프카 `agent-requests` 토픽으로 비동기 전송 및 `202 Accepted` 응답 구현 완료 (`StreamService.kt`)

- [x] **Task 3.6: Kafka `agent-responses` Reactive Listener 구현**
  * 카프카 응답 비동기 수신 및 세션 중계 서비스(`StreamService.kt`) 연결 완료 (`KafkaResponseListener.kt`)

- [x] **Task 3.7: `kotest` 기반 BDD 단위 테스트 작성**
  * `BehaviorSpec` 스타일 기반 세션 맵 생성/제거 및 상태 검증 빌드 통과 완료 (`SessionRegistryTest.kt`)
