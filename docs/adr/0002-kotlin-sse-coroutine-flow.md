# ADR 0002: Kotlin Coroutine Flow based SSE Implementation Strategy

* **Status**: Accepted (확정)
* **Date**: 2026-07-25
* **Authors**: Architecture & Engineering Team
* **Deciders**: Lead Architect & Developer

---

## 1. 배경 및 문제 정의 (Context)

Spring Boot WebFlux 백엔드에서 클라이언트와의 **SSE(Server-Sent Events) 스트리밍 소켓**을 구현할 때 선택 가능한 두 가지 핵심 리액티브 통신 패러다임이 존재합니다:
1. **Project Reactor API (`Flux<ServerSentEvent<T>>` / `Sinks.Many`)**: 자바/Spring 기본 비동기 리액티브 패브릭
2. **Kotlin Coroutines API (`Flow<ServerSentEvent<T>>` / `callbackFlow`)**: 코틀린 언어 표준 비동기 데이터 스트림 패브릭

게이트웨이 서버의 동시성 세션 관리, 메모리 세션 레지스트리 설계, 연결 종료 시의 세션 자원 정리(Cleanup) 및 코드 가독성을 다각도로 검토하여 표준 구현 방식을 확정해야 합니다.

---

## 2. 검토된 대안들 (Considered Options)

### 옵션 1: Project Reactor 순수 `Flux` (`Sinks.Many` / `FluxSink`) 방식
* **원리**: `Sinks.many().multicast().onBackpressureBuffer()` 객체를 사용해 세션을 등록하고 `sink.asFlux()`를 SSE 엔드포인트에서 반환합니다.
* **장점**: Spring WebFlux의 자바 기본 스펙과 동일하며 별도 코루틴 패키지 라이브러리 추가가 필요 없음.
* **단점**:
  * 리액티브 체이닝(`map`, `flatMap`, `doOnCancel`) 스타일로 인해 코드 가독성이 저하됨.
  * 클라이언트 연결 끊김 시 `doOnCancel`, `doOnError`, `doFinally` 콜백을 여러 개 달아줘야 해서 세션 자원 해제 코드가 복잡하고 세션 누수 위험 존재.
  * 에러 발생 시 리액티브 스택 트레이스가 길어져 디버깅이 어려움.

### 옵션 2: Kotlin Coroutine `Flow` (`callbackFlow` / `SendChannel`) 방식 (Selected)
* **원리**: `callbackFlow { ... }` 핫 스트림 생성기 내부에서 `channel`을 세션 레지스트리에 등록하고 `Flow<ServerSentEvent<T>>`를 반환합니다.
* **장점**:
  * **Kotlin Idiomatic (코틀린 관례 준수)**: 명령형 스타일(`try-catch`, `suspend`)로 코드를 작성할 수 있어 가독성이 압도적임.
  * **완벽한 자원 정리 (`awaitClose`)**: `awaitClose { ... }` 블록 하나만 정의하면 클라이언트가 브라우저 창을 닫거나 커넥션이 끊길 때 **자동으로 실행되어 세션 해제 보장**.
  * **Spring WebFlux Native 지원**: Spring Boot 2.2+ / Spring 5.2+ 이상부터 `Flow` 및 `suspend` 컨트롤러를 최우선(First-class) 지원함.
* **단점**: `kotlinx-coroutines-core` 및 `kotlinx-coroutines-reactor` 의존성 추가 필요 (Spring Boot Starter WebFlux 포함).

---

## 3. 최종 의사결정 (Decision Outcome)

**결정: 옵션 2 (Kotlin Coroutine `Flow` / `callbackFlow` 방식) 채택**

### 선택 이유 (Rationale)
1. **자원 해제 안전성 (`awaitClose`)**: SSE 연결 끊김 시 메모리 세션 레지스트리(`ConcurrentHashMap`)에서 세션을 제거하는 `awaitClose` 자원 해제 메커니즘이 가장 직관적이고 안전합니다.
2. **글로벌 개발 규칙 부합**: 코틀린 관례(Kotlin Idioms)를 준수하고 보일러플레이트 콜백 체이닝 코드를 대폭 줄일 수 있습니다.
3. **디버깅 용이성 및 Kotest 연동**: Coroutine 스코프 기반의 디버깅 및 `kotest` 테스트 작성 시 `flow.first()`, `flow.toList()` 등의 우수한 테스트 유틸리티 활용 가능.

---

## 4. 아키텍처 및 구현에 미치는 영향 (Consequences)

### 게이트웨이 세션 레지스트리 구조 변경
* **이전 (Reactor)**: `ConcurrentHashMap<String, Sinks.Many<ServerSentEvent<String>>>`
* **변경 (Coroutine Flow)**: `ConcurrentHashMap<String, SendChannel<ServerSentEvent<String>>>`

### SSE 컨트롤러 엔드포인트 변경
```kotlin
@GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
fun streamEvents(): Flow<ServerSentEvent<String>> = callbackFlow {
    val sessionId = UUID.randomUUID().toString()
    sessionRegistry.register(sessionId, this.channel)
    
    // 초기 INIT 전송
    trySend(ServerSentEvent.builder<String>().event("INIT").data("""{"sessionId":"$sessionId"}""").build())
    
    awaitClose {
        sessionRegistry.remove(sessionId)
    }
}
```
