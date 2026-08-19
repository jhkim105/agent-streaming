# Real-time AI Researcher Agent - 아키텍처 재정의 및 작업 내역

이 문서는 실시간 AI 리서처 에이전트 시스템의 라우팅 구조 개선, 도메인 모델 정립 및 API 경로 표준화 작업 내역을 정리한 문서입니다.

---

## 1. 개요 및 변경 배경

기존 시스템은 `sessionId`와 `conversationId` 2개의 식별자로 라우팅을 처리하였으나 다음과 같은 구조적 한계가 존재했습니다:

1. **라우팅 간격 및 다중 디바이스 이슈**: 동일 대화(`conversationId`)를 다중 디바이스 또는 다중 탭에서 접속 시, 특정 디바이스에서 전송한 질문의 응답이 물리적 소켓으로 올바르게 타깃팅되지 않거나 전체 브로드캐스팅되는 문제가 존재함.
2. **도메인 모델의 불명확성**: 요청 메시지와 에이전트 이벤트 간의 CQRS 구분 미흡.
3. **API 경로 표준화 미비**: `/api/chat/conversations` 등 불필요한 `/api/chat` 접두사 포함.

이를 해결하기 위해 **4대 핵심 식별자 모델**과 **CQRS 기반 `AgentCommand` / `AgentEvent` 도메인 모델**, 그리고 **`/api/conversations` RESTful 경로**로 시스템을 전면 재정의하고 구현에 반영하였습니다.

---

## 2. 4대 핵심 식별자 체계 (Identity Model)

| 식별자 | 명칭 | 발급 시점 | 범위 & 역할 |
|---|---|---|---|
| **`conversationId`** | 대화 스레드 ID | 새 대화 생성 시 (`POST /api/conversations`) | 대화 전체 생명주기 (대화 이력 저장 및 복원 기준) |
| **`commandId`** | AgentCommand ID | 명령 제출 시 (`POST /api/conversations/{id}/commands`) | 커맨드 1건 식별 및 이벤트 Correlation 기준 (`requestId` 대체) |
| **`connectionId`** | SSE 연결 소켓 ID | SSE 연결 수립 시 (`GET /api/conversations/{id}/events`) | 물리적 SSE 연결 식별자 (요청 디바이스 타깃팅 기준) |
| **`eventId`** | AgentEvent ID | Agent 이벤트 발행 시 (Python Worker) | 이벤트 1건 식별자 (W3C SSE `id` 및 미수신 복원 기준) |

### 계층 및 포함 관계
```text
Conversation C100
│
├── Connection CONN-A (PC 브라우저 탭 1)
│     ├── AgentCommand CMD-001 ("삼성전자 분석해줘")
│     │     ├── AgentEvent EVT-001 (AGENT_STARTED)
│     │     ├── AgentEvent EVT-002 (STATUS)
│     │     ├── AgentEvent EVT-003 (TOKEN)
│     │     └── AgentEvent EVT-004 (DONE)
│     │
│     └── AgentCommand CMD-003 ("추가 실적 알려줘")
│           └── AgentEvent EVT-008...
│
└── Connection CONN-B (모바일 앱 또는 탭 2)
      └── AgentCommand CMD-002 ("SK하이닉스 분석해줘")
            └── AgentEvent EVT-005...
```

---

## 3. 분산 연결 라우팅 체계 (Routing Flow)

다중 서버 노드(Scale-out) 환경에서도 질문을 전송한 물리적 소켓(`connectionId`)으로만 스트리밍 이벤트가 배달되도록 다단계 동적 라우팅을 구현하였습니다.

```text
commandId ➔ connectionId ➔ hostId ➔ Node-based Redis Stream ➔ Local SSE Connection
```

### Redis 데이터 키 구조
1. **`connection:host:{connectionId}`**: 해당 SSE 소켓 연결이 존재하는 코틀린 서버 노드 ID (`hostId`)
2. **`command:connection:{commandId}`**: 해당 명령(`commandId`)을 전송한 SSE 소켓 ID (`connectionId`)
3. **`stream:host:{hostId}`**: 서버 노드별 전용 무유실 릴레이 Redis Stream

---

## 4. REST API 엔드포인트 개편

| 기동 방식 | REST API 엔드포인트 | 설명 |
|---|---|---|
| `POST` | `/api/conversations` | 명시적 새 대화 스레드 생성 (`conversationId` 발급) |
| `GET` | `/api/conversations/{conversationId}/events` | 특정 대화의 실시간 SSE 스트림 연결 (`connectionId` 발급) |
| `POST` | `/api/conversations/{conversationId}/commands` | 해당 대화에 `AgentCommand` 제출 (`commandId` 발급) |
| `GET` | `/api/conversations` | 이전 대화 목록 요약 조회 |
| `GET` | `/api/conversations/{conversationId}` | 특정 대화의 전체 이력 및 완결 리포트 상세 조회 |

---

## 5. 컴포넌트별 작업 내역

### 5.1. Kotlin Agent Stream Server (`agent-stream-server`)
- **Controller 개편**: `ChatController` ➔ `ConversationController` 로 클래스명 및 API 경로 변경 (`/api/conversations`).
- **DTO 정제**: `AgentCommand`, `AgentCommandResponse`, `AgentEvent` DTO 정의 및 구버전 `ChatMessageRequest.kt` 삭제.
- **분산 레지스트리**: `RedisConnectionRegistry.kt` 구현 (`connection:host:` 및 `command:connection:` 매핑 등록/조회).
- **Service & Listener**:
  - `StreamService.kt`: `submitCommand` 및 `handleAgentEvent` (다단계 라우팅) 작성.
  - `KafkaEventListener.kt`: Kafka `agent-events` 토픽 구독.
- **Kafka 자동 토픽 생성 설정**: `AgentStreamConfig.kt`에 `topicCommands` 및 `topicEvents` `NewTopic` 빈 등록으로 `UNKNOWN_TOPIC_OR_PARTITION` 이슈 해결.
- **테스트 코드 정제**: Kotest 통합 테스트 100% 통과 (`./gradlew test`).

### 5.2. Python Agent Worker (`agent-worker`)
- **Kafka Topic 변경**: 수신 토픽 `agent-commands`, 송신 토픽 `agent-events` 적용 (`config.py`, `kafka_client.py`).
- **AgentEvent 발행**: 각 이벤트마다 `eventId` (`evt-...`) 부여 및 `command_id` 중심 라우팅 (`agent_graph.py`, `main.py`).
- **글로벌 주석 규칙 준수**: 파이썬 파일 라인 단위 친절한 한글 주석 적용.

### 5.3. React SPA Frontend (`frontend/`)
- **상태 및 통신 개편**: `useAgentStream.ts`에서 신규 API 규격 (`/api/conversations`, `/events`, `/commands`) 및 `connectionId`, `AgentCommand` 적용.
- **UI 및 빌드 검증**: `App.tsx` 타입 호환 정제 및 프로덕션 빌드 성공 (`npm run build`).

### 5.4. 문서화 (Docs)
- **문서명 표준화**: 파일명 순번 제거 (`requirements.md`, `architecture.md`, `spec.md`), 불필요한 `plan.md` 삭제.
- **아키텍처 및 기술 명세서 전면 갱신**: 4대 식별자, `AgentCommand`/`AgentEvent` CQRS 모델, 라우팅 시퀀스 다이어그램 반영.
