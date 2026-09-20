# 🗄️ 데이터 저장소 스키마 및 캐시 정의서 (Storage & Cache Schema)

본 문서는 **대화 세션 영속성 스토리지(RDB/Memory)**와 **분산 라우팅 캐시(Redis)**의 데이터 구조를 정의합니다.

---

## 🏛️ 1. Conversation & Run 스토리지 모델

현재 `agent-server`의 [ConversationHistoryStore](file:///Users/jihwankim/workspace/agent-streaming/agent-server/src/main/kotlin/com/agent/stream/service/ConversationHistoryStore.kt)를 통해 인메모리 관리되며, RDB(PostgreSQL/MySQL 등) 전환 시 아래 스키마를 따릅니다.

### 1) `conversations` (대화 스레드)
| 컬럼명 | 타입 | Nullable | 설명 |
|---|---|---|---|
| `conversation_id` | `VARCHAR(64)` | PK | 대화 고유 ID (`conv-xxxx`) |
| `title` | `VARCHAR(255)` | NOT NULL | 대화 제목 (첫 질문 또는 DONE 시 스마트 타이틀) |
| `category` | `VARCHAR(50)` | NOT NULL | 대화 카테고리 (`general`, `tech`, `business`) |
| `created_at` | `BIGINT / TIMESTAMP` | NOT NULL | 생성 시각 (Epoch millis) |
| `updated_at` | `BIGINT / TIMESTAMP` | NOT NULL | 최종 수정 시각 (Epoch millis) |

### 2) `conversation_runs` (1턴 실행 단위)
| 컬럼명 | 타입 | Nullable | 설명 |
|---|---|---|---|
| `run_id` | `VARCHAR(64)` | PK | 턴 고유 ID (`run-xxxx`) |
| `conversation_id` | `VARCHAR(64)` | FK | 연관 대화 ID |
| `user_prompt` | `TEXT` | NOT NULL | 사용자 질문/액션 내용 |
| `full_report` | `LONGTEXT` | NULL | 에이전트 생성 마크다운 본문 |
| `a2ui_payload` | `JSON / TEXT` | NULL | 최종 A2UI 대시보드 JSON |
| `is_completed` | `BOOLEAN` | NOT NULL | 스트림 완결 여부 |
| `created_at` | `BIGINT / TIMESTAMP` | NOT NULL | 턴 시작 시각 |

### 3) `conversation_timeline_events` (사고과정 타임라인)
| 컬럼명 | 타입 | Nullable | 설명 |
|---|---|---|---|
| `id` | `BIGINT` | PK (Auto) | 이벤트 시퀀스 ID |
| `run_id` | `VARCHAR(64)` | FK | 연관 턴 ID (`run-xxxx`) |
| `sse_event_id` | `VARCHAR(64)` | NOT NULL | 이벤트 ID (`evt-xxxx`) |
| `message_id` | `VARCHAR(64)` | NOT NULL | 출력 블록 ID (`msg-xxxx`) |
| `status_text` | `TEXT` | NOT NULL | 사고 과정 메시지 (`🔍 검색 중...`) |
| `created_at` | `BIGINT / TIMESTAMP` | NOT NULL | 발생 시각 |

---

## ⚡ 2. Redis 캐시 및 라우팅 키 규격

| Key / Channel 패턴 | Data Type | TTL | 설명 |
|---|---|---|---|
| `conn:{connectionId}:host` | String | 1시간 | 해당 SSE 소켓이 연결된 게이트웨이 노드 호스트명 (`Host-1`) |
| `run:{runId}:conn` | String | 1시간 | 해당 턴(Run)을 발행한 클라이언트의 `connectionId` 매핑 |
| `stream:host:{hostname}` | Stream (XADD) | - | 특정 게이트웨이 호스트 노드로 전달되는 다중 노드 릴레이 스트림 |
