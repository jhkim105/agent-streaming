# 🚀 ChatGPT 스타일 대화형 에이전트 & SSE/AGUI/A2UI 시스템 전환 기술 계획서

> **문서 생성일**: 2026-08-22  
> **프로젝트**: Agent Streaming PoC (`agent-streaming`)  
> **주요 목적**: 리서처 전용 단방향 보고서 아키텍처에서 탈피하여, ChatGPT 스타일의 범용 대화형 UX와 SSE 스트리밍, AGUI/A2UI 선언적 동적 UI 렌더링, 4대 식별자 분산 라우팅 기술 PoC 체계로 대전환.

---

## 📌 1. 개요 및 배경

기존 시스템은 "특정 분석 보고서 생성"이라는 특화된 리서처 유즈케이스에 치우쳐 있어, **SSE 스트리밍 라우팅**, **4대 식별자 모델**, **AGUI(Agent Guided UI) / A2UI(Agent-to-UI) 선언적 UI 렌더링**과 같은 핵심 기술 PoC의 범용적 가치를 전달하기에 제한적이었습니다.

따라서 본 전환 작업을 통해 **ChatGPT와 같은 익숙하고 심플한 대화형 에이전트 UI/UX**로 대전환하여 아래의 핵심 기술 가치를 극대화합니다:

1. **ChatGPT형 멀티턴 대화 UX**: 
   - 좌측 사이드바(새 채팅, 이전 대화 목록, 스레드 전환)
   - 우측 대화 타임라인 말풍선 (User Message vs Agent Streaming Stream)
   - 하단 픽스된 플로팅 메세지 입력 바
2. **에이전트 사고과정(Thinking Process) 접기/펴기**: 
   - 에이전트의 내부 추론 단계(`STATUS`)를 ChatGPT의 'Think' 기능처럼 깔끔하게 표시
3. **인라인 A2UI 동적 대시보드 렌더링**: 
   - 에이전트 답변 메시지 내에 동적 메트릭 수치 카드 및 후속 행동 버튼(Human-in-the-Loop)을 선언적으로 삽입
4. **4대 식별자 기반 분산 SSE 라우팅**: 
   - `conversationId`, `commandId`, `connectionId`, `eventId` 체계를 통한 무유실 실시간 소켓 배달

---

## 🏗️ 2. 시스템 아키텍처 및 4대 식별자 모델

```text
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ ChatGPT Style Frontend (React SPA)                                      │
 │                                                                         │
 │ ┌───────────────────┐ ┌───────────────────────────────────────────────┐ │
 │ │ Left Sidebar      │ │ Right Main Chat Window                        │ │
 │ │ - [+ 새 채팅]     │ │ - User Message (오른쪽 말풍선)                │ │
 │ │ - 이전 대화 목록  │ │ - Agent Response (좌측 마크다운 토큰 스트림)  │ │
 │ │ - 세션 스레드 전환│ │ - Agent Thinking Accordion (사고과정)         │ │
 │ │                   │ │ - Inline A2UI Dashboard (동적 카드/버튼)      │ │
 │ └───────────────────┘ └───────────────────────────────────────────────┘ │
 └────────────────────────────────────┬────────────────────────────────────┘
                                      │ (1) POST /api/conversations (conversationId)
                                      │ (2) GET /api/conversations/{id}/events (connectionId)
                                      │ (3) POST /api/conversations/{id}/commands (commandId)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Agent Stream Server Cluster (Kotlin WebFlux + Coroutines)                       │
│  - SessionRegistry & RedisConnectionRegistry (다단계 라우팅)                     │
│  - ConversationHistoryStore (멀티턴 챗 타임라인 & A2UI 페이로드 영속성)          │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │ (4) Kafka agent-commands
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Python Agent Worker (LangGraph Multi-Turn Engine)                               │
│  - Query Processing -> LLM Streaming -> STATUS / CHUNK / A2UI_RENDER / DONE    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 3. 단계별 이행 플랜 (Step-by-Step Plan)

### Step 1: 관련 기술 설계 문서 전면 재정의
처음부터 이 요구사항으로 프로젝트가 시작된 것처럼 모든 기술 문서를 현행화합니다.
- `docs/requirements.md`: 대화형 에이전트, SSE 스트리밍, AGUI/A2UI 동적 UI 기능 정의
- `docs/architecture.md`: ChatGPT 멀티턴 타임라인 및 4대 식별자 분산 라우팅 시퀀스 정의
- `docs/spec.md`: 대화 세션, 메시지 페이로드, A2UI 스키마 및 REST/SSE 명세 정의
- `README.md`: 신규 ChatGPT 스타일 에이전트 시스템 안내서 재작성

### Step 2: 백엔드 & 파이썬 워커 데이터 모델 확충
- **`agent-stream-server`**: `ConversationHistoryStore.kt` 내 대화 스레드별 사용자 질문, 에이전트 마크다운 답변, STATUS 타임라인, A2UI 페이로드를 챗 메시지 구조체 리스트(`List<ChatMessageDto>`)로 확장 관리
- **`agent-worker`**:
  - `agent_graph.py`: 일반 질문 답변 LLM 스트리밍(`CHUNK`) + 사고과정(`STATUS`) + 조건별 `A2UI_RENDER` 대시보드 생성
  - `main.py`: 멀티스레드 동시 수신 처리 유지

### Step 3: React SPA 프론트엔드 ChatGPT UI 대전환
- **`Sidebar.tsx`**: ChatGPT 룩앤필 좌측 패널 (`+ 새 채팅`, 최근 대화 목록)
- **`ChatThreadWindow.tsx`**:
  - 사용자 질문(우측 말풍선) vs 에이전트 답변(좌측 토큰 스트리밍)
  - 에이전트 사고과정(`Thinking Accordion`) 토글
  - 인라인 A2UI 대시보드 렌더링 (`A2UIRenderer.tsx`)
- **`ChatInputBar.tsx`**: 하단 중앙 고정 캡슐형 둥근 입력 바 (`Ask anything...`)
- **`App.tsx` & `App.css`**: ChatGPT 어두운 테마(#212121 / #171717) 기반 레이아웃 재구성
