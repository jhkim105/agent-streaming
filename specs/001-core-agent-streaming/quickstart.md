# 빠른 검증 및 실행 가이드 (Quickstart & Validation Guide)

**대상 스펙**: [spec.md](./spec.md)  
**작성일자**: 2026-09-12  

---

## 🚀 1. 사전 준비 (Prerequisites)
- Docker & Docker Compose (Kafka, Zookeeper, Redis)
- JDK 21+ (Spring Boot 3.x)
- Python 3.11+ (uv 설치 필수)
- Node.js 18+ (pnpm 또는 npm)

---

## 🧪 2. E2E 엔드투엔드 검증 시나리오

### 1단계: 인프라 및 서비스 기동
```bash
# 1. 메시지 브로커 및 캐시 기동
docker compose up -d

# 2. 백엔드 게이트웨이 기동 (Port 8080)
cd agent-server && ./gradlew bootRun

# 3. Python 에이전트 런타임 기동
cd agent-runtime && uv run python main.py

# 4. 프론트엔드 기동 (Port 5173)
cd frontend && npm run dev
```

### 2단계: 핵심 기능 검증 절차
1. **대화 생성**: 브라우저에서 `http://localhost:5173` 접속 후 `[+ 새 채팅]` 클릭 ➔ 신규 `conversationId` 발급 확인.
2. **질문 전송**: `"AGUI 스트리밍 알려줘"` 입력 및 전송 ➔ 우측 말풍선 즉시 생성.
3. **사고과정 검증**: 좌측 Thinking Accordion에 `STATUS` 이벤트 실시간 누적 확인.
4. **마크다운 스트리밍 검증**: 마크다운 본문이 타자기 효과로 스트리밍 렌더링되는지 확인.
5. **AGUI 인터랙션 검증**: 하단에 지표 카드 및 후속 추천 버튼 노출 확인 ➔ 추천 버튼 클릭 시 후속 답변이 동일 스레드에서 이어지는지 확인.
6. **히스토리 복원 검증**: 좌측 사이드바에서 이전 대화를 클릭했을 때 본문, 타임라인, AGUI 카드가 완벽히 복원되는지 확인.
