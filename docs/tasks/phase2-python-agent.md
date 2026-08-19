# Task Phase 2: Python 에이전트 & Kafka 연동 개발

* **목표**: LangGraph 기반 추론 엔진을 구축하고 연산 과정을 Kafka로 스트리밍 발행합니다.
* **글로벌 개발 규칙 준수**:
  * 초고속 패키지 관리 도구 **`uv`** 사용 (`uv venv`, `uv pip install`)
  * 기초 문법 및 노드 제어 로직에 대해 **라인 단위 상세 한글 주석** 필수 작성
* **관련 문서**: [기술 명세서](../spec.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 2.1: `uv` 환경 구축 및 의존성 다운로드**
  * `uv venv` 생성 및 가상환경 활성화 완료
  * `pyproject.toml` 작성: `langgraph`, `langchain`, `duckduckgo-search`, `confluent-kafka`, `pydantic` 패키지 설치 완료

- [x] **Task 2.2: Kafka Producer/Consumer 래퍼 모듈 개발**
  * `agent-requests` 토픽을 바인딩하는 Kafka Consumer 모듈 (`kafka_client.py`) 작성 완료
  * `agent-responses` 토픽으로 JSON 메시지(`STATUS`, `CHUNK`, `DONE`, `ERROR`)를 전송하는 Kafka Producer 모듈 작성 완료

- [x] **Task 2.3: `duckduckgo-search` 기반 웹 검색 도구 개발**
  * 질문 텍스트로부터 최신 검색어 수집 및 결과 URL/초록 리스트 파싱 도구 (`search_tool.py`) 작성 완료

- [x] **Task 2.4: BeautifulSoup 기반 웹 페이지 스크래퍼 개발**
  * URL 본문을 가져와 마크다운 텍스트로 변환하는 도구 (`scraper_tool.py`) 작성 완료

- [x] **Task 2.5: LangGraph 추론제어 그래프 설계 및 작성**
  * State 정의: `session_id`, `host_id`, `query`, `search_results`, `scraped_texts`, `final_report`
  * 그래프 노드 정의 (`agent_graph.py`): `Query Analysis` ➔ `Search` ➔ `Scrape` ➔ `Report Generation`

- [x] **Task 2.6: 추론 이벤트 발행 핸들러 작성**
  * 노드 전환 시 `type: STATUS` 메시지 Kafka 전송
  * LLM 토큰 생성 시 `type: CHUNK` 메시지 Kafka 실시간 타자기 전송
  * 예외 시 `type: ERROR`, 완료 시 `type: DONE` 전송 구현 완료

- [x] **Task 2.7: 라인 단위 한글 주석 작성 및 Python 워커 단독 테스트**
  * 소스 코드 전반에 파이썬 기초 라인 단위 한글 주석 명시 완료
  * 파이썬 모듈 및 DuckDuckGo 웹 검색 도구 실행 검증 완료
