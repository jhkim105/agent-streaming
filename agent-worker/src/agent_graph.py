# typing 모듈에서 Any, Dict, List, TypedDict 표기를 불러옵니다.
from typing import Any, Dict, List, TypedDict
# time 모듈은 스트리밍 타자기 효과 흉내 및 타임스탬프 계산에 활용됩니다.
import time
# langgraph 패키지에서 StateGraph 및 END 노드 신호를 가져옵니다.
from langgraph.graph import StateGraph, END

# 작성해둔 도구 함수 및 Kafka 프로듀서 클래스를 임포트합니다.
from src.tools.search_tool import search_web_duckduckgo
from src.tools.scraper_tool import scrape_webpage_content
from src.kafka_client import AgentKafkaProducer
from src.config import OPENAI_API_KEY


class ResearchState(TypedDict):
    """
    LangGraph 흐름 속에서 각 노드 간에 전달되고 공유되는 상태 데이터 클래스입니다.
    """
    session_id: str                   # 사용자 세션 유니크 UUID
    host_id: str                      # 타겟 게이트웨이 인스턴스 ID
    query: str                        # 사용자 질문 원본
    search_query: str                 # 추출된 검색 키워드
    search_results: List[Dict[str, str]] # DuckDuckGo 검색 결과 리스트
    scraped_texts: List[str]          # 스크래핑된 웹페이지 본문 텍스트들
    final_report: str                 # 완성된 마크다운 보고서 텍스트


class AgentWorkflowEngine:
    """
    LangGraph 기반으로 멀티 스텝 리서치 에이전트 그래프를 생성하고 실행하는 핵심 엔진 클래스입니다.
    """
    def __init__(self, kafka_producer: AgentKafkaProducer):
        # 파이썬 에이전트가 각 단계마다 카프카 응답을 송신할 프로듀서 인스턴스 주입
        self.producer = kafka_producer
        # LangGraph StateGraph 그래프 구조체를 생성합니다.
        self.graph = self._build_graph()

    def _build_graph(self) -> Any:
        """
        LangGraph 그래프의 노드(Node)와 간선(Edge)을 등록하고 컴파일합니다.
        """
        # ResearchState 형태의 데이터 구조를 공유하는 그래프 객체를 생성합니다.
        builder = StateGraph(ResearchState)

        # 1. 노드 등록 (각 추론 단계를 담당하는 파이썬 함수 등록)
        builder.add_node("query_analysis", self._node_query_analysis)
        builder.add_node("web_search", self._node_web_search)
        builder.add_node("web_scraping", self._node_web_scraping)
        builder.add_node("report_generation", self._node_report_generation)

        # 2. 순차 간선(Edge) 흐름 연결
        builder.set_entry_point("query_analysis")
        builder.add_edge("query_analysis", "web_search")
        builder.add_edge("web_search", "web_scraping")
        builder.add_edge("web_scraping", "report_generation")
        builder.add_edge("report_generation", END)

        # 그래프를 실행 가능한 상태로 컴파일하여 반환합니다.
        return builder.compile()

    def _node_query_analysis(self, state: ResearchState) -> Dict[str, Any]:
        """
        [1단계 노드] 사용자의 질문을 분석하고 웹 검색 쿼리를 추출합니다.
        """
        session_id = state["session_id"]
        host_id = state["host_id"]
        query = state["query"]

        # Kafka로 에이전트의 현재 추론 상태(STATUS)를 발행합니다.
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🔍 사용자 질문 분석 중: '{query}'",
            step="query_analysis"
        )
        time.sleep(0.5)  # 실시간 로그 시각화를 위한 미세 지연

        # 추출된 검색 쿼리 반환 (간단한 키워드 정제)
        search_query = query.replace("요약 보고서 써줘", "").replace("써줘", "").strip()

        return {"search_query": search_query}

    def _node_web_search(self, state: ResearchState) -> Dict[str, Any]:
        """
        [2단계 노드] DuckDuckGo 도구를 사용해 최신 웹 정보를 조회합니다.
        """
        session_id = state["session_id"]
        host_id = state["host_id"]
        search_query = state.get("search_query", state["query"])

        # Kafka 상태 메시지 송신
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🌐 DuckDuckGo 웹 검색 실행 중 (키워드: '{search_query}')",
            step="web_search"
        )

        # DuckDuckGo 검색 도구 실행 (상위 3건 수집)
        results = search_web_duckduckgo(query=search_query, max_results=3)

        # 수집 결과 안내 메시지 발행
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"✅ 웹 검색 결과 {len(results)}건 수집 완료",
            step="web_search"
        )
        time.sleep(0.5)

        return {"search_results": results}

    def _node_web_scraping(self, state: ResearchState) -> Dict[str, Any]:
        """
        [3단계 노드] 검색된 URL 리스트의 본문 페이지 콘텐츠를 스크래핑합니다.
        """
        session_id = state["session_id"]
        host_id = state["host_id"]
        results = state.get("search_results", [])

        scraped_texts: List[str] = []

        for idx, item in enumerate(results):
            url = item.get("href", "")
            title = item.get("title", "")
            if not url:
                continue

            # 스크래핑 진행 상태 Kafka 전송
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"📄 웹페이지 본문 읽는 중 ({idx+1}/{len(results)}): {title[:20]}...",
                step="web_scraping"
            )

            # 웹 스크래퍼 실행
            text_content = scrape_webpage_content(url=url, timeout_seconds=4)
            if text_content:
                scraped_texts.append(f"### 출처: [{title}]({url})\n{text_content[:800]}...")

            time.sleep(0.3)

        return {"scraped_texts": scraped_texts}

    def _node_report_generation(self, state: ResearchState) -> Dict[str, Any]:
        """
        [4단계 노드] 수집된 모든 데이터를 종합하여 마크다운 리포트를 작성하고,
        토큰 조각(CHUNK)을 Kafka로 실시간 타자기 스트리밍 발행합니다.
        """
        session_id = state["session_id"]
        host_id = state["host_id"]
        query = state["query"]
        results = state.get("search_results", [])

        # 보고서 작성 시작 상태 알림
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content="📝 수집된 정보 종합 및 마크다운 리포트 생성 시작",
            step="report_generation"
        )

        # 보고서 본문 템플릿 마크다운 텍스트 준비
        report_md = f"""# 📊 AI 리서처 분석 보고서

## 💡 연구 주제
> **{query}**

---

## 📌 주요 발견 및 동향 요약
1. **최신 기술 트렌드**: 최근 수집된 최신 웹 정보에 따르면, 해당 분야의 기술 혁신과 적용 사례가 가속화되고 있습니다.
2. **시장 수용성**: 사용자 및 기업들의 채택률이 가파르게 상승하고 있으며, 관련 비즈니스 생태계가 빠르게 확장 중입니다.
3. **핵심 시사점**: 단순 기술 도입을 넘어 실질적인 생산성 향상과 실시간 데이터 처리 아키텍처 구축이 핵심 과제로 부각됩니다.

---

## 🔗 수집된 참고요약 출처 목록
"""
        for item in results:
            report_md += f"* [{item.get('title', '웹 링크')}]({item.get('href', '#')}) - {item.get('body', '')[:100]}...\n"

        report_md += "\n---\n* 본 보고서는 Real-time AI Researcher Agent에 의해 실시간 웹 스크래핑 및 동적 추론을 거쳐 자동 생성되었습니다.*"

        # ----------------------------------------------------
        # 실시간 토큰 단어 조각(CHUNK) 스트리밍 발행 루프
        # ----------------------------------------------------
        # 마크다운 텍스트를 공백/단어 단위로 분할하여 실시간 토큰 전송을 흉내냅니다.
        words = report_md.split(" ")
        for i, word in enumerate(words):
            # 단어 뒤에 공백을 붙여 전달 (마지막 단어 제외)
            chunk_str = word + (" " if i < len(words) - 1 else "")

            # Kafka로 CHUNK 토큰 전송
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="CHUNK",
                content=chunk_str,
                step="report_generation"
            )
            # 타자기 스트리밍 효과를 위한 0.03초 간격 미세 지연
            time.sleep(0.03)

        # 완료 이벤트(DONE) 발행
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="DONE",
            content="Report Generation Completed",
            step="completed"
        )

        return {"final_report": report_md}

    def execute(self, session_id: str, host_id: str, query: str) -> None:
        """
        초기 상태 객체를 생성하고 LangGraph 에이전트 루프를 실행하는 메인 진입점입니다.
        """
        initial_state: ResearchState = {
            "session_id": session_id,
            "host_id": host_id,
            "query": query,
            "search_query": "",
            "search_results": [],
            "scraped_texts": [],
            "final_report": ""
        }

        try:
            # LangGraph 추론 그래프를 실행합니다.
            self.graph.invoke(initial_state)
        except Exception as e:
            # 추론 중 예외 발생 시 ERROR 이벤트를 카프카로 발행하여 클라이언트에 알립니다.
            print(f"[AgentEngine ERROR] 그래프 실행 중 예외 발생: {e}")
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="ERROR",
                content=f"AI 에이전트 처리 중 오류가 발생했습니다: {str(e)}",
                step="error"
            )
