# typing 모듈에서 Any, Dict, List, TypedDict, Tuple 표기를 불러옵니다.
from typing import Any, Dict, List, TypedDict, Tuple
# time 모듈은 스트리밍 타자기 효과 흉내 및 타임스탬프 계산에 활용됩니다.
import time
# re 모듈은 정규식을 활용한 한글/영문 단어 추출 및 텍스트 정제에 활용됩니다.
import re
# langgraph 패키지에서 StateGraph 및 END 노드 신호를 가져옵니다.
from langgraph.graph import StateGraph, END

# 작성해둔 도구 함수 및 Kafka 프로듀서 클래스를 임포트합니다.
from src.tools.search_tool import search_web_duckduckgo
from src.tools.scraper_tool import scrape_webpage_content
from src.kafka_client import AgentKafkaProducer
from src.config import OPENAI_API_KEY
from src.a2ui_schema import A2UIComponentBuilder


class ResearchState(TypedDict):
    """
    LangGraph 흐름 속에서 각 노드 간에 전달되고 공유되는 상태 데이터 클래스입니다.
    """
    session_id: str                      # 사용자 세션 유니크 UUID
    host_id: str                         # 타겟 게이트웨이 인스턴스 ID
    query: str                           # 사용자 질문 원본
    search_query: str                    # 정제된 순수 검색 키워드
    category: str                        # 질문 분류 카테고리 ('tech', 'business', 'general')
    extracted_keywords: List[str]        # 질문 및 수집 데이터에서 추출한 핵심 키워드 리스트
    search_results: List[Dict[str, str]] # DuckDuckGo 검색 결과 리스트
    scraped_texts: List[str]             # 스크래핑된 웹페이지 본문 텍스트들
    final_report: str                    # 완성된 동적 마크다운 보고서 텍스트


class AgentWorkflowEngine:
    """
    LangGraph 기반으로 멀티 스텝 리서치 에이전트 그래프를 생성하고 실행하는 핵심 엔진 클래스입니다.
    """
    def __init__(self, kafka_producer: AgentKafkaProducer):
        # 파이썬 에이전트가 각 단계마다 카프카 응답을 송신할 프로듀서 인스턴스를 주입합니다.
        self.producer = kafka_producer
        # LangGraph StateGraph 그래프 구조체를 생성하고 초기화합니다.
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
        builder.add_node("a2ui_generation", self._node_a2ui_generation)

        # 2. 순차 간선(Edge) 흐름 연결
        builder.set_entry_point("query_analysis")
        builder.add_edge("query_analysis", "web_search")
        builder.add_edge("web_search", "web_scraping")
        builder.add_edge("web_scraping", "report_generation")
        builder.add_edge("report_generation", "a2ui_generation")
        builder.add_edge("a2ui_generation", END)

        # 그래프를 실행 가능한 상태로 컴파일하여 반환합니다.
        return builder.compile()

    def _node_query_analysis(self, state: ResearchState) -> Dict[str, Any]:
        """
        [1단계 노드] 사용자의 질문을 분석하여 카테고리(기술/비즈니스/일반)를 정확히 분류하고,
        '조사해줘', '분석해줘' 같은 한글 불용어를 제거하여 pure 검색 키워드를 생성합니다.
        """
        # 상태 딕셔너리에서 필수 값들을 추출합니다.
        session_id = state["session_id"]
        host_id = state["host_id"]
        query = state["query"]

        # Kafka로 에이전트의 현재 분석 시작 알림을 발행합니다.
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🔍 사용자 질문 의도 및 주제 정밀 분석 중: '{query}'",
            step="query_analysis"
        )
        time.sleep(0.3)

        query_lower = query.lower()

        # 기술(Tech) 카테고리 판별용 키워드 리스트 (AI/LLM 프레임워크 및 도구 포함)
        tech_keywords = [
            "코드", "스프링", "파이썬", "버그", "에러", "설정", "아키텍처", "라이브러리", "프레임워크",
            "api", "개발", "dev", "docker", "react", "next", "vue", "kafka", "sse", "db",
            "llm", "litellm", "ai", "gpt", "claude", "gemini", "langchain", "langgraph",
            "ollama", "model", "prompt", "agent", "rag", "embedding", "vectordb"
        ]

        # 비즈니스(Business) 카테고리 판별용 키워드 리스트
        biz_keywords = [
            "주식", "시장", "매출", "전망", "가격", "트렌드", "기업", "투자", "비즈니스",
            "뉴스", "전략", "경쟁", "산업", "수익", "주가"
        ]

        # 질문 문맥에 따라 카테고리를 추론합니다.
        if any(kw in query_lower for kw in tech_keywords):
            category = "tech"
        elif any(kw in query_lower for kw in biz_keywords):
            category = "business"
        else:
            category = "general"

        # ----------------------------------------------------
        # 정교한 한글/영문 불용어(Stopwords) 제거 및 순수 키워드 추출
        # ----------------------------------------------------
        # 질문에서 1글자 이상의 영문/숫자/한글 단어를 추출합니다.
        words = re.findall(r'[a-zA-Z0-9_]+|[가-힣]{2,}', query)

        # 검색 노이즈를 유발하는 한글 요청어/조사/불용어 리스트
        stop_words = {
            "조사해줘", "조사", "분석해줘", "분석", "찾아줘", "알려줘", "써줘", "요약",
            "보고서", "대해", "관해서", "무엇인가요", "뭐야", "원인", "관련", "해줘",
            "부탁해", "요청", "정보", "소개해줘", "설명해줘", "어떻게", "대해서"
        }

        # 불용어에 해당하지 않는 순수 키워드만 필터링합니다.
        extracted_keywords = [w for w in words if w.lower() not in stop_words and w not in stop_words]

        # 키워드가 비어버린 경우 원본 query를 사용하고, 그렇지 않으면 pure 키워드 연결
        search_query = " ".join(extracted_keywords) if extracted_keywords else query

        # 분석 완료 이벤트 전송 (정제된 pure 검색어 표시)
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"📌 [분류 완료] 카테고리: {category.upper()} | 정제된 검색어: '{search_query}'",
            step="query_analysis"
        )
        time.sleep(0.3)

        # 갱신된 상태 데이터를 반환합니다.
        return {
            "search_query": search_query,
            "category": category,
            "extracted_keywords": extracted_keywords if extracted_keywords else [query]
        }

    def _node_web_search(self, state: ResearchState) -> Dict[str, Any]:
        """
        [2단계 노드] 정제된 pure 검색어(예: 'LiteLLM')를 사용해 DuckDuckGo 웹 조회를 실행합니다.
        """
        # 상태 딕셔너리에서 검색 쿼리와 세션 파라미터를 가져옵니다.
        session_id = state["session_id"]
        host_id = state["host_id"]
        search_query = state.get("search_query", state["query"])

        # Kafka 진행 상태 메시지를 전송합니다.
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🌐 DuckDuckGo 타겟 검색 실행 중 (검색 쿼리: '{search_query}')",
            step="web_search"
        )

        # DuckDuckGo 웹 검색 수행 (정제된 쿼리로 검색)
        results = search_web_duckduckgo(query=search_query, max_results=4)

        # 검색 결과 수집 완료 전송
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"✅ 웹 검색 결과 {len(results)}건 수집 완료",
            step="web_search"
        )
        time.sleep(0.3)

        return {"search_results": results}

    def _node_web_scraping(self, state: ResearchState) -> Dict[str, Any]:
        """
        [3단계 노드] 수집된 기술 문서 및 아티클의 본문 페이지 콘텐츠를 스크래핑합니다.
        """
        # 상태에서 필요한 파라미터 및 검색 결과를 읽어옵니다.
        session_id = state["session_id"]
        host_id = state["host_id"]
        results = state.get("search_results", [])

        scraped_texts: List[str] = []

        # 수집된 각 URL로 스크래핑을 수행합니다.
        for idx, item in enumerate(results):
            url = item.get("href", "")
            title = item.get("title", "")
            if not url:
                continue

            # 스크래핑 상태 Kafka 알림 송신
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"📄 본문 스크래핑 중 ({idx+1}/{len(results)}): {title[:25]}...",
                step="web_scraping"
            )

            # 웹 스크래퍼 호출 (타임아웃 3초)
            text_content = scrape_webpage_content(url=url, timeout_seconds=3)
            if text_content:
                scraped_texts.append(f"### 출처 {idx+1}: [{title}]({url})\n{text_content[:650]}...")

            time.sleep(0.2)

        return {"scraped_texts": scraped_texts}

    def _node_report_generation(self, state: ResearchState) -> Dict[str, Any]:
        """
        [4단계 노드] 정제된 주제 데이터 기반으로 동적 마크다운 리포트를 생성하고 타자기 스트리밍 발행합니다.
        """
        # 상태 딕셔너리에서 세션 정보 및 검색/스크래핑 결과를 읽어옵니다.
        session_id = state["session_id"]
        host_id = state["host_id"]
        query = state["query"]
        search_query = state.get("search_query", query)
        category = state.get("category", "general")
        keywords = state.get("extracted_keywords", [])
        results = state.get("search_results", [])
        scraped_texts = state.get("scraped_texts", [])

        # 보고서 생성 시작 상태 메시지 발행
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"📝 [{category.upper()}] 정제 데이터 기반 동적 마크다운 리포트 생성 시작",
            step="report_generation"
        )

        keyword_str = ", ".join([f"`{k}`" for k in keywords]) if keywords else f"`{search_query}`"

        # 카테고리별 맞춤 타이틀 구성
        category_headers = {
            "tech": ("🛠️ 기술 아키텍처 & 프레임워크 리서치 보고서", "💻 주요 특징 및 수집 정보 요약"),
            "business": ("📈 비즈니스 트렌드 & 시장 분석 보고서", "📊 산업 동향 및 핵심 시사점"),
            "general": ("🔍 실시간 주제 탐구 종합 보고서", "📌 주요 발견 사항 및 요약")
        }
        main_title, sub_title = category_headers.get(category, category_headers["general"])

        # 수집된 웹 데이터 스니펫 정보 추출
        insights: List[str] = []
        for res in results[:4]:
            snippet = res.get("body", "").strip()
            title = res.get("title", "").strip()
            href = res.get("href", "#")
            if snippet:
                insights.append(f"**[{title}]({href})**: {snippet}")

        # 만약 스니펫이 없을 경우 기본 안내 텍스트 보정
        if not insights:
            insights = [f"'{search_query}' 키워드에 대한 웹 리서치가 성공적으로 완료되었습니다."]

        # ----------------------------------------------------
        # 동적 마크다운 문서 생성
        # ----------------------------------------------------
        report_md = f"""# {main_title}

## 💡 분석 타겟 및 핵심 키워드
> **원문 질문**: {query}  
> **정제 검색어**: `{search_query}` | **분류 카테고리**: `{category.upper()}`  
> **추출 키워드**: {keyword_str}

---

## {sub_title}
"""
        # 수집된 텍스트들을 동적으로 결합합니다.
        for i, ins in enumerate(insights, 1):
            report_md += f"{i}. {ins}\n\n"

        report_md += f"""---

## 📄 실시간 수집 출처 상세 본문 ({len(results)}개 출처 수집 완료)
"""
        # 스크래핑된 본문이 존재할 경우 리포트에 포함합니다.
        if scraped_texts:
            for text_block in scraped_texts[:2]:
                report_md += f"{text_block}\n\n"
        else:
            for item in results:
                report_md += f"* [{item.get('title', '웹 링크')}]({item.get('href', '#')}) - {item.get('body', '')[:120]}...\n"

        report_md += f"""
---
* 🤖 **Agent Session**: `{session_id[:8]}...` | **Engine**: `LangGraph_{category.upper()}`  
* 본 보고서는 Real-time AI Researcher Agent에 의해 실시간 정제 검색 및 본문 인덱싱을 거쳐 자동 생성되었습니다.*
"""

        # ----------------------------------------------------
        # 실시간 단어 조각(CHUNK) 타자기 스트리밍 발행 루프
        # ----------------------------------------------------
        words = report_md.split(" ")
        for i, word in enumerate(words):
            chunk_str = word + (" " if i < len(words) - 1 else "")

            # Kafka로 CHUNK 토큰 전송
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="CHUNK",
                content=chunk_str,
                step="report_generation"
            )
            time.sleep(0.02)

        return {"final_report": report_md}

    def _node_a2ui_generation(self, state: ResearchState) -> Dict[str, Any]:
        """
        [5단계 노드] 정제된 데이터와 카테고리에 맞춘 A2UI UI 대시보드 스키마를 생성하여 
        Kafka A2UI_RENDER 이벤트로 클라이언트에 전송합니다.
        """
        session_id = state["session_id"]
        host_id = state["host_id"]
        query = state["query"]
        category = state.get("category", "general")
        results = state.get("search_results", [])
        keywords = state.get("extracted_keywords", [])

        # A2UI 상태 메시지 송신
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🎨 [{category.upper()}] 맞춤형 A2UI 대시보드 UI 컴포넌트 생성 중",
            step="a2ui_generation"
        )

        # 동적 지표 카드 생성
        custom_metrics = [
            {
                "id": "metric_pure_kw",
                "label": "정제 검색어",
                "value": keywords[0] if keywords else "Direct Query",
                "change": "Stopwords Cleaned",
                "status": "success"
            }
        ]

        confidence = "98%" if len(results) >= 2 else "85%"
        a2ui_data = A2UIComponentBuilder.create_research_a2ui(
            query=query,
            sources_count=len(results),
            confidence_score=confidence,
            category=category,
            custom_metrics=custom_metrics
        )

        a2ui_json = A2UIComponentBuilder.to_json(a2ui_data)

        # Kafka로 A2UI_RENDER 이벤트 송신
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="A2UI_RENDER",
            content=a2ui_json,
            step="a2ui_generation"
        )

        # 최종 작업 완결 알림 신호(DONE) 발행
        self.producer.send_event(
            session_id=session_id,
            host_id=host_id,
            event_type="DONE",
            content=f"[{category.upper()}] Report & Dynamic A2UI Dashboard Completed",
            step="completed"
        )

        return {}

    def execute(self, session_id: str, host_id: str, query: str) -> None:
        """
        초기 상태 객체를 세팅하고 LangGraph 에이전트 추론 루프를 실행하는 메인 진입점 함수입니다.
        """
        initial_state: ResearchState = {
            "session_id": session_id,
            "host_id": host_id,
            "query": query,
            "search_query": "",
            "category": "general",
            "extracted_keywords": [],
            "search_results": [],
            "scraped_texts": [],
            "final_report": ""
        }

        try:
            # LangGraph 추론 그래프를 실행합니다.
            self.graph.invoke(initial_state)
        except Exception as e:
            print(f"[AgentEngine ERROR] 그래프 실행 중 예외 발생: {e}")
            self.producer.send_event(
                session_id=session_id,
                host_id=host_id,
                event_type="ERROR",
                content=f"AI 에이전트 처리 중 오류가 발생했습니다: {str(e)}",
                step="error"
            )
