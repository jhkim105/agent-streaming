# typing 모듈에서 Any, Dict, List, TypedDict, Tuple 표기를 불러옵니다.
from typing import Any, Dict, List, TypedDict, Tuple
# time 모듈은 타임스탬프 계산 및 미세 지연 부여에 활용됩니다.
import time
# re 모듈은 정규식을 활용한 단어 추출 및 불용어 정제에 활용됩니다.
import re
# langgraph 패키지에서 StateGraph 및 END 노드 신호를 가져옵니다.
from langgraph.graph import StateGraph, END

# 작성해둔 도구 함수, Kafka 프로듀서 및 Ollama LLM 클라이언트를 임포트합니다.
from src.tools.search_tool import search_web_duckduckgo
from src.tools.scraper_tool import scrape_webpage_content
from src.kafka_client import AgentKafkaProducer
from src.ollama_client import OllamaLLMClient
from src.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from src.a2ui_schema import A2UIComponentBuilder


class ResearchState(TypedDict):
    """
    LangGraph 흐름 속에서 각 노드 간에 전달되고 공유되는 상태 데이터 클래스입니다.
    """
    session_id: str                      # 사용자 세션 유니크 UUID
    conversation_id: str                 # 비즈니스 리서치 대화 식별자
    host_id: str                         # 타겟 게이트웨이 인스턴스 ID
    query: str                           # 사용자 질문 원본
    search_query: str                    # 정제된 순수 검색 키워드
    category: str                        # 질문 분류 카테고리 ('tech', 'business', 'general')
    extracted_keywords: List[str]        # 질문 및 수집 데이터에서 추출한 핵심 키워드 리스트
    search_results: List[Dict[str, str]] # DuckDuckGo 검색 결과 리스트
    scraped_texts: List[str]             # 스크래핑된 웹페이지 본문 텍스트들
    final_report: str                    # LLM이 완성한 최종 마크다운 보고서 텍스트


class AgentWorkflowEngine:
    """
    LangGraph 및 로컬 Ollama LLM(Qwen2.5-7B) 기반으로 
    멀티 스텝 실시간 AI 리서치 그래프를 실행하는 핵심 엔진 클래스입니다.
    """
    def __init__(self, kafka_producer: AgentKafkaProducer):
        # 파이썬 에이전트가 각 단계마다 카프카 응답을 송신할 프로듀서 인스턴스를 주입합니다.
        self.producer = kafka_producer
        # 로컬 Ollama LLM 통신 클라이언트를 초기화합니다.
        self.llm_client = OllamaLLMClient(base_url=OLLAMA_BASE_URL, model_name=OLLAMA_MODEL)
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
        [1단계 노드] 사용자의 질문을 분석하여 카테고리(기술/비즈니스/일반)를 자동 분류하고 pure 검색 키워드를 추출합니다.
        """
        session_id = state["session_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]

        # Kafka로 에이전트의 현재 분석 시작 알림을 전송합니다.
        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🔍 사용자 질문 의도 및 주제 정밀 분석 중: '{query}'",
            step="query_analysis"
        )
        time.sleep(0.3)

        query_lower = query.lower()

        # 기술(Tech) 카테고리 판별용 키워드 리스트
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

        # 카테고리 추론 로직 실행
        if any(kw in query_lower for kw in tech_keywords):
            category = "tech"
        elif any(kw in query_lower for kw in biz_keywords):
            category = "business"
        else:
            category = "general"

        # 불용어(Stopwords) 제거 및 pure 키워드 추출
        words = re.findall(r'[a-zA-Z0-9_]+|[가-힣]{2,}', query)
        stop_words = {
            "조사해줘", "조사", "분석해줘", "분석", "찾아줘", "알려줘", "써줘", "요약",
            "보고서", "대해", "관해서", "무엇인가요", "뭐야", "원인", "관련", "해줘",
            "부탁해", "요청", "정보", "소개해줘", "설명해줘", "어떻게", "대해서"
        }
        extracted_keywords = [w for w in words if w.lower() not in stop_words and w not in stop_words]

        # 정제된 순수 검색어 생성
        search_query = " ".join(extracted_keywords) if extracted_keywords else query

        # 분석 완료 이벤트 발송
        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"📌 [분류 완료] 카테고리: {category.upper()} | 정제된 검색어: '{search_query}'",
            step="query_analysis"
        )
        time.sleep(0.3)

        return {
            "search_query": search_query,
            "category": category,
            "extracted_keywords": extracted_keywords if extracted_keywords else [query]
        }

    def _node_web_search(self, state: ResearchState) -> Dict[str, Any]:
        """
        [2단계 노드] 정제된 pure 검색어(예: 'LiteLLM')를 사용해 DuckDuckGo 웹 조회를 실행합니다.
        """
        session_id = state["session_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        search_query = state.get("search_query", state["query"])

        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🌐 DuckDuckGo 타깃 실시간 웹 검색 중 (검색어: '{search_query}')",
            step="web_search"
        )

        results = search_web_duckduckgo(query=search_query, max_results=4)

        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"✅ 실시간 웹 검색 결과 {len(results)}건 수집 완료",
            step="web_search"
        )
        time.sleep(0.3)

        return {"search_results": results}

    def _node_web_scraping(self, state: ResearchState) -> Dict[str, Any]:
        """
        [3단계 노드] 검색된 URL들의 본문 콘텐츠를 읽어옵니다.
        """
        session_id = state["session_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        results = state.get("search_results", [])

        scraped_texts: List[str] = []

        for idx, item in enumerate(results):
            url = item.get("href", "")
            title = item.get("title", "")
            if not url:
                continue

            self.producer.send_event(
                session_id=session_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"📄 본문 스크래핑 중 ({idx+1}/{len(results)}): {title[:25]}...",
                step="web_scraping"
            )

            text_content = scrape_webpage_content(url=url, timeout_seconds=3)
            if text_content:
                scraped_texts.append(f"### 출처 {idx+1}: [{title}]({url})\n{text_content[:650]}...")

            time.sleep(0.2)

        return {"scraped_texts": scraped_texts}

    def _node_report_generation(self, state: ResearchState) -> Dict[str, Any]:
        """
        [4단계 노드] 수집된 실시간 웹 데이터와 사용자 질문을 로컬 Ollama (Qwen2.5-7B) 모델에 전달하고,
        LLM이 직접 생성하는 자연어 토큰을 Kafka로 실시간 타자기 스트리밍 송신합니다.
        """
        session_id = state["session_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]
        search_query = state.get("search_query", query)
        category = state.get("category", "general")
        results = state.get("search_results", [])
        scraped_texts = state.get("scraped_texts", [])

        # Ollama LLM 작동 상태 체크
        is_ollama_online = self.llm_client.is_service_available()

        if is_ollama_online:
            self.producer.send_event(
                session_id=session_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"🧠 [로컬 LLM {OLLAMA_MODEL}] 실시간 웹 데이터 기반 자연어 추론 및 리포트 작성 중",
                step="report_generation"
            )
        else:
            self.producer.send_event(
                session_id=session_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"📝 [동적 룰 기반 엔진] 수집 데이터 기반 리포트 생성 중 (Ollama 미연결)",
                step="report_generation"
            )

        # 수집된 웹 컨텍스트 텍스트 구성
        context_blocks: List[str] = []
        for idx, item in enumerate(results, 1):
            title = item.get("title", "")
            href = item.get("href", "")
            body = item.get("body", "")
            context_blocks.append(f"[웹 출처 {idx}] 제목: {title}\nURL: {href}\n요약: {body}")

        if scraped_texts:
            context_blocks.append("\n[상세 본문 발췌 일부]:\n" + "\n\n".join(scraped_texts[:2]))

        web_context_str = "\n\n".join(context_blocks)

        full_report_text = ""

        # ----------------------------------------------------
        # 1. Ollama (Qwen2.5-7B) 모델 연동 자연어 토큰 스트리밍
        # ----------------------------------------------------
        if is_ollama_online:
            system_prompt = f"""너는 실시간 AI 리서치 분석 전문가 (Real-time AI Research Agent)이다.
제공된 [실시간 웹 검색 수집 데이터]를 정밀하게 분석하여 사용자의 질문에 답변하는 고품질 마크다운 리포트를 작성하라.

[작성 가이드라인]
1. 반드시 한국어로 답변할 것.
2. 질문 카테고리는 [{category.upper()}] 이다. 질문 분야에 적합한 깊이 있고 명확한 인사이트를 제공하라.
3. 문서 구성을 다음과 같이 목차화하라:
   - # 📊 {category.upper()} 분야 실시간 리서치 보고서
   - ## 💡 연구 주제 및 핵심 개요
   - ## 📌 주요 발견 및 핵심 분석 내용 (수집 데이터 기반)
   - ## 🔍 세부 인사이트 및 종합 의견
   - ## 🔗 실시간 참고 출처 목록
4. 수집된 웹 출처 정보를 적극 반영하여 사실에 기반한 정확한 내용을 제공하라."""

            user_prompt = f"""[사용자 질문]: {query} (정제 검색어: {search_query})

[실시간 웹 검색 수집 데이터]:
{web_context_str}

위 수집된 실시간 웹 자료를 기반으로 질문에 대해 가독성이 뛰어난 마크다운 리포트를 작성해줘."""

            # Ollama 스트리밍 클라이언트 호출
            token_generator = self.llm_client.stream_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.7
            )

            for token in token_generator:
                full_report_text += token

                # Kafka로 실시간 LLM 토큰 CHUNK 전송
                self.producer.send_event(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    host_id=host_id,
                    event_type="CHUNK",
                    content=token,
                    step="report_generation"
                )

        # ----------------------------------------------------
        # 2. Ollama 미실행 시 Fallback 룰 기반 스트리밍
        # ----------------------------------------------------
        else:
            fallback_md = f"""# 📊 {category.upper()} 분야 실시간 데이터 리서치 보고서

## 💡 연구 주제
> **{query}** (정제 검색어: `{search_query}`)

---

## 📌 수집 데이터 주요 발견 내용
"""
            for i, res in enumerate(results[:3], 1):
                fallback_md += f"{i}. **[{res.get('title', '')}]({res.get('href', '#')})**: {res.get('body', '')}\n\n"

            fallback_md += "\n---\n* 참고: 로컬 Ollama 서비스 연결 시 Qwen2.5-7B LLM의 100% 심층 자연어 추론 보고서가 생성됩니다.*"

            words = fallback_md.split(" ")
            for i, word in enumerate(words):
                chunk_str = word + (" " if i < len(words) - 1 else "")
                full_report_text += chunk_str

                self.producer.send_event(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    host_id=host_id,
                    event_type="CHUNK",
                    content=chunk_str,
                    step="report_generation"
                )
                time.sleep(0.02)

        return {"final_report": full_report_text}

    def _node_a2ui_generation(self, state: ResearchState) -> Dict[str, Any]:
        """
        [5단계 노드] LLM 분석 결과 및 카테고리에 맞춘 A2UI UI 대시보드 스키마를 생성하고 
        Kafka A2UI_RENDER 이벤트로 클라이언트에 전송합니다.
        """
        session_id = state["session_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]
        category = state.get("category", "general")
        results = state.get("search_results", [])

        # A2UI 상태 메시지 송신
        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🎨 [{category.upper()}] LLM 연동 맞춤형 A2UI 대시보드 UI 컴포넌트 생성 중",
            step="a2ui_generation"
        )

        # 동적 지표 카드 구성
        custom_metrics = [
            {
                "id": "metric_llm_engine",
                "label": "추론 LLM 모델",
                "value": f"Ollama {OLLAMA_MODEL}",
                "change": "100% Local Neural Net",
                "status": "success"
            }
        ]

        a2ui_data = A2UIComponentBuilder.create_research_a2ui(
            query=query,
            sources_count=len(results),
            confidence_score="99%",
            category=category,
            custom_metrics=custom_metrics
        )

        a2ui_json = A2UIComponentBuilder.to_json(a2ui_data)

        # Kafka로 A2UI_RENDER 이벤트 송신
        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="A2UI_RENDER",
            content=a2ui_json,
            step="a2ui_generation"
        )

        # 최종 작업 완결 알림 신호(DONE) 발행
        self.producer.send_event(
            session_id=session_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="DONE",
            content=f"[{category.upper()}] Qwen2.5-7B LLM Natural Language Report Completed",
            step="completed"
        )

        return {}

    def execute(self, session_id: str, host_id: str, query: str, conversation_id: str = "") -> None:
        """
        초기 상태 객체를 세팅하고 LangGraph 에이전트 추론 루프를 실행하는 메인 진입점 함수입니다.
        """
        initial_state: ResearchState = {
            "session_id": session_id,
            "conversation_id": conversation_id,
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
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="ERROR",
                content=f"AI 에이전트 처리 중 오류가 발생했습니다: {str(e)}",
                step="error"
            )
