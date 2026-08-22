# typing 모듈에서 Any, Dict, List, TypedDict 표기를 불러옵니다.
from typing import Any, Dict, List, TypedDict
# time 모듈은 미세 타임스탬프 계산 및 지연 부여에 활용됩니다.
import time
# langgraph 패키지에서 StateGraph 및 END 노드 신호를 가져옵니다.
from langgraph.graph import StateGraph, END

# 외부 웹 탐색 도구, Kafka 프로듀서 및 Ollama LLM 클라이언트를 임포트합니다.
from src.tools.search_tool import search_web_duckduckgo
from src.kafka_client import AgentKafkaProducer
from src.ollama_client import OllamaLLMClient
from src.config import OLLAMA_BASE_URL, OLLAMA_MODEL


class AgentState(TypedDict):
    """
    LangGraph 대화 흐름 속에서 노드 간에 공유되는 Agent Runtime 상태 클래스입니다.
    """
    command_id: str                      # 사용자 커맨드 식별자 (commandId)
    conversation_id: str                 # 대화 스레드 식별자 (conversationId)
    host_id: str                         # 타겟 게이트웨이 노드 ID (hostId)
    query: str                           # 사용자 질문 원본 텍스트
    needs_search: bool                   # 실시간 웹 탐색 필요 여부 플래그
    search_results: List[Dict[str, str]] # 실시간 수집된 웹 검색 결과 리스트
    final_response: str                  # LLM이 완성한 최종 응답 텍스트
    smart_title: str                     # 완결 시 생성되는 대화 제목


class AgentRuntimeEngine:
    """
    Ollama LLM(Qwen2.5-7B) 중심의 미세 청크 버퍼링 및 고속 대화 스트리밍 
    Agent Runtime 멀티 스텝 엔진 클래스입니다.
    """
    def __init__(self, kafka_producer: AgentKafkaProducer):
        self.producer = kafka_producer
        self.llm_client = OllamaLLMClient(base_url=OLLAMA_BASE_URL, model_name=OLLAMA_MODEL)
        self.graph = self._build_graph()

    def _build_graph(self) -> Any:
        builder = StateGraph(AgentState)

        builder.add_node("intent_detection", self._node_intent_detection)
        builder.add_node("conditional_search", self._node_conditional_search)
        builder.add_node("generate_response", self._node_generate_response)

        builder.set_entry_point("intent_detection")
        builder.add_edge("intent_detection", "conditional_search")
        builder.add_edge("conditional_search", "generate_response")
        builder.add_edge("generate_response", END)

        return builder.compile()

    def _node_intent_detection(self, state: AgentState) -> Dict[str, Any]:
        """
        [1단계 노드] 사용자의 질문을 분석하여 실시간 외부 정보 탐색(주가, 날씨, 뉴스 등)이 필요한지 감지합니다.
        """
        command_id = state["command_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]

        search_triggers = [
            "주가", "주식", "시세", "날씨", "뉴스", "최신", "오늘", "환율", "코스피", "코스닥",
            "현재가", "가격", "트렌드", "전망", "지수", "실시간", "금리", "실적"
        ]

        query_lower = query.lower()
        needs_search = any(kw in query_lower for kw in search_triggers)

        if needs_search:
            status_msg = f"💭 질문 분석 완료: 실시간 외부 정보 탐색(웹검색)이 필요한 질문입니다."
        else:
            status_msg = f"💭 질문 분석 완료: 로컬 LLM 지식 기반 답변 생성 단계로 진입합니다."

        self.producer.send_event(
            command_id=command_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=status_msg,
            step="intent_detection"
        )

        return {"needs_search": needs_search}

    def _node_conditional_search(self, state: AgentState) -> Dict[str, Any]:
        """
        [2단계 노드] 실시간 탐색이 필요한 경우에만 웹 검색 도구를 조건부 구동합니다.
        """
        command_id = state["command_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]
        needs_search = state.get("needs_search", False)

        search_results: List[Dict[str, str]] = []

        if needs_search:
            self.producer.send_event(
                command_id=command_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"🌐 실시간 정보 탐색 중 (DuckDuckGo Search: '{query}')",
                step="conditional_search"
            )

            search_results = search_web_duckduckgo(query=query, max_results=3)

            self.producer.send_event(
                command_id=command_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="STATUS",
                content=f"✅ 실시간 수집 결과 {len(search_results)}건 수집 완료",
                step="conditional_search"
            )

        return {"search_results": search_results}

    def _node_generate_response(self, state: AgentState) -> Dict[str, Any]:
        """
        [3단계 노드] 사용자의 질문을 종합하여 Ollama LLM을 통해 자연스러운 답변을 고속 스트리밍 송신합니다.
        """
        command_id = state["command_id"]
        conversation_id = state.get("conversation_id", "")
        host_id = state["host_id"]
        query = state["query"]
        search_results = state.get("search_results", [])

        is_ollama_online = self.llm_client.is_service_available()
        full_text = ""

        context_str = ""
        if search_results:
            context_blocks = []
            for idx, res in enumerate(search_results, 1):
                title = res.get("title", "")
                href = res.get("href", "")
                body = res.get("body", "")
                context_blocks.append(f"[참조 자료 {idx}] {title} ({href}): {body}")
            context_str = "\n".join(context_blocks)

        self.producer.send_event(
            command_id=command_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="STATUS",
            content=f"🧠 [Ollama {OLLAMA_MODEL}] 답변 고속 스트리밍 생성 중",
            step="generate_response"
        )

        # 1. Ollama LLM 자연어 토큰 미세 버퍼 고속 스트리밍
        if is_ollama_online:
            if context_str:
                system_prompt = f"""너는 유능하고 친절한 범용 AI 지능형 에이전트(AI Assistant)이다.
제공된 [실시간 검색 자료]를 참고하여 사용자의 질문에 친절하고 명확하게 한국어로 답변하라.
출처 번호를 억지로 표기하지 말고, 자연스러운 마크다운 문맥 속에 정보를 녹여서 답변할 것."""

                user_prompt = f"""[사용자 질문]: {query}

[실시간 수집된 검색 자료]:
{context_str}

위 자료를 바탕으로 질문에 대해 명확하고 친절하게 답변해줘."""
            else:
                system_prompt = """너는 유능하고 친절한 범용 AI 지능형 에이전트(AI Assistant)이다.
사용자의 질문에 대해 자연스럽고 명확하며 친절하게 한국어로 답변하라.
필요 시 마크다운 개머리 기호(bullet points)나 코드 블록을 사용하여 가독성을 높여라."""

                user_prompt = f"{query}"

            token_generator = self.llm_client.stream_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.7
            )

            buffer_chunk = ""
            for token in token_generator:
                full_text += token
                buffer_chunk += token

                if len(buffer_chunk) >= 3 or "\n" in buffer_chunk:
                    self.producer.send_event(
                        command_id=command_id,
                        conversation_id=conversation_id,
                        host_id=host_id,
                        event_type="CHUNK",
                        content=buffer_chunk,
                        step="streaming"
                    )
                    buffer_chunk = ""

            if buffer_chunk:
                self.producer.send_event(
                    command_id=command_id,
                    conversation_id=conversation_id,
                    host_id=host_id,
                    event_type="CHUNK",
                    content=buffer_chunk,
                    step="streaming"
                )

        # 2. Fallback
        else:
            fallback = f"안녕하세요! 질문 **'{query}'**에 대한 안내입니다.\n\n"
            if search_results:
                fallback += "실시간 수집 정보는 다음과 같습니다:\n"
                for i, res in enumerate(search_results, 1):
                    fallback += f"{i}. [{res.get('title', '')}]({res.get('href', '#')}): {res.get('body', '')}\n"

            full_text = fallback
            self.producer.send_event(
                command_id=command_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="CHUNK",
                content=fallback,
                step="streaming"
            )

        # 3. 대화 타이틀 생성
        clean_title = query.strip()
        if len(clean_title) > 16:
            clean_title = clean_title[:16] + "..."
        smart_title = f"💬 {clean_title}"

        # 4. 완결 DONE 신호 송신
        self.producer.send_event(
            command_id=command_id,
            conversation_id=conversation_id,
            host_id=host_id,
            event_type="DONE",
            content="Response Completed",
            title=smart_title,
            step="completed"
        )

        return {
            "final_response": full_text,
            "smart_title": smart_title
        }

    def execute(self, command_id: str, host_id: str, query: str, conversation_id: str = "") -> None:
        """
        Agent Runtime 추론 파이프라인 메인 실행 함수입니다.
        """
        initial_state: AgentState = {
            "command_id": command_id,
            "conversation_id": conversation_id,
            "host_id": host_id,
            "query": query,
            "needs_search": False,
            "search_results": [],
            "final_response": "",
            "smart_title": ""
        }

        try:
            self.graph.invoke(initial_state)
        except Exception as e:
            print(f"[AgentRuntime ERROR] 파이프라인 실행 예외 발생: {e}")
            self.producer.send_event(
                command_id=command_id,
                conversation_id=conversation_id,
                host_id=host_id,
                event_type="ERROR",
                content=f"AgentRuntime 처리 중 오류가 발생했습니다: {str(e)}",
                step="error"
            )
