# sys 모듈은 프로세스 제어 및 시스템 출력을 위해 필요합니다.
import sys
# typing 모듈의 Dict, Any 타입 힌트를 임포트합니다.
from typing import Dict, Any

# 작성한 카프카 클라이언트 및 에이전트 워크플로우 엔진 모듈을 임포트합니다.
from src.kafka_client import AgentKafkaProducer, AgentKafkaConsumer
from src.agent_graph import AgentWorkflowEngine

# 모듈 전역에서 공유할 프로듀서 및 에이전트 엔진 인스턴스를 초기화합니다.
kafka_producer = AgentKafkaProducer()
workflow_engine = AgentWorkflowEngine(kafka_producer=kafka_producer)


def handle_agent_request(request_data: Dict[str, Any]) -> None:
    """
    Kafka 'agent-requests' 토픽으로부터 수신받은 JSON 파싱 딕셔너리를 처리하는 콜백 함수입니다.
    
    :param request_data: {"sessionId": "...", "conversationId": "...", "hostId": "...", "query": "..."}
    """
    # 딕셔너리로부터 필수 항목인 sessionId, conversationId, hostId, query를 추출합니다.
    session_id = request_data.get("sessionId", "")
    conversation_id = request_data.get("conversationId", "")
    host_id = request_data.get("hostId", "")
    query = request_data.get("query", "")
    action_id = request_data.get("actionId", "")
    payload = request_data.get("payload", {})

    # A2UI 사용자 버튼 클릭 액션인 경우 쿼리를 변환합니다.
    if action_id:
        selected_label = payload.get("label", action_id)
        query = f"A2UI 후속 분석 요청: {selected_label}"

    print(f"\n[AgentWorker] 🚀 새로운 리서치 요청 수신! (Conv: {conversation_id}, Session: {session_id}, Host: {host_id})")
    print(f"[AgentWorker] ❓ 질문/액션 내용: '{query}'")

    # 세션 ID나 질문이 누락된 유효하지 않은 요청인 경우 예외 로그 출력 후 스킵합니다.
    if not session_id or not query:
        print("[AgentWorker WARNING] 필수 파라미터(sessionId 또는 query)가 누락되어 요청을 거부합니다.")
        return

    # LangGraph 에이전트 워크플로우 엔진을 기동하여 다단계 리서치를 실행합니다.
    workflow_engine.execute(
        session_id=session_id,
        host_id=host_id,
        query=query,
        conversation_id=conversation_id
    )
    print(f"[AgentWorker] ✨ 리서치 작업 완료 (Conv: {conversation_id}, Session: {session_id})\n")


def main() -> None:
    """
    파이썬 에이전트 프로세스의 메인 진입점 함수입니다.
    Kafka 요청 리스너를 기동하여 요청 대기 상태에 들어갑니다.
    """
    print("=" * 60)
    print("🤖 Real-time AI Researcher Agent Python Worker Running...")
    print("=" * 60)

    # Kafka Consumer 인스턴스를 생성합니다.
    consumer = AgentKafkaConsumer()

    # 메시지 수신 시 handle_agent_request 콜백 함수가 호출되도록 리스너 루프를 시작합니다.
    consumer.start_listening(message_handler=handle_agent_request)


# 본 파일이 스크립트로 직접 실행된 경우 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()
