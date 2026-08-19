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


def handle_agent_command(command_data: Dict[str, Any]) -> None:
    """
    Kafka 'agent-commands' 토픽으로부터 수신받은 AgentCommand JSON 파싱 딕셔너리를 처리하는 콜백 함수입니다.
    
    :param command_data: {"commandId": "...", "conversationId": "...", "hostId": "...", "type": "RESEARCH", "payload": {...}}
    """
    # 딕셔너리로부터 commandId, conversationId, hostId 및 payload 정보를 추출합니다.
    command_id = command_data.get("commandId", command_data.get("sessionId", ""))
    conversation_id = command_data.get("conversationId", "")
    host_id = command_data.get("hostId", "")
    command_type = command_data.get("type", "RESEARCH")
    payload = command_data.get("payload", {})

    # query 또는 action 정보를 payload에서 추출하거나 최상위 필드에서 추출합니다.
    query = payload.get("query", command_data.get("query", ""))
    action_id = payload.get("actionId", command_data.get("actionId", ""))

    # A2UI 사용자 버튼 클릭 액션인 경우 쿼리를 변환합니다.
    if action_id or command_type == "ACTION":
        selected_label = payload.get("label", action_id)
        query = f"A2UI 후속 분석 요청: {selected_label}"

    print(f"\n[AgentWorker] 🚀 새로운 AgentCommand 수신! (Cmd: {command_id}, Conv: {conversation_id}, Host: {host_id})")
    print(f"[AgentWorker] ❓ 질문/액션 내용: '{query}'")

    # commandId나 질문이 누락된 유효하지 않은 요청인 경우 예외 로그 출력 후 스킵합니다.
    if not command_id or not query:
        print("[AgentWorker WARNING] 필수 파라미터(commandId 또는 query)가 누락되어 커맨드를 거부합니다.")
        return

    # LangGraph 에이전트 워크플로우 엔진을 기동하여 다단계 리서치를 실행합니다.
    workflow_engine.execute(
        command_id=command_id,
        host_id=host_id,
        query=query,
        conversation_id=conversation_id
    )
    print(f"[AgentWorker] ✨ AgentCommand 처리 완료 (Cmd: {command_id}, Conv: {conversation_id})\n")


def main() -> None:
    """
    파이썬 에이전트 프로세스의 메인 진입점 함수입니다.
    Kafka 커맨드 리스너를 기동하여 요청 대기 상태에 들어갑니다.
    """
    print("=" * 60)
    print("🤖 Real-time AI Researcher Agent Python Worker Running...")
    print("=" * 60)

    # Kafka Consumer 인스턴스를 생성합니다.
    consumer = AgentKafkaConsumer()

    # 메시지 수신 시 handle_agent_command 콜백 함수가 호출되도록 리스너 루프를 시작합니다.
    consumer.start_listening(message_handler=handle_agent_command)


# 본 파일이 스크립트로 직접 실행된 경우 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()
