# sys 모듈은 프로세스 제어 및 시스템 출력을 위해 필요합니다.
import sys
# threading 모듈을 이용해 에이전트 추론을 동시 멀티 스레드로 실행합니다.
import threading
# typing 모듈의 Dict, Any 타입 힌트를 임포트합니다.
from typing import Dict, Any

# 작성한 카프카 클라이언트 및 Agent Runtime 엔진 모듈을 임포트합니다.
from src.kafka_client import AgentKafkaProducer, AgentKafkaConsumer
from src.agent_graph import AgentRuntimeEngine

# 모듈 전역에서 공유할 프로듀서 및 에이전트 엔진 인스턴스를 초기화합니다.
kafka_producer = AgentKafkaProducer()
runtime_engine = AgentRuntimeEngine(kafka_producer=kafka_producer)


def process_command_task(command_id: str, conversation_id: str, host_id: str, query: str) -> None:
    """
    백그라운드 스레드에서 AgentRuntime 파이프라인 추론을 동시 수행하는 전용 작업 함수입니다.
    """
    print(f"\n[AgentRuntime Thread] 🚀 동시 스레드 추론 개시! (Cmd: {command_id}, Conv: {conversation_id}, Host: {host_id})")
    print(f"[AgentRuntime Thread] ❓ 질문/액션 내용: '{query}'")

    # AgentRuntime 파이프라인 엔진을 기동하여 실시간 대화를 실행합니다.
    runtime_engine.execute(
        command_id=command_id,
        host_id=host_id,
        query=query,
        conversation_id=conversation_id
    )
    print(f"[AgentRuntime Thread] ✨ AgentCommand 처리 완료 (Cmd: {command_id}, Conv: {conversation_id})\n")


def handle_agent_command(command_data: Dict[str, Any]) -> None:
    """
    Kafka 'agent-commands' 토픽으로부터 수신받은 AgentCommand JSON 파싱 딕셔너리를 처리하는 콜백 함수입니다.
    카프카 컨슈머 루프를 블로킹하지 않도록 데몬 스레드로 비동기 처리합니다.
    """
    command_id = command_data.get("commandId", command_data.get("sessionId", ""))
    conversation_id = command_data.get("conversationId", "")
    host_id = command_data.get("hostId", "")
    command_type = command_data.get("type", "RESEARCH")
    payload = command_data.get("payload", {})

    query = payload.get("query", command_data.get("query", ""))
    action_id = payload.get("actionId", command_data.get("actionId", ""))

    if action_id or command_type == "ACTION":
        selected_label = payload.get("label", action_id)
        query = f"A2UI 후속 요청: {selected_label}"

    if not command_id or not query:
        print("[AgentRuntime WARNING] 필수 파라미터(commandId 또는 query)가 누락되어 커맨드를 거부합니다.")
        return

    # 데몬 스레드로 비동기 처리
    runtime_thread = threading.Thread(
        target=process_command_task,
        args=(command_id, conversation_id, host_id, query),
        daemon=True
    )
    runtime_thread.start()


def main() -> None:
    """
    파이썬 Agent Runtime 프로세스의 메인 진입점 함수입니다.
    Kafka 커맨드 리스너를 기동하여 요청 대기 상태에 들어갑니다.
    """
    print("=" * 60)
    print("🤖 Agent Runtime Python Engine Running (Multi-Threaded)...")
    print("=" * 60)

    consumer = AgentKafkaConsumer()
    consumer.start_listening(message_handler=handle_agent_command)


if __name__ == "__main__":
    main()
