# json 모듈은 파이썬 객체를 JSON 문자열로 변환(직렬화)하거나 반대 작업을 수행합니다.
import json
# time 모듈은 현재 시간(타임스탬프) 측정에 활용됩니다.
import time
# typing 모듈의 Optional, Dict, Any, Callable은 타입 힌트를 명확히 명시하기 위해 사용합니다.
from typing import Optional, Dict, Any, Callable
# confluent_kafka 라이브러리에서 Producer, Consumer, KafkaError 클래스를 임포트합니다.
from confluent_kafka import Producer, Consumer, KafkaError

# 프로젝트 설정 파일에서 카프카 접속 정보 및 토픽명을 불러옵니다.
from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_AGENT_RESPONSES, KAFKA_CONSUMER_GROUP_ID, TOPIC_AGENT_REQUESTS


class AgentKafkaProducer:
    """
    파이썬 에이전트의 상태(STATUS), 토큰 청크(CHUNK), 완료(DONE), 에러(ERROR) 이벤트를
    Kafka 응답 토픽으로 발행(Produce)하는 전용 클래스입니다.
    """
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        # 카프카 프로듀서에 필요한 설정값을 딕셔너리로 작성합니다.
        # 'bootstrap.servers': 카프카 브로커 접속 주소
        # 'linger.ms': 메시지 전송 대기 시간(0으로 설정해 생성 즉시 실시간 송신)
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 0
        }
        # confluent_kafka의 Producer 인스턴스를 생성합니다.
        self.producer = Producer(conf)

    def _delivery_report(self, err, msg):
        """
        카프카 메시지 전송 결과를 비동기로 콜백받는 내부 함수입니다.
        전송 실패 시 에러 내용을 출력합니다.
        """
        # err 객체가 존재하는 경우 전송에 실패한 것입니다.
        if err is not None:
            print(f"[KafkaProducer ERROR] 메시지 전송 실패: {err}")

    def send_event(
        self,
        session_id: str,
        host_id: str,
        event_type: str,
        content: str,
        conversation_id: str = "",
        step: str = ""
    ) -> None:
        """
        클라이언트 게이트웨이가 이해할 수 있는 규격화된 Kafka 응답 메시지를 전송합니다.
        - session_id: 사용자 네트워크 소켓 세션 유니크 ID (UUID)
        - host_id: 타겟 코틀린 게이트웨이 서버 ID
        - event_type: 이벤트 종류 (STATUS | CHUNK | A2UI_RENDER | DONE | ERROR)
        - content: 텍스트 내용 (상태 로그 메시지 또는 LLM 생성 토큰 조각)
        - conversation_id: 비즈니스 리서치 대화 식별자
        - step: 에이전트의 현재 추론 단계 이름 (예: query_analysis, search, scraping)
        """
        # 카프카 응답 스키마 규격에 맞게 파이썬 딕셔너리를 구성합니다.
        payload: Dict[str, Any] = {
            "sessionId": session_id,
            "conversationId": conversation_id,
            "hostId": host_id,
            "type": event_type,
            "content": content,
            "metadata": {
                "step": step,
                "timestamp": int(time.time() * 1000)  # 밀리초 단위 현재 타임스탬프
            }
        }

        # 딕셔너리를 UTF-8 인코딩된 JSON 문자열 바이트로 변환합니다.
        value_bytes = json.dumps(payload, ensure_ascii=False).encode('utf-8')

        # 지정된 카프카 응답 토픽(agent-responses)으로 메시지를 전송(Produce)합니다.
        self.producer.produce(
            topic=TOPIC_AGENT_RESPONSES,
            key=session_id.encode('utf-8'),  # 세션 ID를 파티션 키로 지정
            value=value_bytes,
            callback=self._delivery_report   # 비동기 콜백 지정
        )

        # 프로듀서 버퍼를 즉시 비워(Flush) 실시간으로 메시지가 전송되도록 보장합니다.
        self.producer.flush()


class AgentKafkaConsumer:
    """
    Kafka 요청 토픽(agent-requests)으로부터 사용자의 리서치 질문 메시지를
    지속적으로 수신(Consume)하는 폴링 클래스입니다.
    """
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = KAFKA_CONSUMER_GROUP_ID,
        topic: str = TOPIC_AGENT_REQUESTS
    ):
        # 카프카 컨슈머에 필요한 설정값을 딕셔너리로 작성합니다.
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',  # 가장 최신 메시지부터 읽기 시작
            'enable.auto.commit': True
        }
        # Consumer 인스턴스를 생성하고 대상 토픽을 구독(Subscribe)합니다.
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])

    def start_listening(self, message_handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        무한 루프를 돌며 카프카 메시지를 폴링하고, 수신된 메시지를 콜백 함수로 전달합니다.
        """
        print(f"[KafkaConsumer] Listening on topic '{TOPIC_AGENT_REQUESTS}'...")
        try:
            while True:
                # 1.0초 타임아웃으로 메시지를 폴링(Poll)합니다.
                msg = self.consumer.poll(timeout=1.0)

                # 수신된 메시지가 없으면 다음 타임아웃까지 대기합니다.
                if msg is None:
                    continue

                # 폴링 중 에러가 발생한 경우 에러 핸들링을 수행합니다.
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KafkaConsumer ERROR] {msg.error()}")
                    continue

                # 정상적으로 메시지 바이트를 수신한 경우 JSON 파싱을 진행합니다.
                try:
                    raw_data = msg.value().decode('utf-8')
                    request_dict = json.loads(raw_data)
                    # 수신된 파이썬 딕셔너리 객체를 처리 콜백 함수로 전달합니다.
                    message_handler(request_dict)
                except Exception as e:
                    print(f"[KafkaConsumer ERROR] 메시지 파싱 중 오류 발생: {e}")

        except KeyboardInterrupt:
            # 사용자가 Ctrl+C를 입력하면 안전하게 리스너를 종료합니다.
            print("[KafkaConsumer] Shutdown requested by user.")
        finally:
            # 컨슈머 커넥션을 안전하게 닫습니다.
            self.consumer.close()
