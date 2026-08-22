# json 모듈은 파이썬 객체를 JSON 문자열로 변환(직렬화)하거나 반대 작업을 수행합니다.
import json
# time 모듈은 현재 시간(타임스탬프) 측정에 활용됩니다.
import time
# uuid 모듈은 유니크한 eventId 생성에 활용됩니다.
import uuid
# typing 모듈의 Optional, Dict, Any, Callable은 타입 힌트를 명확히 명시하기 위해 사용합니다.
from typing import Optional, Dict, Any, Callable
# confluent_kafka 라이브러리에서 Producer, Consumer, KafkaError 클래스를 임포트합니다.
from confluent_kafka import Producer, Consumer, KafkaError

# 프로젝트 설정 파일에서 카프카 접속 정보 및 토픽명을 불러옵니다.
from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_AGENT_EVENTS, KAFKA_CONSUMER_GROUP_ID, TOPIC_AGENT_COMMANDS


class AgentKafkaProducer:
    """
    파이썬 에이전트의 상태(STATUS), 토큰 청크(CHUNK), 완료(DONE), 에러(ERROR) 이벤트를
    Kafka 이벤트 토픽(agent-events)으로 비동기 고속 발행(Produce)하는 전용 클래스입니다.
    """
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 0
        }
        self.producer = Producer(conf)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"[KafkaProducer ERROR] 이벤트 메시지 전송 실패: {err}")

    def send_event(
        self,
        command_id: str,
        conversation_id: str,
        host_id: str,
        event_type: str,
        content: str,
        title: str = "",
        step: str = ""
    ) -> None:
        """
        AgentEvent Kafka 메시지를 블로킹 없이 논블로킹 비동기로 속도감 있게 전송합니다.
        """
        event_id = f"evt-{uuid.uuid4()}"

        payload: Dict[str, Any] = {
            "eventId": event_id,
            "commandId": command_id,
            "conversationId": conversation_id,
            "hostId": host_id,
            "type": event_type,
            "content": content,
            "metadata": {
                "step": step,
                "title": title,
                "timestamp": int(time.time() * 1000)
            },
            "timestamp": int(time.time() * 1000)
        }

        value_bytes = json.dumps(payload, ensure_ascii=False).encode('utf-8')

        self.producer.produce(
            topic=TOPIC_AGENT_EVENTS,
            key=command_id.encode('utf-8'),
            value=value_bytes,
            callback=self._delivery_report
        )

        # 1자마다 전체 블로킹 flush()하는 대신 poll(0)으로 논블로킹 송신 버퍼 소진하여 속도 향상
        if event_type == "DONE":
            self.producer.flush()
        else:
            self.producer.poll(0)


class AgentKafkaConsumer:
    """
    Kafka 커맨드 토픽(agent-commands)으로부터 클라이언트의 AgentCommand 메시지를
    지속적으로 수신(Consume)하는 안전한 폴링 클래스입니다.
    """
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = KAFKA_CONSUMER_GROUP_ID,
        topic: str = TOPIC_AGENT_COMMANDS
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.consumer = self._create_consumer()

    def _create_consumer(self) -> Consumer:
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,
            'max.poll.interval.ms': 300000,
            'heartbeat.interval.ms': 15000
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        return consumer

    def start_listening(self, message_handler: Callable[[Dict[str, Any]], None]) -> None:
        print(f"[KafkaConsumer] Listening on topic '{self.topic}' (Group: {self.group_id})...")
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KafkaConsumer ERROR] {msg.error()}")
                    continue

                raw_data = msg.value().decode('utf-8')
                command_dict = json.loads(raw_data)
                message_handler(command_dict)

            except KeyboardInterrupt:
                print("[KafkaConsumer] Shutdown requested by user.")
                break
            except Exception as e:
                print(f"[KafkaConsumer EXCEPTION] 수신 중 오류 발생, 컨슈머 재수립 시도: {e}")
                time.sleep(1)
                try:
                    self.consumer.close()
                except Exception:
                    pass
                self.consumer = self._create_consumer()

        self.consumer.close()
