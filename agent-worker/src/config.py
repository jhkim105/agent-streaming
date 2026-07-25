# os 모듈은 시스템 환경 변수에 접근하기 위한 파이썬 표준 라이브러리입니다.
import os
# dotenv 패키지의 load_dotenv 함수를 사용해 .env 파일의 환경변수를 읽어옵니다.
from dotenv import load_dotenv

# .env 파일이 존재하는 경우 환경 변수로 로드합니다.
load_dotenv()

# Kafka 브로커의 접속 주소를 설정합니다. 기본값은 로컬 브로커 포트인 'localhost:9092'입니다.
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# 파이썬 에이전트가 질문 요청을 수신할 Kafka 토픽 이름입니다.
TOPIC_AGENT_REQUESTS: str = os.getenv("TOPIC_AGENT_REQUESTS", "agent-requests")

# 파이썬 에이전트가 진행 상태 및 토큰 응답을 발행할 Kafka 토픽 이름입니다.
TOPIC_AGENT_RESPONSES: str = os.getenv("TOPIC_AGENT_RESPONSES", "agent-responses")

# Kafka Consumer가 속할 소비자 그룹(Consumer Group)의 ID입니다.
KAFKA_CONSUMER_GROUP_ID: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "python-agent-group")

# OpenAI API Key를 환경 변수에서 가져옵니다. (없을 경우 기본값 None)
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
