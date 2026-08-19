# os 모듈은 시스템 환경 변수에 접근하기 위한 파이썬 표준 라이브러리입니다.
import os
# dotenv 패키지의 load_dotenv 함수를 사용해 .env 파일의 환경변수를 읽어옵니다.
from dotenv import load_dotenv

# .env 파일이 존재하는 경우 환경 변수로 로드합니다.
load_dotenv()

# Kafka 브로커의 접속 주소를 설정합니다. 기본값은 로컬 브로커 포트인 'localhost:9092'입니다.
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# 파이썬 에이전트가 클라이언트 커맨드를 수신할 Kafka 토픽 이름입니다. (기본: agent-commands)
TOPIC_AGENT_COMMANDS: str = os.getenv("TOPIC_AGENT_COMMANDS", os.getenv("TOPIC_AGENT_REQUESTS", "agent-commands"))

# 파이썬 에이전트가 진행 상태 및 토큰 이벤트를 발행할 Kafka 토픽 이름입니다. (기본: agent-events)
TOPIC_AGENT_EVENTS: str = os.getenv("TOPIC_AGENT_EVENTS", os.getenv("TOPIC_AGENT_RESPONSES", "agent-events"))

# 하위 호환성을 위한 기존 별칭 정의
TOPIC_AGENT_REQUESTS: str = TOPIC_AGENT_COMMANDS
TOPIC_AGENT_RESPONSES: str = TOPIC_AGENT_EVENTS

# Kafka Consumer가 속할 소비자 그룹(Consumer Group)의 ID입니다.
KAFKA_CONSUMER_GROUP_ID: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "python-agent-group")

# OpenAI API Key를 환경 변수에서 가져옵니다. (없을 경우 기본값 빈 문자열)
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

# 로컬 Ollama 서비스 호스트 엔드포인트 주소입니다. (기본: http://localhost:11434)
OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

# Ollama에서 실행할 기본 로컬 대형 언어 모델(LLM) 이름입니다. (기본: qwen2.5:7b)
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
