# typing 모듈에서 Any, Dict, Generator, Optional 표기를 불러와 타입 힌트에 적용합니다.
from typing import Any, Dict, Generator, Optional
# httpx 모듈은 고성능 HTTP 요청 및 스트리밍 응답 처리에 활용됩니다.
import httpx
# json 모듈은 스트리밍 라인 단위 JSON 응답 파싱에 활용됩니다.
import json
# config 모듈에서 Ollama 호스트 주소 및 모델 이름을 불러옵니다.
from src.config import OLLAMA_BASE_URL, OLLAMA_MODEL


class OllamaLLMClient:
    """
    로컬 Ollama 서비스(http://localhost:11434)와 통신하여 
    자연어 추론 및 실시간 토큰 스트리밍 생성을 담당하는 클라이언트 클래스입니다.
    """
    def __init__(self, base_url: str = "http://localhost:11434", model_name: str = "qwen2.5:7b"):
        # Ollama API 기본 접속 엔드포인트 URL을 저장합니다.
        self.base_url = base_url
        # 활용할 로컬 LLM 모델명을 저장합니다. (기본: qwen2.5:7b)
        self.model_name = model_name

    def is_service_available(self) -> bool:
        """
        Ollama 서비스가 로컬에서 정상 실행 중인지 헬스체크를 수행합니다.
        
        :return: 서비스 정상 작동 여부 (True/False)
        """
        try:
            # GET /api/tags 엔드포인트를 호출하여 서비스 핑을 확인합니다.
            response = httpx.get(f"{self.base_url}/api/tags", timeout=2.0)
            # 상태 코드가 200인 경우 정상 작동으로 판단합니다.
            return response.status_code == 200
        except Exception:
            # 접속 불가 예외 발생 시 False를 반환합니다.
            return False

    def stream_chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7
    ) -> Generator[str, None, None]:
        """
        Ollama /api/chat 엔드포인트를 스트리밍 방식으로 호출하여
        생성되는 단어 조각(Token)을 하나씩 yield 스트리밍 반환합니다.
        
        :param system_prompt: AI 에이전트의 역할 및 가이드라인 프롬프트
        :param user_prompt: 검색 데이터와 질문이 포함된 사용자 프롬프트
        :param temperature: 답변 창의성 온도값 (0.0~1.0)
        :return: 스트리밍 토큰 문자열 제너레이터
        """
        # POST 요청을 보낼 chat API 엔드포인트 URL 생성
        url = f"{self.base_url}/api/chat"

        # Ollama API로 전달할 JSON 페이로드 딕셔너리를 구성합니다.
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": True,  # 토큰 단위 실시간 스트리밍 활성화
            "options": {
                "temperature": temperature
            }
        }

        try:
            # httpx.Client의 stream 매서드를 활용해 HTTP 파이프라인 스트리밍 접속을 엽니다.
            with httpx.Client(timeout=60.0) as client:
                with client.stream("POST", url, json=payload) as response:
                    # 응답 상태 코드가 200이 아니면 오류를 발생시킵니다.
                    response.raise_for_status()

                    # 스트리밍 라인(line)을 하나씩 실시간으로 읽습니다.
                    for line in response.iter_lines():
                        # 빈 줄이 아닌 파싱 가능한 텍스트 라인인 경우
                        if line and line.strip():
                            try:
                                # JSON 문자열 데이터를 딕셔너리로 파싱합니다.
                                data = json.loads(line)
                                # data 딕셔너리에서 message.content 조각 텍스트를 추출합니다.
                                token = data.get("message", {}).get("content", "")
                                if token:
                                    # 생성된 단어 토큰을 호출자에게 yield 전달합니다.
                                    yield token
                            except json.JSONDecodeError:
                                continue
        except Exception as e:
            # 예외 발생 시 에러 알림 문자열을 yield 전달합니다.
            print(f"[OllamaClient ERROR] 스트리밍 생성 중 오류 발생: {e}")
            yield f"\n\n⚠️ [Ollama 연동 에러]: {str(e)}"
