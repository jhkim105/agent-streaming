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
    자연어 추론 및 실시간 토큰 스트리밍 생성을 담당하는 최적화 클라이언트 클래스입니다.
    """
    def __init__(self, base_url: str = "http://localhost:11434", model_name: str = "qwen2.5:7b"):
        self.base_url = base_url
        self.model_name = model_name

    def is_service_available(self) -> bool:
        """
        Ollama 서비스 헬스체크를 수행합니다.
        """
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=2.0)
            return response.status_code == 200
        except Exception:
            return False

    def stream_chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7
    ) -> Generator[str, None, None]:
        """
        Ollama /api/chat 엔드포인트를 고성능 스트리밍 방식으로 호출합니다.
        (num_thread: 8 옵션으로 CPU/GPU 병렬 추론 속도를 최대로 끌어올림)
        """
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": True,
            "options": {
                "temperature": temperature,
                "num_thread": 8,       # 8스레드 병렬 추론 속도 극대화
                "num_ctx": 4096,       # 컨텍스트 윈도우 크기
                "repeat_penalty": 1.1 # 불필요한 반복 억제
            }
        }

        try:
            with httpx.Client(timeout=120.0) as client:
                with client.stream("POST", url, json=payload) as response:
                    response.raise_for_status()

                    for line in response.iter_lines():
                        if line and line.strip():
                            try:
                                data = json.loads(line)
                                token = data.get("message", {}).get("content", "")
                                if token:
                                    yield token
                            except json.JSONDecodeError:
                                continue
        except Exception as e:
            print(f"[OllamaClient ERROR] 스트리밍 생성 중 오류 발생: {e}")
            yield f"\n\n⚠️ [Ollama 연동 에러]: {str(e)}"
