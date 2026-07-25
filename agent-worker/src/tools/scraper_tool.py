# httpx 라이브러리는 동기 및 비동기 HTTP 요청을 보낼 수 있는 현대적인 파이썬 클라이언트입니다.
import httpx
# bs4(BeautifulSoup)는 HTML 문서 구조를 파싱해 특정 태그나 본문 텍스트만 추출하는 도구입니다.
from bs4 import BeautifulSoup
# typing 모듈의 Optional 타입 표기를 임포트합니다.
from typing import Optional


def scrape_webpage_content(url: str, timeout_seconds: int = 5) -> str:
    """
    특정 웹페이지 URL에 HTTP GET 요청을 보내 HTML 본문을 읽어온 뒤,
    불필요한 스크립트/태그를 제거하고 깨끗한 텍스트 본문만 추출합니다.
    
    :param url: 스크래핑할 대상 웹페이지 URL 주소
    :param timeout_seconds: 요청 응답 타임아웃 제한 시간 (기본 5초)
    :return: 정제된 웹페이지 본문 텍스트 (실패 시 빈 문자열 반환)
    """
    # 일반 브라우저로 위장하기 위한 User-Agent 헤더를 정의합니다.
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # httpx 클라이언트로 지정된 URL에 HTTP GET 요청을 보냅니다.
        response = httpx.get(url, headers=headers, timeout=timeout_seconds, follow_redirects=True)

        # HTTP 응답 상태 코드가 200 OK인지 확인합니다.
        if response.status_code == 200:
            # html.parser를 사용해 HTML 구조를 구조화 객체로 변환합니다.
            soup = BeautifulSoup(response.text, "html.parser")

            # 본문 추출에 방해가 되는 script, style, nav, footer 태그 요소를 제거합니다.
            for element in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                element.decompose()

            # HTML 구조 태그 안의 순수 텍스트 내용만 추출합니다.
            lines = (line.strip() for line in soup.get_text().splitlines())
            # 여러 개의 공백 라인을 제거하고 깔끔하게 개행문자로 연결합니다.
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            clean_text = "\n".join(chunk for chunk in chunks if chunk)

            # 본문 텍스트가 너무 길 경우 프롬프트 토큰 과다 소비 방지를 위해 3000자로 제한 자릅니다.
            return clean_text[:3000]

    except Exception as e:
        # 타임아웃이나 네트워킹 예외 발생 시 에러 로그를 출력하고 처리합니다.
        print(f"[ScraperTool WARNING] URL 스크래핑 실패 ({url}): {e}")

    # 실패 시 빈 텍스트 반환
    return ""
