# typing 모듈에서 List 및 Dict 타입을 임포트해 반환 타입을 명확히 표기합니다.
from typing import List, Dict, Any
# duckduckgo_search / ddgs 패키지에서 DDGS 클래스를 임포트합니다.
try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS


def search_web_duckduckgo(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    """
    DuckDuckGo 검색 엔진을 이용해 입력받은 키워드(query)에 대한 웹 검색을 수행합니다.
    
    :param query: 사용자 질의어 기반으로 정제된 검색 쿼리 문자열 (예: 'LiteLLM')
    :param max_results: 수집할 최신 검색 결과의 최대 개수 (기본 5개)
    :return: [{'title': 제목, 'href': URL, 'body': 요약문}] 형태의 딕셔너리 리스트
    """
    # 검색 결과를 저장할 빈 리스트를 생성합니다.
    results_list: List[Dict[str, str]] = []

    try:
        # DDGS 세션 객체를 컨텍스트 매니저(with) 구문으로 안전하게 기동합니다.
        with DDGS() as ddgs:
            # 1차 시도: 입력받은 검색 쿼리로 텍스트 검색을 실행합니다.
            # region="wt-wt" (전 세계) 설정으로 검색 노이즈를 줄입니다.
            raw_results = list(ddgs.text(keywords=query, max_results=max_results))

            # 검색 결과가 없거나 관련 없는 무관한 결과(예: Hotmail, MS Support 등)가 섞이는 것을 방지하기 위해,
            # 검색 쿼리에 영문 기술 키워드가 포함된 경우 영문 위주 검색 2차 보정 시도를 할 수 있습니다.
            if not raw_results and " " in query:
                # 쿼리의 첫 번째 핵심 단어만으로 재검색 1회 시도 (Fallback)
                first_keyword = query.split()[0]
                raw_results = list(ddgs.text(keywords=first_keyword, max_results=max_results))

            # 수집된 원시 검색 결과를 순회하며 필요한 데이터만 구조화합니다.
            for item in raw_results:
                title = item.get("title", "")
                href = item.get("href", "")
                body = item.get("body", "")

                # 딕셔너리 객체로 포맷팅하여 결과 리스트에 보관합니다.
                results_list.append({
                    "title": title,
                    "href": href,
                    "body": body
                })

    except Exception as e:
        # 웹 검색 중 예외 발생 시 에러 로그를 출력하고 빈 리스트를 반환합니다.
        print(f"[SearchTool ERROR] DuckDuckGo 검색 도구 실행 중 오류 발생: {e}")

    # 최종 정제된 검색 결과 리스트를 반환합니다.
    return results_list
