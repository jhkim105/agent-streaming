# typing 모듈에서 Any, Dict, List, Optional 표기를 불러옵니다.
from typing import Any, Dict, List, Optional
# json 모듈은 JSON 직렬화에 활용됩니다.
import json


class A2UIComponentBuilder:
    """
    에이전트가 리서치 데이터 및 후속 조치 옵션을 선언적 A2UI 구조로 쉽게 생성하도록 돕는 헬퍼 클래스입니다.
    """

    @staticmethod
    def create_research_a2ui(
        query: str,
        sources_count: int,
        confidence_score: str = "95%",
        options: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        리서치 완료 후 화면 하단에 렌더링할 A2UI 종합 카드 및 동적 액션 버튼 스키마 딕셔너리를 생성합니다.
        
        :param query: 사용자 리서치 질문 원본
        :param sources_count: 분석에 활용된 수집 웹페이지 수
        :param confidence_score: 데이터 신뢰도 점수 (기본값 95%)
        :param options: 사용자가 추가 선택 가능한 후속 탐색 옵션 리스트
        :return: A2UI 선언적 JSON 객체 (Dict 형태)
        """
        # 기본 후속 탐색 옵션 설정 (전달받은 옵션이 없을 경우 기본 3개 제공)
        if not options:
            options = [
                {
                    "id": "deep_dive",
                    "label": "🔬 기술 상세 심층 분석 요청",
                    "description": "최신 구현 아키텍처 및 코드 예제를 추가 조사합니다."
                },
                {
                    "id": "market_impact",
                    "label": "📈 시장 영향도 및 사업 기회 분석",
                    "description": "산업군별 파급력 및 비즈니스 유스케이스를 분석합니다."
                },
                {
                    "id": "summary_pdf",
                    "label": "📄 리포트 가공 및 내보내기",
                    "description": "핵심 인사이트 요약본을 대시보드 형태로 정리합니다."
                }
            ]

        # A2UI 최상위 레이아웃 구조체 생성
        a2ui_payload = {
            "version": "1.0",
            "title": "📊 리서치 데이터 인터랙티브 대시보드",
            "metrics": [
                {
                    "id": "metric_sources",
                    "label": "수집된 데이터 출처",
                    "value": f"{sources_count}개 사이트",
                    "change": "+100%",
                    "status": "normal"
                },
                {
                    "id": "metric_confidence",
                    "label": "정보 분석 신뢰도",
                    "value": confidence_score,
                    "change": "High Quality",
                    "status": "success"
                },
                {
                    "id": "metric_topic",
                    "label": "핵심 주제",
                    "value": query[:15] + ("..." if len(query) > 15 else ""),
                    "change": "Auto Classified",
                    "status": "normal"
                }
            ],
            "action_section": {
                "title": "💡 에이전트 후속 조치 (Human-in-the-Loop)",
                "description": "원하시는 분석 옵션을 선택하시면 에이전트가 즉시 연동 리서치를 계속 수행합니다.",
                "options": [
                    {
                        "action_id": opt["id"],
                        "label": opt["label"],
                        "description": opt.get("description", ""),
                        "payload": {"selected_option": opt["id"], "label": opt["label"]}
                    }
                    for opt in options
                ]
            }
        }

        # 생성된 A2UI 데이터 반환
        return a2ui_payload

    @staticmethod
    def to_json(a2ui_payload: Dict[str, Any]) -> str:
        """
        A2UI 딕셔너리 데이터 구조를 문자열 JSON 형태 포맷으로 직렬화합니다.
        """
        # json.dumps를 사용하여 한글 깨짐 없이 UTF-8 기반 문자열로 변환
        return json.dumps(a2ui_payload, ensure_ascii=False)
