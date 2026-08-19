# typing 모듈에서 Any, Dict, List, Optional 표기를 불러와 타입 힌트에 활용합니다.
from typing import Any, Dict, List, Optional
# json 모듈은 딕셔너리 구조 데이터를 JSON 포맷 문자열로 변환할 때 사용합니다.
import json


class A2UIComponentBuilder:
    """
    에이전트가 수집한 리서치 데이터와 동적 액션 옵션을 A2UI 규격의 JSON으로 생성해 주는 헬퍼 클래스입니다.
    """

    @staticmethod
    def get_category_options(category: str) -> List[Dict[str, str]]:
        """
        질문 주제 카테고리(tech, business, general)에 따라 적절한 후속 탐색 액션 버튼 옵션을 반환하는 static 메서드입니다.
        
        :param category: 질문 카테고리 문자열 ('tech', 'business', 'general')
        :return: A2UI 버튼용 옵션 딕셔너리 리스트
        """
        # 기술/개발 관련 질문일 경우 맞춤형 후속 버튼 제공
        if category == "tech":
            return [
                {
                    "id": "tech_code_example",
                    "label": "💻 실제 구현 코드 및 연동 예제 요청",
                    "description": "해당 기술을 프로젝트에 적용할 수 있는 구체적 코드와 샘플을 생성합니다."
                },
                {
                    "id": "tech_benchmark",
                    "label": "⚡ 성능 벤치마크 및 장단점 비교",
                    "description": "대안 기술과의 장단점 및 트래픽 처리 성능을 비교 분석합니다."
                },
                {
                    "id": "tech_migration",
                    "label": "⚙️ 마이그레이션 & 버전 호환성 가이드",
                    "description": "기존 환경에서 최신 버전으로 이전 시 주의해야 할 패키지 변경점을 조사합니다."
                }
            ]
        # 비즈니스/시장 관련 질문일 경우 맞춤형 후속 버튼 제공
        elif category == "business":
            return [
                {
                    "id": "biz_competitor",
                    "label": "📊 주요 경쟁사 제품 비교 분석",
                    "description": "동일 시장 내 주요 경쟁사 서비스의 점유율 및 특장점을 비교합니다."
                },
                {
                    "id": "biz_roi",
                    "label": "💰 도입 비용 및 예상 ROI 분석",
                    "description": "기술 도입 시 발생하는 예상 예산 및 생산성 향상 가치를 산출합니다."
                },
                {
                    "id": "biz_forecast",
                    "label": "🔮 향후 3개년 시장 성장 전망",
                    "description": "관련 산업군 시장 규모 변화와 미래 성장 가능성을 전망합니다."
                }
            ]
        # 그 외 일반 질문일 경우 맞춤형 후속 버튼 제공
        else:
            return [
                {
                    "id": "gen_faq",
                    "label": "❓ 자주 묻는 질문 (FAQ) Top 5 정리",
                    "description": "사용자들이 가장 많이 궁금해하는 핵심 질문과 답변을 정리합니다."
                },
                {
                    "id": "gen_summary",
                    "label": "📋 한 줄 요약 체크리스트 생성",
                    "description": "복잡한 내용을 핵심만 바로 파악할 수 있는 3단계 체크리스트로 가공합니다."
                },
                {
                    "id": "gen_deep_dive",
                    "label": "📖 세부 심층 아티클 탐색 요청",
                    "description": "관련 학술 논문 및 공식 문서 출처를 추가 탐색하여 리포트를 보강합니다."
                }
            ]

    @staticmethod
    def create_research_a2ui(
        query: str,
        sources_count: int,
        confidence_score: str = "95%",
        category: str = "general",
        custom_metrics: Optional[List[Dict[str, Any]]] = None,
        options: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        리서치 결과를 A2UI UI 대시보드 스키마 딕셔너리로 직렬화합니다.
        
        :param query: 사용자의 질문 문자열
        :param sources_count: 검색되어 활용된 출처 개수
        :param confidence_score: 계산된 정보 신뢰도 수치 (예: '98%')
        :param category: 분류된 카테고리 ('tech', 'business', 'general')
        :param custom_metrics: 동적으로 추가할 지표 수치 카드 리스트
        :param options: 사용자가 선택할 후속 액션 버튼 옵션 리스트
        :return: A2UI 규격의 딕셔너리 객체
        """
        # 전달받은 옵션이 없을 경우 카테고리에 맞는 맞춤형 버튼 옵션을 자동 추출합니다.
        if not options:
            options = A2UIComponentBuilder.get_category_options(category)

        # 기본 지표 카드 리스트를 구성합니다.
        metrics = [
            {
                "id": "metric_sources",
                "label": "수집된 웹 출처",
                "value": f"{sources_count}개 사이트",
                "change": "Real-time Scraped",
                "status": "normal"
            },
            {
                "id": "metric_confidence",
                "label": "분석 신뢰도",
                "value": confidence_score,
                "change": f"{category.upper()} Verified",
                "status": "success"
            },
            {
                "id": "metric_topic",
                "label": "분류된 카테고리",
                "value": f"[{category.upper()}] {query[:10]}...",
                "change": "Auto Classified",
                "status": "normal"
            }
        ]

        # 커스텀 지표가 전달된 경우 기본 지표 뒤에 병합합니다.
        if custom_metrics:
            metrics.extend(custom_metrics)

        # A2UI 최상위 선언적 페이로드 구조체를 완성합니다.
        a2ui_payload = {
            "version": "1.0",
            "title": f"📊 {category.upper()} 분야 실시간 데이터 대시보드",
            "metrics": metrics,
            "action_section": {
                "title": f"💡 [{category.upper()}] 에이전트 맞춤형 후속 탐색 (Human-in-the-Loop)",
                "description": f"'{query[:20]}...' 질문에 맞춰 최적화된 아래 옵션을 선택하면 탐색을 계속합니다.",
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

        # 생성된 A2UI 데이터 딕셔너리를 반환합니다.
        return a2ui_payload

    @staticmethod
    def to_json(a2ui_payload: Dict[str, Any]) -> str:
        """
        A2UI 딕셔너리 구조를 한글 깨짐 없이 UTF-8 기반 JSON 문자열로 직렬화합니다.
        
        :param a2ui_payload: A2UI 구성 딕셔너리
        :return: JSON 직렬화 문자열
        """
        # json.dumps에 ensure_ascii=False 파라미터를 전달하여 한글이 유니코드 이스케이프(\uXXXX)로 깨지지 않게 변환합니다.
        return json.dumps(a2ui_payload, ensure_ascii=False)

