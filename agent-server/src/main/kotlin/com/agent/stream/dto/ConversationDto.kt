package com.agent.stream.dto

/**
 * 대화 목록 조회 시 사용되는 요약 DTO입니다.
 */
data class ConversationSummaryDto(
    val conversationId: String,
    val title: String,
    val category: String = "general",
    val createdAt: Long,
    val updatedAt: Long
)

/**
 * 1개 턴(사용자 질문 + 에이전트 응답 세트) 상세 DTO입니다.
 */
data class ConversationRunDetailDto(
    val runId: String,
    val userPrompt: String = "",
    val timelineEvents: List<AgentEvent> = emptyList(), // 해당 턴의 STATUS 이벤트 목록
    val fullReport: String = "",                         // 해당 턴의 마크다운 리포트
    val a2uiPayload: String? = null,                      // 해당 턴의 A2UI JSON 대시보드 데이터
    val isCompleted: Boolean = false,                     // 해당 턴의 DONE 완료 여부
    val createdAt: Long = System.currentTimeMillis()
)

/**
 * 특정 대화 상세 복원 및 새로고침 조회 시 사용되는 상세 DTO입니다.
 */
data class ConversationDetailDto(
    val conversationId: String,
    val title: String,
    val category: String,
    val createdAt: Long,
    val updatedAt: Long,
    val runs: List<ConversationRunDetailDto> = emptyList(), // 멀티턴 목록
    val timelineEvents: List<AgentEvent> = emptyList(),     // 하위 호환용 전체 타임라인
    val fullReport: String = "",                             // 하위 호환용 전체 리포트
    val a2uiPayload: String? = null,                          // 하위 호환용 최신 A2UI
    val isCompleted: Boolean = false
)
