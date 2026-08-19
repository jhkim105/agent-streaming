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
 * 특정 대화 상세 복원 및 새로고침 조회 시 사용되는 상세 DTO입니다.
 */
data class ConversationDetailDto(
    val conversationId: String,
    val title: String,
    val category: String,
    val createdAt: Long,
    val updatedAt: Long,
    val timelineEvents: List<AgentEvent> = emptyList(), // STATUS 이벤트 목록 (타임라인용)
    val fullReport: String = "",                         // 합성된 완성 마크다운 리포트
    val a2uiPayload: String? = null,                      // A2UI JSON 대시보드 데이터
    val isCompleted: Boolean = false                      // DONE 완료 여부
)
