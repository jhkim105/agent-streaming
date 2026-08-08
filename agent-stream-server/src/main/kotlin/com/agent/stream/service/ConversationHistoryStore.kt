package com.agent.stream.service

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.dto.ConversationDetailDto
import com.agent.stream.dto.ConversationSummaryDto
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

private val logger = KotlinLogging.logger {}

/**
 * 대화 스레드(Conversation) 및 개별 스트리밍 이벤트를 영속성/인메모리에 축적 관리하는 저장소 서비스입니다.
 */
@Component
class ConversationHistoryStore {

    // conversationId -> ConversationSummaryDto 매핑
    private val conversations = ConcurrentHashMap<String, ConversationSummaryDto>()

    // conversationId -> 타임라인 STATUS 이벤트 리스트 매핑
    private val timelineEventsMap = ConcurrentHashMap<String, CopyOnWriteArrayList<AgentResponseEvent>>()

    // conversationId -> 축적된 마크다운 보고서 StringBuilder 매핑
    private val reportBuilderMap = ConcurrentHashMap<String, StringBuilder>()

    // conversationId -> A2UI 대시보드 JSON 저장 매핑
    private val a2uiPayloadMap = ConcurrentHashMap<String, String>()

    // conversationId -> DONE 완료 여부 매핑
    private val completionMap = ConcurrentHashMap<String, Boolean>()

    /**
     * conversationId가 없으면 신규 생성하고, 있으면 기존 대화를 가져옵니다.
     */
    fun getOrCreateConversation(conversationIdInput: String?, query: String? = null): String {
        val conversationId = if (!conversationIdInput.isNullOrBlank()) {
            conversationIdInput
        } else {
            "conv-" + UUID.randomUUID().toString().take(8)
        }

        if (!conversations.containsKey(conversationId)) {
            val title = if (!query.isNullOrBlank()) {
                if (query.length > 25) query.take(25) + "..." else query
            } else {
                "신규 리서치 대화 (${conversationId.takeLast(6)})"
            }
            val now = System.currentTimeMillis()
            logger.info { "신규 대화 스레드 생성: conversationId=$conversationId, title='$title'" }
            conversations[conversationId] = ConversationSummaryDto(
                conversationId = conversationId,
                title = title,
                category = "general",
                createdAt = now,
                updatedAt = now
            )
        }

        return conversationId
    }

    /**
     * 카프카/Redis로 전달받은 스트리밍 이벤트를 대화 이력에 축적합니다.
     */
    fun appendEvent(event: AgentResponseEvent) {
        val conversationId = event.conversationId
        if (conversationId.isBlank()) return

        val now = System.currentTimeMillis()

        // 대화 생성 타임스탬프 갱신
        conversations[conversationId]?.let { existing ->
            val updatedCategory = if (event.content.contains("[TECH]") || event.content.contains("TECH")) "tech"
            else if (event.content.contains("[BUSINESS]") || event.content.contains("BUSINESS")) "business"
            else existing.category

            conversations[conversationId] = existing.copy(
                category = updatedCategory,
                updatedAt = now
            )
        }

        when (event.type) {
            "STATUS" -> {
                // 타임라인 진행 상태 이벤트 축적
                timelineEventsMap.computeIfAbsent(conversationId) { CopyOnWriteArrayList() }.add(event)
            }
            "CHUNK" -> {
                // 마크다운 리포트 토큰 조각 축적
                reportBuilderMap.computeIfAbsent(conversationId) { StringBuilder() }.append(event.content)
            }
            "A2UI_RENDER" -> {
                // A2UI JSON 대시보드 저장
                a2uiPayloadMap[conversationId] = event.content
            }
            "DONE" -> {
                // 작업 완료 처리
                completionMap[conversationId] = true
                logger.info { "대화 스레드 리포트 생성 완료: conversationId=$conversationId" }
            }
        }
    }

    /**
     * 전체 대화 스레드 요약 목록을 최신 순으로 반환합니다. (GET /api/chat/conversations)
     */
    fun getConversationSummaries(): List<ConversationSummaryDto> {
        return conversations.values.sortedByDescending { it.updatedAt }
    }

    /**
     * 특정 대화의 상세 타임라인 및 완결된 마크다운 보고서를 조회합니다. (새로고침 복원 & 상세 조회용)
     */
    fun getConversationDetail(conversationId: String): ConversationDetailDto? {
        val summary = conversations[conversationId] ?: return null
        val timeline = timelineEventsMap[conversationId] ?: emptyList()
        val fullReport = reportBuilderMap[conversationId]?.toString() ?: ""
        val a2uiPayload = a2uiPayloadMap[conversationId]
        val isCompleted = completionMap[conversationId] ?: false

        return ConversationDetailDto(
            conversationId = summary.conversationId,
            title = summary.title,
            category = summary.category,
            createdAt = summary.createdAt,
            updatedAt = summary.updatedAt,
            timelineEvents = timeline,
            fullReport = fullReport,
            a2uiPayload = a2uiPayload,
            isCompleted = isCompleted
        )
    }
}
