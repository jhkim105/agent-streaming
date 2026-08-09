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
     * conversationId가 없으면 신규 생성하고, 질문 유입 시 질문 원본 텍스트로 타이틀을 1차 갱신합니다.
     */
    fun getOrCreateConversation(conversationIdInput: String?, query: String? = null): String {
        val conversationId = if (!conversationIdInput.isNullOrBlank()) {
            conversationIdInput
        } else {
            "conv-" + UUID.randomUUID().toString().take(8)
        }

        val now = System.currentTimeMillis()
        val formattedTitle = if (!query.isNullOrBlank()) {
            if (query.length > 35) query.take(35) + "..." else query
        } else {
            "신규 리서치 대화 (${conversationId.takeLast(6)})"
        }

        conversations.compute(conversationId) { id, existing ->
            if (existing == null) {
                logger.info { "신규 대화 스레드 생성: conversationId=$id, title='$formattedTitle'" }
                ConversationSummaryDto(
                    conversationId = id,
                    title = formattedTitle,
                    category = "general",
                    createdAt = now,
                    updatedAt = now
                )
            } else {
                // 기존 대화 제목이 '신규 리서치 대화'인 경우 실제 질문 텍스트로 1차 갱신
                val updatedTitle = if (existing.title.startsWith("신규 리서치 대화") && !query.isNullOrBlank()) {
                    formattedTitle
                } else existing.title

                existing.copy(title = updatedTitle, updatedAt = now)
            }
        }

        return conversationId
    }

    /**
     * 카프카/Redis로 전달받은 스트리밍 이벤트를 축적하고, DONE 스트림 완결 시점에 1회 일괄 저장 및 LLM 스마트 타이틀 갱신을 실행합니다.
     */
    fun appendEvent(event: AgentResponseEvent) {
        val conversationId = event.conversationId
        if (conversationId.isBlank()) return

        // 1. 이벤트 유형별 스트림 축적
        when (event.type) {
            "STATUS" -> {
                timelineEventsMap.computeIfAbsent(conversationId) { CopyOnWriteArrayList() }.add(event)
            }
            "CHUNK" -> {
                reportBuilderMap.computeIfAbsent(conversationId) { StringBuilder() }.append(event.content)
            }
            "A2UI_RENDER" -> {
                a2uiPayloadMap[conversationId] = event.content
            }
            "DONE" -> {
                // 2. [스트림 완결 DONE 시점] 1회 일괄 저장(Batch Persistence) & 스마트 타이틀 1회 확정 갱신!
                val now = System.currentTimeMillis()
                val smartTitle = event.metadata.title

                conversations[conversationId]?.let { existing ->
                    val finalTitle = if (smartTitle.isNotBlank()) smartTitle else existing.title

                    val finalCategory = if (event.content.contains("[TECH]") || event.content.contains("TECH")) "tech"
                    else if (event.content.contains("[BUSINESS]") || event.content.contains("BUSINESS")) "business"
                    else existing.category

                    logger.info { "대화 스트림 완결 1회 일괄 저장 & 스마트 타이틀 갱신 완료: conversationId=$conversationId, title='$finalTitle'" }

                    conversations[conversationId] = existing.copy(
                        title = finalTitle,
                        category = finalCategory,
                        updatedAt = now
                    )
                }

                completionMap[conversationId] = true
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
