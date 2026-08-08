package com.agent.stream.history

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.dto.EventMetadata
import com.agent.stream.service.ConversationHistoryStore
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 conversationId 대화 이력 보존 및 새로고침 복원 검증 통합 테스트입니다.
 */
class ConversationHistoryIntegrationTest : BehaviorSpec({

    given("대화 이력 영속성 저장소(ConversationHistoryStore)가 준비되었을 때") {
        val historyStore = ConversationHistoryStore()

        `when`("사용자가 새로운 질문을 전송하여 conversationId가 생겨나면") {
            val userQuery = "LiteLLM 프레임워크 최신 동향 조사해줘"
            val conversationId = historyStore.getOrCreateConversation(null, userQuery)

            then("신규 conversationId가 할당되고 대화 목록에 요약 정보가 등록되어야 한다") {
                conversationId shouldNotBe ""
                conversationId.startsWith("conv-") shouldBe true

                val summaries = historyStore.getConversationSummaries()
                summaries shouldHaveSize 1
                summaries[0].conversationId shouldBe conversationId
                summaries[0].title.contains("LiteLLM") shouldBe true
            }

            `when`("에이전트가 STATUS, CHUNK, A2UI, DONE 이벤트를 연속으로 전송하면") {
                // STATUS 1
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "STATUS",
                        content = "🔍 사용자 질문 분석 중...",
                        metadata = EventMetadata(step = "query_analysis")
                    )
                )

                // CHUNK 1 & 2 (마크다운 토큰 축적)
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "CHUNK",
                        content = "# 📊 LiteLLM 조사 보고서\n\n",
                        metadata = EventMetadata(step = "report_generation")
                    )
                )
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "CHUNK",
                        content = "LiteLLM은 다양한 LLM API를 단일 인터페이스로 제공하는 프레임워크입니다.",
                        metadata = EventMetadata(step = "report_generation")
                    )
                )

                // A2UI_RENDER
                val sampleA2uiJson = "{\"version\":\"1.0\",\"title\":\"LiteLLM 대시보드\"}"
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "A2UI_RENDER",
                        content = sampleA2uiJson,
                        metadata = EventMetadata(step = "a2ui_generation")
                    )
                )

                // DONE
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "DONE",
                        content = "리포트 완결",
                        metadata = EventMetadata(step = "completed")
                    )
                )

                then("새로고침 시 해당 conversationId의 대화 상세 내역과 완결된 마크다운 보고서가 완벽히 복원되어야 한다") {
                    val detail = historyStore.getConversationDetail(conversationId)

                    detail shouldNotBe null
                    detail!!.conversationId shouldBe conversationId
                    detail.timelineEvents shouldHaveSize 1
                    detail.timelineEvents[0].content shouldBe "🔍 사용자 질문 분석 중..."

                    detail.fullReport shouldBe "# 📊 LiteLLM 조사 보고서\n\nLiteLLM은 다양한 LLM API를 단일 인터페이스로 제공하는 프레임워크입니다."
                    detail.a2uiPayload shouldBe sampleA2uiJson
                    detail.isCompleted shouldBe true
                }
            }
        }
    }
})
