package com.agent.stream.history

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.dto.EventMetadata
import com.agent.stream.service.ConversationHistoryStore
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 conversationId 대화 이력 보존 및 LLM 스마트 타이틀 갱신 검증 통합 테스트입니다.
 */
class ConversationHistoryIntegrationTest : BehaviorSpec({

    given("대화 이력 영속성 저장소(ConversationHistoryStore)가 준비되었을 때") {
        val historyStore = ConversationHistoryStore()

        `when`("사용자가 새로운 질문을 전송하여 conversationId가 생겨나면") {
            val userQuery = "spring boot kotlin 조사해줘"
            val conversationId = historyStore.getOrCreateConversation(null, userQuery)

            then("신규 conversationId가 할당되고 대화 목록에 질문 원본 텍스트로 1차 타이틀이 등록되어야 한다") {
                conversationId shouldNotBe ""
                conversationId.startsWith("conv-") shouldBe true

                val summaries = historyStore.getConversationSummaries()
                summaries shouldHaveSize 1
                summaries[0].conversationId shouldBe conversationId
                summaries[0].title shouldBe "spring boot kotlin 조사해줘"
            }

            `when`("파이썬 에이전트가 LLM 스마트 이모지 타이틀(🌱 Spring Boot & Kotlin 동향)을 수신하면") {
                val llmSmartTitle = "🌱 Spring Boot & Kotlin 동향"

                // STATUS 이벤트 (LLM 스마트 타이틀 메타데이터 수신!)
                historyStore.appendEvent(
                    AgentResponseEvent(
                        sessionId = "sse-session-1",
                        conversationId = conversationId,
                        hostId = "kotlin-node-1",
                        type = "STATUS",
                        content = "🔍 사용자 질문 의도 분석 중...",
                        metadata = EventMetadata(step = "query_analysis", title = llmSmartTitle)
                    )
                )

                then("대화 스레드의 타이틀이 무미건조한 기본명 대신 LLM 스마트 타이틀로 즉시 갱신되어야 한다") {
                    val summaries = historyStore.getConversationSummaries()
                    summaries[0].title shouldBe llmSmartTitle
                }

                `when`("에이전트가 CHUNK 및 DONE 이벤트를 연속으로 전송하면") {
                    // CHUNK 1
                    historyStore.appendEvent(
                        AgentResponseEvent(
                            sessionId = "sse-session-1",
                            conversationId = conversationId,
                            hostId = "kotlin-node-1",
                            type = "CHUNK",
                            content = "# 📊 Spring Boot & Kotlin 리포트\n\n내용...",
                            metadata = EventMetadata(step = "report_generation", title = llmSmartTitle)
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
                            metadata = EventMetadata(step = "completed", title = llmSmartTitle)
                        )
                    )

                    then("새로고침 시 해당 conversationId의 대화 상세 내역과 스마트 타이틀이 완벽히 유지되어야 한다") {
                        val detail = historyStore.getConversationDetail(conversationId)

                        detail shouldNotBe null
                        detail!!.conversationId shouldBe conversationId
                        detail.title shouldBe llmSmartTitle
                        detail.fullReport shouldBe "# 📊 Spring Boot & Kotlin 리포트\n\n내용..."
                        detail.isCompleted shouldBe true
                    }
                }
            }
        }
    }
})
