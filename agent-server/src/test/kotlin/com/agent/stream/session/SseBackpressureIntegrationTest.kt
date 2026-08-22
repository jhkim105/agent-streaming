package com.agent.stream.session

import com.agent.stream.dto.AgentEvent
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.RedisStreamRoutingService
import com.agent.stream.service.StreamService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.mockito.Mockito.mock
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 Issue #5: trySend() vs send() 배압(Backpressure) 검증 통합 테스트입니다.
 */
class SseBackpressureIntegrationTest : BehaviorSpec({

    val objectMapper: ObjectMapper = jacksonObjectMapper()

    given("Issue #5: 느린 클라이언트 환경에서 SSE 채널 버퍼 용량이 1개(capacity=1)로 제한되어 있을 때") {

        val sessionRegistry = SessionRegistry()
        val mockRedisConnectionRegistry = mock(RedisConnectionRegistry::class.java)
        val mockRedisTemplate = mock(ReactiveStringRedisTemplate::class.java)
        val conversationHistoryStore = ConversationHistoryStore()
        @Suppress("UNCHECKED_CAST")
        val fakeKafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>

        val redisStreamRoutingService = RedisStreamRoutingService(
            redisTemplate = mockRedisTemplate,
            sessionRegistry = sessionRegistry,
            hostId = "kotlin-node-test",
            objectMapper = objectMapper
        )

        val streamService = StreamService(
            kafkaTemplate = fakeKafkaTemplate,
            sessionRegistry = sessionRegistry,
            redisConnectionRegistry = mockRedisConnectionRegistry,
            redisStreamRoutingService = redisStreamRoutingService,
            conversationHistoryStore = conversationHistoryStore,
            hostId = "kotlin-node-test",
            objectMapper = objectMapper
        )

        `when`("버퍼 용량이 1개인 채널에 5개의 이벤트를 trySend() 방식으로 전송하면") {
            val smallCapacityChannel = Channel<ServerSentEvent<String>>(capacity = 1)
            val connectionId = "conn-slow-client-1"
            sessionRegistry.register(connectionId, smallCapacityChannel)

            val successCount = (1..5).count { i ->
                val sseEvent = ServerSentEvent.builder<String>()
                    .id("evt-$i")
                    .event("CHUNK")
                    .data("Data $i")
                    .build()
                smallCapacityChannel.trySend(sseEvent).isSuccess
            }

            then("버퍼 초과로 인해 대부분의 이벤트 전송이 실패(유실)하여 성공 횟수가 1~2개에 불과해야 한다") {
                successCount shouldBe 1
            }
        }

        `when`("버퍼 용량이 1개인 채널에 코루틴 suspending send() 배압 방식으로 5개의 이벤트를 전송하고 클라이언트가 지연 후 소비하면") {
            val smallCapacityChannel = Channel<ServerSentEvent<String>>(capacity = 1)
            val connectionId = "conn-slow-client-2"
            sessionRegistry.register(connectionId, smallCapacityChannel)

            val receivedEvents = mutableListOf<String>()

            // 클라이언트 비동기 소비자 루프 (50ms 지연하며 천천히 소비)
            val consumerJob = launch {
                for (sse in smallCapacityChannel) {
                    receivedEvents.add(sse.data() ?: "")
                    delay(50)
                }
            }

            // 생산자가 5개의 이벤트를 suspend fun dispatchToLocalClient()를 사용해 배압을 존중하며 전송
            for (i in 1..5) {
                val event = AgentEvent(
                    eventId = "evt-send-$i",
                    commandId = "cmd-1",
                    conversationId = "conv-1",
                    type = "CHUNK",
                    content = "Stream Data $i"
                )
                streamService.dispatchToLocalClient(event, connectionId)
            }

            delay(400) // 클라이언트 수신 완결 대기
            smallCapacityChannel.close()
            consumerJob.join()

            then("배압이 존중되어 단 1개의 이벤트도 유실되지 않고 5개 전량이 100% 정상 수신되어야 한다") {
                receivedEvents shouldHaveSize 5
                receivedEvents[0] shouldBe "{\"eventId\":\"evt-send-1\",\"commandId\":\"cmd-1\",\"conversationId\":\"conv-1\",\"hostId\":\"\",\"type\":\"CHUNK\",\"content\":\"Stream Data 1\",\"metadata\":{},\"timestamp\":${receivedEvents[0].let { objectMapper.readValue(it, AgentEvent::class.java).timestamp }}}"
                receivedEvents[4].contains("Stream Data 5") shouldBe true
            }
        }
    }
})
