package com.agent.stream.routing

import com.agent.stream.dto.AgentResponseEvent
import com.agent.stream.dto.EventMetadata
import com.agent.stream.service.ConversationHistoryStore
import com.agent.stream.service.RedisStreamRoutingService
import com.agent.stream.service.StreamService
import com.agent.stream.session.RedisSessionRegistry
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.channels.Channel
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import reactor.core.publisher.Mono

/**
 * Kotest BehaviorSpec BDD 스타일로 작성된 다중 노드 세션 라우팅 검증 통합 테스트입니다.
 */
class MultiNodeRoutingIntegrationTest : BehaviorSpec({

    val objectMapper: ObjectMapper = jacksonObjectMapper()

    given("두 개의 스트리밍 서버 노드 (Node 1, Node 2) 환경이 구성되어 있을 때") {

        val node1HostId = "kotlin-node-1"
        val node1SessionRegistry = SessionRegistry()

        val node2HostId = "kotlin-node-2"
        val node2SessionRegistry = SessionRegistry()
        val conversationHistoryStore = ConversationHistoryStore()

        var lastPublishedTargetStreamKey = ""
        var lastPublishedPayloadJson = ""

        @Suppress("UNCHECKED_CAST")
        val fakeKafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>
        val mockRedisTemplate = mock(ReactiveStringRedisTemplate::class.java)
        val mockRedisSessionRegistry = mock(RedisSessionRegistry::class.java)

        val node2RedisStreamRoutingService = object : RedisStreamRoutingService(
            redisTemplate = mockRedisTemplate,
            sessionRegistry = node2SessionRegistry,
            hostId = node2HostId,
            objectMapper = objectMapper
        ) {
            override fun publishToTargetStream(targetHostId: String, event: AgentResponseEvent): Mono<String> {
                lastPublishedTargetStreamKey = "stream:host:$targetHostId"
                lastPublishedPayloadJson = objectMapper.writeValueAsString(event)

                if (targetHostId == node1HostId) {
                    val channel = node1SessionRegistry.getChannel(event.sessionId)
                    if (channel != null) {
                        val sseEvent = ServerSentEvent.builder<String>()
                            .event(event.type)
                            .data(objectMapper.writeValueAsString(event))
                            .build()
                        channel.trySend(sseEvent)
                    }
                }
                return Mono.just("1786187000-0")
            }
        }

        val node2StreamService = StreamService(
            kafkaTemplate = fakeKafkaTemplate,
            sessionRegistry = node2SessionRegistry,
            redisSessionRegistry = mockRedisSessionRegistry,
            redisStreamRoutingService = node2RedisStreamRoutingService,
            conversationHistoryStore = conversationHistoryStore,
            hostId = node2HostId,
            objectMapper = objectMapper
        )

        `when`("Node 1에 사용자의 SSE 소켓(sessionId: test-user-session-100)이 등록되어 있을 때") {
            val userSessionId = "test-user-session-100"
            val userSseChannel = Channel<ServerSentEvent<String>>(10)

            node1SessionRegistry.register(userSessionId, userSseChannel)

            given(mockRedisSessionRegistry.getSessionHost(userSessionId))
                .willReturn(Mono.just(node1HostId))

            `when`("Node 2가 Kafka에서 Node 1을 타깃으로 하는 응답 이벤트를 수신하면") {
                val incomingEvent = AgentResponseEvent(
                    sessionId = userSessionId,
                    conversationId = "conv-test-123",
                    hostId = node1HostId,
                    type = "CHUNK",
                    content = "Ollama Qwen2.5-7B 토큰 테스트 데이터",
                    metadata = EventMetadata(step = "report_generation")
                )

                node2StreamService.handleAgentResponse(incomingEvent)

                then("Node 2는 본인 소켓이 아님을 감지하고 Node 1의 Redis Stream(stream:host:kotlin-node-1)으로 라우팅해야 한다") {
                    lastPublishedTargetStreamKey shouldBe "stream:host:kotlin-node-1"
                    lastPublishedPayloadJson shouldNotBe ""
                }

                then("Node 1에 연결된 사용자의 SSE Channel로 해당 토큰 이벤트가 정상 배달되어야 한다") {
                    val receivedSse = userSseChannel.tryReceive().getOrNull()

                    receivedSse shouldNotBe null
                    receivedSse?.event() shouldBe "CHUNK"
                    receivedSse?.data() shouldNotBe null
                    receivedSse?.data()!!.contains("Ollama Qwen2.5-7B 토큰 테스트 데이터") shouldBe true
                }
            }
        }
    }
})
