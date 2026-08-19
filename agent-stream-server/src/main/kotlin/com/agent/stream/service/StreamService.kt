package com.agent.stream.service

import com.agent.stream.dto.AgentCommand
import com.agent.stream.dto.AgentEvent
import com.agent.stream.session.RedisConnectionRegistry
import com.agent.stream.session.SessionRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class StreamService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val sessionRegistry: SessionRegistry,
    private val redisConnectionRegistry: RedisConnectionRegistry,
    private val redisStreamRoutingService: RedisStreamRoutingService,
    private val conversationHistoryStore: ConversationHistoryStore,
    private val hostId: String,
    private val objectMapper: ObjectMapper
) {
    @Value("\${app.kafka.topic-commands:agent-commands}")
    private lateinit var topicCommands: String

    private val serviceScope = CoroutineScope(Dispatchers.Default)

    /**
     * AgentCommand를 등록하고 Redis에 commandId -> connectionId 매핑 저장 후 카프카 커맨드 토픽으로 전송합니다.
     */
    fun submitCommand(command: AgentCommand): String {
        val validConvId = conversationHistoryStore.getOrCreateConversation(
            command.conversationId,
            command.payload["query"] as? String
        )

        val finalCommand = command.copy(
            conversationId = validConvId,
            timestamp = System.currentTimeMillis()
        )

        // 1. Redis에 commandId -> connectionId 매핑 등록
        if (finalCommand.connectionId.isNotBlank()) {
            redisConnectionRegistry.registerCommandConnection(finalCommand.commandId, finalCommand.connectionId).subscribe()
        }

        // 2. Kafka 커맨드 토픽으로 발행
        val payloadMap = mapOf(
            "commandId" to finalCommand.commandId,
            "conversationId" to finalCommand.conversationId,
            "connectionId" to finalCommand.connectionId,
            "hostId" to hostId,
            "type" to finalCommand.type,
            "payload" to finalCommand.payload,
            "timestamp" to finalCommand.timestamp
        )
        val jsonPayload = objectMapper.writeValueAsString(payloadMap)

        logger.info { "Kafka 커맨드 토픽 전송 ($topicCommands): commandId=${finalCommand.commandId}, conversationId=$validConvId, connectionId=${finalCommand.connectionId}" }
        kafkaTemplate.send(topicCommands, finalCommand.commandId, jsonPayload)

        return finalCommand.commandId
    }

    /**
     * AgentEvent 수신 시 commandId -> connectionId -> hostId 다단계 동적 매핑 조회 후
     * 본인 노드 소켓 직통 배달 또는 타 노드 Redis Stream 릴레이를 수행합니다.
     */
    fun handleAgentEvent(event: AgentEvent) {
        // 1. 대화 이력 저장소에 실시간 이벤트 축적
        conversationHistoryStore.appendEvent(event)

        // 2. commandId로 해당 명령을 보낸 connectionId 동적 조회
        redisConnectionRegistry.getConnectionByCommand(event.commandId)
            .defaultIfEmpty("")
            .flatMap { targetConnectionId ->
                if (targetConnectionId.isBlank()) {
                    logger.debug { "commandId에 매핑된 connectionId 없음: commandId=${event.commandId}" }
                    serviceScope.launch { dispatchToLocalClient(event, "") }
                    return@flatMap reactor.core.publisher.Mono.empty<Void>()
                }

                // 3. connectionId로 해당 소켓이 위치한 타깃 서버 노드(targetHostId) 동적 조회
                redisConnectionRegistry.getConnectionHost(targetConnectionId)
                    .defaultIfEmpty(event.hostId.ifBlank { hostId })
                    .doOnNext { targetHostId ->
                        if (targetHostId == hostId) {
                            // 본인 노드인 경우 직통 배달 (Issue #5: CoroutineScope에서 send() 배압 배달)
                            logger.debug { "본인 노드 소켓 직통 배달 (hostId=$hostId): commandId=${event.commandId}, connectionId=$targetConnectionId" }
                            serviceScope.launch { dispatchToLocalClient(event, targetConnectionId) }
                        } else {
                            // 타 노드인 경우 Redis Streams XADD 릴레이
                            logger.info { "타 노드 소켓 감지 ➔ Redis Streams XADD 릴레이 (본인=$hostId, 타겟=$targetHostId): commandId=${event.commandId}, connectionId=$targetConnectionId" }
                            redisStreamRoutingService.publishToTargetStream(targetHostId, targetConnectionId, event).subscribe()
                        }
                    }
                    .then()
            }
            .subscribe({}, { err ->
                logger.error(err) { "동적 연결 위치 조회 중 오류 발생: commandId=${event.commandId}" }
            })
    }

    /**
     * local SSE SendChannel 세션에 이벤트를 전송합니다. (Issue #5: send() 호출로 배압 보장)
     */
    suspend fun dispatchToLocalClient(event: AgentEvent, connectionId: String) {
        val channel = sessionRegistry.getChannel(connectionId)
        if (channel != null) {
            val sseEvent = ServerSentEvent.builder<String>()
                .id(event.eventId)
                .event(event.type)
                .data(objectMapper.writeValueAsString(event))
                .build()

            try {
                // Issue #5: trySend 대신 send()로 채널 가득 차도 유실 없이 배압 대기
                channel.send(sseEvent)
                logger.debug { "Client SSE 배달 성공 (send 배압 보장): type=${event.type}, connectionId=$connectionId" }
            } catch (e: Exception) {
                logger.warn(e) { "Client SSE 배달 실패 (Channel closed): connectionId=$connectionId" }
            }
        } else {
            logger.debug { "해당 connectionId의 로컬 세션을 찾을 수 없음: connectionId=$connectionId" }
        }
    }
}
