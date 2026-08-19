package com.agent.stream.session

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.channels.SendChannel
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * 인-메모리 SSE 소켓 연결 세션 레지스트리입니다.
 * connectionId ➔ SSE SendChannel 매핑을 관리합니다.
 */
@Component
class SessionRegistry {

    private val sessions = ConcurrentHashMap<String, SendChannel<ServerSentEvent<String>>>()

    fun register(connectionId: String, channel: SendChannel<ServerSentEvent<String>>) {
        sessions[connectionId] = channel
        logger.info { "SSE 소켓 연결 등록 완료: connectionId=$connectionId (현재 세션 수: ${sessions.size})" }
    }

    fun remove(connectionId: String) {
        sessions.remove(connectionId)?.also {
            logger.info { "SSE 소켓 연결 해제 완료: connectionId=$connectionId (현재 세션 수: ${sessions.size})" }
        }
    }

    fun remove(connectionId: String, channel: SendChannel<ServerSentEvent<String>>) {
        val removed = sessions.remove(connectionId, channel)
        if (removed) {
            logger.info { "SSE 소켓 연결 안전 해제 완료: connectionId=$connectionId (현재 세션 수: ${sessions.size})" }
        } else {
            logger.debug { "이전 SSE 소켓 연결 해제 무시 (새로운 소켓 채널로 재바인딩됨): connectionId=$connectionId" }
        }
    }

    fun getChannel(connectionId: String): SendChannel<ServerSentEvent<String>>? = sessions[connectionId]

    fun hasSession(connectionId: String): Boolean = sessions.containsKey(connectionId)

    fun activeSessionCount(): Int = sessions.size
}
