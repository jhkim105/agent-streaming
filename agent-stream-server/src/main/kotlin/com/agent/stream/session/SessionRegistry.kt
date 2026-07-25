package com.agent.stream.session

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.channels.SendChannel
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

@Component
class SessionRegistry {

    private val sessions = ConcurrentHashMap<String, SendChannel<ServerSentEvent<String>>>()

    fun register(sessionId: String, channel: SendChannel<ServerSentEvent<String>>) {
        sessions[sessionId] = channel
        logger.info { "SSE 세션 등록 완료: sessionId=$sessionId (현재 세션 수: ${sessions.size})" }
    }

    fun remove(sessionId: String) {
        sessions.remove(sessionId)?.also {
            logger.info { "SSE 세션 해제 완료: sessionId=$sessionId (현재 세션 수: ${sessions.size})" }
        }
    }

    fun getChannel(sessionId: String): SendChannel<ServerSentEvent<String>>? = sessions[sessionId]

    fun hasSession(sessionId: String): Boolean = sessions.containsKey(sessionId)

    fun activeSessionCount(): Int = sessions.size
}
