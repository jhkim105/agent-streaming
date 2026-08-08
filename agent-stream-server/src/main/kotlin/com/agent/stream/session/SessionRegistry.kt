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

    /**
     * 단순 sessionId로 세션을 제거합니다.
     */
    fun remove(sessionId: String) {
        sessions.remove(sessionId)?.also {
            logger.info { "SSE 세션 해제 완료: sessionId=$sessionId (현재 세션 수: ${sessions.size})" }
        }
    }

    /**
     * Issue #2 세션 오삭제 방지: 현재 열린 채널 객체가 매개변수의 채널과 동일할 때만 안전하게 제거합니다.
     */
    fun remove(sessionId: String, channel: SendChannel<ServerSentEvent<String>>) {
        val removed = sessions.remove(sessionId, channel)
        if (removed) {
            logger.info { "SSE 세션 안전 해제 완료: sessionId=$sessionId (현재 세션 수: ${sessions.size})" }
        } else {
            logger.debug { "이전 SSE 세션 해제 무시 (새로운 소켓 채널로 재바인딩됨): sessionId=$sessionId" }
        }
    }

    fun getChannel(sessionId: String): SendChannel<ServerSentEvent<String>>? = sessions[sessionId]

    fun hasSession(sessionId: String): Boolean = sessions.containsKey(sessionId)

    fun activeSessionCount(): Int = sessions.size
}
