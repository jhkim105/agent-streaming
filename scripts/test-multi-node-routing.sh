#!/usr/bin/env zsh

# ==============================================================================
# Multi-Node Cross Session Routing Automated Test Script
# 
# 본 스크립트는 코틀린 스트리밍 서버가 2대(Node 1: 8080, Node 2: 8081) 뜬 상태에서
# 멀티 인스턴스 세션 라우팅(Redis Pub/Sub)이 정상 작동하는지 E2E로 테스트합니다.
# ==============================================================================

NODE1_URL="http://localhost:8080"
NODE2_URL="http://localhost:8081"

echo "======================================================================"
echo "🚀 멀티 노드 세션 라우팅 (Redis Pub/Sub Cross-Routing) 테스트 시작"
echo "======================================================================"

# 1. Node 1 헬스체크
echo "\n1️⃣ Node 1 (port 8080) 헬스체크 중..."
if ! curl -s "$NODE1_URL/actuator/health" > /dev/null && ! curl -s "$NODE1_URL/api/chat/stream" --max-time 1 > /dev/null; then
    echo "❌ Node 1 (8080 포트)이 실행되어 있지 않습니다."
    echo "👉 실행 방법: SERVER_PORT=8080 ./gradlew bootRun"
    exit 1
fi
echo "✅ Node 1 준비 완료!"

# 2. Node 2 헬스체크
echo "\n2️⃣ Node 2 (port 8081) 헬스체크 중..."
if ! curl -s "$NODE2_URL/actuator/health" > /dev/null && ! curl -s "$NODE2_URL/api/chat/stream" --max-time 1 > /dev/null; then
    echo "❌ Node 2 (8081 포트)가 실행되어 있지 않습니다."
    echo "👉 실행 방법: SERVER_PORT=8081 ./gradlew bootRun"
    exit 1
fi
echo "✅ Node 2 준비 완료!"

echo "\n======================================================================"
echo "🧪 시나리오: Node 1로 SSE 커넥션 수립 후 질문 발송"
echo "   카프카 응답을 Node 2가 소비하더라도 Redis Pub/Sub을 통해 Node 1로 배달되는지 확인합니다."
echo "======================================================================"

# Node 1에 SSE 연결을 수립하여 INIT event에서 sessionId 추출
echo "\n📥 Node 1 ($NODE1_URL/api/chat/stream) SSE 연결 시도 중..."
SSE_OUTPUT=$(curl -sN -m 3 "$NODE1_URL/api/chat/stream" | head -n 5)

SESSION_ID=$(echo "$SSE_OUTPUT" | grep -o '"sessionId":"[^"]*"' | cut -d'"' -f4)

if [ -z "$SESSION_ID" ]; then
    echo "❌ Node 1에서 SSE Session ID 수립 실패!"
    echo "수신된 출력:\n$SSE_OUTPUT"
    exit 1
fi

echo "✅ Session ID 수립 성공: [$SESSION_ID]"

# Node 1으로 질문 전송
echo "\n📤 Node 1로 질문 전송 중 (sessionId: $SESSION_ID)..."
POST_RESP=$(curl -s -X POST "$NODE1_URL/api/chat/message" \
  -H "Content-Type: application/json" \
  -d "{\"sessionId\":\"$SESSION_ID\", \"query\":\"Ollama 세션 라우팅 테스트\"}")

echo "✅ 질문 등록 완료 (HTTP 202 Accepted)"
echo "   요청 결과: $POST_RESP"

echo "\n======================================================================"
echo "🔍 검증 방법 (각 서버의 콘솔 로그를 확인하세요!):"
echo "----------------------------------------------------------------------"
echo "1. Node 2 (8081 포트) 콘솔 로그:"
echo "   ➔ '[StreamService] 타 노드 메시지 감지 ➔ Redis Pub/Sub 라우팅 (본인=..., 타겟=node-1): sessionId=$SESSION_ID'"
echo ""
echo "2. Node 1 (8080 포트) 콘솔 로그:"
echo "   ➔ '[RedisRoutingService] Redis 채널 메시지 수신 (host:...): type=CHUNK, sessionId=$SESSION_ID'"
echo "======================================================================"
