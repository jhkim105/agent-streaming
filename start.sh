#!/usr/bin/env bash

# 스크립트 에러 발생 시 즉시 중단 설정
set -e

# 프로젝트 루트 디렉터리로 이동
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================================================="
echo "🚀 Agent Streaming System One-Touch Local Environment Launcher"
echo "=========================================================================="

# 1. 이전 프로세스 깔끔하게 정리 (Cleanup)
echo "[1/4] 🧹 기존 구동 중인 프로세스 정리 중..."
pkill -f "AgentStreamServerApplication" || true
pkill -f "AgentServerApplication" || true
pkill -f "agent-runtime" || true
pkill -f "vite" || true
sleep 1

# 2. 로컬 도커 인프라 컨테이너 구동 (KRaft Kafka + Redis)
echo "[2/4] 🐳 Docker Compose 인프라 (Kafka, Redis) 상태 확인 및 기동 중..."
docker-compose up -d

# Kafka 준비 완료 대기 (최대 15초 대기)
echo "⏳ Kafka 및 Redis 인프라 헬스체크 대기 중..."
sleep 3

# 3. 파이썬 Agent Runtime 기동
echo "[3/4] 🐍 Agent Runtime 프로세스 백그라운드 기동 중..."
cd "$PROJECT_ROOT/agent-runtime"
if [ ! -d ".venv" ]; then
    echo "  - uv 가상환경(.venv) 생성 중..."
    uv venv
fi
source .venv/bin/activate
echo "  - 의존성 패키지 동기화 중 (uv pip install)..."
uv pip install -r pyproject.toml > /dev/null 2>&1 || true

# 파이썬 에이전트 백그라운드 실행
python -m src.main > "$PROJECT_ROOT/agent-runtime.log" 2>&1 &
AGENT_PID=$!
echo "  - Agent Runtime PID: $AGENT_PID"

# 4. 코틀린 Agent Server 백그라운드 기동
echo "[4/4] ☕ Kotlin Agent Server 빌드 및 백그라운드 기동 중..."
cd "$PROJECT_ROOT/agent-server"
./gradlew bootRun > "$PROJECT_ROOT/server.log" 2>&1 &
SERVER_PID=$!
echo "  - Agent Server PID: $SERVER_PID"

# 5. 프론트엔드 Vite 개발 서버 구동
cd "$PROJECT_ROOT/frontend"
echo "  - Vite 프론트엔드 개발 서버 시작 (http://localhost:5173)..."
echo "=========================================================================="
echo "✨ 백엔드 서버: http://localhost:8080"
echo "✨ 백엔드 SSE Raw Inspector: http://localhost:8080/sse-debug.html"
echo "=========================================================================="

# 프론트엔드 Vite 포그라운드 실행
npm run dev
