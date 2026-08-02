#!/usr/bin/env bash
# ==============================================================================
# Real-time AI Researcher Agent - All-in-One Integrated Starter Script
# ==============================================================================
# 본 스크립트는 Docker 인프라(Kafka/Redis), Python Agent Worker,
# Kotlin Stream Server, React Frontend 개발 서버를 한 번에 구동하고 관리합니다.
# 종료 시 (Ctrl+C) 모든 백그라운드 프로세스가 안전하게 일괄 정지됩니다.
# ==============================================================================

set -e

# 터미널 컬러 서식 설정
BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
RESET='\033[0m'

# 작업 루트 디렉터리 고정
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo -e "${BOLD}${CYAN}"
echo "================================================================="
echo "🚀 Real-time AI Researcher Agent - One-Touch System Starter"
echo "================================================================="
echo -e "${RESET}"

# ------------------------------------------------------------------------------
# 0. 프로세스 자동 종료 트랩 (Trap) 설정
# ------------------------------------------------------------------------------
cleanup() {
    echo -e "\n${YELLOW}🛑 모든 서브 시스템 프로세스를 종료하는 중입니다...${RESET}"
    kill 0 2>/dev/null || true
    echo -e "${GREEN}✨ 모든 프로세스가 안전하게 정지되었습니다.${RESET}"
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# ------------------------------------------------------------------------------
# 1. 로컬 인프라 (Kafka, Redis) 체크 및 자동 실행 (Docker Compose)
# ------------------------------------------------------------------------------
echo -e "${BOLD}[Step 1/4] 📦 로컬 인프라 서비스 점검 (Kafka & Redis)...${RESET}"

if ! nc -z localhost 9092 2>/dev/null || ! nc -z localhost 6379 2>/dev/null; then
    echo -e "${YELLOW}-> Kafka 또는 Redis가 실행 중이지 않습니다. Docker Compose 기동을 시작합니다...${RESET}"
    docker compose up -d
    echo -e "${GREEN}-> Docker 인프라 서비스 기동 완료.${RESET}"
    echo -e "${CYAN}-> Kafka UI: http://localhost:8989 (admin / admin)${RESET}"
    sleep 2
else
    echo -e "${GREEN}-> Kafka (9092) 및 Redis (6379) 인프라 정상 구동 확인.${RESET}"
fi

# ------------------------------------------------------------------------------
# 2. Python Agent Worker 기동
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 2/4] 🐍 Python Agent Worker 프로세스 구동...${RESET}"
cd "$ROOT_DIR/agent-worker"

if command -v uv &> /dev/null; then
    echo "-> uv 패키지 관리자를 사용하여 Python Worker를 백그라운드로 시작합니다."
    uv run python -m src.main &
else
    echo -e "${YELLOW}-> uv가 없습니다. 기본 python3 모듈을 실행합니다.${RESET}"
    python3 -m src.main &
fi
WORKER_PID=$!
echo -e "${GREEN}-> Python Agent Worker 기동됨 (PID: $WORKER_PID)${RESET}"

# ------------------------------------------------------------------------------
# 3. Kotlin Agent Stream Server 기동
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 3/4] ☕ Kotlin Agent Stream Server (Spring WebFlux) 구동...${RESET}"
cd "$ROOT_DIR/agent-stream-server"

./gradlew bootRun &
SERVER_PID=$!
echo -e "${GREEN}-> Kotlin Stream Server 기동됨 (PID: $SERVER_PID, Port: 8080)${RESET}"

# 서버 바인딩 시점 확보를 위한 대기
sleep 3

# ------------------------------------------------------------------------------
# 4. React SPA 프론트엔드 개발 서버 기동
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 4/4] ⚛️ React SPA 프론트엔드 (Vite Dev Server) 구동...${RESET}"
cd "$ROOT_DIR/frontend"

if [ ! -d "node_modules" ]; then
    echo "-> node_modules가 없습니다. npm install을 실행합니다..."
    npm install
fi

npm run dev &
FRONTEND_PID=$!
echo -e "${GREEN}-> React Frontend 개발 서버 기동됨 (PID: $FRONTEND_PID)${RESET}"

# ------------------------------------------------------------------------------
# 5. 실행 상태 안내 대시보드
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${GREEN}"
echo "================================================================="
echo "🎉 모든 서비스가 성공적으로 구동되었습니다!"
echo "================================================================="
echo -e "${RESET}"
echo -e "🌐 ${BOLD}React 프론트엔드 앱 접속 URL:${RESET}   ${CYAN}http://localhost:5173${RESET}"
echo -e "🔌 ${BOLD}Kotlin SSE 스트리밍 서버:${RESET}     ${CYAN}http://localhost:8080${RESET}"
echo -e "📊 ${BOLD}Kafka UI 관리 대시보드:${RESET}       ${CYAN}http://localhost:8989${RESET}"
echo -e "\n${YELLOW}💡 프로세스를 종료하려면 터미널에서 [Ctrl + C]를 누르세요.${RESET}\n"

# 서브 프로세스 대기
wait
