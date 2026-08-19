#!/usr/bin/env bash
# ==============================================================================
# Real-time AI Researcher Agent - Multi-Node Test Cluster Launcher
# ==============================================================================
# 본 스크립트는 멀티 노드 세션 라우팅(Redis Pub/Sub Cross-Routing) 테스트를 위해
# Kafka/Redis 인프라, Python Agent Worker, 그리고 Kotlin Stream Server 2대
# (Node 1: 8080, Node 2: 8081)를 한 번에 기동하고 일괄 관리합니다.
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
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo -e "${BOLD}${CYAN}"
echo "================================================================="
echo "🔀 Multi-Node Session Routing Test Cluster Starter"
echo "================================================================="
echo -e "${RESET}"

# ------------------------------------------------------------------------------
# 0. 프로세스 자동 종료 트랩 (Trap) 설정
# ------------------------------------------------------------------------------
cleanup() {
    echo -e "\n${YELLOW}🛑 모든 멀티 노드 서버 프로세스를 종료하는 중입니다...${RESET}"
    kill 0 2>/dev/null || true
    echo -e "${GREEN}✨ 모든 인스턴스가 안전하게 정지되었습니다.${RESET}"
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# ------------------------------------------------------------------------------
# 1. 로컬 인프라 (Kafka & Redis) 점검 및 Docker Compose 기동
# ------------------------------------------------------------------------------
echo -e "${BOLD}[Step 1/4] 📦 로컬 인프라 서비스 점검 (Kafka & Redis)...${RESET}"

if ! nc -z localhost 9092 2>/dev/null || ! nc -z localhost 6379 2>/dev/null; then
    echo -e "${YELLOW}-> Kafka(9092) 또는 Redis(6379)가 구동 중이지 않습니다. Docker Compose를 기동합니다...${RESET}"
    docker compose up -d
    echo -e "${GREEN}-> Docker 인프라 서비스 기동 완료.${RESET}"
    sleep 2
else
    echo -e "${GREEN}-> Kafka (9092) 및 Redis (6379) 인프라 정상 구동 확인.${RESET}"
fi

# ------------------------------------------------------------------------------
# 2. Python Agent Worker 기동 (Ollama Qwen2.5-7B 연동)
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 2/4] 🐍 Python Agent Worker 프로세스 구동...${RESET}"
cd "$ROOT_DIR/agent-worker"

if command -v uv &> /dev/null; then
    echo "-> uv 패키지 관리자를 사용하여 Python Worker를 백그라운드로 실행합니다."
    uv run python -m src.main &
else
    echo -e "${YELLOW}-> uv가 없습니다. 기본 python3 모듈을 실행합니다.${RESET}"
    python3 -m src.main &
fi
WORKER_PID=$!
echo -e "${GREEN}-> Python Agent Worker 기동됨 (PID: $WORKER_PID)${RESET}"

# ------------------------------------------------------------------------------
# 3. Kotlin Stream Server Node 1 기동 (Port 8080)
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 3/4] ☕ Kotlin Stream Server Node 1 구동 (Port 8080)...${RESET}"
cd "$ROOT_DIR/agent-stream-server"

SERVER_PORT=8080 ./gradlew bootRun &
NODE1_PID=$!
echo -e "${GREEN}-> Stream Server Node 1 기동됨 (PID: $NODE1_PID, Port: 8080)${RESET}"

# ------------------------------------------------------------------------------
# 4. Kotlin Stream Server Node 2 기동 (Port 8081)
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}[Step 4/4] ☕ Kotlin Stream Server Node 2 구동 (Port 8081)...${RESET}"

SERVER_PORT=8081 ./gradlew bootRun &
NODE2_PID=$!
echo -e "${GREEN}-> Stream Server Node 2 기동됨 (PID: $NODE2_PID, Port: 8081)${RESET}"

# ------------------------------------------------------------------------------
# 5. 멀티 노드 멀티 인스턴스 클러스터 실행 대시보드
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${GREEN}"
echo "================================================================="
echo "🎉 멀티 노드 테스트 클러스터가 성공적으로 구동되었습니다!"
echo "================================================================="
echo -e "${RESET}"
echo -e "🔌 ${BOLD}Stream Server Node 1:${RESET}     ${CYAN}http://localhost:8080${RESET}"
echo -e "🔌 ${BOLD}Stream Server Node 2:${RESET}     ${CYAN}http://localhost:8081${RESET}"
echo -e "📊 ${BOLD}Kafka UI 관리 대시보드:${RESET}   ${CYAN}http://localhost:8989${RESET}"
echo -e "\n${BOLD}${YELLOW}🧪 라우팅 테스트 실행 방법:${RESET}"
echo -e "   새 터미널을 열고 아래 스크립트를 실행하세요:"
echo -e "   ${CYAN}./scripts/test-multi-node-routing.sh${RESET}"
echo -e "\n${YELLOW}💡 프로세스를 종료하려면 터미널에서 [Ctrl + C]를 누르세요.${RESET}\n"

# 서브 프로세스 대기
wait
