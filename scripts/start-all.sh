#!/usr/bin/env bash
# Real-time AI Researcher Agent System Starter Script

echo "=========================================================="
echo "🚀 Real-time AI Researcher Agent - All Services Launcher"
echo "=========================================================="

echo "1. Checking Kafka (9092) and Redis (6379)..."
nc -zv localhost 9092 || { echo "Kafka is not running!"; exit 1; }
nc -zv localhost 6379 || { echo "Redis is not running!"; exit 1; }

echo "2. Starting Python Agent Worker..."
cd ../agent-worker
source .venv/bin/activate
python -m src.main &
PYTHON_PID=$!

echo "3. Starting Agent Stream Server Node 1 (Port 8080)..."
cd ../agent-stream-server
SERVER_PORT=8080 ./gradlew bootRun &
NODE1_PID=$!

echo "4. Starting Agent Stream Server Node 2 (Port 8081)..."
SERVER_PORT=8081 ./gradlew bootRun &
NODE2_PID=$!

echo "=========================================================="
echo "All backend services initiated in background!"
echo "PIDs -> Python: $PYTHON_PID, Node1: $NODE1_PID, Node2: $NODE2_PID"
echo "=========================================================="
