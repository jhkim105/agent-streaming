#!/usr/bin/env bash
# ==============================================================================
# Multi-Node Test Cluster Shortcut Script
# ==============================================================================
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$ROOT_DIR/scripts/start-multi-node.sh" "$@"
