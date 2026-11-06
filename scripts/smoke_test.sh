#!/usr/bin/env bash
# Compose 冒烟测试：构建镜像 -> 启动整套服务 -> 验证 dispatch -> worker 端到端 -> 验证优雅退出。
# 用独立的 compose 项目名，down -v 不会影响本地开发栈的数据卷。
set -euo pipefail

PROJECT=task-platform-smoke
BASE_URL=${BASE_URL:-http://127.0.0.1:8000}

dc() { docker compose -p "$PROJECT" "$@"; }

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "::group::compose state and logs"
    dc ps -a || true
    dc logs --tail=80 || true
    echo "::endgroup::"
  fi
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

step() { echo "==> $*"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

# wait_until <描述> <超时秒> <命令...>
wait_until() {
  local desc=$1 timeout=$2
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      fail "timed out waiting for: $desc"
    fi
    sleep 2
  done
}

api_healthy() {
  local body
  body=$(curl -fsS "$BASE_URL/health") && [[ $body == *'"status":"ok"'* ]]
}

two_successful_runs() {
  local body
  body=$(curl -fsS "$BASE_URL/api/v1/executions?status=SUCCESS&page_size=1") || return 1
  python3 -c "import json,sys; sys.exit(0 if json.loads(sys.argv[1])['total'] >= 2 else 1)" "$body"
}

step "build and start postgres, redis, init and api"
dc up -d --build api
wait_until "API /health" 120 api_healthy

step "init finished successfully"
init_exit=$(docker inspect -f '{{.State.ExitCode}}' "$(dc ps -a -q init)")
[ "$init_exit" = "0" ] || fail "init exited with $init_exit"

step "add a schedule"
dc run --rm -T cli schedule demo-success --interval 2

step "start dispatch and worker"
dc up -d dispatch worker
wait_until "two SUCCESS executions visible through the API" 90 two_successful_runs

step "metrics and alerts endpoints"
metrics=$(curl -fsS "$BASE_URL/metrics")
[[ $metrics == *'queue_depth{queue='* ]] || fail "queue_depth missing from /metrics"
curl -fsS "$BASE_URL/api/v1/alerts" >/dev/null

step "worker shuts down gracefully on SIGTERM"
worker_id=$(dc ps -a -q worker)
dc stop worker >/dev/null
worker_exit=$(docker inspect -f '{{.State.ExitCode}}' "$worker_id")
[ "$worker_exit" = "0" ] || fail "worker exited with $worker_exit (expected 0)"

echo "smoke test passed"
