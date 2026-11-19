#!/usr/bin/env bash
# 无 Docker 时的本地运行脚本: 注册中心(守护循环, 支持网页"重启注册中心") + 两台后端
# 用法: bash run_local.sh   然后访问 http://127.0.0.1:8000
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
DEMO=/tmp/sd-demo
mkdir -p "$DEMO"
PY=${PYTHON:-python3}

cleanup() {
  pkill -f "uvicorn app:app" 2>/dev/null
  exit 0
}
trap cleanup EXIT INT TERM

# 注册中心: 退出后 1s 自动拉起(模拟 docker restart 策略)
(
  cd "$ROOT/registry"
  while true; do
    DB_PATH="$DEMO/registry.db" "$PY" -m uvicorn app:app --host 127.0.0.1 --port 8000
    echo "[run_local] registry exited, restarting in 1s..."
    sleep 1
  done
) &

REGISTRY_URL=http://127.0.0.1:8000 SERVICE_NAME=config-service INSTANCE_ID=config-1 \
ADVERTISE_ADDRESS=http://127.0.0.1:8001 LISTEN_PORT=8001 \
PRESET_CONFIG='{"db_dsn":"postgres://db.internal:5432/app","cache":"redis://cache.internal:6379/0","feature_flags":{"new_ui":true},"preset_by":"backend-1"}' \
"$PY" -m uvicorn app:app --host 127.0.0.1 --port 8001 --app-dir "$ROOT/backend" &

REGISTRY_URL=http://127.0.0.1:8000 SERVICE_NAME=config-service INSTANCE_ID=config-2 \
ADVERTISE_ADDRESS=http://127.0.0.1:8002 LISTEN_PORT=8002 \
PRESET_CONFIG='{"db_dsn":"postgres://db.internal:5432/app","cache":"redis://cache.internal:6379/0","feature_flags":{"new_ui":false},"preset_by":"backend-2"}' \
"$PY" -m uvicorn app:app --host 127.0.0.1 --port 8002 --app-dir "$ROOT/backend" &

echo "[run_local] registry=http://127.0.0.1:8000 backend1=:8001 backend2=:8002 (Ctrl-C 停止)"
wait
