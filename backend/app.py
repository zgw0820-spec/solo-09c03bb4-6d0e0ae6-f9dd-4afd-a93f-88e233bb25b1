"""配置读取后端 (HTTP backend)

- GET /config: 返回预置配置 + 实例身份(instance_id / token / host / seq)
- 启动后自动向注册中心注册, 之后按周期心跳续租
- 心跳收到 404/410(记录丢失或租约过期) -> 自动重新注册
- 心跳收到 409(token 不匹配, 已被同名新进程接替) -> 判定自己被 fencing,
  停止心跳, 绝不抢注, 等待运维处置
- /admin/* 管理口供值班台演练: 暂停/恢复心跳、模拟同名进程重启、
  用旧 token 发"迟到心跳/迟到注销"、故障注入(挂起/500)
"""
import json
import os
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

REGISTRY = os.environ.get("REGISTRY_URL", "http://127.0.0.1:8000").rstrip("/")
SERVICE = os.environ.get("SERVICE_NAME", "config-service")
INSTANCE_ID = os.environ.get("INSTANCE_ID", socket.gethostname())
PORT = int(os.environ.get("LISTEN_PORT", "8000"))
ADDRESS = os.environ.get("ADVERTISE_ADDRESS", f"http://127.0.0.1:{PORT}").rstrip("/")
TTL = float(os.environ.get("LEASE_TTL", "8"))
HB_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "2"))
PRESET = json.loads(os.environ.get("PRESET_CONFIG", "{}"))

S = {
    "token": uuid.uuid4().hex,   # 进程级 fencing token, 每次进程启动随机生成
    "stale_token": None,         # "模拟同名重启"前的旧 token, 用于迟到请求演练
    "paused": False,
    "superseded": False,
    "failmode": "none",          # none | hang | error
    "seq": 0,
    "last_register": None,
    "last_heartbeat": None,
}


def _safe(r):
    try:
        return r.json()
    except Exception:
        return r.text


def register():
    body = {"service": SERVICE, "instance_id": INSTANCE_ID, "token": S["token"],
            "address": ADDRESS, "ttl": TTL}
    try:
        r = httpx.post(REGISTRY + "/api/registry/register", json=body, timeout=5)
        S["last_register"] = {"time": time.time(), "status": r.status_code, "body": _safe(r)}
        return r.status_code == 200
    except Exception as e:
        S["last_register"] = {"time": time.time(), "status": None,
                              "body": f"{type(e).__name__}: {e}"}
        return False


def heartbeat_once():
    body = {"service": SERVICE, "instance_id": INSTANCE_ID, "token": S["token"]}
    try:
        r = httpx.post(REGISTRY + "/api/registry/heartbeat", json=body, timeout=5)
        S["last_heartbeat"] = {"time": time.time(), "status": r.status_code, "body": _safe(r)}
        if r.status_code == 200:
            return
        if r.status_code == 409:
            # 已被同名新进程接替: 旧进程被 fencing, 停止心跳, 不抢注
            S["superseded"] = True
        elif r.status_code in (404, 410):
            # 记录丢失或租约过期: 重新注册
            register()
    except Exception as e:
        S["last_heartbeat"] = {"time": time.time(), "status": None,
                               "body": f"{type(e).__name__}: {e}"}


def _loop():
    while not register():
        time.sleep(1.0)
    while True:
        time.sleep(HB_INTERVAL)
        if S["paused"] or S["superseded"]:
            continue
        heartbeat_once()


def _deregister():
    try:
        httpx.post(REGISTRY + "/api/registry/deregister",
                   json={"service": SERVICE, "instance_id": INSTANCE_ID,
                         "token": S["token"]}, timeout=2)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=_loop, daemon=True).start()
    yield
    _deregister()  # 优雅退出时注销; 若名字已被新进程接替, 注册中心会拒绝(409)


app = FastAPI(title=f"backend {INSTANCE_ID}", lifespan=lifespan)


@app.get("/config")
def config():
    S["seq"] += 1
    if S["failmode"] == "hang":
        time.sleep(30)  # 注入"后端不响应", 让注册中心转发超时
    if S["failmode"] == "error":
        return JSONResponse({"error": "injected_failure", "instance_id": INSTANCE_ID}, 500)
    return {"service": SERVICE, "instance_id": INSTANCE_ID,
            "token": S["token"][:8], "host": socket.gethostname(),
            "seq": S["seq"], "config": PRESET}


# ---------------- 管理口(供值班台演练) ----------------

@app.post("/admin/pause")
def pause():
    S["paused"] = True
    return {"ok": True, "paused": True}


@app.post("/admin/resume")
def resume():
    S["paused"] = False
    if not S["superseded"]:
        heartbeat_once()  # 立即补一次心跳; 若租约已过期会自动重新注册
    return {"ok": True, "paused": False, "superseded": S["superseded"],
            "last_heartbeat": S["last_heartbeat"], "last_register": S["last_register"]}


@app.post("/admin/restart")
def restart():
    """模拟同名实例进程重启: 换发新 token 重新注册; 旧 token 留作迟到请求演练。"""
    old = S["token"]
    S["stale_token"] = old
    S["token"] = uuid.uuid4().hex
    S["superseded"] = False
    S["seq"] = 0
    ok = register()
    return {"ok": ok, "old_token": old[:8], "new_token": S["token"][:8],
            "register_result": S["last_register"]}


@app.post("/admin/stale-heartbeat")
def stale_heartbeat():
    """用旧进程的 token 发迟到心跳, 期望注册中心 409 fencing。"""
    if not S["stale_token"]:
        return JSONResponse({"ok": False, "error": "no_stale_token",
                             "detail": "请先执行'模拟同名重启'"}, 400)
    r = httpx.post(REGISTRY + "/api/registry/heartbeat",
                   json={"service": SERVICE, "instance_id": INSTANCE_ID,
                         "token": S["stale_token"]}, timeout=5)
    return {"ok": r.status_code == 200, "registry_status": r.status_code,
            "registry_body": _safe(r),
            "expect": "409 token_mismatch = 旧进程迟到心跳被隔离, 旧租约不恢复"}


@app.post("/admin/stale-deregister")
def stale_deregister():
    """用旧进程的 token 发迟到注销, 期望注册中心 409, 新实例记录保留。"""
    if not S["stale_token"]:
        return JSONResponse({"ok": False, "error": "no_stale_token",
                             "detail": "请先执行'模拟同名重启'"}, 400)
    r = httpx.post(REGISTRY + "/api/registry/deregister",
                   json={"service": SERVICE, "instance_id": INSTANCE_ID,
                         "token": S["stale_token"]}, timeout=5)
    return {"ok": r.status_code == 200, "registry_status": r.status_code,
            "registry_body": _safe(r),
            "expect": "409 token_mismatch = 旧进程迟到注销摘不掉新实例"}


@app.post("/admin/failmode/{mode}")
def failmode(mode: str):
    if mode not in ("none", "hang", "error"):
        return JSONResponse({"ok": False, "error": "bad_mode"}, 400)
    S["failmode"] = mode
    return {"ok": True, "failmode": mode}


@app.get("/admin/state")
def admin_state():
    return {"ok": True, "service": SERVICE, "instance_id": INSTANCE_ID,
            "token": S["token"][:8],
            "stale_token": (S["stale_token"][:8] if S["stale_token"] else None),
            "paused": S["paused"], "superseded": S["superseded"],
            "failmode": S["failmode"],
            "last_register": S["last_register"], "last_heartbeat": S["last_heartbeat"]}
