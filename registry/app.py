"""服务发现注册中心 (FastAPI + SQLite)

职责:
- 接收后端实例的注册 / 心跳续租 / 注销
- 租约过期的实例不参与选取; 过期后的迟到心跳不能恢复旧租约, 必须重新注册
- 同名新进程注册后, 旧进程的迟到心跳 / 注销被 fencing(token 不匹配)拒绝,
  既不能恢复旧租约, 也摘不掉新实例
- 注册中心重启后(新的 boot_id), 未经重新确认的记录不参与转发
- 网页值班台: 查看实例状态, 暂停/恢复心跳, 模拟同名重启, 发起真实配置请求
"""
import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager, closing

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

DB_PATH = os.environ.get("DB_PATH", "/data/registry.db")
FORWARD_TIMEOUT = float(os.environ.get("FORWARD_TIMEOUT", "3.0"))

# 本次注册中心进程的身份: 重启后改变, 用于"重启后必须重新确认"的判定
BOOT_ID = uuid.uuid4().hex
BOOT_TIME = time.time()

_LOCK = threading.Lock()
_RR = {}  # service -> 轮询计数器

SCHEMA = """
CREATE TABLE IF NOT EXISTS instances (
  service        TEXT NOT NULL,
  instance_id    TEXT NOT NULL,
  token          TEXT NOT NULL,   -- 进程级 fencing token, 每次进程启动随机生成
  address        TEXT NOT NULL,
  epoch          INTEGER NOT NULL, -- 同名实例每注册一次 +1, 便于观察接替
  registered_at  REAL NOT NULL,
  last_heartbeat REAL NOT NULL,
  ttl            REAL NOT NULL,
  confirmed_boot TEXT NOT NULL,   -- 最后一次心跳/注册时的注册中心 boot_id
  PRIMARY KEY (service, instance_id)
);
"""


def _conn():
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _init_db():
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    with closing(_conn()) as c:
        c.executescript(SCHEMA)
        c.execute("PRAGMA journal_mode=WAL")
        c.commit()


@asynccontextmanager
async def lifespan(app):
    _init_db()
    yield


app = FastAPI(title="config-service 服务发现注册中心", lifespan=lifespan)


def _get(c, service, iid):
    return c.execute(
        "SELECT * FROM instances WHERE service=? AND instance_id=?", (service, iid)
    ).fetchone()


# ---------------- 注册协议 ----------------

@app.post("/api/registry/register")
async def register(req: Request):
    """注册(含同名重新注册)。同名新进程注册即接替: token 替换、epoch+1。"""
    b = await req.json()
    service = b["service"]
    iid = b["instance_id"]
    token = b["token"]
    address = b["address"].rstrip("/")
    ttl = float(b.get("ttl", 8.0))
    now = time.time()
    with _LOCK, closing(_conn()) as c:
        row = _get(c, service, iid)
        epoch = (row["epoch"] + 1) if row else 1
        c.execute(
            """INSERT INTO instances(service,instance_id,token,address,epoch,
                                     registered_at,last_heartbeat,ttl,confirmed_boot)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(service,instance_id) DO UPDATE SET
                 token=excluded.token, address=excluded.address, epoch=excluded.epoch,
                 registered_at=excluded.registered_at,
                 last_heartbeat=excluded.last_heartbeat,
                 ttl=excluded.ttl, confirmed_boot=excluded.confirmed_boot""",
            (service, iid, token, address, epoch, now, now, ttl, BOOT_ID),
        )
        c.commit()
    return {"ok": True, "epoch": epoch, "boot_id": BOOT_ID, "ttl": ttl}


@app.post("/api/registry/heartbeat")
async def heartbeat(req: Request):
    """心跳续租。token 不匹配(已被接替)->409; 租约已过期->410, 必须重新注册。"""
    b = await req.json()
    service, iid, token = b["service"], b["instance_id"], b["token"]
    now = time.time()
    with _LOCK, closing(_conn()) as c:
        row = _get(c, service, iid)
        if row is None:
            return JSONResponse(
                {"ok": False, "error": "unknown_instance", "action": "re-register"}, 404)
        if row["token"] != token:
            return JSONResponse(
                {"ok": False, "error": "token_mismatch",
                 "detail": "该实例名已由新进程接替, 旧进程的迟到心跳被 fencing, 旧租约不恢复"},
                409)
        if now > row["last_heartbeat"] + row["ttl"]:
            return JSONResponse(
                {"ok": False, "error": "lease_expired", "action": "re-register",
                 "detail": "租约已过期, 迟到心跳不能恢复旧租约, 必须重新注册"},
                410)
        c.execute(
            "UPDATE instances SET last_heartbeat=?, confirmed_boot=? "
            "WHERE service=? AND instance_id=?",
            (now, BOOT_ID, service, iid))
        c.commit()
    return {"ok": True, "boot_id": BOOT_ID}


@app.post("/api/registry/deregister")
async def deregister(req: Request):
    """注销。token 不匹配(旧进程的迟到注销)不能摘掉新实例。"""
    b = await req.json()
    service, iid, token = b["service"], b["instance_id"], b["token"]
    with _LOCK, closing(_conn()) as c:
        row = _get(c, service, iid)
        if row is None:
            return JSONResponse({"ok": False, "error": "unknown_instance"}, 404)
        if row["token"] != token:
            return JSONResponse(
                {"ok": False, "error": "token_mismatch",
                 "detail": "token 不匹配, 旧进程的迟到注销被拒绝, 新实例记录保留"},
                409)
        c.execute("DELETE FROM instances WHERE service=? AND instance_id=?",
                  (service, iid))
        c.commit()
    return {"ok": True}


# ---------------- 状态与转发 ----------------

def _view(r, now):
    remaining = r["last_heartbeat"] + r["ttl"] - now
    confirmed = r["confirmed_boot"] == BOOT_ID
    expired = remaining <= 0
    d = dict(r)
    d.update({
        "age": round(now - r["last_heartbeat"], 2),
        "lease_remaining": round(max(0.0, remaining), 2),
        "expired": expired,
        "confirmed": confirmed,
        "eligible": confirmed and not expired,
    })
    return d


@app.get("/api/state")
def state():
    now = time.time()
    with _LOCK, closing(_conn()) as c:
        rows = c.execute(
            "SELECT * FROM instances ORDER BY service, instance_id").fetchall()
    return {
        "boot_id": BOOT_ID,
        "boot_time": BOOT_TIME,
        "now": now,
        "instances": [_view(r, now) for r in rows],
    }


@app.post("/api/fetch/{service}")
async def fetch(service: str):
    """选取一个合格实例并转发真实 GET /config。无实例或转发失败都如实返回。"""
    now = time.time()
    with _LOCK, closing(_conn()) as c:
        rows = c.execute(
            "SELECT * FROM instances WHERE service=? ORDER BY instance_id",
            (service,)).fetchall()
    eligible, rejected = [], []
    for r in rows:
        if r["confirmed_boot"] != BOOT_ID:
            rejected.append({"instance_id": r["instance_id"],
                             "reason": "unconfirmed_after_registry_restart"})
        elif now > r["last_heartbeat"] + r["ttl"]:
            rejected.append({"instance_id": r["instance_id"], "reason": "lease_expired"})
        else:
            eligible.append(r)
    if not eligible:
        return {"ok": False, "error": "no_eligible_instance", "service": service,
                "known_instances": len(rows), "rejected": rejected,
                "detail": "没有可用实例(租约过期或注册中心重启后未重新确认), 不转发"}
    n = _RR.get(service, 0)
    _RR[service] = n + 1
    pick = eligible[n % len(eligible)]
    sel = {"service": pick["service"], "instance_id": pick["instance_id"],
           "token": pick["token"][:8], "epoch": pick["epoch"],
           "address": pick["address"]}
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=FORWARD_TIMEOUT) as cli:
            resp = await cli.get(pick["address"] + "/config")
        latency = round((time.time() - t0) * 1000, 1)
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        out = {"ok": resp.status_code == 200, "selected": sel,
               "upstream_status": resp.status_code, "response": body,
               "latency_ms": latency}
        if resp.status_code != 200:
            out["error"] = "upstream_error"
            out["detail"] = "后端返回非 200, 如实展示"
        return out
    except Exception as e:
        return {"ok": False, "error": "forward_failed", "selected": sel,
                "detail": f"{type(e).__name__}: {e}",
                "latency_ms": round((time.time() - t0) * 1000, 1)}


# ---------------- 值班台管理指令(转发到后端管理口) ----------------

_ADMIN_CMDS = {"pause", "resume", "restart", "stale-heartbeat", "stale-deregister",
               "fail-none", "fail-hang", "fail-error", "state"}


@app.post("/api/admin/{service}/{instance_id}/{command}")
async def admin_cmd(service: str, instance_id: str, command: str):
    if command not in _ADMIN_CMDS:
        return JSONResponse({"ok": False, "error": "unknown_command"}, 404)
    with _LOCK, closing(_conn()) as c:
        row = _get(c, service, instance_id)
    if row is None:
        return JSONResponse(
            {"ok": False, "error": "no_record",
             "detail": "注册中心没有该实例记录(可能已注销), 无法下达指令"}, 404)
    addr = row["address"]
    if command == "state":
        method, url = "GET", addr + "/admin/state"
    elif command.startswith("fail-"):
        method, url = "POST", addr + "/admin/failmode/" + command[len("fail-"):]
    else:
        method, url = "POST", addr + "/admin/" + command
    try:
        async with httpx.AsyncClient(timeout=5.0) as cli:
            resp = await (cli.get(url) if method == "GET" else cli.post(url))
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {"ok": resp.status_code < 400,
                "target": f"{service}/{instance_id}", "command": command,
                "backend_status": resp.status_code, "backend_response": body}
    except Exception as e:
        return {"ok": False, "target": f"{service}/{instance_id}", "command": command,
                "error": "admin_forward_failed", "detail": f"{type(e).__name__}: {e}"}


@app.post("/api/admin/reboot")
def reboot():
    """退出注册中心进程。docker 的 restart 策略(或本地守护循环)会把它拉起来,
    起来后是新的 BOOT_ID: 已有记录必须经心跳/注册重新确认才参与转发。"""
    threading.Timer(0.6, lambda: os._exit(0)).start()
    return {"ok": True, "old_boot_id": BOOT_ID,
            "detail": "registry 进程即将退出并重启, 重启后 boot_id 改变"}


# ---------------- 网页值班台 ----------------

PAGE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>服务发现值班台</title>
<style>
 body{font-family:system-ui,monospace;margin:20px;background:#f6f7f9;color:#222}
 .card{background:#fff;border:1px solid #ddd;border-radius:8px;padding:12px 16px;margin-bottom:14px}
 table{border-collapse:collapse;width:100%;font-size:13px}
 th,td{border:1px solid #e0e0e0;padding:4px 8px;text-align:left}
 th{background:#f0f0f0}
 tr.ok td{background:#f2fbf2} tr.bad td{background:#fdf2f2}
 button{margin:1px 2px;padding:2px 8px;cursor:pointer}
 pre{background:#0d1117;color:#c9d1d9;padding:10px;border-radius:6px;overflow:auto;max-height:340px}
 pre.good{outline:2px solid #2da44e} pre.bad{outline:2px solid #cf222e}
 #log{max-height:220px}
 .muted{color:#777;font-size:12px}
</style>
</head>
<body>
<h2>内网配置读取服务 · 服务发现值班台</h2>

<div class="card">
  <b>注册中心</b> boot=<b id="boot">?</b>
  · 启动于 <span id="boottime">?</span>
  · 当前 <span id="now">?</span>
  <button id="rebootBtn">重启注册中心</button>
  <span id="rebootMsg" class="muted"></span>
  <div class="muted">重启后 boot 改变: 所有记录必须经心跳/注册"重新确认"才会参与转发。</div>
</div>

<div class="card">
  <b>发起真实配置请求</b>
  <span class="muted">(注册中心在合格实例中轮询选取, 转发 GET /config, 展示响应与所选实例)</span><br>
  service = <input id="svc" value="config-service">
  <button id="fetchBtn">发送配置请求</button>
  <pre id="fetchOut">(尚未发起)</pre>
</div>

<div class="card">
  <b>实例列表</b> <span class="muted">(每秒刷新; 过期或未确认的实例不参与选取)</span>
  <table id="tbl">
    <thead><tr>
      <th>service</th><th>实例</th><th>epoch</th><th>token</th><th>地址</th>
      <th>心跳距今</th><th>租约剩余</th><th>重启后已确认</th><th>可转发</th><th>操作</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>

<div class="card"><b>操作日志</b><pre id="log"></pre></div>

<script>
const $ = id => document.getElementById(id);
function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function ts(){return new Date().toLocaleTimeString();}
function logLine(o){
  $('log').textContent = `[${ts()}] ` + (typeof o==='string'?o:JSON.stringify(o)) + "\\n" + $('log').textContent;
}
async function poll(){
  try{
    const r = await fetch('/api/state'); const s = await r.json();
    $('boot').textContent = s.boot_id.slice(0,8);
    $('boottime').textContent = new Date(s.boot_time*1000).toLocaleTimeString();
    $('now').textContent = new Date(s.now*1000).toLocaleTimeString();
    render(s.instances);
  }catch(e){ /* 注册中心重启中, 下轮再试 */ }
}
function render(rows){
  const tb = document.querySelector('#tbl tbody');
  tb.innerHTML='';
  if(!rows.length){ tb.innerHTML='<tr><td colspan="10">(暂无注册记录)</td></tr>'; return; }
  for(const r of rows){
    const tr=document.createElement('tr');
    tr.className = r.eligible ? 'ok' : 'bad';
    const mk=(c,label)=>`<button onclick="cmd('${r.service}','${r.instance_id}','${c}')">${label}</button>`;
    tr.innerHTML =
      `<td>${esc(r.service)}</td><td><b>${esc(r.instance_id)}</b></td><td>${r.epoch}</td>`+
      `<td title="${r.token}">${r.token.slice(0,8)}</td><td>${esc(r.address)}</td>`+
      `<td>${r.age.toFixed(1)}s</td><td>${r.lease_remaining.toFixed(1)}s</td>`+
      `<td>${r.confirmed?'✅':'❌'}</td><td>${r.eligible?'✅ 可转发':'❌ 不转发'}</td>`+
      `<td>${mk('pause','暂停心跳')}${mk('resume','恢复心跳')}${mk('restart','模拟同名重启')}`+
      `${mk('stale-heartbeat','旧进程迟到心跳')}${mk('stale-deregister','旧进程迟到注销')}${mk('state','查后端状态')}<br>`+
      `故障注入: ${mk('fail-none','正常')}${mk('fail-hang','挂起')}${mk('fail-error','返回500')}</td>`;
    tb.appendChild(tr);
  }
}
async function cmd(svc,iid,c){
  logLine(`>>> ${c} ${svc}/${iid}`);
  try{
    const r=await fetch(`/api/admin/${svc}/${iid}/${c}`,{method:'POST'});
    logLine(await r.json());
  }catch(e){logLine('指令下发失败: '+e);}
  poll();
}
async function doFetch(){
  const svc=$('svc').value||'config-service';
  const out=$('fetchOut');
  out.textContent='请求中...'; out.className='';
  try{
    const r=await fetch(`/api/fetch/${svc}`,{method:'POST'});
    const j=await r.json();
    out.textContent=JSON.stringify(j,null,2);
    out.className = j.ok ? 'good' : 'bad';
  }catch(e){ out.textContent='请求异常: '+e; out.className='bad'; }
  poll();
}
async function rebootReg(){
  if(!confirm('确认重启注册中心进程? (由 docker restart 策略或本地守护循环拉起)'))return;
  try{ await fetch('/api/admin/reboot',{method:'POST'}); }catch(e){}
  $('rebootMsg').textContent='重启指令已发出, 等待注册中心恢复...';
}
$('fetchBtn').onclick=doFetch;
$('rebootBtn').onclick=rebootReg;
setInterval(poll,1000); poll();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
