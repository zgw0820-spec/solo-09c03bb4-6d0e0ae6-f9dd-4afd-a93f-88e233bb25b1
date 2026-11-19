#!/usr/bin/env python3
"""端到端自测: 对运行中的服务发现系统(注册中心+两台后端)逐项验证需求。

用法:  python3 tests/selftest.py           # 默认打 http://127.0.0.1:8000
       BASE=http://localhost:8000 python3 tests/selftest.py

覆盖:
 1) 双实例注册并自动续租, 配置请求被真实转发, 轮询选取, 响应含预置配置与实例身份
 2) 暂停心跳 -> 租约过期 -> 不再被选取, 另一台仍可供答
 3) 过期后迟到心跳不能恢复旧租约(410), 后端自动重新注册(epoch+1)后恢复
 4) 同名新进程接替后, 旧进程迟到心跳/迟到注销均被 409 fencing, 新实例不受影响
 5) 转发失败(后端挂起)与后端 500 都如实返回
 6) 无实例可用时如实返回 no_eligible_instance
 7) 注册中心重启后, 未经重新确认的记录不参与转发; 重新确认后恢复
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE", "http://127.0.0.1:8000").rstrip("/")
SVC = "config-service"
I1, I2 = "config-1", "config-2"

RESULTS = []


def req(method, path, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw or "{}")
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def get(path):
    return req("GET", path)


def post(path, body=None):
    return req("POST", path, body)


def check(name, cond, extra=""):
    RESULTS.append((name, bool(cond)))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    if not cond and extra:
        print("       " + str(extra)[:600])


def wait_for(desc, cond, timeout=40, interval=0.5):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        last = cond()
        if last:
            return last
        time.sleep(interval)
    raise SystemExit(f"TIMEOUT waiting for: {desc}; last={last}")


def state():
    st, body = get("/api/state")
    assert st == 200, body
    return body


def inst(iid):
    for r in state()["instances"]:
        if r["instance_id"] == iid:
            return r
    return None


def admin(iid, cmd):
    return post(f"/api/admin/{SVC}/{iid}/{cmd}")[1]


def fetch():
    return post(f"/api/fetch/{SVC}")[1]


def both_eligible():
    a, b = inst(I1), inst(I2)
    return (a and a["eligible"] and b and b["eligible"]) and (a, b) or None


def main():
    print(f"== 目标注册中心: {BASE} ==\n-- 1. 启动与自动注册 --")
    a, b = wait_for("两台实例都 eligible", both_eligible)
    check("两台实例注册且可转发", True)
    ep1 = a["epoch"]

    print("-- 2. 配置请求真实转发 + 轮询选取 --")
    picks = [fetch() for _ in range(4)]
    ok_all = all(p.get("ok") for p in picks)
    ids = [p["selected"]["instance_id"] for p in picks]
    check("4 次请求全部成功", ok_all, picks)
    check("两台实例都被选中(轮询)", sorted(ids) == [I1, I1, I2, I2], ids)
    p0 = picks[0]
    check("响应含预置配置与实例身份",
          p0["response"].get("config", {}).get("db_dsn") is not None
          and p0["response"].get("instance_id") == p0["selected"]["instance_id"]
          and p0["response"].get("token") == p0["selected"]["token"], p0)

    print("-- 3. 暂停心跳 -> 租约过期 -> 不参与选取 --")
    admin(I1, "pause")
    wait_for(f"{I1} 租约过期", lambda: not inst(I1)["eligible"] or None, timeout=20)
    picks = [fetch() for _ in range(3)]
    check("过期实例不被选取, 另一台仍供答",
          all(p.get("ok") and p["selected"]["instance_id"] == I2 for p in picks), picks)

    print("-- 4. 过期后恢复: 迟到心跳 410, 重新注册后恢复 --")
    r = admin(I1, "resume")
    hb_status = (r.get("backend_response") or {}).get("last_heartbeat", {}).get("status")
    check("恢复时迟到心跳被 410 拒绝(须重新注册)", hb_status == 410, r)
    a, b = wait_for(f"{I1} 重新 eligible", both_eligible)
    check("重新注册后 epoch 递增", a["epoch"] > ep1, f"epoch {ep1} -> {a['epoch']}")

    print("-- 5. 同名新进程接替: 旧进程迟到心跳/注销被 fencing --")
    old_tok = inst(I1)["token"]
    old_epoch = inst(I1)["epoch"]
    r = admin(I1, "restart")
    check("模拟同名重启成功", r.get("ok"), r)
    time.sleep(0.3)
    now1 = inst(I1)
    check("token 更换且 epoch+1",
          now1["token"] != old_tok and now1["epoch"] == old_epoch + 1,
          f"{old_tok[:8]}/{old_epoch} -> {now1['token'][:8]}/{now1['epoch']}")
    r = admin(I1, "stale-heartbeat")
    check("旧进程迟到心跳 409 fencing",
          r.get("backend_response", {}).get("registry_status") == 409, r)
    r = admin(I1, "stale-deregister")
    check("旧进程迟到注销 409, 新实例记录保留",
          r.get("backend_response", {}).get("registry_status") == 409
          and inst(I1) is not None and inst(I1)["eligible"], r)
    p = fetch()
    check("接替后实例正常供答", p.get("ok"), p)

    print("-- 6. 转发失败如实显示 --")
    admin(I1, "pause")
    wait_for(f"{I1} 过期", lambda: not inst(I1)["eligible"] or None, timeout=20)
    admin(I2, "fail-hang")
    p = fetch()
    check("后端挂起 -> forward_failed 如实返回",
          not p.get("ok") and p.get("error") == "forward_failed", p)
    admin(I2, "fail-error")
    p = fetch()
    check("后端 500 -> upstream_error 如实返回",
          not p.get("ok") and p.get("upstream_status") == 500, p)
    admin(I2, "fail-none")
    p = fetch()
    check("故障恢复后正常供答", p.get("ok") and p["selected"]["instance_id"] == I2, p)
    admin(I1, "resume")
    wait_for(f"{I1} 恢复", both_eligible)

    print("-- 7. 无实例可用如实显示 --")
    admin(I1, "pause")
    admin(I2, "pause")
    wait_for("两台都不可选",
             lambda: (not inst(I1)["eligible"] and not inst(I2)["eligible"]) or None,
             timeout=20)
    p = fetch()
    check("no_eligible_instance 如实返回(含拒绝原因)",
          not p.get("ok") and p.get("error") == "no_eligible_instance"
          and len(p.get("rejected", [])) == 2, p)
    admin(I1, "resume")
    admin(I2, "resume")
    wait_for("两台恢复", both_eligible)

    print("-- 8. 注册中心重启: 未重新确认的记录不转发 --")
    admin(I1, "pause")
    admin(I2, "pause")  # 冻结心跳, 防止重启期间自动重新确认
    old_boot = state()["boot_id"]
    post("/api/admin/reboot")

    def registry_back():
        try:
            s = state()
            return s["boot_id"] != old_boot and s or None
        except Exception:
            return None

    s = wait_for("注册中心以新 boot_id 回来", registry_back, timeout=60)
    check("注册中心重启, boot_id 改变", True, f"{old_boot[:8]} -> {s['boot_id'][:8]}")
    rows = {r["instance_id"]: r for r in s["instances"]}
    check("旧记录仍在但均未重新确认",
          len(rows) == 2 and all(not r["confirmed"] for r in rows.values()), rows)
    p = fetch()
    check("未确认记录不参与转发",
          not p.get("ok") and p.get("error") == "no_eligible_instance"
          and all(x["reason"] == "unconfirmed_after_registry_restart"
                  for x in p.get("rejected", [])), p)
    admin(I1, "resume")
    admin(I2, "resume")
    a, b = wait_for("重新确认后两台恢复", both_eligible)
    p = fetch()
    check("重新确认后正常供答", p.get("ok"), p)

    print()
    n_ok = sum(1 for _, ok in RESULTS if ok)
    print(f"== 结果: {n_ok}/{len(RESULTS)} 通过 ==")
    sys.exit(0 if n_ok == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
