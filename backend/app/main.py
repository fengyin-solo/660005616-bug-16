import asyncio
import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0
RUN_ID = 0

# ---- 告警与执行状态（服务端持久保存，页面重开/第二天仍可见） ----
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
ALERTS_FILE = os.path.join(DATA_DIR, "alerts.json")
LOCK = threading.RLock()
ALERTS = []                       # 全量历史告警（含已处置）
ACTIVE_ALERTS = {}                # fingerprint -> 未解决的告警，用于去重收敛
LAST_STATE = {"payload": None}    # 最近一次执行快照

# ---- 执行引擎参数 ----
MAX_RETRIES = 3
CIRCUIT_FAILURE_THRESHOLD = 3
CIRCUIT_COOLDOWN = 5.0
# 环节卡死阈值：预期耗时的 2.5 倍且不少于下限。生产环境按 SLA 调大（如 3600s）
TASK_TIMEOUT_FACTOR = 2.5
TASK_TIMEOUT_MIN = 5.0
# 流水线级兜底看门狗：整个运行多久无任何状态推进即判定卡死。生产建议 3600s
WORKFLOW_STALL_TIMEOUT = 30.0
TICK_INTERVAL = 0.3
# 故障注入（演示）：12% 卡死、15% 失败
HANG_RATE = 0.12
FAILURE_RATE = 0.15

# uvicorn 的主事件循环，startup 时捕获；工作线程必须通过它向 WS 推送
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None


@app.on_event("startup")
async def _capture_loop():
    global MAIN_LOOP
    # async startup 保证在 uvicorn 主事件循环上执行；工作线程必须通过它向 WS 推送
    MAIN_LOOP = asyncio.get_running_loop()
    _load_alerts()


# ---------- 告警存储 ----------

def _load_alerts():
    try:
        with open(ALERTS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    for a in data:
        ALERTS.append(a)
        if a.get("status") != "RESOLVED":
            ACTIVE_ALERTS[a["fingerprint"]] = a


def _persist_alerts():
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = ALERTS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ALERTS, f, ensure_ascii=False)
    os.replace(tmp, ALERTS_FILE)


def raise_alert(fingerprint, alert_type, severity, title, message, task_id=None, detail=None):
    """产生一条告警；相同指纹的未解决告警收敛为同一条（计数 +1），不再重复弹出。"""
    now = time.time()
    with LOCK:
        alert = ACTIVE_ALERTS.get(fingerprint)
        if alert is not None and alert["status"] != "RESOLVED":
            alert["occurrenceCount"] += 1
            alert["lastSeen"] = now
            alert["updatedAt"] = now
            alert["message"] = message
            # 保留最新现场（如最新一次被阻塞的下游清单、卡死耗时）
            if detail:
                alert["detail"] = detail
            if severity == "critical":
                alert["severity"] = severity
        else:
            alert = {
                "id": f"a-{uuid.uuid4().hex[:12]}",
                "fingerprint": fingerprint,
                "type": alert_type,
                "severity": severity,
                "taskId": task_id,
                "title": title,
                "message": message,
                "detail": detail or {},
                "status": "FIRING",
                "occurrenceCount": 1,
                "firstSeen": now,
                "lastSeen": now,
                "handledBy": None,
                "handledAt": None,
                "handleComment": None,
                "updatedAt": now,
            }
            ALERTS.append(alert)
            ACTIVE_ALERTS[fingerprint] = alert
        _persist_alerts()
    broadcast(get_state())
    return alert


def get_state():
    """对外快照：最近一次执行状态 + 全量告警（含历史处置结果）。"""
    with LOCK:
        state = dict(LAST_STATE["payload"]) if LAST_STATE["payload"] else {
            "workflow": None, "logs": [], "circuitBreakers": [],
            "completed": False, "success": False, "runId": RUN_ID,
        }
        state["alerts"] = [dict(a) for a in ALERTS]
    return state


def broadcast(payload):
    loop = MAIN_LOOP
    if loop is None:
        return
    msg = json.dumps(payload, ensure_ascii=False, default=str)
    for ws in list(ACTIVE_CLIENTS):
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(msg), loop)
        except Exception:
            # 单个客户端发送失败不影响其他客户端和执行线程
            pass


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"


class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


class HandleAlertRequest(BaseModel):
    action: str  # acknowledge | resolve
    operator: str = "anonymous"
    comment: str = ""


def generate_dag_workflow(name: str):
    """Create a realistic DAG pipeline"""
    nodes = [
        {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0},
        {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5},
        {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8},
        {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2},
        {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0},
        {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0},
        {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5},
        {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0},
        {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8},
        {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2},
        {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5},
    ]
    positions = [
        (0, 0), (0, 1), (-1, 2), (1, 2), (-1, 3),
        (0.5, 3), (-0.3, 4), (-0.3, 5), (-1, 6), (0.5, 6), (-0.3, 7)
    ]
    for i, n in enumerate(nodes):
        n["x"] = positions[i][0] * 2.5 + 2.5
        n["y"] = positions[i][1] * 0.9
        n["status"] = "PENDING"
        n["retries"] = 0
        n["startTime"] = None
        n["endTime"] = None

    edges = []
    for n in nodes:
        for d in n["deps"]:
            edges.append([d, n["id"]])

    return {"nodes": [{
        "id": n["id"], "name": n["name"], "deps": n["deps"],
        "x": n["x"], "y": n["y"], "status": n["status"],
        "startTime": None, "endTime": None, "retries": n["retries"]
    } for n in nodes], "edges": edges, "durations": {n["id"]: n["duration"] for n in nodes}}


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    global RUN_ID
    RUN_ID += 1
    dag = generate_dag_workflow("workflow")
    t = threading.Thread(
        target=execute_workflow, args=(dag, req.workers, req.strategy, RUN_ID), daemon=True
    )
    t.start()
    return {
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "alerts": [dict(a) for a in ALERTS],
        "completed": False, "success": False, "runId": RUN_ID,
    }


@app.get("/api/alerts")
def list_alerts():
    """全量告警（含历史与处置结果），供页面（重新）打开时恢复。"""
    with LOCK:
        return [dict(a) for a in ALERTS]


@app.get("/api/state")
def state_endpoint():
    return get_state()


@app.post("/api/alerts/{alert_id}/handle")
def handle_alert(alert_id: str, req: HandleAlertRequest):
    """确认 / 解决告警；落盘并向所有在线页面广播，保证列表与后端同步。"""
    if req.action not in ("acknowledge", "resolve"):
        raise HTTPException(status_code=400, detail="action 必须是 acknowledge 或 resolve")
    now = time.time()
    with LOCK:
        alert = next((a for a in ALERTS if a["id"] == alert_id), None)
        if alert is None:
            raise HTTPException(status_code=404, detail="告警不存在")
        if req.action == "acknowledge":
            if alert["status"] == "RESOLVED":
                raise HTTPException(status_code=400, detail="已解决的告警不能再确认")
            alert["status"] = "ACKNOWLEDGED"
        else:
            alert["status"] = "RESOLVED"
            ACTIVE_ALERTS.pop(alert["fingerprint"], None)
        alert["handledBy"] = (req.operator or "anonymous").strip() or "anonymous"
        alert["handledAt"] = now
        alert["handleComment"] = req.comment
        alert["updatedAt"] = now
        _persist_alerts()
    broadcast(get_state())
    return {"alert": alert}


def execute_workflow(dag, workers, strategy, run_id):
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    running_tasks = {}
    completed = set()
    failed = set()
    blocked = set()
    start_ts = time.time()
    last_progress = start_ts

    def log(tid, status, message):
        logs.append({"taskId": tid, "status": status, "timestamp": time.time(), "message": message})

    def publish(done=False, success=False):
        payload = {
            "workflow": {"id": 1, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-50:],
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "completed": done,
            "success": success,
            "runId": run_id,
        }
        with LOCK:
            LAST_STATE["payload"] = payload
        broadcast(get_state())

    def task_timeout(tid):
        return max(durations.get(tid, 1.5) * TASK_TIMEOUT_FACTOR, TASK_TIMEOUT_MIN)

    def collect_impacted(tid):
        """沿依赖边收集所有可达、且尚未启动的下游环节。"""
        result = []
        queue = deque(adj[tid])
        seen = set()
        while queue:
            d = queue.popleft()
            if d in seen or d in completed or d in failed or d in blocked:
                continue
            seen.add(d)
            dn = node_map[d]
            if d not in running_tasks and dn["status"] == "PENDING":
                result.append(d)
            queue.extend(adj[d])
        return result

    def fail_terminal(tid, status, alert_type, title, message, detail):
        """环节终态失败：标记节点、收敛为一条告警、阻塞全部下游。"""
        nonlocal ready, last_progress
        node = node_map[tid]
        node["status"] = status
        node["endTime"] = time.time()
        failed.add(tid)
        raise_alert(
            f"{alert_type}:{tid}", alert_type, "critical", title, message,
            task_id=tid, detail=detail,
        )
        # 沿依赖边传播：所有可达的未启动下游标记为 BLOCKED，不再调度
        impacted = collect_impacted(tid)
        for d in impacted:
            node_map[d]["status"] = "BLOCKED"
            blocked.add(d)
            log(d, "BLOCKED", f"上游环节 {node['name']} 失败，该环节被阻塞")
        ready = deque(t for t in ready if t not in blocked)
        last_progress = time.time()
        return impacted

    while ready or running_tasks:
        now = time.time()

        # 调度可运行任务
        deferred = []  # 熔断冷却中的任务，本轮放回队首
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            # 只有 PENDING 才能被调度（终态失败/已阻塞的节点不得重新执行）
            if node["status"] != "PENDING" or tid in running_tasks:
                continue
            cb = cb_state[tid]
            if cb["state"] == "OPEN" and now < cb["cooldownUntil"]:
                # 熔断冷却中：稍后放回队首并停止调度，避免忙等死循环
                deferred.append(tid)
                break
            if cb["state"] == "OPEN":
                cb["state"] = "HALF_OPEN"
                log(tid, "CIRCUIT_HALF_OPEN", "熔断冷却结束，半开试探执行")

            node["status"] = "RUNNING"
            node["startTime"] = now
            roll = random.random()
            will_hang = roll < HANG_RATE
            will_fail = (not will_hang) and roll < HANG_RATE + FAILURE_RATE
            timeout = task_timeout(tid)
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {
                # 卡死任务的结束时间被放到很远，只能靠超时检测截获
                "end_time": now + (3600 if will_hang else runtime),
                "started": now,
                "will_fail": will_fail,
                "timeout": timeout,
            }
            log(tid, "RUNNING", f"开始执行 {node['name']}")
            last_progress = now
        for tid in deferred:
            ready.appendleft(tid)

        # 检查运行中任务：每次尝试一旦结算，立即释放任务槽，杜绝陈旧槽被重复结算
        now = time.time()
        for tid in list(running_tasks.keys()):
            info = running_tasks[tid]
            node = node_map[tid]
            cb = cb_state[tid]
            elapsed = now - info["started"]

            # 1) 卡死/超时检测：独立说明原因，绝不让环节无限期挂起
            if elapsed >= info["timeout"]:
                timeout = info["timeout"]
                running_tasks.pop(tid, None)
                fail_terminal(
                    tid, "TIMEOUT", "TASK_STUCK",
                    f"环节「{node['name']}」卡死超时",
                    f"环节 {tid} 已运行 {elapsed:.0f}s，超过卡死阈值 {timeout:.0f}s，"
                    f"已终止该环节并阻塞其下游。请检查该环节依赖的外部服务、资源等待与死锁情况。",
                    {
                        "taskId": tid,
                        "elapsedSeconds": round(elapsed, 1),
                        "timeoutSeconds": round(timeout, 1),
                        "expectedDurationSeconds": durations.get(tid, 1.5),
                        "possibleCauses": ["下游服务无响应", "资源/锁等待", "外部依赖不可用", "死锁或长时间 I/O"],
                    },
                )
                continue

            if now < info["end_time"]:
                continue

            # 尝试结束，立刻释放槽位
            running_tasks.pop(tid, None)

            # 2) 到达正常结束时间
            if info["will_fail"] and node["retries"] < MAX_RETRIES:
                node["retries"] += 1
                node["status"] = "PENDING"
                ready.appendleft(tid)
                cb["failureCount"] += 1
                log(tid, "FAILED", f"执行失败，准备重试 {node['retries']}/{MAX_RETRIES}")
                # 同一环节多次失败收敛为一条告警（指纹去重，仅计数，不重复提醒）
                raise_alert(
                    f"TASK_FAILED:{tid}", "TASK_FAILED", "warning",
                    f"环节「{node['name']}」执行失败",
                    f"第 {node['retries']}/{MAX_RETRIES} 次执行失败，将自动重试。",
                    task_id=tid,
                    detail={"taskId": tid, "retries": node["retries"], "maxRetries": MAX_RETRIES},
                )
                if cb["state"] == "HALF_OPEN" or cb["failureCount"] >= CIRCUIT_FAILURE_THRESHOLD:
                    cb["state"] = "OPEN"
                    cb["cooldownUntil"] = now + CIRCUIT_COOLDOWN
                    raise_alert(
                        f"CIRCUIT_OPEN:{tid}", "CIRCUIT_OPEN", "critical",
                        f"环节「{node['name']}」熔断器打开",
                        f"连续失败已达阈值，熔断 {CIRCUIT_COOLDOWN:.0f}s 后进入半开试探。",
                        task_id=tid,
                        detail={"failureCount": cb["failureCount"], "cooldownSeconds": CIRCUIT_COOLDOWN},
                    )
            elif info["will_fail"]:
                # 重试次数已用满：终态失败，绝不再当作正常结束
                downstream = collect_impacted(tid)
                fail_terminal(
                    tid, "FAILED", "RETRIES_EXHAUSTED",
                    f"环节「{node['name']}」重试耗尽",
                    f"已自动重试 {MAX_RETRIES} 次仍然失败，判定为终态失败，"
                    f"{len(downstream)} 个下游环节已被阻塞，需要人工介入。",
                    {
                        "taskId": tid,
                        "retries": MAX_RETRIES,
                        "downstreamBlocked": downstream,
                    },
                )
            else:
                node["status"] = "SUCCESS"
                node["endTime"] = now
                completed.add(tid)
                cb["failureCount"] = 0
                if cb["state"] == "HALF_OPEN":
                    cb["state"] = "CLOSED"
                log(tid, "SUCCESS", f"完成 {node['name']}")
                for next_tid in adj[tid]:
                    in_degree[next_tid] -= 1
                    if in_degree[next_tid] == 0 and node_map[next_tid]["status"] != "BLOCKED":
                        ready.append(next_tid)
            last_progress = now

        # 3) 流水线级兜底看门狗：有环节在跑但长时间无任何进展
        if running_tasks and now - last_progress > WORKFLOW_STALL_TIMEOUT:
            stuck_tasks = [
                {"taskId": t, "elapsedSeconds": round(now - i["started"], 1)}
                for t, i in running_tasks.items()
            ]
            raise_alert(
                f"WORKFLOW_STALLED:{run_id}", "WORKFLOW_STALLED", "critical",
                "整条流水线卡死",
                f"流水线已 {WORKFLOW_STALL_TIMEOUT:.0f}s 没有任何状态推进，"
                f"仍有 {len(running_tasks)} 个环节未结束：{', '.join(t['taskId'] for t in stuck_tasks)}。",
                detail={"idleSeconds": round(now - last_progress, 1), "runningTasks": stuck_tasks},
            )
            last_progress = now  # 指纹去重之外再避免每个 tick 重复写盘

        publish()
        time.sleep(TICK_INTERVAL)

    # 终态汇总
    success = len(completed) == len(nodes)
    if success:
        raise_alert(
            f"WORKFLOW_SUCCEEDED:{run_id}", "WORKFLOW_SUCCEEDED", "info",
            "流水线运行成功",
            f"全部 {len(nodes)} 个环节执行成功，耗时 {time.time() - start_ts:.1f}s。",
        )
    else:
        failed_names = "、".join(node_map[t]["name"] for t in sorted(failed)) or "未知"
        raise_alert(
            f"WORKFLOW_FAILED:{run_id}", "WORKFLOW_FAILED", "critical",
            "流水线运行失败",
            f"运行结束：{len(failed)} 个环节终态失败（{failed_names}），"
            f"{len(blocked)} 个下游环节被阻塞，请处理后重跑。",
            detail={"failedTasks": sorted(failed), "blockedTasks": sorted(blocked)},
        )
    publish(done=True, success=success)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        # 连上即推完整快照：告警列表、历史处置结果、最近执行状态一次同步
        await ws.send_text(json.dumps(get_state(), ensure_ascii=False, default=str))
        while True:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
