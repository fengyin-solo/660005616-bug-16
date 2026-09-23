import asyncio
import time
import random
import json
import threading
import sqlite3
import os
from collections import defaultdict, deque
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS: List[WebSocket] = []
WORKFLOW_ID = 0
# 必须在 async 主线程里捕获事件循环；工作线程中 get_event_loop() 得到的循环从未运行，
# 会导致 run_coroutine_threadsafe 投递的协程永远不执行（告警静默丢失的根因之一）
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None
LATEST_PAYLOAD: Optional[dict] = None

# ---- 告警/调度参数（演示环境为秒级，生产可按分钟级配置）----
MAX_RETRIES = 3            # 最大重试次数
CB_THRESHOLD = 3           # 熔断连续失败阈值
CB_COOLDOWN = 8.0          # 熔断冷却时间（秒）
STALL_SECONDS = 6.0        # 流水线全局无进展告警阈值（秒）
STALL_EMIT_INTERVAL = 2.0  # 停滞告警刷新原因/计数的最小间隔
# ---- 故障画像概率（节点在一次运行内的属性，重试沿用同一画像）----
FAULT_PERSIST_FAIL = 0.05  # 节点在本次运行中持续失败（每次都失败，直到重试耗尽）
FAULT_PERSIST_STUCK = 0.03  # 节点在本次运行中持续卡死（永不返回，直到超时）
FAULT_TRANSIENT = 0.05     # 节点首次失败、重试后恢复

DB_PATH = os.path.join(os.path.dirname(__file__), "alerts.db")
db_lock = threading.Lock()
db: Optional[sqlite3.Connection] = None


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"


class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


class HandleAlertRequest(BaseModel):
    note: str = ""
    handledBy: str = "值班人"


# ---------------------------------------------------------------------------
# 告警持久化（SQLite）：页面重开后仍能看到历史告警及其处置结果
# ---------------------------------------------------------------------------

def init_db() -> None:
    global db
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            task_id TEXT,
            task_name TEXT,
            type TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            count INTEGER NOT NULL DEFAULT 1,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL,
            resolved_at REAL,
            handled_at REAL,
            handled_by TEXT,
            handle_note TEXT
        )
        """
    )
    db.commit()


def _serialize(r: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": r["id"],
        "runId": r["run_id"],
        "taskId": r["task_id"],
        "taskName": r["task_name"],
        "type": r["type"],
        "severity": r["severity"],
        "title": r["title"],
        "message": r["message"],
        "status": r["status"],
        "count": r["count"],
        "firstSeen": r["first_seen"],
        "lastSeen": r["last_seen"],
        "resolvedAt": r["resolved_at"],
        "handledAt": r["handled_at"],
        "handledBy": r["handled_by"],
        "handleNote": r["handle_note"],
    }


def list_alerts(status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    assert db is not None
    with db_lock:
        if status:
            rows = db.execute(
                "SELECT * FROM alerts WHERE status = ? ORDER BY last_seen DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM alerts ORDER BY last_seen DESC LIMIT ?", (limit,)
            ).fetchall()
    return [_serialize(r) for r in rows]


def raise_alert(run_id: str, task_id: Optional[str], task_name: Optional[str],
                atype: str, severity: str, title: str, message: str) -> Dict[str, Any]:
    """产生或收敛一条告警。

    去重键：(run_id, task_id, type, status='active')。
    同一运行中同一节点的同类未处置告警只保留一条，重复发生时 count+1 并刷新时间与原因，
    避免短时间内反复弹出重复提醒。
    """
    assert db is not None
    now = time.time()
    with db_lock:
        existing = db.execute(
            "SELECT * FROM alerts WHERE run_id = ? AND task_id IS ? AND type = ? "
            "AND status = 'active' ORDER BY id DESC LIMIT 1",
            (run_id, task_id, atype),
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE alerts SET count = count + 1, last_seen = ?, title = ?, message = ? WHERE id = ?",
                (now, title, message, existing["id"]),
            )
            alert_id = existing["id"]
        else:
            cur = db.execute(
                "INSERT INTO alerts (run_id, task_id, task_name, type, severity, title, message, "
                "status, count, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?)",
                (run_id, task_id, task_name, atype, severity, title, message, now, now),
            )
            alert_id = cur.lastrowid
        db.commit()
        row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        alert = _serialize(row)
    broadcast({"type": "alerts", "alerts": list_alerts(limit=100)})
    return alert


def resolve_alerts(run_id: str, task_id: Optional[str] = None,
                   atype: Optional[str] = None) -> None:
    """将符合条件的活动告警置为已解除（恢复正常时自动收敛）。"""
    assert db is not None
    sql = "UPDATE alerts SET status = 'resolved', resolved_at = ? " \
          "WHERE run_id = ? AND status = 'active'"
    params: List[Any] = [time.time(), run_id]
    if task_id is not None:
        sql += " AND task_id = ?"
        params.append(task_id)
    if atype is not None:
        sql += " AND type = ?"
        params.append(atype)
    with db_lock:
        changed = db.execute(sql, params).rowcount
        db.commit()
    if changed:
        broadcast({"type": "alerts", "alerts": list_alerts(limit=100)})


# ---------------------------------------------------------------------------
# WebSocket 推送（工作线程安全）
# ---------------------------------------------------------------------------

def broadcast(payload: dict) -> None:
    loop = MAIN_LOOP
    if loop is None:
        return
    data = json.dumps(payload, ensure_ascii=False)
    dead = []
    for ws in list(ACTIVE_CLIENTS):
        try:
            fut = asyncio.run_coroutine_threadsafe(ws.send_text(data), loop)
            fut.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        except RuntimeError:
            dead.append(ws)
    for ws in dead:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)


# ---------------------------------------------------------------------------
# 工作流定义
# ---------------------------------------------------------------------------

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


@app.on_event("startup")
async def _on_startup():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    init_db()


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    dag = generate_dag_workflow("workflow")
    run_id = f"run-{int(time.time() * 1000)}"
    t = threading.Thread(target=execute_workflow,
                         args=(run_id, dag, req.workers, req.strategy), daemon=True)
    t.start()
    return {
        "type": "execution",
        "runId": run_id,
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "completed": False, "success": True,
        "alerts": list_alerts(limit=100),
    }


# ---------------------------------------------------------------------------
# 告警 REST API（处置结果落库，重开页面仍可见）
# ---------------------------------------------------------------------------

@app.get("/api/alerts")
def get_alerts(status: Optional[str] = None):
    return list_alerts(status=status or None)


@app.post("/api/alerts/{alert_id}/handle")
def handle_alert(alert_id: int, req: HandleAlertRequest):
    assert db is not None
    now = time.time()
    with db_lock:
        row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="告警不存在")
        db.execute(
            "UPDATE alerts SET status = 'handled', handled_at = ?, handled_by = ?, handle_note = ? WHERE id = ?",
            (now, req.handledBy, req.note, alert_id),
        )
        db.commit()
        row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        alert = _serialize(row)
    broadcast({"type": "alerts", "alerts": list_alerts(limit=100)})
    return alert


@app.post("/api/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int):
    assert db is not None
    with db_lock:
        row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="告警不存在")
        db.execute(
            "UPDATE alerts SET status = 'resolved', resolved_at = ? WHERE id = ?",
            (time.time(), alert_id),
        )
        db.commit()
        row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        alert = _serialize(row)
    broadcast({"type": "alerts", "alerts": list_alerts(limit=100)})
    return alert


# ---------------------------------------------------------------------------
# 执行引擎
# ---------------------------------------------------------------------------

def execute_workflow(run_id: str, dag, workers: int, strategy: str):
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    predecessors = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)
        predecessors[v].append(u)

    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    blocked = deque()  # 熔断冷却中、暂时不可调度的任务（避免忙等死循环）
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    running_tasks: Dict[str, dict] = {}
    completed = set()
    bad_nodes = set()  # 永久失败或被跳过的节点
    # 故障画像按节点一次性决定（而非每次重试重新掷骰子），
    # 真实线上故障通常是节点持续出错/持续卡死
    fault_mode: Dict[str, str] = {}

    def decide_fault() -> str:
        roll = random.random()
        if roll < FAULT_PERSIST_FAIL:
            return "persist_fail"
        if roll < FAULT_PERSIST_FAIL + FAULT_PERSIST_STUCK:
            return "persist_stuck"
        if roll < FAULT_PERSIST_FAIL + FAULT_PERSIST_STUCK + FAULT_TRANSIENT:
            return "transient"
        return "healthy"

    last_progress = time.time()
    last_stall_emit = 0.0

    def build_payload(completed_flag: bool, run_success: bool) -> dict:
        return {
            "type": "execution",
            "runId": run_id,
            "workflow": {"id": 1, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-50:],
            "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
            "completed": completed_flag,
            "success": run_success,
            "alerts": list_alerts(limit=100),
        }

    def send_update(completed_flag=False, run_success=True):
        global LATEST_PAYLOAD
        payload = build_payload(completed_flag, run_success)
        LATEST_PAYLOAD = payload
        broadcast(payload)
        time.sleep(0.3)

    def settle(tid: str, is_bad: bool):
        """节点终态处理：成功则释放下游；失败则把下游链路标记为 SKIPPED。"""
        queue = deque([(tid, is_bad)])
        while queue:
            nid, bad = queue.popleft()
            completed.add(nid)
            if bad:
                bad_nodes.add(nid)
            for nxt in adj[nid]:
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    if any(p in bad_nodes for p in predecessors[nxt]):
                        node_map[nxt]["status"] = "SKIPPED"
                        logs.append({"taskId": nxt, "status": "SKIPPED", "timestamp": time.time(),
                                     "message": f"上游失败，跳过 {node_map[nxt]['name']}"})
                        queue.append((nxt, True))
                    else:
                        ready.append(nxt)

    def stall_reason(now: float) -> str:
        """单独说明流水线/环节卡住的具体原因。"""
        if blocked:
            tid = blocked[0]
            cb = cb_state[tid]
            left = max(0, cb["cooldownUntil"] - now)
            return (f"节点「{node_map[tid]['name']}」熔断冷却中（剩余 {left:.0f}s），"
                    f"其下游环节全部被阻塞，无任务可调度")
        if running_tasks:
            tid, info = max(running_tasks.items(), key=lambda kv: now - kv[1]["start"])
            elapsed = now - info["start"]
            limit = info["timeout_at"] - info["start"]
            return (f"节点「{node_map[tid]['name']}」已运行 {elapsed:.0f}s 未结束"
                    f"（超时阈值 {limit:.0f}s），疑似卡死/无响应，阻塞下游")
        return "调度器既无运行中任务也无就绪任务"

    while ready or blocked or running_tasks:
        now = time.time()

        # 冷却结束的阻塞任务重新进入就绪队列
        pending_blocked = len(blocked)
        for _ in range(pending_blocked):
            tid = blocked.popleft()
            cb = cb_state[tid]
            if cb["state"] == "OPEN" and now < cb["cooldownUntil"]:
                blocked.append(tid)
            else:
                ready.appendleft(tid)

        # 启动就绪任务
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            cb = cb_state[tid]
            if cb["state"] == "OPEN" and now < cb["cooldownUntil"]:
                blocked.append(tid)
                continue
            if cb["state"] == "OPEN":
                cb["state"] = "HALF_OPEN"

            start = time.time()
            node["status"] = "RUNNING"
            node["startTime"] = start

            # 故障在节点首次调度时确定，重试沿用同一画像
            mode = fault_mode.setdefault(tid, decide_fault())
            will_stuck = mode == "persist_stuck"
            will_fail = mode in ("persist_fail",) or (mode == "transient" and node["retries"] == 0)
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            timeout_after = durations.get(tid, 1.5) * 2.5
            running_tasks[tid] = {
                "start": start,
                "end_time": None if will_stuck else start + runtime,
                "timeout_at": start + timeout_after,
                "will_fail": will_fail,
            }
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": start,
                         "message": f"开始执行 {node['name']}"})

        # 检查任务结果 / 超时
        now = time.time()
        finished = []
        for tid, info in list(running_tasks.items()):
            node = node_map[tid]
            outcome = None
            if now >= info["timeout_at"]:
                outcome = "timeout"
            elif info["end_time"] is not None and now >= info["end_time"]:
                outcome = "fail" if info["will_fail"] else "success"
            if outcome is None:
                continue

            cb = cb_state[tid]
            finished.append(tid)
            last_progress = now

            if outcome == "success":
                node["status"] = "SUCCESS"
                node["endTime"] = now
                cb["failureCount"] = 0
                cb["state"] = "CLOSED"
                # 运行恢复正常，自动解除该节点相关的运行态告警（失败告警仍保留待人工确认的除外）
                resolve_alerts(run_id, tid, "TASK_TIMEOUT")
                resolve_alerts(run_id, tid, "CIRCUIT_OPEN")
                resolve_alerts(run_id, None, "PIPELINE_STALL")
                logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now,
                             "message": f"完成 {node['name']}"})
                settle(tid, False)
                continue

            # fail / timeout：还有重试额度则重试
            if node["retries"] < MAX_RETRIES:
                node["retries"] += 1
                node["status"] = "PENDING"
                ready.appendleft(tid)
                cb["failureCount"] += 1

                if outcome == "timeout":
                    elapsed = now - info["start"]
                    limit = info["timeout_at"] - info["start"]
                    logs.append({"taskId": tid, "status": "TIMEOUT", "timestamp": now,
                                 "message": f"执行超时（{elapsed:.0f}s>{limit:.0f}s），重试 {node['retries']}/{MAX_RETRIES}"})
                    raise_alert(
                        run_id, tid, node["name"], "TASK_TIMEOUT", "warning",
                        "节点执行超时（疑似卡死）",
                        f"节点「{node['name']}」已运行 {elapsed:.0f}s 仍未完成，超过阈值 {limit:.0f}s，"
                        f"判定为卡死/无响应，正在进行第 {node['retries']}/{MAX_RETRIES} 次重试",
                    )
                else:
                    logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                 "message": f"执行失败，重试 {node['retries']}/{MAX_RETRIES}"})

                if cb["failureCount"] >= CB_THRESHOLD:
                    # 熔断开启；HALF_OPEN 试探请求再次失败时也重新 OPEN 并续期冷却
                    cb["state"] = "OPEN"
                    cb["cooldownUntil"] = now + CB_COOLDOWN
                    logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now,
                                 "message": f"熔断! {cb['failureCount']}次连续失败，冷却{CB_COOLDOWN:.0f}s"})
                    raise_alert(
                        run_id, tid, node["name"], "CIRCUIT_OPEN", "error",
                        "熔断器开启",
                        f"节点「{node['name']}」连续失败 {cb['failureCount']} 次，熔断器已开启，"
                        f"冷却 {CB_COOLDOWN:.0f}s 后才会放行试探请求，期间流水线被阻塞",
                    )
            else:
                # 重试次数用满：必须明确判失败，绝不能当作正常结束
                node["status"] = "FAILED"
                node["endTime"] = now
                logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                             "message": f"重试 {MAX_RETRIES} 次后仍失败，终止该环节"})
                resolve_alerts(run_id, tid, "TASK_TIMEOUT")
                resolve_alerts(run_id, tid, "CIRCUIT_OPEN")
                skipped_count = len([n for n in nodes
                                     if n["id"] not in completed and n["id"] != tid
                                     and any(p in bad_nodes.union({tid}) for p in predecessors[n["id"]])])
                raise_alert(
                    run_id, tid, node["name"], "TASK_FAILED", "error",
                    "节点重试耗尽，执行失败",
                    f"节点「{node['name']}」已重试 {MAX_RETRIES} 次仍未成功，正式判定失败；"
                    f"其下游环节将被跳过，流水线无法正常完成",
                )
                settle(tid, True)

        for tid in finished:
            running_tasks.pop(tid, None)

        # 全局停滞看门狗：超过 STALL_SECONDS 没有任何进展即告警（同一运行只维护一条，自动收敛）
        now = time.time()
        stalled = (ready or blocked or running_tasks) and (now - last_progress > STALL_SECONDS)
        if stalled:
            if now - last_stall_emit >= STALL_EMIT_INTERVAL:
                reason = stall_reason(now)
                raise_alert(
                    run_id, None, None, "PIPELINE_STALL", "error",
                    "流水线停滞告警",
                    f"整条流水线已 {now - last_progress:.0f}s 无进展。原因：{reason}",
                )
                last_stall_emit = now
        else:
            resolve_alerts(run_id, None, "PIPELINE_STALL")

        send_update()
        if len(completed) == len(nodes):
            break

    # 运行结束：运行态告警自动解除；TASK_FAILED 保留活动状态等待人工处置
    resolve_alerts(run_id, None, "TASK_TIMEOUT")
    resolve_alerts(run_id, None, "CIRCUIT_OPEN")
    resolve_alerts(run_id, None, "PIPELINE_STALL")
    send_update(True, run_success=not bad_nodes)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        # 新连接 / 页面重开：先补发历史告警，再补发最近一次执行快照
        await ws.send_text(json.dumps({"type": "alerts", "alerts": list_alerts(limit=100)},
                                      ensure_ascii=False))
        if LATEST_PAYLOAD is not None:
            await ws.send_text(json.dumps(LATEST_PAYLOAD, ensure_ascii=False))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
