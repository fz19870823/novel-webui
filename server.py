"""
novel-webui 服务入口
Flask 提供：
  - 前端静态页（/）
  - API：配置读写/测试连接/拉模型 / 启动/续传/停止 / 状态拉取(兼容) / 确认应答 / 下载成品
  - WebSocket /ws：状态/日志/实时正文/待确认项推送（前端实时显示主通道）
生成任务在后台线程运行，不依赖前端连接，提交后即可关闭页面。

用法： python server.py [--host 127.0.0.1] [--port 8000] [--confirm 5]
WS 经反向代理时：反代需转发 Upgrade/Connection 头（见 README「反向代理」一节）。
"""

import argparse
import os
import threading
import time
import json
import urllib.request
import urllib.error
from datetime import timedelta
from urllib.parse import urlparse

from flask import (Flask, request, jsonify, send_from_directory, send_file,
                   redirect, session)
from flask_sock import Sock

from config import (load_config, save_config, mask_api_key, DATA_DIR,
                    DEFAULT_BASE_URL, DEFAULT_MODEL, DEFAULT_CONFIRM_SECONDS)
from auth import (has_users as auth_has_users,
                  create_user as auth_create_user,
                  verify_user as auth_verify_user,
                  session_secret as auth_session_secret,
                  setup_token as auth_setup_token,
                  verify_setup_token as auth_verify_setup_token,
                  login_block_remaining as auth_login_block,
                  record_login_failure as auth_record_fail,
                  record_login_success as auth_record_ok)
from state import load_resume_state
from controller import manager, LOG_RETURN
from worker import GeneratorWorker, RefusalResolver

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# 成品/日志文件名判定：worker 落盘为「标题_YYYYMMDD_HHMMSS.txt」(标题可中文)，
# 兼容旧式 novel_*.txt / log_*.txt 前缀命名。用于文件列表与下载白名单。
import re as _re
_OUTPUT_NAME_RE = _re.compile(r"^(?:novel_|log_|.+(?:_\d{8}_\d{6}))\.txt$")


def is_output_name(fn: str) -> bool:
    return bool(_OUTPUT_NAME_RE.match(fn or ""))

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024  # 2MB，仅 JSON 配置
# 会话签名密钥持久化在数据目录（重启后已登录会话不失效）
app.secret_key = auth_session_secret()
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
# 会话 cookie 加固：SameSite=Lax 抵御跨站请求携带 cookie（CSRF/CSWSH 面收窄）。
# 若服务经 HTTPS 反代暴露，可设 NOVEL_COOKIE_SECURE=1 追加 Secure 标记。
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_HTTPONLY"] = True
if os.environ.get("NOVEL_COOKIE_SECURE", "").strip() in ("1", "true", "yes"):
    app.config["SESSION_COOKIE_SECURE"] = True
sock = Sock(app)

# 是否信任反代传入的 X-Forwarded-For（用于登录限流按真实来源 IP 计数）。
# 默认关闭：直连暴露时 XFF 可伪造，贸然信任等于绕过限流。
TRUST_PROXY = os.environ.get("NOVEL_TRUST_PROXY", "").strip() in ("1", "true", "yes")

# 无需登录即可访问的路径（首次初始化 / 登录页与对应 API）
PUBLIC_PATHS = {"/setup.html", "/login.html",
                "/api/auth_state", "/api/setup", "/api/login"}

# 全局：当前 worker（同一时刻只允许一个生成任务）
_worker = None
_worker_lock = threading.Lock()

# ── WebSocket 订阅端（/ws）──
WS_HEARTBEAT = 15          # 秒：空闲时发送 ping 保活（反代 read_timeout 默认常为 60s）
WS_RECEIVE_TIMEOUT = 30    # 秒：服务端 receive 阻塞上限
WS_MIN_INTERVAL = 0.1      # 秒：两帧之间最小间隔（合并高频 chunk，≤10 帧/秒）


class WsClient:
    """单条 /ws 连接。记录已推送的 rev/日志 seq/正文长度，用于增量 diff。"""

    def __init__(self, ws):
        self.ws = ws
        self.rev = -1      # 已推状态 rev（-1 = 尚未推 init）
        self.seq = 0       # 已推日志 seq
        self.clen = -1     # 已推正文「全文」长度（-1 = 尚未推）
        self.alive = True
        # 在线计数只减一次：发送线程与 handler 线程都会走到 close()
        self.registered = True
        manager.register_ws()

    def close(self):
        self.alive = False
        if self.registered:
            self.registered = False
            manager.unregister_ws()
        with manager._cond:
            manager._cond.notify_all()


def _ws_send(cli: WsClient, obj: dict) -> bool:
    """发送一帧；失败(连接已断)返回 False。send 线程安全（simple-websocket）。"""
    try:
        cli.ws.send(json.dumps(obj, ensure_ascii=False))
        return True
    except Exception:
        return False


def _ws_build(cli: WsClient) -> dict | None:
    """计算本客户端当前需要的帧，并推进其已推水位；无需推送时返回 None。

    只做计算，不做网络 IO（调用方需持有 manager._cond）。返回 None 之外的
    帧由调用方在锁外发送，避免慢客户端阻塞其它连接的变更通知。
    """
    st = manager.get_status()
    rev, seq = st.pop("rev"), st["log_seq"]

    if cli.rev < 0:
        # 首次连接：全量 init（状态 + 最近日志 + 正文尾），不等心跳周期
        logs = manager.get_logs_since(0)["logs"][-LOG_RETURN:]
        clen = manager.get_content_len()
        cli.rev, cli.seq, cli.clen = rev, seq, clen
        return {"type": "init", "status": st,
                "logs": [m for _, m in logs],
                "content": manager.get_content_tail(), "clen": clen}

    frame: dict = {"type": "diff"}
    changed = False
    if rev != cli.rev:
        cli.rev = rev
        frame["status"] = st
        changed = True
    if seq != cli.seq:
        d = manager.get_logs_since(cli.seq)
        frame["logs"] = [m for _, m in d["logs"]]
        frame["seq"] = d["seq"]
        cli.seq = d["seq"]
        changed = True
    # 变更检测用「全文长度」，不能用尾部长度（尾部封顶 CONTENT_TAIL，
    # 超过后长度恒定 → 判定为无变化 → 正文停止推送）。
    clen = manager.get_content_len()
    if clen != cli.clen:
        cli.clen = clen
        frame["content"] = manager.get_content_tail()
        frame["clen"] = clen
        changed = True
    return frame if changed else None


def _ws_sender(cli: WsClient):
    """订阅线程：连接即推 init，此后变更推 diff，空闲超时发心跳。

    三个关键点：
    - 「先构建、后 wait」在同一把 _cond 内完成 → 状态变更不会在
      「已读完状态、尚未进入 wait」的窗口里丢失唤醒（否则最坏要等满 15s 心跳）。
    - 网络发送在锁外 → 单个慢客户端不会阻塞其它连接的变更通知。
    - 两帧间隔下限 WS_MIN_INTERVAL → 流式期间每个 token 一次回调被合并成
      最多 10 帧/秒，避免逐 token 全量推送。
    """
    try:
        while cli.alive:
            with manager._cond:
                frame = _ws_build(cli)
                if frame is None:
                    if manager._cond.wait(timeout=WS_HEARTBEAT):
                        continue            # 有变更 → 立即重新构建
                    frame = {"type": "ping"}  # 空闲超时 → 心跳保活（反代空闲断连）
            if not _ws_send(cli, frame):
                return
            if WS_MIN_INTERVAL > 0:
                time.sleep(WS_MIN_INTERVAL)
    finally:
        cli.close()


def _origin_ok() -> bool:
    """校验 WebSocket 握手的 Origin 是否同源（防跨站 WebSocket 劫持 CSWSH）。

    - 只比较**主机名**，忽略端口与协议：端口差异不构成跨站（同主机不同端口
      仍是同一信任域），且部分客户端/代理的 Host 头不带端口；协议也不能比
      （反代常在 TLS 终结，服务端看到的是 http，浏览器 Origin 是 https）。
    - 浏览器一定带 Origin；非浏览器客户端（脚本/测试）不带 → 放行：这类请求
      本就没有浏览器 cookie 自动附带能力，且已被会话鉴权守卫拦住。
    - 信息不足（无 Host / 无法解析）时不误杀，交由会话鉴权兜底。
    """
    origin = request.headers.get("Origin")
    if not origin:
        return True
    try:
        o_host = (urlparse(origin).hostname or "").lower()
        r_host = (urlparse("//" + (request.host or "")).hostname or "").lower()
    except ValueError:
        return False
    if not o_host or not r_host:
        return True
    return o_host == r_host


@sock.route("/ws")
def ws_handler(ws):
    """/ws：浏览器订阅端。连接建立即推 init，此后 manager 变更推送 diff。"""
    if not _origin_ok():
        try:
            ws.close()
        except Exception:
            pass
        return
    cli = WsClient(ws)
    threading.Thread(target=_ws_sender, args=(cli,), daemon=True).start()
    try:
        while True:
            msg = ws.receive(timeout=WS_RECEIVE_TIMEOUT)
            if msg is None:
                continue  # receive 超时，非断开
            try:
                obj = json.loads(msg)
            except (ValueError, TypeError):
                continue
            t = obj.get("type") if isinstance(obj, dict) else None
            if t == "ping":        # 浏览器侧探测存活
                _ws_send(cli, {"type": "pong"})
            # 其余消息当前无客户端→服务端语义，忽略
    except Exception:
        pass  # 连接关闭/异常 → 清理
    finally:
        cli.close()


def _stage_info():
    """返回启动页需要的默认配置 + 可续传标记（不返回 key）。"""
    cfg = load_config()
    return {
        "base_url": cfg.get("base_url", DEFAULT_BASE_URL),
        "model": cfg.get("model", DEFAULT_MODEL),
        "api_key_masked": mask_api_key(cfg.get("api_key", "")),
        "api_key_env": bool(os.environ.get("NOVEL_AI_API_KEY", "")),
        "chapters_count": cfg.get("chapters_count", ""),
        "words_per_chapter": cfg.get("words_per_chapter", ""),
        "theme": cfg.get("theme", ""),
        "requirements": cfg.get("requirements", ""),
        "has_resume": load_resume_state() is not None,
        "confirm_seconds": DEFAULT_CONFIRM_SECONDS,
    }


def _current_worker() -> GeneratorWorker | None:
    global _worker
    with _worker_lock:
        return _worker


def _can_start() -> tuple[bool, str]:
    """同一时刻只允许一个任务。返回 (允许?, 原因)。"""
    global _worker
    with _worker_lock:
        if _worker and _worker.is_alive():
            return False, "已有生成任务在运行"
        return True, ""


# ═══════════ 鉴权 ═══════════

def _client_ip() -> str:
    """来源 IP（登录限流计数键）。

    仅当显式声明 NOVEL_TRUST_PROXY=1（服务在可信反代之后）才采信
    X-Forwarded-For 首段；否则用 remote_addr——直连暴露时 XFF 可任意伪造，
    贸然信任等于给爆破者提供无限次的免费重试。
    """
    if TRUST_PROXY:
        xff = request.headers.get("X-Forwarded-For", "")
        if xff:
            return xff.split(",")[0].strip() or "?"
    return request.remote_addr or "?"


@app.before_request
def _auth_guard():
    """统一鉴权守卫。

    - 未初始化（无账号）→ 任何页面访问都被导向首次设置页
    - 已初始化但未登录 → 页面 302 到登录页；API/WS 返回 401
    - /ws 在握手阶段即被拒绝（401），不建立无用连接
    """
    p = request.path
    if p == "/":
        if not auth_has_users():
            return redirect("/setup.html")
        if not session.get("uid"):
            return redirect("/login.html")
        return None                      # 放行到 index()
    if p in PUBLIC_PATHS:
        return None
    if session.get("uid"):
        return None
    if p == "/ws" or p.startswith("/api/") or p.startswith("/static/"):
        return ("unauthorized", 401)
    return None                          # 其它路径（404 等）交给 Flask


@app.route("/setup.html")
def setup_page():
    return send_from_directory(STATIC_DIR, "setup.html")


@app.route("/login.html")
def login_page():
    return send_from_directory(STATIC_DIR, "login.html")


@app.route("/api/auth_state")
def api_auth_state():
    """鉴权状态探测（登录页/首次设置页/前端断线判断共用，免登录）。"""
    return jsonify({
        "setup_required": not auth_has_users(),
        "authed": bool(session.get("uid")),
        "user": session.get("uid"),
    })


@app.route("/api/setup", methods=["POST"])
def api_setup():
    """首次初始化：创建唯一管理员账号（之后此接口永久失效）。

    需要一次性初始化口令（setup token）：服务启动时打印到日志、并持久化在
    数据目录 .setup_token，创建管理员后即失效。这样即便服务直接暴露公网，
    第一个访问者也无法抢注管理员——口令只有能读到启动日志/数据目录的人知道。
    """
    if auth_has_users():
        return jsonify({"ok": False, "message": "已初始化过，请直接登录"}), 400
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    if not auth_verify_setup_token(token):
        return jsonify({"ok": False, "message": "初始化口令不正确（见服务启动日志）"}), 403
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    confirm = data.get("confirm")
    if confirm is not None and password != confirm:
        return jsonify({"ok": False, "message": "两次输入的密码不一致"}), 400
    ok, msg = auth_create_user(username, password)
    if not ok:
        return jsonify({"ok": False, "message": msg}), 400
    return jsonify({"ok": True, "message": "管理员账号已创建"})


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"ok": False, "message": "请输入账号和密码"}), 400
    ip = _client_ip()
    wait = auth_login_block(ip)
    if wait > 0:
        return jsonify({"ok": False,
                        "message": f"登录失败次数过多，请 {wait} 秒后再试"}), 429
    if not auth_verify_user(username, password):
        auth_record_fail(ip)
        return jsonify({"ok": False, "message": "账号或密码错误"}), 401
    auth_record_ok(ip)
    session.clear()                     # 防会话固定
    session["uid"] = username
    session.permanent = True
    return jsonify({"ok": True, "user": username})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


# ═══════════ 前端静态 ═══════════

@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)


# ═══════════ API: 配置 ═══════════

@app.route("/api/config", methods=["GET"])
def api_config_get():
    return jsonify(_stage_info())


@app.route("/api/config", methods=["POST"])
def api_config_set():
    data = request.get_json(silent=True) or {}
    cfg = load_config()
    for k in ("base_url", "model", "chapters_count", "words_per_chapter", "theme", "requirements"):
        if k in data and data[k] is not None:
            cfg[k] = str(data[k]).strip()
    # API Key：非空才写入（会持久化到配置文件；有环境变量时文件不存明文，见 save_config 语义）
    new_key = (data.get("api_key") or "").strip()
    if new_key:
        cfg["api_key"] = new_key
    save_config(cfg)
    return jsonify(_stage_info())


@app.route("/api/test", methods=["POST"])
def api_test():
    from openai import OpenAI
    data = request.get_json(silent=True) or {}
    key = (data.get("api_key") or "").strip() or load_config().get("api_key", "")
    base_url = (data.get("base_url") or "").strip() or DEFAULT_BASE_URL
    model = (data.get("model") or "").strip() or DEFAULT_MODEL
    if not key:
        return jsonify({"ok": False, "message": "未填写 API Key"})
    try:
        client = OpenAI(api_key=key, base_url=base_url,
                        default_headers={"User-Agent": "Mozilla/5.0"})
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "回复OK"}],
            max_tokens=10)
        return jsonify({"ok": True, "message": (resp.choices[0].message.content or "")})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/models", methods=["POST"])
def api_models():
    data = request.get_json(silent=True) or {}
    key = (data.get("api_key") or "").strip() or load_config().get("api_key", "")
    base_url = ((data.get("base_url") or "").strip() or DEFAULT_BASE_URL).rstrip("/")
    if not base_url:
        return jsonify({"ok": False, "message": "未填写 Base URL"})
    try:
        req = urllib.request.Request(base_url + "/models")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", "Mozilla/5.0")
        if key:
            req.add_header("Authorization", f"Bearer {key}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if isinstance(data, dict) and "data" in data:
            ids = sorted(m["id"] for m in data["data"])
        elif isinstance(data, list):
            ids = sorted(m["id"] if isinstance(m, dict) else str(m) for m in data)
        else:
            raise ValueError("未识别的返回格式")
        return jsonify({"ok": True, "models": ids})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


# ═══════════ API: 任务控制 ═══════════

@app.route("/api/start", methods=["POST"])
def api_start():
    global _worker
    ok, reason = _can_start()
    if not ok:
        return jsonify({"ok": False, "message": reason}), 409
    data = request.get_json(silent=True) or {}
    theme = (data.get("theme") or "").strip()
    if not theme:
        return jsonify({"ok": False, "message": "请输入小说主题"})

    cfg = load_config()
    key = (data.get("api_key") or "").strip() or cfg.get("api_key", "")
    base_url = (data.get("base_url") or "").strip() or cfg.get("base_url", DEFAULT_BASE_URL)
    model = (data.get("model") or "").strip() or cfg.get("model", DEFAULT_MODEL)
    if not key:
        return jsonify({"ok": False, "message": "请填写 API Key（建议写入环境变量 NOVEL_AI_API_KEY）"})

    # 字数参数
    cc, wpc = None, None
    try:
        v = (data.get("chapters_count") or "").strip()
        if v:
            cc = int(v)
            if cc < 1:
                return jsonify({"ok": False, "message": "章节数必须大于0"})
    except ValueError:
        return jsonify({"ok": False, "message": "章节数必须是整数"})
    try:
        v = (data.get("words_per_chapter") or "").strip()
        if v:
            wpc = int(v)
            if wpc < 100:
                return jsonify({"ok": False, "message": "每章字数至少100字"})
    except ValueError:
        return jsonify({"ok": False, "message": "每章字数必须是整数"})

    # 保存界面偏好（不含 key）
    save_config({**cfg, "theme": theme,
                 "requirements": (data.get("requirements") or "").strip(),
                 "base_url": base_url, "model": model,
                 "chapters_count": str(cc) if cc else "",
                 "words_per_chapter": str(wpc) if wpc else ""})

    with _worker_lock:
        _worker = GeneratorWorker(
            theme=theme,
            requirements=(data.get("requirements") or "").strip(),
            api_key=key,
            base_url=base_url,
            model=model,
            chapters_count=cc,
            words_per_chapter=wpc,
        )
        _worker.start()
    return jsonify({"ok": True, "message": "任务已启动（后台运行）"})


@app.route("/api/resume", methods=["POST"])
def api_resume():
    global _worker
    ok, reason = _can_start()
    if not ok:
        return jsonify({"ok": False, "message": reason}), 409
    st = load_resume_state()
    if not st:
        return jsonify({"ok": False, "message": "没有可续传的断点"})

    cfg = load_config()
    data = request.get_json(silent=True) or {}
    key = (data.get("api_key") or "").strip() or cfg.get("api_key", "")
    base_url = (data.get("base_url") or "").strip() or st.get("config", {}).get("base_url", cfg.get("base_url", DEFAULT_BASE_URL))
    model = (data.get("model") or "").strip() or st.get("config", {}).get("model", cfg.get("model", DEFAULT_MODEL))
    if not key:
        return jsonify({"ok": False, "message": "请填写 API Key（建议写入环境变量 NOVEL_AI_API_KEY）"})

    # 续传 stage
    stage = st.get("stage", "layer1")
    if stage == "done":
        stage = "layer1"

    with _worker_lock:
        _worker = GeneratorWorker(
            theme=st.get("theme", ""),
            requirements=st.get("requirements", ""),
            api_key=key,
            base_url=base_url,
            model=model,
            resume=True,
        )
        _worker.start()
    return jsonify({"ok": True, "message": f"已从「{stage}」阶段续传（后台运行）"})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    w = _current_worker()
    if w:
        manager.set_stopping()
        w.stop()
        return jsonify({"ok": True, "message": "停止请求已发出，进行中的 API 调用返回后即中止"})
    return jsonify({"ok": False, "message": "当前无运行任务"})


@app.route("/api/confirm", methods=["POST"])
def api_confirm():
    data = request.get_json(silent=True) or {}
    action = (data.get("action") or "confirm").strip()
    edited = data.get("edited")  # 可 None
    ok = manager.answer_confirm(action, edited)
    if not ok:
        return jsonify({"ok": False, "message": "没有待确认项或已超时/已应答"})
    return jsonify({"ok": True, "message": "已提交"})


# ═══════════ API: 拒答待处理 ═══════════
# 纯后台运行（无前端在线）时，被模型拒答的部分会登记在这里：
# 该部分留空 + 记录「实际发送内容」与「拒答原文」，用户上线后逐项处理。

@app.route("/api/refusals")
def api_refusals():
    """拒答待处理列表（含发送内容/拒答原文全文，按需拉取避免状态帧过大）。"""
    return jsonify({"items": manager.get_refusal_items()})


@app.route("/api/refusal/resolve", methods=["POST"])
def api_refusal_resolve():
    """处理一条拒答项：resubmit=用（可修改的）发送内容重新提交补写；skip=跳过并移除（保持空白）。"""
    global _worker
    data = request.get_json(silent=True) or {}
    rid = (data.get("id") or "").strip()
    action = (data.get("action") or "").strip()
    item = manager.find_refusal(rid)
    if not item:
        return jsonify({"ok": False, "message": "该项不存在或已被处理"}), 404

    if action == "skip":
        manager.remove_refusal(rid)
        manager._log(f"⏭️ 已跳过拒答项：{item.get('label','')}（该部分保持空白）")
        return jsonify({"ok": True, "message": "已跳过并移除（该部分保持空白）"})

    if action != "resubmit":
        return jsonify({"ok": False, "message": "未知操作"}), 400

    if item.get("stage") not in ("layer4", "layer1"):
        return jsonify({"ok": False,
                        "message": "该阶段（大纲/场景）不支持单独补写，请用「继续上次」重跑"}), 400

    ok, reason = _can_start()
    if not ok:
        return jsonify({"ok": False, "message": reason}), 409

    cfg = load_config()
    key = (data.get("api_key") or "").strip() or cfg.get("api_key", "")
    if not key:
        return jsonify({"ok": False, "message": "请填写 API Key（建议写入环境变量 NOVEL_AI_API_KEY）"})
    base_url = (data.get("base_url") or "").strip() or cfg.get("base_url", DEFAULT_BASE_URL)
    model = (data.get("model") or "").strip() or cfg.get("model", DEFAULT_MODEL)
    # 发送内容：前端提交的编辑结果优先，其次用当初实际发出去的内容
    edited = (data.get("edited") or "").strip() or item.get("prompt", "")

    with _worker_lock:
        _worker = RefusalResolver(item, edited, key, base_url, model)
        _worker.start()
    return jsonify({"ok": True, "message": "已开始补写（后台运行，可用「停止」中止）"})


# ═══════════ API: 状态/内容/下载 ═══════════

@app.route("/api/status")
def api_status():
    since = request.args.get("since", 0, type=int)
    with_content = request.args.get("content", "0") == "1"
    return jsonify(manager.get_status(since_seq=since, with_content=with_content))


@app.route("/api/logs")
def api_logs():
    since = request.args.get("since", 0, type=int)
    return jsonify(manager.get_logs_since(since))


@app.route("/api/files")
def api_files():
    """列出数据目录里的生成成品（标题_时间戳.txt / novel_*.txt / log_*.txt），供下载。"""
    files = []
    for fn in sorted(os.listdir(DATA_DIR)):
        if is_output_name(fn):
            p = os.path.join(DATA_DIR, fn)
            files.append({"name": fn, "size": os.path.getsize(p),
                          "mtime": os.path.getmtime(p)})
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return jsonify({"files": files})


@app.route("/api/download/<path:filename>")
def api_download(filename: str):
    # 只允许生成的成品/日志（中文标题_时间戳.txt 或 novel_/log_ 前缀），防路径穿越
    name = os.path.basename(filename)
    if not is_output_name(name):
        return "forbidden", 403
    p = os.path.join(DATA_DIR, name)
    if not os.path.exists(p):
        return "not found", 404
    # 双保险：确认解析后仍在数据目录内（防符号链接逃逸）
    if not os.path.realpath(p).startswith(os.path.realpath(DATA_DIR) + os.sep):
        return "forbidden", 403
    return send_file(p, as_attachment=True, download_name=name)


# ═══════════ 启动 ═══════════

def main():
    ap = argparse.ArgumentParser(description="novel-webui 服务")
    ap.add_argument("--host", default=os.environ.get("NOVEL_WEBUI_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("NOVEL_WEBUI_PORT", "8000")))
    ap.add_argument("--confirm", type=int, default=DEFAULT_CONFIRM_SECONDS,
                    help="各层确认倒计时秒数（默认5）")
    ap.add_argument("--debug", action="store_true", help="Flask debug")
    args = ap.parse_args()

    manager._confirm_seconds = args.confirm
    print(f"novel-webui 启动: http://{args.host}:{args.port}  确认倒计时={args.confirm}s")
    if not auth_has_users():
        tok = auth_setup_token()
        print("⚠ 首次启动：请先打开页面完成管理员账号初始化（之后每次访问都需登录）。")
        print(f"🔐 初始化口令（setup token）：{tok}")
        print("  该口令仅首次初始化需要，创建管理员后自动失效；也可在数据目录 .setup_token 中查看。")
        print("  忘记密码：删除数据目录 auth_users.json 后重启，可重新初始化（会生成新口令）。")
    print("提示：绑定 0.0.0.0 即可局域网/远程访问；API Key 建议用环境变量 NOVEL_AI_API_KEY，避免明文落盘。")
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True,
            use_reloader=False)


if __name__ == "__main__":
    main()
