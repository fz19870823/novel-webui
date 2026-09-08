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
import json
import urllib.request
import urllib.error

from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_sock import Sock

from config import (load_config, save_config, mask_api_key, DATA_DIR,
                    DEFAULT_BASE_URL, DEFAULT_MODEL, DEFAULT_CONFIRM_SECONDS)
from state import load_resume_state
from controller import manager, LOG_RETURN
from worker import GeneratorWorker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_NAMES = ["novel_*.txt", "log_*.txt"]

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024  # 2MB，仅 JSON 配置
sock = Sock(app)

# 全局：当前 worker（同一时刻只允许一个生成任务）
_worker = None
_worker_lock = threading.Lock()

# ── WebSocket 订阅端（/ws）──
WS_HEARTBEAT = 15          # 秒：空闲时发送 ping 保活（反代 read_timeout 默认常为 60s）
WS_RECEIVE_TIMEOUT = 30    # 秒：服务端 receive 阻塞上限
_ws_clients = set()        # WsClient 集合
_ws_clients_lock = threading.Lock()


class WsClient:
    """单条 /ws 连接。记录已推送的 rev/日志 seq/正文长度，用于增量 diff。"""

    def __init__(self, ws):
        self.ws = ws
        self.rev = -1      # 已推状态 rev（-1 = 尚未推 init）
        self.seq = 0       # 已推日志 seq
        self.clen = -1     # 已推正文尾长度（-1 = 尚未推）
        self.alive = True

    def close(self):
        self.alive = False
        with manager._cond:
            manager._cond.notify_all()


def _ws_send(cli: WsClient, obj: dict) -> bool:
    """发送一帧；失败(连接已断)返回 False。send 线程安全（simple-websocket）。"""
    try:
        cli.ws.send(json.dumps(obj, ensure_ascii=False))
        return True
    except Exception:
        return False


def _ws_tick(cli: WsClient) -> bool:
    """单次 diff 计算与推送。返回 False 表示连接已断，应结束订阅线程。"""
    st = manager.get_status()
    rev, seq = st.pop("rev"), st["log_seq"]

    if cli.rev < 0:
        # 首次连接：全量 init（状态 + 最近日志 + 正文尾），不等心跳周期
        logs = manager.get_logs_since(0)["logs"][-LOG_RETURN:]
        content = manager.get_content_tail()
        cli.rev, cli.seq, cli.clen = rev, seq, len(content)
        return _ws_send(cli, {"type": "init", "status": st,
                              "logs": [m for _, m in logs],
                              "content": content})

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
    clen = len(manager.get_content_tail())
    if clen != cli.clen:
        cli.clen = clen
        frame["content"] = manager.get_content_tail()
        changed = True
    if changed:
        return _ws_send(cli, frame)
    # 无变化：心跳保活（反代 / 中间层空闲断连）
    return _ws_send(cli, {"type": "ping"})


def _ws_sender(cli: WsClient):
    """订阅线程：先立即推一轮(含 init)，此后等变更通知 → 增量 diff；空闲发心跳。"""
    try:
        while cli.alive:
            if not _ws_tick(cli):
                return
            with manager._cond:
                manager._cond.wait(timeout=WS_HEARTBEAT)
    finally:
        cli.close()


@sock.route("/ws")
def ws_handler(ws):
    """/ws：浏览器订阅端。连接建立即推 init，此后 manager 变更推送 diff。"""
    cli = WsClient(ws)
    with _ws_clients_lock:
        _ws_clients.add(cli)
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
        with _ws_clients_lock:
            _ws_clients.discard(cli)


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


# ═══════════ 前端静态 ═══════════

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "static/index.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(os.path.join(BASE_DIR, "static"), filename)


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
    """列出数据目录里生成的 novel_*.txt 成品，供下载。"""
    files = []
    for fn in sorted(os.listdir(DATA_DIR)):
        if fn.startswith("novel_") and fn.endswith(".txt"):
            p = os.path.join(DATA_DIR, fn)
            files.append({"name": fn, "size": os.path.getsize(p),
                          "mtime": os.path.getmtime(p)})
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return jsonify({"files": files})


@app.route("/api/download/<path:filename>")
def api_download(filename: str):
    # 只允许 novel_*.txt / log_*.txt 类产物，防路径穿越
    name = os.path.basename(filename)
    if not (name.startswith("novel_") or name.startswith("log_")) or not name.endswith(".txt"):
        return "forbidden", 403
    p = os.path.join(DATA_DIR, name)
    if not os.path.exists(p):
        return "not found", 404
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
    print("提示：绑定 0.0.0.0 即可局域网/远程访问；API Key 建议用环境变量 NOVEL_AI_API_KEY，避免明文落盘。")
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True,
            use_reloader=False)


if __name__ == "__main__":
    main()
