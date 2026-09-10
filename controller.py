"""
novel-webui 任务控制中枢
把阻塞式、回调式的 NovelGenerator 桥接成线程安全、可被 HTTP 轮询的状态机。

设计要点（远程操作，关键语义）：
- 生成在后台线程跑，不依赖前端连接。提交任务后即使没有任何浏览器打开，任务也会完整跑完。
- 各层"用户确认"由引擎线程发起 request_confirm()，此调用会阻塞等一个结果；
  结果来源二选一：
    1. 倒计时结束（默认自动确认原文），保证无前端也能推进；
    2. 前端在截止前通过 /api/confirm 提交 确认(可带修改)/重新生成/取消。
- 日志/进度/实时内容写入线程安全的快照与环形缓冲，前端用 /api/status 轮询拉取，
  天然支持断线重连、多标签页。拉取时附带 seq，超过容量即截断丢弃最旧日志。
"""

import re
import threading
import time
from collections import deque
from datetime import datetime

from config import DEFAULT_CONFIRM_SECONDS
from state import (save_resume_state, load_resume_state, clear_resume_state,
                   load_refusals, save_refusals, STATE_FILE)

# 引擎确认回调的保留结果，与 engine.py 保持一致
RESULT_CANCEL = "__CANCEL__"
RESULT_REGENERATE = "__REGENERATE__"
RESULT_SKIP = "__SKIP__"          # 用户主动「跳过」：该部分留空，不登记待处理
RESULT_TIMEOUT_SKIP = "__SKIP_TO__"  # 倒计时超时自动跳过：留空 + 登记待处理

LOG_CAPACITY = 400          # 环形日志容量
LOG_RETURN = 120            # 每次推送最多返回的日志条数
CONTENT_TAIL = 2000         # 推送正文时只取尾部这么多字（全文仍在内存中）

# 引擎的 log_callback 已自带 "[HH:MM:SS] " 前缀；若再补一次会出现双时间戳。
_TS_PREFIX_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\]\s")


class JobManager:
    """单例。管理当前生成任务 + 待确认项。所有读写都在锁内完成。"""

    def __init__(self, confirm_seconds: int = None):
        self._lock = threading.RLock()
        self._log_buf = deque(maxlen=LOG_CAPACITY)
        self._log_seq = 0

        # 变更通知（WebSocket 订阅）：_rev 在状态变更时自增，_cond 唤醒订阅线程
        self._rev = 0
        self._cond = threading.Condition()

        # 当前任务快照
        self.task = {
            "running": False,
            "stopping": False,
            "stage": "",            # layer1..layer4 / done / idle
            "stage_name": "就绪",
            "progress": 0,
            "progress_text": "就绪",
            "latest_content": "",   # 实时流式正文（引擎每次传全文，这里存全文）
            "last_error": "",
            "finished": False,      # 本次 run 是否已结束（成功/失败/取消）
            "novel_title": "",
            "result_file": "",      # 完成后落盘的文件名
            "has_resume": False,    # 是否存在可续传断点
            "call_count": 0,
        }

        # 待确认项（引擎线程阻塞中）
        self.confirm = None          # dict 或 None
        self._confirm_seconds = confirm_seconds if confirm_seconds else DEFAULT_CONFIRM_SECONDS

        # WS 订阅端计数：只有真的有前端在线，拒答才弹交互窗；
        # 纯后台运行（无浏览器打开）时不打扰用户，改为登记待处理列表。
        self._ws_count = 0

        # 拒答待处理列表（持久化在数据目录，重启不丢）
        self.refusals = load_refusals()

    # ── 工具 ──
    def _now(self) -> float:
        return time.time()

    def _bump(self):
        """状态/日志/正文/确认项变更后调用：唤醒 /ws 订阅线程。"""
        with self._lock:
            self._rev += 1
        with self._cond:
            self._cond.notify_all()

    def _log(self, message: str):
        """内部日志，入环形缓冲。

        引擎的 log_callback 传入的行已带 "[HH:MM:SS] " 前缀（engine._log 自己加的），
        此处不再重复补时间戳，否则前端会看到 "[09:28:09] [10:00:00] xxx"。
        """
        message = "" if message is None else str(message)
        line = message if _TS_PREFIX_RE.match(message) else \
            f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        with self._lock:
            self._log_seq += 1
            self._log_buf.append((self._log_seq, line))
        self._bump()

    # ── 引擎回调（在引擎线程内被调用）──

    def cb_log(self, message: str):
        self._log(message)

    def cb_progress(self, value: int, text: str):
        with self._lock:
            self.task["progress"] = value
            self.task["progress_text"] = text
        self._bump()

    def cb_content(self, content: str):
        # 引擎的 content_callback 每次传累积后的全文，这里存全文；返回时截尾
        with self._lock:
            self.task["latest_content"] = content
        self._bump()

    def cb_state(self, state: dict):
        save_resume_state(state)
        with self._lock:
            self.task["has_resume"] = True
            self.task["call_count"] = state.get("call_count", self.task["call_count"])
            self.task["stage"] = state.get("stage", self.task["stage"])
            self.task["stage_name"] = _STAGE_NAMES.get(state.get("stage"), self.task["stage_name"])
        self._bump()

    def cb_confirm(self, title: str, content: str, prompt: str, kind: str = "normal") -> str:
        """引擎线程在此阻塞，直到倒计时结束或前端应答。返回引擎要的结果字符串。

        kind="refusal" 时语义不同：内容是被拒的「实际发送内容」（可编辑后重发），
        倒计时结束**不**自动重发（否则会拿同样的内容无限撞拒答），而是跳过留空；
        引擎收到 RESULT_TIMEOUT_SKIP 后会把这部分登记为待处理项。
        """
        pc = {
            "id": int(self._now() * 1000),
            "title": title,
            "prompt": prompt,
            "content": content,
            "original": content,
            "kind": kind,
            "created": self._now(),
            "deadline": self._now() + self._confirm_seconds,
            "duration": self._confirm_seconds,
            "status": "pending",     # pending | answered | timeout
            "result": None,          # 前端提交的最终结果字符串
            "action": None,          # confirm | regenerate | cancel | skip
        }
        with self._lock:
            self.confirm = pc
        if kind == "refusal":
            self._log(f"⏸️ 等待用户处理拒答：{title}（{self._confirm_seconds}秒后自动跳过并登记待处理）")
        else:
            self._log(f"⏸️ 等待确认：{title}（{self._confirm_seconds}秒后自动确认原文）")

        # 阻塞等待：结果就绪(有锁里 result)或超时
        while True:
            with self._lock:
                if pc["result"] is not None:
                    ans = pc["result"]
                    self.confirm = None
                    return ans
                timeout = pc["deadline"] - self._now()
            if timeout <= 0:
                if pc["kind"] == "refusal":
                    # 倒计时结束 → 跳过留空（绝不原样重发），登记交给引擎侧
                    with self._lock:
                        if pc["result"] is None:
                            pc["status"] = "timeout"
                            pc["action"] = "skip"
                            pc["result"] = RESULT_TIMEOUT_SKIP
                            self.confirm = None
                    self._log(f"⏰ {title} 等待超时，自动跳过（该部分留空并登记待处理）")
                    return RESULT_TIMEOUT_SKIP
                # 普通确认：倒计时结束 → 自动确认原文
                with self._lock:
                    if pc["result"] is None:
                        pc["status"] = "timeout"
                        pc["action"] = "confirm"
                        pc["result"] = pc["original"]
                        self.confirm = None
                self._log(f"⏰ {title} 等待超时，自动确认原文")
                return pc["original"]
            time.sleep(0.2)

    # ── 供 Flask 调用 ──

    def answer_confirm(self, action: str, edited: str = None) -> bool:
        """前端应答当前待确认项。action: confirm/regenerate/cancel/skip。"""
        with self._lock:
            pc = self.confirm
            if not pc or pc["status"] != "pending":
                return False
            if self._now() > pc["deadline"]:
                return False
            if action == "confirm":
                final = (edited if edited is not None and edited.strip()
                         else pc["original"]).rstrip("\n")
                pc["result"] = final
                pc["action"] = "confirm"
            elif action == "regenerate":
                pc["result"] = RESULT_REGENERATE
                pc["action"] = "regenerate"
            elif action == "cancel":
                pc["result"] = RESULT_CANCEL
                pc["action"] = "cancel"
            elif action == "skip":
                pc["result"] = RESULT_SKIP
                pc["action"] = "skip"
            else:
                return False
            pc["status"] = "answered"
            # 注意：不在此清 self.confirm，由 cb_confirm 取走后清；先标记避免重复应答
        self._log(f"✅ 已接收前端应答：{action}")
        return True

    def get_confirm_poll(self) -> dict | None:
        """返回给前端的待确认项视图（含全文，前端要展示编辑用）。"""
        with self._lock:
            pc = self.confirm
            if not pc or pc["status"] != "pending":
                return None
            return {
                "id": pc["id"],
                "title": pc["title"],
                "prompt": pc["prompt"],
                "content": pc["content"],
                "kind": pc.get("kind", "normal"),
                "created": pc["created"],
                "deadline": pc["deadline"],
                "remaining": max(0.0, pc["deadline"] - self._now()),
            }

    # ── WS 在线检测 ──

    def register_ws(self):
        with self._lock:
            self._ws_count += 1

    def unregister_ws(self):
        with self._lock:
            self._ws_count = max(0, self._ws_count - 1)

    def has_ws_clients(self) -> bool:
        """当前是否有前端在线（有 WS 订阅端）。引擎据此决定「实时弹窗」还是「登记待处理」。"""
        with self._lock:
            return self._ws_count > 0

    # ── 拒答待处理列表 ──

    def add_refusal(self, stage: str, label: str, chapters, prompt: str,
                    refusal_text: str) -> dict:
        """登记一条拒答待处理项（纯后台运行时调用），并持久化。"""
        rec = {
            "id": f"rf_{int(self._now() * 1000)}",
            "stage": stage or "",
            "label": label or "",
            "chapters": list(chapters or []),
            "prompt": prompt or "",
            "refusal_text": refusal_text or "",
            "created": self._now(),
        }
        with self._lock:
            self.refusals.append(rec)
            snapshot = list(self.refusals)
        save_refusals(snapshot)
        self._log(f"📌 已登记拒答待处理项：{rec['label']}（当前共 {len(snapshot)} 项，可在左侧「拒答待处理」中逐项处理）")
        self._bump()
        return rec

    def get_refusal_items(self) -> list:
        """完整项（含发送内容/拒答原文），供 /api/refusals。"""
        with self._lock:
            return [dict(r) for r in self.refusals]

    def get_refusal_summary(self) -> list:
        """精简项列表（不含大段文本），随状态帧下发。"""
        with self._lock:
            return [{"id": r.get("id", ""), "stage": r.get("stage", ""),
                     "label": r.get("label", ""), "chapters": r.get("chapters", []),
                     "created": r.get("created", 0)} for r in self.refusals]

    def find_refusal(self, rid: str) -> dict | None:
        with self._lock:
            for r in self.refusals:
                if r.get("id") == rid:
                    return dict(r)
        return None

    def remove_refusal(self, rid: str) -> bool:
        with self._lock:
            before = len(self.refusals)
            self.refusals = [r for r in self.refusals if r.get("id") != rid]
            changed = len(self.refusals) != before
            snapshot = list(self.refusals)
        if changed:
            save_refusals(snapshot)
            self._bump()
        return changed

    # ── 日志 / 状态拉取 ──

    def get_logs_since(self, since_seq: int) -> dict:
        with self._lock:
            items = [(s, m) for s, m in self._log_buf if s > since_seq]
            return {"seq": self._log_seq, "logs": items}

    def get_content_tail(self, length: int = CONTENT_TAIL) -> str:
        """返回实时正文尾部（WebSocket 订阅用，避免整段拷贝全量）。"""
        with self._lock:
            return self.task["latest_content"][-length:]

    def get_content_len(self) -> int:
        """返回实时正文的**全文**长度。

        变更检测必须用全文长度：尾部截断后长度封顶在 CONTENT_TAIL，
        一旦正文超过该上限，用 len(get_content_tail()) 判断「是否变化」
        会永远判定为无变化 → 前端实时正文在 2000 字后冻结（已修复的 bug）。
        """
        with self._lock:
            return len(self.task["latest_content"])

    def get_status(self, since_seq: int = 0, with_content: bool = False) -> dict:
        with self._lock:
            t = self.task
            status = {
                "rev": self._rev,
                "running": t["running"],
                "stopping": t["stopping"],
                "finished": t["finished"],
                "stage": t["stage"],
                "stage_name": t["stage_name"],
                "progress": t["progress"],
                "progress_text": t["progress_text"],
                "last_error": t["last_error"],
                "novel_title": t["novel_title"],
                "result_file": t["result_file"],
                "has_resume": t["has_resume"],
                "call_count": t["call_count"],
                "log_seq": self._log_seq,
                "confirm": self.get_confirm_poll(),
                "refusals": self.get_refusal_summary(),
            }
            if with_content:
                status["content_tail"] = t["latest_content"][-CONTENT_TAIL:]
            return status

    def set_running(self, flag: bool, stage: str = "", stage_name: str = ""):
        with self._lock:
            self.task["running"] = flag
            self.task["stopping"] = False
            self.task["finished"] = False if flag else self.task["finished"]
            self.task["last_error"] = ""
            if stage:
                self.task["stage"] = stage
                self.task["stage_name"] = stage_name or _STAGE_NAMES.get(stage, stage)
        self._bump()

    def set_stopping(self):
        with self._lock:
            self.task["stopping"] = True
        self._bump()

    def mark_done(self, novel_title: str, result_file: str):
        with self._lock:
            t = self.task
            t["running"] = False
            t["stopping"] = False
            t["finished"] = True
            t["stage"] = "done"
            t["stage_name"] = "已完成"
            t["progress"] = 100
            t["novel_title"] = novel_title
            t["result_file"] = result_file
            t["has_resume"] = False
            if result_file:
                clear_resume_state()
        self._bump()

    def mark_error(self, message: str):
        with self._lock:
            t = self.task
            t["running"] = False
            t["stopping"] = False
            t["finished"] = True
            t["last_error"] = message
            t["stage_name"] = "出错"
        self._log(f"❌ {message}")
        self._bump()

    def mark_stopped(self):
        """用户手动停止 / 后端不可用提前结束（非错误），保留断点供续传。"""
        with self._lock:
            t = self.task
            t["running"] = False
            t["stopping"] = False
            t["finished"] = True
            t["stage_name"] = "已停止（可续传）"
            t["last_error"] = ""
            # 断点标记按磁盘实际情况刷新：否则前端「继续上次」可能读到停止前的旧值
            t["has_resume"] = load_resume_state() is not None
        self._log("🛑 已手动停止，进度已保留可续传")
        self._bump()

    def mark_incomplete(self, missing: list, novel_title: str = "", result_file: str = ""):
        """任务结束但存在未生成（拒答/失败留空）的章节：不算完成。

        与 mark_done 的关键差别：
        - 不清断点（has_resume=True），用户可续传补写；
        - 进度停 99%、阶段名显式标注缺章，前端不会显示「✅ 完成」。
        """
        with self._lock:
            t = self.task
            t["running"] = False
            t["stopping"] = False
            t["finished"] = True
            t["stage"] = "layer4"
            t["stage_name"] = f"部分完成（缺{len(missing)}章）"
            t["progress"] = 99
            head = "、".join(f"第{c}章" for c in missing[:8])
            more = f" 等{len(missing)}章" if len(missing) > 8 else ""
            t["progress_text"] = f"未完成：{head}{more}留空，可续传补写"
            t["novel_title"] = novel_title
            t["result_file"] = result_file
            t["has_resume"] = True
        self._log(f"⚠️ 任务未完成：{len(missing)} 章留空 {missing}，断点已保留，可「继续上次」补写")
        self._bump()

    def set_has_resume(self, val: bool):
        with self._lock:
            self.task["has_resume"] = val
        self._bump()

    def reset_before_run(self):
        """新任务/续传开始前的快照清理（不覆盖断点文件本身）。"""
        with self._lock:
            self.task["running"] = True
            self.task["stopping"] = False
            self.task["finished"] = False
            self.task["stage"] = ""
            self.task["stage_name"] = "启动中"
            self.task["last_error"] = ""
            self.task["latest_content"] = ""
            self.task["result_file"] = ""
            self.task["novel_title"] = ""
            self.task["call_count"] = 0
            self.confirm = None
        self._bump()


_STAGE_NAMES = {
    "layer1": "设定圣经",
    "layer2": "章节大纲",
    "layer3": "场景分解",
    "layer4": "批量写作",
    "done": "已完成",
}


# 全局单例（Flask 与 worker 共用）
manager = JobManager()
