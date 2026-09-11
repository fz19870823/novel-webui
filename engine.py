"""
novel-ai 通用小说生成引擎
四层递进管线：设定圣经 → 章节大纲 → 场景分解 → 批量写作

v2.0 优化：
  - Layer 3 确认合并（场景+审查+修订一次确认）
  - Layer 4 自适应批大小（质量稳定自动升级 chunk_size）
  - 自适应轮次跳过（字数达标跳第2轮）
  - 断点保存不存 API Key
  - content_callback 统一流式回调（tkinter/PySide6 通用）
"""

import json
import time
import re
import queue
import threading
from datetime import datetime
from typing import List, Dict, Optional, Callable

from openai import OpenAI

from config import (
    load_config, DEFAULT_BASE_URL, DEFAULT_MODEL,
    DEFAULT_WORDS_PER_CHAPTER, DEFAULT_WORDS_PER_SCENE,
)
from state import save_resume_state, load_resume_state

# ── 常量 ──
OUTLINE_BATCH = 15
CONFIRM_COUNTDOWN = 5
WRITE_ROUNDS = 3
RETRY_MAX = 3
# 场景一致性审查的修订轮次上限：首轮修订后必须做二次校验，未过才再修订一轮
SCENE_REVISE_MAX = 2
STREAM_IDLE_TIMEOUT = 10.0
FIRST_CONTENT_TIMEOUT = 30.0

# 模型拒答的强特征短语（内容质量闸用，命中即判失败）。
# 只保留"非对话语境下几乎必然代表拒答"的组合词，避免误伤正文里自然出现的单字词。
_REFUSAL_PATTERNS = [
    "无法协助", "无法帮助", "不能协助", "不能帮助", "无法完成这个",
    "无法完成创作", "不能生成", "无法生成", "拒绝协助",
    "无法创作", "无法生成此", "无法生成这类",
    "我无法", "我不能", "i cannot", "i can't", "i am unable", "i'm unable",
    "i'm sorry, but", "i am sorry, but", "sorry, i",
    "content policy", "against my", "安全规则禁止", "明令禁止", "内容政策",
    "无法满足这个", "无法满足该", "不合规",
]

# 中文计数：统计 CJK 字 + ASCII 字母单词，与正文长文判定口径一致
_CJK_START, _CJK_END = '\u3400', '\u4dbf'
_CJK_EXT_START, _CJK_EXT_END = '\u4e00', '\u9fff'


def _count_text_len(text: str) -> int:
    """粗略统计内容长度（CJK 字符数 + 非空白 ASCII 单词数）。"""
    if not text:
        return 0
    cjk = sum(1 for c in text if _CJK_START <= c <= _CJK_END or _CJK_EXT_START <= c <= _CJK_EXT_END)
    words = sum(1 for seg in re.split(r'\s+', text) if seg and any(ch.isalpha() for ch in seg))
    return cjk + words


def _looks_like_refusal(text: str) -> bool:
    """判断一段输出是否像模型拒答。

    规则：
    1. 仅在开头 300 字内匹配拒答短语，避免正文中部自然出现的关键词误伤；
    2. 长度很短的文本不在本函数判断（由调用方的 `<40` 阈值统一拦截空泛/半句）。
    """
    if not text:
        return True
    if _count_text_len(text) < 40:
        return False
    head = text.strip()[:300]
    lowered = head.lower()
    for pat in _REFUSAL_PATTERNS:
        if pat in lowered:
            return True
    # 拒答常以"抱歉/对不起/（I'm )sorry"整体开篇
    stripped = head.lstrip(" \n\t#*\"'《》【】")
    return stripped.startswith(("抱歉", "对不起", "sorry", "I'm sorry", "I am sorry"))


# 与 controller.py 保持一致的特殊结果字符串
RESULT_CANCEL = "__CANCEL__"          # 用户取消（中止任务）
RESULT_REGENERATE = "__REGENERATE__"  # 用户要求原样重发


class RefusalSkipped(Exception):
    """模型连续拒答后本次调用无产出（已登记为「拒答待处理」项，该部分留空）。

    layer4 写作阶段捕获后对应章节保留空白（不重试、不填充）；layer1-3 前置阶段
    无法留空继续，向上冒泡使任务中止（断点与拒答记录均已保存）。

    异常自带本次「实际发送内容」与「拒答原文」，供上层记录：
    「重新提交」补写（RefusalResolver）再次被拒时就用它登记新一条待处理。
    """

    def __init__(self, refusal_text: str = "", prompt: str = ""):
        super().__init__("模型拒答")
        self.refusal_text = refusal_text or ""
        self.prompt = prompt or ""


# ==============================
#  NovelGenerator 核心引擎
# ==============================

class NovelGenerator:
    def __init__(self, theme: str, requirements: str, api_key: str, base_url: str, model: str,
                 progress_callback: Callable = None,
                 log_callback: Callable = None,
                 confirm_callback: Callable = None,
                 state_callback: Callable = None,
                 content_callback: Callable = None,
                 refusal_callback: Callable = None,
                 resume_state: Dict = None,
                 chapters_count: int = None,
                 words_per_chapter: int = None,
                 single_chapter_scene: bool = None,
                 single_chapter_write: bool = None):
        self.theme = theme
        self.requirements = requirements
        self.api_key = api_key
        self.base_url = base_url
        self.model = model

        self.progress_callback = progress_callback
        self.log_callback = log_callback
        self.confirm_callback = confirm_callback
        self.state_callback = state_callback
        self.content_callback = content_callback
        # 拒答登记回调：模型拒答时把「实际发送内容 + 拒答原文」存进待处理列表，
        # 由用户在列表里统一处理（不再区分有没有前端在线，也不向前端弹窗）。
        self.refusal_callback = refusal_callback

        # 当前调用上下文（供拒答时标注"哪个阶段/哪几章"）
        self._ctx: Dict = {"stage": "", "chapters": []}

        # 字数 = 章节数 × 每章字数（直接计算，不解析 requirements 文本）
        self.words_per_chapter = words_per_chapter or DEFAULT_WORDS_PER_CHAPTER
        self.words_per_scene = DEFAULT_WORDS_PER_SCENE
        self.chapters_count = chapters_count or max(5, 50000 // self.words_per_chapter)
        self.target_words = self.chapters_count * self.words_per_chapter
        self.scenes_per_chapter = max(3, self.words_per_chapter // self.words_per_scene)

        # 批粒度开关：分解（layer3 场景）与正文（layer4 写作）分开控制，互不影响。
        # 三态：None = 未指定 → 回落到断点里记录的值；显式 True/False 优先。
        # 兼容拆分前的单一开关 single_chapter（当时同时管两处）。
        _rcfg = (resume_state or {}).get("config", {}) or {}
        _legacy = _rcfg.get("single_chapter")
        _legacy = False if _legacy is None else bool(_legacy)
        if single_chapter_scene is None:
            single_chapter_scene = _rcfg.get("single_chapter_scene", _legacy)
        if single_chapter_write is None:
            single_chapter_write = _rcfg.get("single_chapter_write", _legacy)
        self.single_chapter_scene = bool(single_chapter_scene)
        self.single_chapter_write = bool(single_chapter_write)

        # 自适应批大小（正文单章模式下禁用升级，chunk_size 恒为 1）
        self.chunk_size = 1
        self._quality_streak = 0
        self._adaptive_chunk_enabled = not self.single_chapter_write
        self.ROUNDS = WRITE_ROUNDS

        # 场景审查的遗留问题说明（二次校验仍未通过时写入，随确认文案一起展示）
        self._scene_review_notes = ""

        self.client = None
        self.call_count = 0
        self.is_running = True
        self._backend_down = False

        # 存储产出
        self.setting_bible = ""
        self.chapter_outlines: List[str] = []
        self.scenes: List[Dict] = []
        self.scene_reviewed = False
        self.chapters: Dict[int, List[str]] = {}
        self.novel_title = ""

        # 从断点恢复
        self._resume_chapter = 1
        if resume_state:
            self.setting_bible = resume_state.get("setting_bible", "")
            self.chapter_outlines = resume_state.get("chapter_outlines", [])
            self.scenes = resume_state.get("scenes", [])
            self.scene_reviewed = resume_state.get("scene_reviewed", False)
            saved_chapters = resume_state.get("chapters", {})
            self.chapters = {int(k): v for k, v in saved_chapters.items()}
            self.call_count = resume_state.get("call_count", 0)
            self._resume_chapter = resume_state.get("layer4_batch", 0) + 1
            if resume_state.get("chapters_count"):
                self.chapters_count = resume_state["chapters_count"]
                self.scenes_per_chapter = resume_state.get("scenes_per_chapter", 4)
                self.words_per_chapter = resume_state.get("words_per_chapter", DEFAULT_WORDS_PER_CHAPTER)
                self.words_per_scene = resume_state.get("words_per_scene", DEFAULT_WORDS_PER_SCENE)
                self.target_words = resume_state.get("target_words", self.target_words)
            self.novel_title = resume_state.get("novel_title", "")
            # 续传起点取第一个空章：拒答跳过/失败留空的章可在续传时补写，
            # 而不是从最后完成批次之后开始、把空章永久遗留。
            first_empty = next((c for c in range(1, self.chapters_count + 1)
                                if not self.chapters.get(c)), None)
            if first_empty:
                self._resume_chapter = first_empty
            self._log(f"📂 从断点恢复 (已写{sum(len(v) for v in self.chapters.values())}个场景, {self.call_count}次调用)")

        self._init_client()

    # ── 内部工具 ──

    def _log(self, message: str):
        if self.log_callback:
            self.log_callback(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def _update_progress(self, value: int, text: str):
        if self.progress_callback:
            self.progress_callback(value, text)

    def _update_content(self, content: str):
        """统一流式内容回调 —— tkinter/PySide6 通用"""
        if self.content_callback:
            self.content_callback(content)

    def stop(self):
        self.is_running = False
        self._log("⚠️ 正在停止...")

    def _init_client(self):
        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url,
                             default_headers={"User-Agent": "Mozilla/5.0"})

    # ── 断点管理 ──

    def _save_state(self, stage: str, layer4_batch: int = 0):
        """保存当前生成进度到断点文件（不存储 API Key）"""
        if not self.state_callback:
            return
        try:
            chapters_str = {str(k): v for k, v in self.chapters.items()}
            state = {
                "theme": self.theme,
                "requirements": self.requirements,
                "config": {
                    "base_url": self.base_url,
                    "model": self.model,
                    "single_chapter_scene": self.single_chapter_scene,
                    "single_chapter_write": self.single_chapter_write
                },
                "stage": stage,
                "layer4_batch": layer4_batch,
                "setting_bible": self.setting_bible,
                "chapter_outlines": self.chapter_outlines,
                "scenes": self.scenes,
                "scene_reviewed": self.scene_reviewed,
                "chapters": chapters_str,
                "call_count": self.call_count,
                "target_words": self.target_words,
                "novel_title": self.novel_title,
                "chapters_count": self.chapters_count,
                "scenes_per_chapter": self.scenes_per_chapter,
                "words_per_chapter": self.words_per_chapter,
                "words_per_scene": self.words_per_scene
            }
            self.state_callback(state)
        except Exception as e:
            self._log(f"⚠️ 保存断点失败: {e}")

    # ── API 调用 ──

    def call_grok(self, prompt: str, system: str = "", max_tokens: int = 4000,
                  temperature: float = 0.75, show_stream: bool = True) -> str:
        if not self.is_running:
            raise Exception("用户停止生成")

        if system:
            prompt = f"{system}\n\n{prompt}"

        messages = [{"role": "user", "content": prompt}]

        def _is_incomplete_stream_error(err: str) -> bool:
            err_lower = err.lower()
            return (
                "incomplete chunked read" in err_lower
                or "peer closed connection without sending complete message body" in err_lower
                or "connection closed" in err_lower
                or "response ended prematurely" in err_lower
            )

        def _has_excessive_repetition(text: str, min_length: int = 6000,
                                       uniqueness_threshold: float = 0.4,
                                       window: int = 2000) -> bool:
            if len(text) < min_length:
                return False
            recent = text[-window:]
            lines = [l.strip() for l in recent.split('\n')]
            content_lines = []
            for ln in lines:
                if not ln:
                    continue
                if ln in ('{', '}', '[', ']'):
                    continue
                if len(ln) < 10:
                    continue
                if re.match(r'^\s*"', ln) and ':' in ln and len(ln) < 50:
                    continue
                if ln == ',':
                    continue
                content_lines.append(ln)
            if len(content_lines) < 4:
                return False
            unique = len(set(content_lines))
            return unique / len(content_lines) < uniqueness_threshold

        def _has_paragraph_repeat(text: str, min_length: int = 4000, window: int = 500) -> bool:
            """检测成段重复：最后 window 字是否已完整出现在前文中"""
            if len(text) < min_length:
                return False
            tail = text[-window:].strip()
            if len(tail) < 100:
                return False
            head = text[:-window]
            return tail in head

        def iter_stream_with_idle_timeout(stream, idle_timeout: float, first_timeout: float):
            chunk_queue = queue.Queue()
            stop_event = threading.Event()
            first_content_received = False

            def reader():
                try:
                    for chunk in stream:
                        if stop_event.is_set():
                            break
                        chunk_queue.put(("chunk", chunk))
                    chunk_queue.put(("done", None))
                except Exception as exc:
                    chunk_queue.put(("error", exc))

            thread = threading.Thread(target=reader, daemon=True)
            thread.start()

            while True:
                if not self.is_running:
                    stop_event.set()
                    raise Exception("用户停止生成")
                try:
                    if first_content_received:
                        kind, payload = chunk_queue.get(timeout=idle_timeout)
                    else:
                        kind, payload = chunk_queue.get(timeout=first_timeout)
                except queue.Empty:
                    if first_content_received:
                        stop_event.set()
                        raise TimeoutError(f"流式输出连续{idle_timeout:g}秒无新内容")
                    stop_event.set()
                    raise TimeoutError(f"首个流式文本等待超过{first_timeout:g}秒")
                if kind == "chunk":
                    if getattr(payload.choices[0].delta, "content", None):
                        first_content_received = True
                    yield payload
                elif kind == "done":
                    return
                elif kind == "error":
                    raise payload

        for attempt in range(RETRY_MAX):
            if not self.is_running:
                raise Exception("用户停止生成")
            content = ""
            try:
                stream = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=90,
                    stream=True
                )

                for chunk in iter_stream_with_idle_timeout(stream, STREAM_IDLE_TIMEOUT, FIRST_CONTENT_TIMEOUT):
                    if not self.is_running:
                        raise Exception("用户停止生成")
                    if chunk.choices[0].delta.content:
                        content += chunk.choices[0].delta.content
                        if len(content) > 6000 and _has_excessive_repetition(content):
                            raise Exception(f"检测到大量重复内容（已输出{len(content)}字，最近2000字内容行重复率>60%），自动中断")
                        if len(content) > 4000 and _has_paragraph_repeat(content):
                            raise Exception(f"检测到成段重复（已输出{len(content)}字，最后500字与前文重复），自动中断重试")
                        if show_stream:
                            self._update_content(content)

                self.call_count += 1
                if not content:
                    self._log(f"⚠️ API返回空内容 ({attempt+1}/{RETRY_MAX})")
                    if attempt < RETRY_MAX - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        raise Exception("API连续3次返回空内容")

                # 全英文检测（改进版：只统计字母和中文字符）
                if len(content) > 200:
                    alpha_cjk = sum(1 for c in content
                                    if c.isascii() and c.isalpha()
                                    or '\u4e00' <= c <= '\u9fff'
                                    or '\u3400' <= c <= '\u4dbf')
                    if alpha_cjk > 0:
                        cjk = sum(1 for c in content if '\u4e00' <= c <= '\u9fff' or '\u3400' <= c <= '\u4dbf')
                        if cjk / alpha_cjk < 0.1:
                            raise Exception(f"检测到全英文内容（中文占有效字符仅{cjk/alpha_cjk:.1%}），重试")

                # 质量闸（成功路径）：模型"完整输出但整段为拒答"也算失败，防止拒答入库。
                # 注意：这里不用 `<40` 过短规则 —— 短输出对书名/测试等小调用是合法的；
                # 过短拦截只保留在下方断流/超时异常分支。
                if _looks_like_refusal(content):
                    self._log(f"⚠️ 返回内容疑似模型拒答({_count_text_len(content)}字)，判定失败 ({attempt+1}/{RETRY_MAX})")
                    if attempt < RETRY_MAX - 1:
                        time.sleep(2 ** attempt)
                        continue

                    # ── 自动重试 3 次仍拒答：一律登记到「拒答待处理」列表并留空 ──
                    # 不再区分有没有前端在线、也不向前端弹交互窗：统一登记，
                    # 用户上线后在列表里查看发送内容与拒答原文，改后重新提交或忽略删除。
                    self._log("📌 模型连续拒答，登记为拒答待处理项，本次调用留空")
                    self._record_refusal(prompt, content)
                    raise RefusalSkipped(content, prompt)

                self._log(f"📊 累计调用: {self.call_count} 次 (流式 {len(content)} 字)")
                return content

            except RefusalSkipped:
                # 拒答留空信号必须原样冒泡：绝不能被下面的通用重试逻辑当成
                # "调用失败" 再拿同样的内容去撞一次拒答。
                raise
            except Exception as e:
                if not self.is_running:
                    raise Exception("用户停止生成")
                err_str = str(e)

                if content and (_is_incomplete_stream_error(err_str) or isinstance(e, TimeoutError)):
                    # 质量闸：断流/超时收到的部分内容若疑似"拒答/空泛"，不算完成，走重试
                    if _looks_like_refusal(content) or _count_text_len(content) < 40:
                        self._log(f"⚠️ 流式中断但内容疑似拒答或过短({_count_text_len(content)}字)，判定失败重试")
                        if attempt < RETRY_MAX - 1:
                            time.sleep(2 ** attempt)
                            continue
                        raise Exception(f"连续{RETRY_MAX}次中断且内容不合格: {err_str[:120]}")
                    self.call_count += 1
                    if isinstance(e, TimeoutError):
                        self._log(f"⚠️ 流式输出连续{STREAM_IDLE_TIMEOUT:g}秒无新内容，已收到 {len(content)} 字，按已完成处理")
                    else:
                        self._log(f"⚠️ 流式连接收尾异常，但已收到 {len(content)} 字，按已完成处理: {err_str[:120]}")
                    self._log(f"📊 累计调用: {self.call_count} 次 (流式 {len(content)} 字)")
                    return content

                if isinstance(e, TimeoutError):
                    err_str = str(e)

                self._log(f"⚠️ 调用失败 ({attempt+1}/{RETRY_MAX}): {err_str[:200]}")

                if "No healthy provider" in err_str or "no healthy provider" in err_str.lower():
                    raise Exception(f"后端服务不可用（恢复可能需数小时），已自动停止: {err_str[:150]}")

                if attempt < RETRY_MAX - 1:
                    if "429" in err_str or "rate_limit" in err_str.lower():
                        wait_sec = 5 * (3 ** attempt)
                        self._log(f"⏳ 限流等待 {wait_sec} 秒...")
                    else:
                        wait_sec = 2 ** attempt
                    time.sleep(wait_sec)
                else:
                    raise
        return ""

    # ── 拒答登记 ──

    _STAGE_CN = {"layer1": "设定圣经", "layer2": "章节大纲", "layer3": "场景分解",
                 "layer4": "正文写作", "title": "小说名"}

    def _refusal_label(self) -> str:
        """拒答项的展示名：「正文写作 · 第3-4章」"""
        stage = self._ctx.get("stage") or ""
        base = self._STAGE_CN.get(stage, stage or "未知阶段")
        chs = self._ctx.get("chapters") or []
        if not chs:
            return base
        if len(chs) == 1:
            return f"{base} · 第{chs[0]}章"
        return f"{base} · 第{chs[0]}-{chs[-1]}章"

    def _record_refusal(self, prompt: str, refusal_text: str):
        """把被拒的部分登记进待处理列表（持久化），用户上线后逐项处理。

        无论有没有前端在线都走这条路：拒答不弹窗、不进流程，只登记 + 本次留空。
        """
        if not self.refusal_callback:
            return
        try:
            self.refusal_callback(self._ctx.get("stage", ""), self._refusal_label(),
                                  list(self._ctx.get("chapters") or []), prompt, refusal_text)
        except Exception as e:
            self._log(f"⚠️ 登记拒答待处理项失败: {e}")

    def _confirm_with_user(self, title: str, content: str, prompt: str = "") -> str:
        """弹出确认窗口，让用户确认或修改内容"""
        if not self.confirm_callback:
            return content

        if not content or not content.strip():
            self._log("⚠️ 生成内容为空，自动重试...")
            return "__REGENERATE__"

        result = self.confirm_callback(title, content, prompt)

        if result == "__CANCEL__":
            self.is_running = False
            raise Exception("用户取消生成")
        elif result == "__REGENERATE__":
            return "__REGENERATE__"
        else:
            return result

    # ═══════════════════════════════
    #  Layer 1: 设定圣经
    # ═══════════════════════════════

    def layer1_setting_bible(self) -> str:
        self._log("\n🏗️ 第1层：生成设定圣经...")
        self._update_progress(5, "正在生成设定圣经...")
        self._ctx = {"stage": "layer1", "chapters": []}

        prompt = f"""
        你是一位专业小说架构师。请根据以下用户输入，生成完整的创作圣经。

        【用户提供的主题】
        {self.theme}

        【用户的具体要求】
        {self.requirements}

        目标总字数：{self.target_words}字（约{self.chapters_count}章）

        请一次性输出以下内容：

        ## 一、核心创意解读
        ## 二、世界观设定
        ## 三、角色档案（主角+3-5个配角+反派）
        ## 四、完整情节大纲（三幕结构）
        ## 五、风格锁定（人称、视角、文风、对话风格）

        注意：这部分是后续所有写作的唯一参考标准，请尽量详细。
        """

        for attempt in range(RETRY_MAX):
            result = self.call_grok(prompt, system="你是专业小说架构师，擅长根据用户需求生成详细设定。", max_tokens=12000, show_stream=True)

            confirmed = self._confirm_with_user(
                "确认设定圣经",
                result,
                "请确认或修改以上设定。这是后续所有写作的唯一参考标准。\n5秒后自动确认。"
            )

            if confirmed == "__REGENERATE__":
                self._log(f"🔄 用户要求重新生成设定（第{attempt+2}次尝试）")
                continue

            self.setting_bible = confirmed
            self._save_state("layer2")
            self._log(f"✅ 设定圣经已确认 ({len(self.setting_bible)}字)")

            # 根据圣经生成小说名
            self._generate_title()
            self._save_state("layer2")

            return confirmed

        raise Exception("设定圣经生成失败，已达到最大重试次数")

    def _sanitize_filename(self, name: str) -> str:
        """清理小说名为合法文件名"""
        # 替换 Windows 非法字符
        for ch in r'\/:*?"<>|':
            name = name.replace(ch, "")
        # 合并多余空格，限制长度
        name = re.sub(r'\s+', ' ', name).strip()
        return name[:80]

    def _generate_title(self):
        """根据设定圣经让 AI 生成小说名"""
        self._log("📖 正在生成小说名...")
        self._ctx = {"stage": "title", "chapters": []}
        prompt = f"""根据以下小说设定，为这部小说取一个吸引人的中文书名。

要求：
- 长度 2-12 个汉字
- 有文学性和辨识度
- 一句话即可，不要加书名号、引号、说明文字

【设定圣经】
{self.setting_bible[:3000]}
"""
        try:
            title = self.call_grok(
                prompt,
                system="你是一位资深出版编辑，擅长为小说命名。只输出书名本身，不超过一行。",
                max_tokens=50,
                temperature=0.8,
                show_stream=False
            )
            title = title.strip().strip('《》"\'').strip()
            title = re.sub(r'^[「【《]*|[」】》]*$', '', title)
            self.novel_title = self._sanitize_filename(title) if title else ""
            if self.novel_title:
                self._log(f"📖 小说名: 《{self.novel_title}》")
            else:
                self._log("⚠️ 小说名生成为空，将使用默认文件名")
        except Exception as e:
            self._log(f"⚠️ 小说名生成失败：{e}，将使用默认文件名")

    # ═══════════════════════════════
    #  Layer 2: 章节大纲
    # ═══════════════════════════════

    def layer2_chapter_outlines(self) -> List[str]:
        self._log("\n📖 第2层：生成章节大纲...")
        self._update_progress(10, "正在生成章节大纲...")

        all_outlines = []
        for batch_start in range(1, self.chapters_count + 1, OUTLINE_BATCH):
            if not self.is_running:
                raise Exception("用户停止生成")
            batch_end = min(batch_start + OUTLINE_BATCH - 1, self.chapters_count)
            self._ctx = {"stage": "layer2", "chapters": list(range(batch_start, batch_end + 1))}
            prev = "\n\n".join(all_outlines[-3:]) if all_outlines else ""
            batch_outlines = self._generate_outlines_batch(batch_start, batch_end, prev)
            all_outlines.extend(batch_outlines)
            self._log(f"📝 第{batch_start}-{batch_end}章大纲完成")

        return self._confirm_outlines(all_outlines)

    def _confirm_outlines(self, outlines: List[str]) -> List[str]:
        formatted = "\n\n".join([f"### {ch}" for ch in outlines])
        confirmed = self._confirm_with_user(
            "确认章节大纲",
            formatted,
            f"请确认或修改以上{len(outlines)}章大纲。\n5秒后自动确认。"
        )
        if confirmed == "__REGENERATE__":
            self._log("🔄 用户要求重新生成大纲")
            return self.layer2_chapter_outlines()

        confirmed_chapters = re.split(r'\n###\s*', confirmed)
        confirmed_chapters = [ch.strip() for ch in confirmed_chapters if ch.strip()]
        self.chapter_outlines = confirmed_chapters[:self.chapters_count]
        self._save_state("layer3")
        self._log(f"✅ 章节大纲已确认，共{len(self.chapter_outlines)}章")
        return self.chapter_outlines

    def _generate_outlines_batch(self, ch_start: int, ch_end: int, prev_context: str) -> List[str]:
        batch_count = ch_end - ch_start + 1
        prompt = f"""
基于以下小说设定，生成第{ch_start}章到第{ch_end}章的详细大纲（共{batch_count}章）。

【小说设定（完整）】
{self.setting_bible}

【前几章大纲参考】
{prev_context or '（首批，无前文）'}

请逐章输出，每章格式如下：

### 第X章《章标题》
- 核心事件：（50字内）
- 冲突点：
- 情绪基调：
- 角色出场：
- 结尾钩子：

从第{ch_start}章开始，到第{ch_end}章结束。
"""
        for attempt in range(RETRY_MAX):
            response = self.call_grok(
                prompt,
                system="输出结构化的章节大纲，注意前后连贯。",
                max_tokens=max(12000, batch_count * 500),
                show_stream=True
            )

            chapters = []
            for sep in [
                r'\n###\s*',
                r'\n##\s*',
                r'\n(?=\*\*###\s*第\d+章)',
                r'\n(?=\*\*##\s*第\d+章)',
                r'\n(?=\*\*第\d+章)',
                r'\n(?=第\d+章)',
            ]:
                parts = re.split(sep, response)
                parts = [p.strip() for p in parts if p.strip()]
                if len(parts) <= 1:
                    continue
                chapters = [p for p in parts if re.match(r'\**(?:###\s*|##\s*)?第\d+章', p)]
                if chapters:
                    break

            chapters = chapters[:batch_count]

            if len(chapters) < batch_count:
                self._log(f"⚠️ 大纲数量不足 ({len(chapters)}/{batch_count})，重试...")
                if attempt < RETRY_MAX - 1:
                    continue

            return chapters

        raise Exception(f"大纲生成失败（{ch_start}-{ch_end}章），已达到最大重试次数")

    # ═══════════════════════════════
    #  Layer 3: 场景分解
    # ═══════════════════════════════

    def layer3_scene_breakdown(self) -> List[Dict]:
        """
        v2.0 优化：场景生成 + AI审查 + 自动修订在后台完成，只向用户展示一次确认。
        用户确认修订后的最终场景（或修改）。
        """
        self._log("\n🔍 第3层：场景分解...")
        self._update_progress(25, "正在分解场景...")

        # 单章处理模式：一次只分解一章，避免 2 章/批时一批出错连带整批重试
        batch_size = 1 if self.single_chapter_scene else 2
        # 断点续传：__init__ 已把断点里保存的场景恢复到 self.scenes。
        # 批次内每章都已有场景的批次视为已完成，直接跳过，只生成缺失批次，
        # 避免停止/续传后把整个场景分解从头重跑。
        all_scenes: List[Dict] = list(self.scenes)
        restored_covered: set = set()
        for _sc in all_scenes:
            try:
                restored_covered.add(int(_sc.get("chapter")))
            except (TypeError, ValueError):
                continue
        if restored_covered:
            self._log(f"📂 断点续传：断点已有 {len(all_scenes)} 个场景，跳过已完成批次")

        chapter_summaries = []
        for i, outline in enumerate(self.chapter_outlines, 1):
            chapter_summaries.append(f"第{i}章: {outline}")

        total_batches = (len(chapter_summaries) + batch_size - 1) // batch_size
        self._log(f"📋 场景分解改为小批生成：共{len(chapter_summaries)}章，每批{batch_size}章（最多约{batch_size * 5}个场景），共{total_batches}批")

        def chapters_covered(scenes: List[Dict], start_ch: int, end_ch: int) -> bool:
            covered = set()
            for scene in scenes:
                try:
                    covered.add(int(scene.get("chapter")))
                except (TypeError, ValueError):
                    continue
            expected = set(range(start_ch, end_ch + 1))
            missing = sorted(expected - covered)
            extra = sorted(covered - expected)
            if missing:
                self._log(f"⚠️ 场景分解批次缺少章节：{missing}")
                return False
            if extra:
                self._log(f"⚠️ 场景分解批次包含范围外章节：{extra}")
                return False
            return True

        for batch_idx in range(total_batches):
            start_index = batch_idx * batch_size
            end_index = min(start_index + batch_size, len(chapter_summaries))
            ch_start = start_index + 1
            ch_end = end_index
            # 该批每章都已有场景（断点中已完成）→ 跳过，避免重复生成
            if all(c in restored_covered for c in range(ch_start, ch_end + 1)):
                self._log(f"📂 断点续传：第{ch_start}-{ch_end}章场景已在断点中，跳过本批")
                continue
            if not self.is_running:
                raise Exception("用户停止生成")
            batch_chapters = chapter_summaries[start_index:end_index]
            batch_scenes_estimate = len(batch_chapters) * 5
            self._ctx = {"stage": "layer3", "chapters": list(range(ch_start, ch_end + 1))}

            prompt = f"""
将第{ch_start}章到第{ch_end}章拆解成具体的写作场景。本批最多约10个场景，请保证每个场景质量，场景描述要足够详细，为后续写作提供充分素材。

【设定摘要】
{self.setting_bible}

【本批章节大纲】
{chr(10).join(batch_chapters)}

输出JSON数组，格式：
[
  {{
    "chapter": {ch_start},
    "scene_id": 1,
    "word_target": {self.words_per_scene},
    "summary": "尽可能详细的场景说明（建议200-400字）：包含关键情节推进、角色动作、对话要点、心理变化、环境氛围、承上启下作用",
    "characters": ["角色名"],
    "emotion": "情绪标签",
    "location": "具体地点及环境特征"
  }}
]

要求：
- 只生成第{ch_start}章到第{ch_end}章，不要生成范围外章节
- 必须覆盖本批每一章，不能只写前几章或最后一章
- 每章必须拆成 2-5 个场景，具体数量由你根据该章情节复杂度自行判断：简单过渡章2个，常规推进章3-4个，高潮/冲突密集章5个
- summary 必须尽可能详细，建议 200-400 字
- 本批最多约10个场景，只输出JSON数组，严禁输出说明文字。
"""

            batch_ok = False
            for attempt in range(RETRY_MAX):
                self._log(f"📋 场景分解：第{batch_idx + 1}/{total_batches}批（第{ch_start}-{ch_end}章），第{attempt + 1}次尝试")
                response = self.call_grok(
                    prompt,
                    system="只输出合法的JSON数组，严禁输出说明文字、Markdown代码块以外的解释。",
                    max_tokens=max(12000, batch_scenes_estimate * 500),
                    show_stream=True
                )

                try:
                    batch_scenes = self._extract_json_array(response)
                    if not isinstance(batch_scenes, list):
                        self._log("⚠️ 场景分解返回值不是 JSON 数组")
                        continue
                    if not self._validate_scenes_json(batch_scenes):
                        self._log("⚠️ 场景结构验证失败，重试...")
                        continue
                    if not chapters_covered(batch_scenes, ch_start, ch_end):
                        self._log("⚠️ 场景章节覆盖不完整，重试...")
                        continue

                    all_scenes.extend(batch_scenes)
                    self._log(f"✅ 第{batch_idx + 1}/{total_batches}批完成：第{ch_start}-{ch_end}章，共{len(batch_scenes)}个场景")
                    batch_ok = True
                    break
                except json.JSONDecodeError as e:
                    self._log(f"⚠️ JSON解析失败: {e}")

            if not batch_ok:
                self._log(f"⚠️ 第{ch_start}-{ch_end}章场景分解失败，使用备用场景补齐本批")
                self._log(f"   ⚠️ 注意：备用场景为占位摘要，将导致本批正文细节缺失，建议后续人工核对该批")
                fallback = self._generate_fallback_scenes(ch_start, ch_end)
                all_scenes.extend(fallback)

            self.scenes = all_scenes
            self._save_state("layer3")

        # ── v2.0: 后台自动审查+修订 ──
        self._log("\n🧪 场景一致性审查（后台自动执行）...")
        reviewed_scenes = self._review_scene_conflicts(self.scenes)
        self.scene_reviewed = True

        # ── 一次性确认 ──
        formatted = json.dumps(reviewed_scenes, ensure_ascii=False, indent=2)
        confirm_note = f"请确认或修改以上{len(reviewed_scenes)}个场景。\n已自动完成冲突审查和修订（含二次校验）。\n5秒后自动确认。"
        if self._scene_review_notes:
            confirm_note = f"请确认或修改以上{len(reviewed_scenes)}个场景。\n{self._scene_review_notes}\n5秒后自动确认。"
        confirmed = self._confirm_with_user(
            "确认场景分解（已自动审查修订）",
            formatted,
            confirm_note
        )

        if confirmed == "__REGENERATE__":
            self._log("🔄 用户要求重新生成场景分解")
            self.scenes = []
            self.scene_reviewed = False
            return self.layer3_scene_breakdown()

        try:
            parsed = json.loads(confirmed)
            if isinstance(parsed, list) and self._validate_scenes_json(parsed):
                self.scenes = parsed
            else:
                self._log("⚠️ 用户确认后的场景结构验证失败，使用自动生成结果")
                self.scenes = reviewed_scenes
        except json.JSONDecodeError as e:
            self._log(f"⚠️ 用户确认内容 JSON解析失败: {e}，使用自动生成结果")
            self.scenes = reviewed_scenes

        self._save_state("layer4")
        self._log(f"✅ 场景分解已确认，共{len(self.scenes)}个场景（含自动审查修订）")
        return self.scenes

    # ── JSON 提取（统一类方法，消除重复） ──

    def _extract_json(self, text: str):
        """从模型输出中剥离 ```json 代码块后解析 JSON。object/array 皆可。"""
        text = text.strip()
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0].strip()
        return json.loads(text)

    def _extract_json_object(self, text: str):
        """解析 JSON 对象（底层复用 _extract_json）。"""
        return self._extract_json(text)

    def _extract_json_array(self, text: str):
        """解析 JSON 数组（底层复用 _extract_json）。"""
        return self._extract_json(text)

    def _scene_key(self, scene: Dict) -> tuple:
        """场景唯一键：(chapter, scene_id)，统一为 int 类型"""
        try:
            ch = int(scene.get("chapter", 0))
        except (TypeError, ValueError):
            ch = 0
        try:
            sid = int(scene.get("scene_id", 0))
        except (TypeError, ValueError):
            sid = 0
        return (ch, sid)

    def _validate_scenes_json(self, scenes: List[Dict]) -> bool:
        required_keys = {"chapter", "scene_id", "word_target", "summary"}
        for scene in scenes:
            if not isinstance(scene, dict):
                self._log(f"⚠️ 场景验证：元素不是 dict，而是 {type(scene).__name__}")
                return False
            if not required_keys.issubset(scene.keys()):
                missing = required_keys - scene.keys()
                self._log(f"⚠️ 场景验证：缺少字段 {missing}")
                return False
            try:
                chapter = int(scene["chapter"])
                if chapter < 1:
                    self._log(f"⚠️ 场景验证：chapter={scene['chapter']} < 1")
                    return False
            except (ValueError, TypeError):
                self._log(f"⚠️ 场景验证：chapter 无法转为 int: {repr(scene.get('chapter'))}")
                return False
            summary = scene.get("summary", "")
            if not isinstance(summary, str) or len(summary) < 10:
                self._log(f"⚠️ 场景验证：summary 太短 ({len(str(summary))}字)")
                return False
        return len(scenes) > 0

    # ── 场景冲突审查 ──

    def _audit_scenes(self, scenes: List[Dict], prev_review: Dict = None) -> Dict:
        """跑一次场景一致性审查，返回结构化审查结果；失败返回 {}（由调用方决定是否降级）。

        prev_review 非空 = 这是**二次校验**：上一轮指出的问题会一并交给审查方，
        要求逐条确认是否真的解决，并额外检查修订有没有引入新的逻辑矛盾。
        """
        # 审查属于「场景」阶段：无论从 layer3 还是 layer4 触发，拒答登记都归到 layer3，
        # 避免被误标成 layer4（那样用户会被引导去"补写正文"，而其实该重跑场景）。
        self._ctx = {"stage": "layer3", "chapters": list(range(1, len(self.chapter_outlines) + 1))}

        chapter_summaries = []
        for i, outline in enumerate(self.chapter_outlines, 1):
            chapter_summaries.append(f"第{i}章: {outline}")

        scenes_json = json.dumps(scenes, ensure_ascii=False, indent=2)

        prev_block = ""
        recheck_rules = ""
        if prev_review:
            prev_issues = prev_review.get("revisions", []) or []
            prev_block = f"""
【上一轮审查指出的问题（请逐条确认是否已解决）】
{json.dumps(prev_issues, ensure_ascii=False, indent=2)}
"""
            recheck_rules = """
6. 复核上一轮问题：逐条核对上面列出的问题是否真的解决了（不能只看措辞变化）
7. 修订副作用：修订是否引入了新的矛盾（如前后文不再衔接、改了此处忘了彼处）
8. 返回要求：只要还有任一条问题存在，就照常放进 revisions（不要因为有"已修订过"就放宽）；
   全部解决且无新问题，才返回 has_issues: false
"""

        prompt = f"""
请审查以下小说场景分解是否存在剧情冲突或结构问题，并返回结构化 JSON。

【章节大纲】
{chr(10).join(chapter_summaries)}

【场景分解 JSON】
{scenes_json}
{prev_block}
重点检查：
1. 重复场景：同一事件是否被多个场景/章节重复写过
2. 结尾提前：最终决战、最终和解等是否过早出现
3. 顺序错乱：因果是否倒置
4. 角色状态冲突：受伤/死亡/离开等是否前后矛盾
5. 节奏问题：某些章节是否场景过少或高潮提前释放
{recheck_rules}
只输出合法 JSON 对象，格式：
{{
  "has_issues": true,
  "summary": "总体结论",
  "global_advice": "全局修订原则",
  "revisions": [
    {{
      "severity": "高/中/低",
      "mode": "single 或 cross_chapter",
      "targets": [{{"chapter": 1, "scene_id": 1}}],
      "problem": "问题说明",
      "suggestion": "具体修改建议"
    }}
  ]
}}

如果没有明显问题，返回：
{{"has_issues": false, "summary": "未发现明显冲突", "global_advice": "", "revisions": []}}
"""
        try:
            response = self.call_grok(
                prompt,
                system="你是专业长篇小说剧情编辑，擅长检查情节连续性和逻辑冲突。只输出合法 JSON 对象。",
                max_tokens=12000,
                temperature=0.25,
                show_stream=True
            )
            review = self._extract_json_object(response)
        except RefusalSkipped:
            # 拒答信号必须原样冒泡（layer1-3 无法"留空继续"，由上层中止任务并保留断点）；
            # 若在此被当成"审查失败"吞掉，流水线会带着未审查的场景继续跑。
            raise
        except Exception as e:
            self._log(f"⚠️ 场景一致性审查调用失败：{str(e)[:150]}")
            return {}

        if not isinstance(review, dict):
            self._log("⚠️ 场景一致性审查返回结构不是 JSON 对象")
            return {}
        return review

    @staticmethod
    def _review_has_issues(review: Dict) -> bool:
        """审查结果是否给出了需要处理的修订项。"""
        if not isinstance(review, dict):
            return False
        return bool(review.get("has_issues")) and bool(review.get("revisions"))

    def _review_scene_conflicts(self, scenes: List[Dict]) -> List[Dict]:
        """场景一致性审查 → 自动修订 → **二次校验**（修订后重审，确认问题真的解决）。

        闭环：首轮审查 → 修订 → 再跑一遍审查复核。
        - 复核通过 → 返回修订结果，日志写明"二次校验通过"；
        - 复核仍有问题 → 用复核结果再修订一轮（上限 SCENE_REVISE_MAX 轮）；
        - 达上限仍有问题 → 保留最后一次修订结果，把遗留问题写进 `_scene_review_notes`
          并带入用户确认文案，交由人工把关，而不是"改过一次就当好了"。
        """
        self._scene_review_notes = ""
        self._log("\n🧪 场景一致性审查：检查重复场景、结尾提前、逻辑冲突...")

        review = self._audit_scenes(scenes)
        if not review:
            self._log("⚠️ 场景一致性审查未取得有效结果，跳过审查（场景未经校验，请注意确认）")
            self._scene_review_notes = "⚠️ 本次自动审查未取得有效结果，场景未经过一致性校验。"
            return scenes

        if not self._review_has_issues(review):
            self._log("✅ 场景冲突审查未发现需要自动修订的问题")
            return scenes

        current = scenes
        for attempt in range(1, SCENE_REVISE_MAX + 1):
            count = len(review.get("revisions", []) or [])
            self._log(f"🔧 第{attempt}轮：发现 {count} 项问题，后台自动修订")
            revised = self._revise_scene_conflicts(current, review)

            if revised is current or revised == current:
                self._log("⚠️ 修订未产生实际改动（修订无产出或目标场景未变），停止后续轮次")
                self._scene_review_notes = self._format_review_notes(review)
                return current

            # ── 二次校验：把上一轮的问题原样交回去，逐条核对 ──
            self._log(f"🔍 二次校验：复核第{attempt}轮修订结果...")
            check = self._audit_scenes(revised, prev_review=review)

            if not check:
                self._log("⚠️ 二次校验未取得有效结果，采用本轮修订结果（未复核，请注意确认）")
                self._scene_review_notes = "⚠️ 二次校验未取得有效结果，修订结果未经复核。"
                return revised

            if not self._review_has_issues(check):
                self._log(f"✅ 二次校验通过：第{attempt}轮修订后未再发现逻辑问题")
                return revised

            remain = len(check.get("revisions", []) or [])
            self._log(f"⚠️ 二次校验仍发现 {remain} 项问题")
            current, review = revised, check

        self._log(f"⚠️ 已达修订上限（{SCENE_REVISE_MAX}轮），仍有逻辑问题未解决，"
                  f"保留最后一次修订结果并在确认环节提示人工把关")
        self._scene_review_notes = self._format_review_notes(review)
        return current

    @staticmethod
    def _format_review_notes(review: Dict) -> str:
        """把仍未解决的问题整理成一段可读提示（带入用户确认文案）。"""
        if not isinstance(review, dict):
            return ""
        summary = (review.get("summary") or "").strip()
        items = []
        for r in (review.get("revisions", []) or []):
            if not isinstance(r, dict):
                continue
            problem = (r.get("problem") or "").strip()
            if not problem:
                continue
            targets = []
            for t in (r.get("targets", []) or []):
                try:
                    targets.append(f"第{int(t.get('chapter', 0))}章·场景{int(t.get('scene_id', 0))}")
                except (TypeError, ValueError):
                    continue
            where = f"（{'、'.join(targets)}）" if targets else ""
            sev = (r.get("severity") or "").strip()
            items.append(f"- [{sev or '未标注'}]{where} {problem}")
        if not summary and not items:
            return ""
        lines = ["⚠️ 自动修订后仍有以下逻辑问题未解决，请在确认前留意："]
        if summary:
            lines.append(f"总体结论：{summary}")
        lines.extend(items[:20])
        return "\n".join(lines)


    def _revise_scene_conflicts(self, scenes: List[Dict], review: Dict) -> List[Dict]:
        """根据结构化审查建议重新生成需要修改的场景（每批最多5个）"""
        revisions = review.get("revisions", []) if isinstance(review, dict) else []
        revisions = [r for r in revisions if isinstance(r, dict)]
        if not revisions:
            return scenes

        scene_map = {}
        for scene in scenes:
            try:
                scene_map[self._scene_key(scene)] = scene
            except (TypeError, ValueError):
                continue

        global_advice = review.get("global_advice", "")

        all_targets = []
        seen_keys = set()
        for item in revisions:
            for target in item.get("targets", []):
                try:
                    key = (int(target.get("chapter", 0)), int(target.get("scene_id", 0)))
                except (TypeError, ValueError):
                    continue
                if key in scene_map and key not in seen_keys:
                    all_targets.append(scene_map[key])
                    seen_keys.add(key)

        if not all_targets:
            return scenes

        MAX_PER_BATCH = 5
        batches = [all_targets[i:i + MAX_PER_BATCH] for i in range(0, len(all_targets), MAX_PER_BATCH)]
        self._log(f"🔧 场景冲突修订：共{len(all_targets)}个场景，分{len(batches)}批处理（每批≤{MAX_PER_BATCH}个）")

        revised_all = []
        for batch_idx, batch_scenes in enumerate(batches):
            batch_keys = [(int(s.get("chapter", 0)), int(s.get("scene_id", 0))) for s in batch_scenes]
            key_labels = [f"第{k[0]}章 场景{k[1]}" for k in batch_keys]

            self._log(f"🔧 修订第{batch_idx + 1}/{len(batches)}批：{', '.join(key_labels)}")
            prompt = f"""
请根据审查建议修订指定的场景。

【全局修订原则】
{global_advice}

【全部审查建议（仅作上下文参考）】
{json.dumps(revisions, ensure_ascii=False, indent=2)}

【本次需修订的指定场景（最多{MAX_PER_BATCH}个，只修订这些）】
{json.dumps(batch_scenes, ensure_ascii=False, indent=2)}

要求：
- 只修订以上{len(batch_scenes)}个指定场景，保留其 chapter 和 scene_id 不变
- 解决重复、提前收尾、顺序错乱、角色状态冲突等问题
- summary 尽可能详细，建议200-400字
- 只返回 JSON 数组
"""
            try:
                response = self.call_grok(prompt, system="你是专业长篇小说剧情编辑。只返回合法 JSON 数组。", max_tokens=max(6000, len(batch_scenes) * 700), temperature=0.35, show_stream=True)
                revised = self._extract_json_array(response)
                if isinstance(revised, list) and self._validate_scenes_json(revised):
                    revised_all.extend(revised)
                    self._log(f"✅ 第{batch_idx + 1}/{len(batches)}批完成")
                else:
                    self._log("⚠️ 批次修订返回结构无效，跳过该批")
            except RefusalSkipped:
                # 同审查：拒答信号不能被当成"该批修订失败"吞掉
                raise
            except Exception as e:
                self._log(f"⚠️ 批次修订失败，跳过：{str(e)[:150]}")

        if revised_all:
            return self._merge_revised_scenes(scenes, revised_all)
        return scenes

    def _merge_revised_scenes(self, original_scenes: List[Dict], revised_scenes: List[Dict]) -> List[Dict]:
        """按 chapter + scene_id 合并修订后的场景"""
        revised_map = {}
        for scene in revised_scenes:
            try:
                revised_map[self._scene_key(scene)] = scene
            except (TypeError, ValueError):
                continue

        merged = []
        replaced = 0
        for scene in original_scenes:
            try:
                key = self._scene_key(scene)
            except (TypeError, ValueError):
                merged.append(scene)
                continue
            if key in revised_map:
                merged.append(revised_map[key])
                replaced += 1
            else:
                merged.append(scene)

        existing_keys = set()
        for scene in merged:
            try:
                existing_keys.add(self._scene_key(scene))
            except (TypeError, ValueError):
                pass
        for key, scene in revised_map.items():
            if key not in existing_keys:
                merged.append(scene)

        self._log(f"🔧 已合并修订场景：替换{replaced}个，当前共{len(merged)}个场景")
        return sorted(merged, key=lambda s: (int(s.get("chapter", 0)), int(s.get("scene_id", 0))))

    # ── 备用场景 ──

    def _generate_fallback_scenes(self, ch_lo: int = None, ch_hi: int = None) -> List[Dict]:
        """生成基础场景结构（当API返回无效JSON时使用）。scene_id 统一为 int。

        默认覆盖全部章节；也可只对 [ch_lo, ch_hi] 章范围生成（避免整本白算）。
        """
        ch_lo = ch_lo or 1
        ch_hi = ch_hi if ch_hi is not None else self.chapters_count
        scenes = []
        for ch_num in range(ch_lo, ch_hi + 1):
            for scene_idx in range(max(2, self.words_per_scene // 100)):
                scenes.append({
                    "chapter": ch_num,
                    "scene_id": scene_idx + 1,
                    "summary": f"第{ch_num}章场景{scene_idx+1}",
                    "word_target": self.words_per_scene,
                    "characters": ["主角"],
                    "location": "故事场景"
                })
        return scenes

    # ── 字数与解析工具 ──

    def _count_words(self, text: str) -> int:
        chinese_count = len(re.findall(r'[一-鿿]', text))
        english_words = len(re.findall(r'\b[a-zA-Z]+\b', text))
        numbers = len(re.findall(r'\d+', text))
        return chinese_count + english_words + numbers

    def _parse_chapters_from_text(self, text: str, ch_start: int, ch_end: int) -> Dict[int, str]:
        chapters = {}
        parts = re.split(r'@@第(\d+)章@@', text)

        for i in range(1, len(parts), 2):
            try:
                ch_num = int(parts[i])
                ch_text = parts[i + 1].strip() if i + 1 < len(parts) else ""
                if ch_start <= ch_num <= ch_end and ch_text:
                    chapters[ch_num] = ch_text
            except (ValueError, IndexError):
                continue

        if not chapters and text.strip():
            chapters[ch_start] = text.strip()

        return chapters

    def _get_dynamic_temperature(self, round_num: int, total_rounds: int) -> float:
        if round_num == 1:
            return 0.85
        elif round_num < total_rounds:
            return 0.7
        else:
            return 0.5

    def _get_prev_context(self, prev_text: str, target_words: int) -> str:
        if not prev_text:
            return "（小说开头）"
        context_len = max(500, int(target_words * 0.4))
        return prev_text[-context_len:] if len(prev_text) > context_len else prev_text

    def _calculate_max_tokens(self, target_words: int, round_num: int, total_rounds: int) -> int:
        base_tokens = int(target_words * 1.5)
        if round_num == 1:
            return int(base_tokens * 1.2)
        elif round_num < total_rounds:
            return int(base_tokens * 1.4)
        else:
            return int(base_tokens * 1.3)

    # ═══════════════════════════════
    #  Layer 4: 批量写作
    # ═══════════════════════════════

    def _check_chapter_quality(self, text: str, target_words: int) -> bool:
        """检查章节质量，用于自适应批大小升级判断"""
        if not text:
            return False
        current_len = self._count_words(text)
        # 字数达标率 > 80%
        if current_len < target_words * 0.8:
            return False
        # 中文占比检测
        alpha_cjk = sum(1 for c in text
                        if c.isascii() and c.isalpha()
                        or '\u4e00' <= c <= '\u9fff'
                        or '\u3400' <= c <= '\u4dbf')
        if alpha_cjk > 0:
            cjk = sum(1 for c in text if '\u4e00' <= c <= '\u9fff' or '\u3400' <= c <= '\u4dbf')
            if cjk / alpha_cjk < 0.3:
                return False
        return True

    def _write_chapter_chunk(self, ch_start: int, ch_end: int, chapter_scenes: Dict, prev_context: str):
        """批量写作一个章节批次，支持自适应轮次跳过"""
        num_chapters = ch_end - ch_start + 1
        total_target = self.words_per_chapter * num_chapters
        self._ctx = {"stage": "layer4", "chapters": list(range(ch_start, ch_end + 1))}

        # 组装这批章的安排
        chunk_scenes = ""
        for c in range(ch_start, ch_end + 1):
            outline = ""
            if c <= len(self.chapter_outlines):
                outline = self.chapter_outlines[c - 1]
            scene_lines = [f"  {i+1}. {s.get('summary','')}"
                           for i, s in enumerate(chapter_scenes.get(c, []))]
            chunk_scenes += f"\n=== 第{c}章 ===\n大纲：{outline}\n场景安排：\n" + "\n".join(scene_lines) + "\n"

        current_text = ""
        current_len = 0

        # 轮次语义：round1=初稿；之后若未达标则"扩充"、已达标则"润色"。
        # 通过 round_num 每轮重算 shortfall 自动切换，无需 actual_rounds。
        for round_num in range(1, self.ROUNDS + 1):
            if not self.is_running:
                raise Exception("用户停止生成")

            self._update_progress(
                25 + int(((ch_start - 1 + (ch_end - ch_start + 1) * round_num / self.ROUNDS) / self.chapters_count) * 70),
                f"第{ch_start}-{ch_end}章 第{round_num}/{self.ROUNDS}轮"
            )

            if round_num == 1:
                per_chapter_target = self.words_per_chapter
                instruction = f"""写出第{ch_start}章到第{ch_end}章的初稿。每章必须达到{per_chapter_target}字以上，总计{total_target}字。只输出小说正文，严禁写说明、备注、注释。"""
            else:
                shortfall = max(0, total_target - current_len)
                if shortfall > 100:
                    instruction = f"""上一轮只写了{current_len}字（目标{total_target}字），还差{shortfall}字！请在保持情节不变的前提下，通过增加叙事描写来扩充每章：
- 环境描写（详细刻画场景）
- 角色内心（想法、感受、回忆）
- 对话互动（每段对话至少3轮）
- 动作细节（具体的肢体语言）
- 氛围渲染
必须扩充到{total_target}字以上。只输出小说正文，严禁输出任何说明文字。"""
                else:
                    instruction = f"润色文字，检查连贯性。保持{total_target}字以上。"

            temperature = self._get_dynamic_temperature(round_num, self.ROUNDS)
            prev_block = f"【上一轮内容】\n{current_text}" if current_text else ""
            dynamic_prev_context = self._get_prev_context(prev_context, self.words_per_chapter)

            prompt = f"""你是专业小说家。请写出第{ch_start}章到第{ch_end}章的正文。

【核心设定】
{self.setting_bible}

【这批章的安排】
{chunk_scenes}

【前情回顾】
{dynamic_prev_context}

{prev_block}

【本轮任务】
{instruction}

【输出格式】
每章开头用「@@第N章@@」独占一行标记（N为章号），标记后直接写正文。严禁输出任何说明、备注、注释、引导语（如"以下是第X章"）。只输出叙事文字。
"""

            system = "你是专业小说家，擅长长篇叙事。重要：只写小说正文，严禁输出说明文字、备注、注释、引导语。每章字数必须达标。"

            response = self.call_grok(
                prompt,
                system=system,
                show_stream=True,
                max_tokens=self._calculate_max_tokens(total_target, round_num, self.ROUNDS),
                temperature=temperature
            )
            current_text = response
            current_len = self._count_words(response)
            self._log(f"📝 第{ch_start}-{ch_end}章 第{round_num}/{self.ROUNDS}轮完成 ({current_len}字/目标{total_target}字)")

            # ── v2.0: 字数达标即停（首轮达标已自然进入下一轮润色，无需跳轮）──
            if current_len >= total_target - 100 and round_num >= 2:
                self._log(f"  ✅ 字数已达标({current_len}字)，停止后续轮次")
                break

        # 拆分各章
        chapters_dict = self._parse_chapters_from_text(current_text, ch_start, ch_end)

        written_chapters: List[int] = []
        for ch_num, ch_text in chapters_dict.items():
            self.chapters[ch_num] = [ch_text]
            written_chapters.append(ch_num)
            self._log(f"✅ 第{ch_num}章完成 ({len(ch_text)}字)")
            # v2.0: 每章完成即保存
            self._save_state("layer4", ch_num)

            # ── v2.0: 自适应批大小质量检查 ──
            if self._adaptive_chunk_enabled and self.chunk_size == 1 and ch_num > 1:
                if self._check_chapter_quality(ch_text, self.words_per_chapter):
                    self._quality_streak += 1
                else:
                    self._quality_streak = 0

                if self._quality_streak >= 2:
                    self.chunk_size = 2
                    self._log(f"📈 连续{self._quality_streak}章质量达标，自动升级批大小为 {self.chunk_size} 章/批")
                    self._quality_streak = 0

        # 上报缺失章节（供外层按"缺章单独补写"推进，而非整段步进跳过）
        missing = [c for c in range(ch_start, ch_end + 1) if c not in chapters_dict]
        if missing:
            self._log(f"⚠️ 第{ch_start}-{ch_end}章中缺失: {missing}，外层将逐章补齐")

        # 返回本批实际写到的最大连续章号：外层据此推进，避免漏写。
        # 若本批开头就缺（如 ch_start 未写成功），返回 ch_start-1，外层重试该章。
        max_written = max(written_chapters) if written_chapters else ch_start - 1
        return max_written

    def layer4_write_scenes(self, start_chapter: int = 1) -> str:
        """第4层：批量写作 + 自适应批大小 + 自适应轮次"""
        self._log(f"\n✍️ 第4层：批量写作（{self.chunk_size}章/批 × {self.ROUNDS}轮迭代）...")
        if self.single_chapter_write:
            self._log("   📊 正文单章模式已开启：始终 1 章/批，不做批量升级")
        else:
            self._log(f"   📊 自适应批大小已启用：初始1章/批，连续2章质量达标后自动升级为2章/批")

        if not self.scenes:
            self.scenes = self._generate_fallback_scenes()
            self.scene_reviewed = False

        if not self.scene_reviewed:
            self._log("⚠️ 场景尚未经过冲突审查，先执行场景冲突审查...")
            self.scenes = self._review_scene_conflicts(self.scenes)
            self.scene_reviewed = True
            self._save_state("layer4", max(0, start_chapter - 1))

        # 按章分组场景
        chapter_scenes = {}
        for sc in self.scenes:
            ch = int(sc.get("chapter", 1))
            chapter_scenes.setdefault(ch, []).append(sc)

        if start_chapter == 1 and not self.chapters:
            self.chapters = {i: [] for i in range(1, self.chapters_count + 1)}

        if start_chapter > 1:
            self._log(f"📂 续传：从第{start_chapter}章开始")

        # 用 while 按"实际已写章节"推进，而非固定步进 chunk_size：
        # 批内若部分缺章（模型漏章），下一轮从缺章处补齐，杜绝静默丢章。
        ch_start = start_chapter
        while ch_start <= self.chapters_count:
            if self._backend_down:
                break
            if not self.is_running:
                raise Exception("用户停止生成")

            ch_end = min(ch_start + self.chunk_size - 1, self.chapters_count)
            self._log(f"📝 正在写作第{ch_start}-{ch_end}章 (批大小={self.chunk_size})...")

            # 前情回顾
            prev = "（小说开头）"
            if ch_start > 1 and self.chapters.get(ch_start - 1) and self.chapters[ch_start - 1]:
                full_prev = self.chapters[ch_start - 1][0]
                prev = full_prev[-1000:] if len(full_prev) > 1000 else full_prev

            chunk_ok = False
            refusal_skipped = False
            written_upto = ch_start - 1
            for retry in range(RETRY_MAX):
                try:
                    written_upto = self._write_chapter_chunk(ch_start, ch_end, chapter_scenes, prev)
                    chunk_ok = True
                    break
                except RefusalSkipped:
                    refusal_skipped = True
                    break
                except Exception as e:
                    if "用户停止生成" in str(e):
                        raise
                    err_str = str(e)
                    self._log(f"❌ 第{ch_start}-{ch_end}章失败 ({retry+1}/{RETRY_MAX}): {err_str[:150]}")
                    if "No healthy provider" in err_str or "no healthy provider" in err_str.lower():
                        self._log(f"🛑 后端不可用，停止生成。当前进度已保存，可稍后继续。")
                        self._backend_down = True
                        self.is_running = False
                        break
                    if retry < RETRY_MAX - 1:
                        wait = 10 * (retry + 1)
                        self._log(f"⏳ {wait}秒后重试...")
                        time.sleep(wait)

            # 拒答留空（用户跳过 / 超时跳过 / 后台登记待处理）：不重试、不填充，
            # 本批章节保持空白直接推进；有空章时任务不会被标记完成。
            if refusal_skipped:
                self._log(f"⏭️ 第{ch_start}-{ch_end}章因拒答跳过，保留空白（可续传或在「拒答待处理」中补写）")
                for c in range(ch_start, ch_end + 1):
                    self.chapters.setdefault(c, [])
                self._save_state("layer4", ch_end)
                ch_start = ch_end + 1
                time.sleep(0.3)
                continue

            # 本批最终仍失败：回退批大小为1，且不让外层死循环卡在坏章上
            if not chunk_ok:
                self._log(f"⚠️ 第{ch_start}-{ch_end}章最终失败，将被跳过")
                if self.chunk_size > 1:
                    self.chunk_size = 1
                    self._quality_streak = 0
                ch_start = ch_end + 1
                time.sleep(0.3)
                continue

            # 批内缺章 → 本批暂到 written_upto，回退批大小=1，从缺章处逐章补齐
            if written_upto < ch_end:
                # 单章且完全没写出来（chunk_size==1 时 written_upto==ch_start-1）：
                # 说明该章被模型反复拒/漏写，不能无限重试，设一个逐章放弃上限。
                if self.chunk_size == 1 and written_upto == ch_start - 1:
                    self._skip_single_chapter_counter = getattr(self, "_skip_single_chapter_counter", 0) + 1
                    if self._skip_single_chapter_counter >= 3:
                        self._log(f"⚠️ 第{ch_start}章连续多次无法产出（可能被内容策略拦截），放弃本章继续")
                        self.chapters[ch_start] = self.chapters.get(ch_start) or []
                        ch_start = ch_start + 1
                        self._skip_single_chapter_counter = 0
                        time.sleep(0.3)
                        continue
                else:
                    self._skip_single_chapter_counter = 0

                self._log(f"⚠️ 本批仅写到第{written_upto}章（缺 {list(range(written_upto+1, ch_end+1))}），回退批大小并补齐")
                self.chunk_size = 1
                self._quality_streak = 0
                ch_start = written_upto + 1
                time.sleep(0.3)
                continue

            self._skip_single_chapter_counter = 0
            ch_start = ch_end + 1
            time.sleep(0.3)

        missing = self.missing_chapters()
        if missing:
            self._log(f"⚠️ 以下章节未生成（留空）: {missing}")

        return self._merge_to_novel()

    def missing_chapters(self) -> List[int]:
        """返回 1..chapters_count 中未生成（空白）的章号列表。"""
        return [c for c in range(1, self.chapters_count + 1) if not self.chapters.get(c)]

    # ── 拒答待处理项的「重新提交」补写 ──

    def fill_chapters_from_prompt(self, prompt: str, chapters: List[int]) -> Dict[int, str]:
        """用用户确认过的发送内容补写指定章节（层4 拒答项的重新提交路径）。

        与正常写作流程的区别：prompt 是用户在前端看到并（可能）修改过的原文，
        不再重新拼装；产出按 @@第N章@@ 解析，只接受目标章，写回 chapters 并保存断点。
        返回实际写成功的 {章号: 正文}（空字典表示仍无产出）。
        """
        chapters = sorted({int(c) for c in (chapters or [])})
        if not chapters:
            return {}
        ch_start, ch_end = chapters[0], chapters[-1]
        self._ctx = {"stage": "layer4", "chapters": chapters}
        self._log(f"🔁 补写第{ch_start}-{ch_end}章（使用前端确认的发送内容）…")
        resp = self.call_grok(
            prompt,
            max_tokens=self._calculate_max_tokens(self.words_per_chapter * len(chapters), 1, self.ROUNDS),
            temperature=self._get_dynamic_temperature(1, self.ROUNDS),
            show_stream=True,
        )
        parsed = self._parse_chapters_from_text(resp or "", ch_start, ch_end)
        parsed = {c: t for c, t in parsed.items() if c in chapters and t and t.strip()}
        for c in chapters:
            if c in parsed:
                self.chapters[c] = [parsed[c]]
                self._log(f"✅ 第{c}章补写完成 ({len(parsed[c])}字)")
                self._save_state("layer4", c)
            else:
                self.chapters.setdefault(c, [])
        if not parsed:
            self._log("⚠️ 补写未产出可用正文，该部分仍留空")
        return parsed

    def fill_bible_from_prompt(self, prompt: str) -> str:
        """用用户确认过的发送内容重生成设定圣经（层1 拒答项的重新提交路径）。"""
        self._ctx = {"stage": "layer1", "chapters": []}
        self._log("🔁 重新生成设定圣经（使用前端确认的发送内容）…")
        bible = self.call_grok(prompt, max_tokens=12000, show_stream=True)
        if not (bible or "").strip():
            self._log("⚠️ 设定圣经补写为空")
            return ""
        self.setting_bible = bible
        self._save_state("layer2")
        self._log(f"✅ 设定圣经已更新 ({len(bible)}字)")
        return bible

    def _merge_to_novel(self) -> str:
        """合并成品。未生成的章节保留章节标题 + 空正文（"空着"），
        而不是静默丢弃 —— 缺章位置在成品里必须可见，且缺章不允许标记完成。"""
        full_story = []
        for ch_num in range(1, self.chapters_count + 1):
            title = ""
            if ch_num <= len(self.chapter_outlines):
                title_match = re.search(r'《([^》]+)》', self.chapter_outlines[ch_num-1])
                if title_match:
                    title = f"《{title_match.group(1)}》"
            texts = self.chapters.get(ch_num) or []
            chapter_text = texts[0] if texts else ""
            full_story.append(f"\n## 第{ch_num}章{title}\n\n{chapter_text}".rstrip())
        return "\n\n".join(full_story)

    # ═══════════════════════════════
    #  主流程
    # ═══════════════════════════════

    def run(self, start_stage: str = "layer1") -> str:
        try:
            if start_stage in ("layer1",):
                self.layer1_setting_bible()
            if not self.is_running: return ""

            if start_stage in ("layer1", "layer2"):
                self.layer2_chapter_outlines()
            if not self.is_running: return ""

            if start_stage in ("layer1", "layer2", "layer3"):
                self.layer3_scene_breakdown()
            if not self.is_running: return ""

            story = self.layer4_write_scenes(start_chapter=max(1, self._resume_chapter))

            if self._backend_down:
                self._log("⚠️ 因后端不可用提前结束，当前进度已保存，稍后可继续。")
                return self._merge_to_novel()

            self._update_progress(100, "完成！")
            self._log(f"\n🎉 生成完成！共调用 {self.call_count} 次")
            self._save_state("done")

            return story
        except Exception as e:
            if str(e) != "用户停止生成":
                self._log(f"❌ 生成失败: {e}")
            raise
