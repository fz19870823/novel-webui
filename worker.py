"""
novel-webui 生成 worker
在独立后台线程中运行 NovelGenerator，把阻塞式回调接到 controller.JobManager。
用户可随时 stop()；stop 只在引擎下一个检查点生效（进行中的单次 API 流式调用需返回后才中止），
与原 GUI 语义一致。

拒答语义（两条路）：
- 有前端在线（WS 订阅端存在）→ 引擎弹确认窗，用户查看/修改发送内容后重发，或跳过留空；
- 无前端在线（纯后台运行）→ 不打扰用户，把被拒的部分登记进「拒答待处理」列表
  （持久化在数据目录），该部分留空继续；用户上线后逐项处理（见 RefusalResolver）。
- 只要存在留空章节，任务就不会被标记完成（mark_incomplete），断点保留可续传。
"""

import os
import re
import threading
from datetime import datetime

from engine import NovelGenerator, RefusalSkipped
from controller import manager
from config import DATA_DIR, load_config
from state import load_resume_state, clear_resume_state


class GeneratorWorker:
    def __init__(self, theme: str, requirements: str, api_key: str,
                 base_url: str, model: str,
                 chapters_count=None, words_per_chapter=None,
                 single_chapter_scene=None, single_chapter_write=None,
                 resume: bool = False):
        self.theme = theme
        self.requirements = requirements
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.chapters_count = chapters_count
        self.words_per_chapter = words_per_chapter
        # None = 未指定（续传时回落到断点里记录的粒度）
        self.single_chapter_scene = single_chapter_scene
        self.single_chapter_write = single_chapter_write
        self.resume = resume

        self._thread = None
        self._generator = None
        self._cancel_flag = threading.Event()

    # ── 公共控制 ──
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="novel-gen")
        self._thread.start()

    def stop(self):
        """请求停止。返回后任务可能仍在途，需等线程结束。"""
        if self._generator:
            try:
                self._generator.stop()
            except Exception:
                pass

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ── 主流程 ──
    def _run(self):
        manager.reset_before_run()
        manager.set_running(True)
        manager._log(f"\n{'='*48}")
        manager._log("🚀 任务已提交（后台运行，无需保持页面打开）")

        resume_state = None
        start_stage = "layer1"
        if self.resume:
            resume_state = load_resume_state()
            if not resume_state:
                manager.mark_error("没有可续传的断点文件，无法续传")
                manager._log(f"  断点文件不存在：{STATE_HINT}")
                return
            stg = resume_state.get("stage", "layer1")
            start_stage = stg if stg != "done" else "layer1"
            if stg == "done":
                clear_resume_state()
            manager._log(f"📂 从断点续传：阶段 {start_stage}")
            # 续传时回填界面参数（theme/req 已由调用方带上，仅日志提示）
            manager._log(f"  已写 {sum(len(v) for v in (resume_state.get('chapters') or {}).values())} 个场景, "
                         f"调用 {resume_state.get('call_count', 0)} 次")

        try:
            self._generator = NovelGenerator(
                theme=self.theme,
                requirements=self.requirements,
                api_key=self.api_key,
                base_url=self.base_url,
                model=self.model,
                progress_callback=manager.cb_progress,
                log_callback=manager.cb_log,
                confirm_callback=manager.cb_confirm,
                state_callback=manager.cb_state,
                content_callback=manager.cb_content,
                refusal_callback=manager.add_refusal,
                viewer_check=manager.has_ws_clients,
                resume_state=resume_state,
                chapters_count=self.chapters_count,
                words_per_chapter=self.words_per_chapter,
                single_chapter_scene=self.single_chapter_scene,
                single_chapter_write=self.single_chapter_write,
            )

            story = self._generator.run(start_stage=start_stage)

            # 后端不可用提前结束：部分成品落盘，但断点必须保留
            # （旧逻辑此处 story 非空会走 mark_done 并清掉断点，与日志"断点已保留"自相矛盾）
            if getattr(self._generator, "_backend_down", False):
                filename = _write_novel_file(self._generator.novel_title or "", story)
                manager._log(f"⚠️ 因后端不可用提前结束，部分成品已保存：{filename}；断点保留，可稍后「继续上次」")
                manager.mark_stopped()
                return

            if story:
                title = self._generator.novel_title or ""
                missing = self._generator.missing_chapters()
                filename = _write_novel_file(title, story)
                if missing:
                    # 有空着的部分 → 绝不标记完成
                    manager.mark_incomplete(missing, title, filename)
                else:
                    manager.mark_done(title, filename)
                    manager._log(f"🎉 生成完成！已保存：{filename}")
            else:
                # 后端不可用 / 被取消时 story 为空，但断点应已保留
                st = load_resume_state()
                if st and st.get("stage") != "done":
                    manager._log("⚠️ 流程提前结束，断点已保留，可稍后继续。")
                    manager.mark_stopped()
                else:
                    manager.mark_done("", "")
        except RefusalSkipped:
            # layer1-3 拒答留空：前置内容没法"空着继续"（大纲/场景缺失会让后续全空），
            # 因此任务中止；断点与拒答记录都已保存，用户处理后可用「继续上次」重跑。
            manager.mark_error("模型拒答且该部分留空：前置阶段（设定/大纲/场景）无法留空继续，任务已中止；"
                               "断点与拒答记录均已保存，可在「拒答待处理」中处理后用「继续上次」重跑")
        except Exception as e:
            msg = str(e)
            if msg == "用户停止生成" or msg == "用户取消生成":
                st = load_resume_state()
                if st:
                    manager.mark_stopped()
                else:
                    manager.mark_error(msg)
            else:
                manager.mark_error(msg)
        finally:
            self._generator = None


class RefusalResolver:
    """处理「拒答待处理」项：用（可能已修改的）发送内容补写对应部分，或跳过保持空白。

    与 GeneratorWorker 同构（start/stop/is_alive + 后台线程），复用同一套 manager
    回调，所以前端日志/实时正文/断点语义完全一致；补写期间同样可用 /api/stop 中止。

    补写过程中若再次被拒答：不重复登记（原待处理项保留），直接标记失败，
    用户可继续修改发送内容重试 —— 避免同一条拒答在列表里滚雪球。
    """

    def __init__(self, item: dict, prompt: str, api_key: str,
                 base_url: str, model: str):
        self.item = dict(item or {})
        self.prompt = prompt or ""
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self._thread = None
        self._generator = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="novel-refill")
        self._thread.start()

    def stop(self):
        if self._generator:
            try:
                self._generator.stop()
            except Exception:
                pass

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _run(self):
        item_id = self.item.get("id", "")
        label = self.item.get("label", "")
        stage = self.item.get("stage", "")
        chapters = []
        for c in (self.item.get("chapters") or []):
            try:
                chapters.append(int(c))
            except (TypeError, ValueError):
                continue

        prev_file = manager.task.get("result_file", "")   # reset 前先记住成品文件名
        manager.reset_before_run()
        manager.set_running(True, stage="layer4", stage_name="补写中")
        manager._log(f"\n{'='*48}")
        manager._log(f"🔁 处理拒答项：{label}")

        state = load_resume_state()
        if not state:
            manager.mark_error("断点已不存在（生成已完成或被清除），无法补写该项")
            return
        if stage not in ("layer4", "layer1"):
            manager.mark_error("该阶段（大纲/场景）不支持单独补写，请处理后用「继续上次」重跑")
            return
        if stage == "layer4" and not chapters:
            manager.mark_error("该项未记录章号，无法补写")
            return

        try:
            self._generator = NovelGenerator(
                theme=state.get("theme", ""),
                requirements=state.get("requirements", ""),
                api_key=self.api_key,
                base_url=self.base_url,
                model=self.model,
                progress_callback=manager.cb_progress,
                log_callback=manager.cb_log,
                # 不挂确认窗：补写期间不再弹交互（避免用户在弹窗里再点弹窗），
                # 一旦再次拒答就走"留空"路径并退出，原待处理项保留。
                confirm_callback=None,
                state_callback=manager.cb_state,
                content_callback=manager.cb_content,
                refusal_callback=None,
                viewer_check=lambda: False,
                resume_state=state,
            )
            gen = self._generator

            if stage == "layer1":
                ok = bool(gen.fill_bible_from_prompt(self.prompt))
            else:
                ok = bool(gen.fill_chapters_from_prompt(self.prompt, chapters))

            if not ok:
                manager.mark_error("补写仍被拒答或无产出，该项保留在待处理列表中（可修改发送内容后重试）")
                return

            manager.remove_refusal(item_id)
            missing = gen.missing_chapters()
            story = gen._merge_to_novel()
            filename = _write_novel_file(gen.novel_title or "", story, prev=prev_file)
            if missing:
                manager.mark_incomplete(missing, gen.novel_title or "", filename)
            else:
                manager.mark_done(gen.novel_title or "", filename)
                manager._log(f"🎉 全部章节已补齐，成品已更新：{filename}")
        except RefusalSkipped:
            manager.mark_error("补写仍被模型拒答，该项保留在待处理列表中（可修改发送内容后重试）")
        except Exception as e:
            manager.mark_error(f"补写失败：{e}")
        finally:
            self._generator = None


def _write_novel_file(title: str, story: str, prev: str = "") -> str:
    """把完整小说落盘（写入 DATA_DIR），返回文件名。

    prev 非空且该文件存在 → **覆盖同一份成品**（补写/续传不产生多份重复文件），
    覆盖前先把旧内容备份为 <name>.bak，避免误覆盖造成数据丢失。
    """
    prev_name = os.path.basename(prev or "")
    prev_path = os.path.join(DATA_DIR, prev_name) if prev_name else ""
    if prev_path and os.path.exists(prev_path):
        try:
            with open(prev_path, "r", encoding="utf-8") as f:
                old = f.read()
            with open(prev_path + ".bak", "w", encoding="utf-8") as f:
                f.write(old)
        except OSError as e:
            print(f"[WARN] 备份旧成品失败: {e}")
        name = prev_name
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = re.sub(r'[\\/*?:"<>|]', '', title or "").strip()[:80]
        name = f"{safe}_{ts}.txt" if safe else f"novel_{ts}.txt"
    with open(os.path.join(DATA_DIR, name), "w", encoding="utf-8") as f:
        f.write(story)
    return name


def _state_path():
    return os.path.join(DATA_DIR, "novel_resume_state.json")


STATE_HINT = _state_path()
