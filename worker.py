"""
novel-webui 生成 worker
在独立后台线程中运行 NovelGenerator，把阻塞式回调接到 controller.JobManager。
用户可随时 stop()；stop 只在引擎下一个检查点生效（进行中的单次 API 流式调用需返回后才中止），
与原 GUI 语义一致。
"""

import os
import re
import threading
from datetime import datetime

from engine import NovelGenerator
from controller import manager
from config import DATA_DIR, load_config
from state import load_resume_state, clear_resume_state


class GeneratorWorker:
    def __init__(self, theme: str, requirements: str, api_key: str,
                 base_url: str, model: str,
                 chapters_count=None, words_per_chapter=None,
                 resume: bool = False):
        self.theme = theme
        self.requirements = requirements
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.chapters_count = chapters_count
        self.words_per_chapter = words_per_chapter
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
                resume_state=resume_state,
                chapters_count=self.chapters_count,
                words_per_chapter=self.words_per_chapter,
            )

            story = self._generator.run(start_stage=start_stage)

            if story:
                title = self._generator.novel_title or ""
                filename = _write_novel_file(title, story)
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


def _write_novel_file(title: str, story: str) -> str:
    """把完整小说落盘（写入 DATA_DIR），返回文件名。"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = re.sub(r'[\\/*?:"<>|]', '', title or "").strip()[:80]
    name = f"{safe}_{ts}.txt" if safe else f"novel_{ts}.txt"
    with open(os.path.join(DATA_DIR, name), "w", encoding="utf-8") as f:
        f.write(story)
    return name


def _state_path():
    return os.path.join(DATA_DIR, "novel_resume_state.json")


STATE_HINT = _state_path()
