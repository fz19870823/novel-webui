"""
novel-ai 断点续传状态管理模块
保存/加载/清除断点状态文件。断点中不再存储 config 区块（API Key 安全）。
"""

import json
import os

from config import DATA_DIR

STATE_FILE = os.path.join(DATA_DIR, "novel_resume_state.json")


def save_resume_state(state: dict):
    """保存断点状态到文件。自动剥离 config 区块的 API Key。"""
    safe = dict(state)
    # 剥离 config 中的敏感信息
    if "config" in safe:
        stripped = dict(safe["config"])
        stripped.pop("api_key", None)
        safe["config"] = stripped
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(safe, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] 保存断点状态失败: {e}")


def load_resume_state() -> dict | None:
    """加载断点状态，不存在返回 None"""
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARN] 加载断点状态失败: {e}")
    return None


def clear_resume_state():
    """清除断点文件（生成完成后调用）"""
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except OSError as e:
            print(f"[WARN] 删除断点文件失败: {e}")
