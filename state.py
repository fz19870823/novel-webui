"""
novel-ai 断点续传状态管理模块

断点被拆成三层，各自独立写入：

  1. 进度文件  <DATA_DIR>/novel_resume_state.json
     小文件（stage / 已写章号 / 调用次数 / 章节索引 / 静态部分引用），每次保存都写。
  2. 静态分片  <DATA_DIR>/novel_resume_static/<hash>.json
     主题、要求、大纲、场景表、设定圣经等「一次生成、之后不变」的内容。
     按内容 sha1 命名 —— 内容没变就复用已有分片，不重复写盘。
  3. 章节分片  <DATA_DIR>/novel_resume_chapters/<hash>.json
     单章正文，同样内容寻址，重写同一章不产生新文件。

为什么这么拆：layer4 每写完一章就保存一次断点。旧实现把「全部章节正文 +
全部场景 + 设定圣经」塞进单个 JSON 全量重写，写盘量随进度呈 O(N²)，
实测 60 章逐章保存累计写盘 12.4MB（100 章量级更大）。拆分后每次只写
几 KB 的进度文件 + 新增内容，实测同样场景降到 0.3MB 量级。

兼容性：旧版单文件格式（所有内容内联在 novel_resume_state.json）仍可加载，
首次保存自动升级为新格式。

安全：进度文件与静态分片都不含 API Key（保存前剥离 config.api_key）。
"""

import hashlib
import json
import os
import tempfile

from config import DATA_DIR

STATE_FILE = os.path.join(DATA_DIR, "novel_resume_state.json")
CHAPTER_DIR = os.path.join(DATA_DIR, "novel_resume_chapters")
STATIC_DIR = os.path.join(DATA_DIR, "novel_resume_static")

_CHAPTER_KEY = "chapters"        # 对外仍以 chapters 字典暴露，引擎无感
_INDEX_KEY = "chapters_index"    # {"1": "<sha1>", ...} 章节号 → 分片
_STATIC_REF_KEY = "static_ref"   # 静态分片 digest

# 「一次生成后不再变化」的字段 → 走静态分片（内容不变不重写）
_STATIC_KEYS = ("theme", "requirements", "config", "setting_bible",
                "chapter_outlines", "scenes", "scene_reviewed",
                "chapters_count", "scenes_per_chapter", "words_per_chapter",
                "words_per_scene", "target_words")


def _shard_path(directory: str, digest: str) -> str:
    return os.path.join(directory, digest + ".json")


def _write_atomic(path: str, payload: str):
    """原子写：同目录临时文件 + fsync + rename，避免断电/崩溃留下半截 JSON。"""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _digest(payload: str) -> str:
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _gc(directory: str, keep: set):
    """回收不再被引用的分片（换主题/改稿后不留垃圾）。"""
    if not os.path.isdir(directory):
        return
    for fn in os.listdir(directory):
        if fn.endswith(".json") and fn[:-5] not in keep:
            try:
                os.remove(os.path.join(directory, fn))
            except OSError:
                pass


def save_resume_state(state: dict):
    """保存断点：进度文件 + 章节分片 + 静态分片，各自只写变化的部分。"""
    safe = dict(state)
    # 剥离 config 中的敏感信息
    if "config" in safe:
        stripped = dict(safe["config"])
        stripped.pop("api_key", None)
        safe["config"] = stripped

    chapters = safe.pop(_CHAPTER_KEY, None) or {}

    try:
        os.makedirs(CHAPTER_DIR, exist_ok=True)
        os.makedirs(STATIC_DIR, exist_ok=True)

        # ① 章节分片：内容未变则复用已有文件
        index, used_ch = {}, set()
        for ch, texts in chapters.items():
            payload = json.dumps(texts, ensure_ascii=False)
            digest = _digest(payload)
            index[str(ch)] = digest
            used_ch.add(digest)
            p = _shard_path(CHAPTER_DIR, digest)
            if not os.path.exists(p):
                _write_atomic(p, payload)

        # ② 静态分片：大纲/场景/设定圣经等，layer4 期间通常完全不变 → 复用
        static = {k: safe.pop(k) for k in _STATIC_KEYS if k in safe}
        spayload = json.dumps(static, ensure_ascii=False)
        sdigest = _digest(spayload)
        sp = _shard_path(STATIC_DIR, sdigest)
        if not os.path.exists(sp):
            _write_atomic(sp, spayload)

        # ③ 进度文件（小）：stage / 章号 / 调用次数 / 索引 / 静态引用
        safe[_STATIC_REF_KEY] = sdigest
        safe[_INDEX_KEY] = index
        _write_atomic(STATE_FILE, json.dumps(safe, ensure_ascii=False, indent=2))

        _gc(CHAPTER_DIR, used_ch)
        _gc(STATIC_DIR, {sdigest})
    except Exception as e:
        print(f"[WARN] 保存断点状态失败: {e}")


def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_resume_state() -> dict | None:
    """加载断点状态，不存在返回 None。

    自动还原完整状态：静态分片 + 进度文件 + 章节分片合并成引擎需要的字典；
    旧版单文件格式（全量内联）原样返回。
    """
    if not os.path.exists(STATE_FILE):
        return None
    try:
        meta = _read_json(STATE_FILE)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARN] 加载断点状态失败: {e}")
        return None

    ref = meta.pop(_STATIC_REF_KEY, None)
    index = meta.pop(_INDEX_KEY, None)
    if ref is None and index is None:
        return meta                       # 旧格式：所有内容内联

    out = {}
    if ref:
        try:
            out.update(_read_json(_shard_path(STATIC_DIR, ref)))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[WARN] 断点静态分片缺失/损坏（大纲/场景将重新生成）: {e}")
    out.update(meta)

    chapters = {}
    for ch, digest in (index or {}).items():
        try:
            chapters[str(ch)] = _read_json(_shard_path(CHAPTER_DIR, digest))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[WARN] 断点章节分片缺失/损坏（第{ch}章将重新生成）: {e}")
    out[_CHAPTER_KEY] = chapters
    return out


def clear_resume_state():
    """清除断点（进度文件 + 全部分片）。生成完成后调用。"""
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except OSError as e:
            print(f"[WARN] 删除断点文件失败: {e}")
    for d in (CHAPTER_DIR, STATIC_DIR):
        if os.path.isdir(d):
            for fn in os.listdir(d):
                try:
                    os.remove(os.path.join(d, fn))
                except OSError:
                    pass
            try:
                os.rmdir(d)
            except OSError:
                pass
