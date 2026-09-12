"""
novel-ai 配置管理模块

API Key 优先级：环境变量 NOVEL_AI_API_KEY > 配置文件中的加密值。
- 若设置环境变量 NOVEL_AI_API_KEY，则以环境变量为准，文件不重复存 Key。
- 否则 API Key 经 Fernet 加密后落盘（见 save_config / load_config），
  配置文件里不出现明文；解密密钥存于本地 SECRET_KEY_FILE（gitignore 已排除）。
  缺 cryptography 依赖时，仅在“需要保存/解密 Key”时才报错，其余字段不受影响。
"""

import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 运行期数据目录：成品小说 / 断点 / 本地配置统一存放处。
# 默认与代码同目录（保持原有本地行为）；Docker 容器内用 NOVEL_DATA_DIR=/data 覆盖，
# 便于把全部运行数据挂到独立卷，代码层保持只读。
DATA_DIR = os.environ.get("NOVEL_DATA_DIR", "").strip() or BASE_DIR

CONFIG_FILE = os.path.join(DATA_DIR, "novel_generator_config.json")
SECRET_KEY_FILE = os.path.join(DATA_DIR, ".model_secret")   # Fernet 密钥文件，勿提交/泄露

DEFAULT_BASE_URL = "https://api.x.ai/v1"
DEFAULT_MODEL = "grok-4-1-fast-non-reasoning"
DEFAULT_WORDS_PER_CHAPTER = 1200
DEFAULT_WORDS_PER_SCENE = 280
DEFAULT_TARGET_WORDS = 50000
DEFAULT_CONFIRM_SECONDS = 5

# 本地无审查兜底 API 默认上下文预算（字符近似；超出先由主 API 压缩再发送）
DEFAULT_FALLBACK_CTX_LIMIT = 64000


def _default_config() -> dict:
    return {
        "api_key": "",
        "api_key_enc": False,          # api_key 字段是否为 Fernet 密文
        "base_url": DEFAULT_BASE_URL,
        "model": DEFAULT_MODEL,
        "theme": "",
        "requirements": "",
        "chapters_count": "",
        "words_per_chapter": "",
        # 批粒度开关（分解 / 正文分开控制）：True = 每次只处理一章
        "single_chapter_scene": False,   # layer3 场景分解：1 章/批（关 = 2 章/批）
        "single_chapter_write": False,   # layer4 正文写作：1 章/批 且不做批量升级
        # 本地无审查兜底 API（模型连续拒答后用它补写被拒部分；url 留空 = 禁用）
        # 兜底 API（OpenAI 兼容：本机 Ollama，或**其他机器**上的自建服务；url 留空 = 禁用）
        "fallback_api_url": "",        # 如 http://127.0.0.1:11434/v1 或 http://192.168.1.50:8000/v1
        "fallback_api_key": "",        # 自建服务一般不需要；非空会加密落盘
        "fallback_api_key_enc": False, # fallback_api_key 是否为 Fernet 密文
        "fallback_model": "",
        "fallback_context_limit": DEFAULT_FALLBACK_CTX_LIMIT,
        "fallback_proxy": "auto",      # auto=内网直连/公网走代理，direct=强制直连，system=强制走代理
    }


def _migrate_single_chapter(saved: dict) -> dict:
    """兼容拆分前的单一开关 `single_chapter`（当时同时控制分解与正文）。

    仅当新键缺失时才用旧值补齐，避免覆盖用户已经分别设置过的值。
    """
    if not isinstance(saved, dict) or "single_chapter" not in saved:
        return saved
    legacy = bool(saved.pop("single_chapter"))
    saved.setdefault("single_chapter_scene", legacy)
    saved.setdefault("single_chapter_write", legacy)
    return saved


# ─────────── Fernet 密钥 / 加解密 ───────────

def _load_or_create_secret_key() -> bytes:
    """返回本地 Fernet 密钥(bytes)。首次调用时生成并落盘。"""
    if os.path.exists(SECRET_KEY_FILE):
        with open(SECRET_KEY_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if raw:
            return raw.encode("ascii")
    # 生成新密钥
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    with open(SECRET_KEY_FILE, "w", encoding="utf-8") as f:
        f.write(key.decode("ascii") + "\n")
    return key


def _fernet():
    from cryptography.fernet import Fernet
    return Fernet(_load_or_create_secret_key())


def encrypt_api_key(plain: str) -> str:
    """明文 Key → Fernet 密文(字符串)。"""
    plain = (plain or "").strip()
    if not plain:
        return ""
    return _fernet().encrypt(plain.encode("utf-8")).decode("ascii")


def decrypt_api_key(token: str) -> str:
    """Fernet 密文 → 明文 Key。解密失败(密钥文件丢失/被改)返回空串。"""
    token = (token or "").strip()
    if not token:
        return ""
    try:
        return _fernet().decrypt(token.encode("ascii")).decode("utf-8")
    except Exception as e:
        print(f"[WARN] API Key 解密失败(密钥文件可能已丢失): {e}")
        return ""


# ─────────── 配置读写 ───────────

def load_config() -> dict:
    """加载配置。API Key 优先环境变量；否则解密配置文件中的密文。"""
    config = _default_config()

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            config.update(_migrate_single_chapter(saved))
        except (json.JSONDecodeError, IOError) as e:
            print(f"[WARN] 加载配置失败: {e}")

    # 解密 Key(兼容旧版明文文件：无 api_key_enc 标记时按明文处理)
    if config.get("api_key"):
        if config.get("api_key_enc"):
            config["api_key"] = decrypt_api_key(config["api_key"])
        # 否则保留明文(旧文件)继续使用；下次保存会自动转为密文

    # 兜底 Key 同样按密文解密
    if config.get("fallback_api_key") and config.get("fallback_api_key_enc"):
        config["fallback_api_key"] = decrypt_api_key(config["fallback_api_key"])

    # 环境变量优先覆盖
    env_key = os.environ.get("NOVEL_AI_API_KEY", "").strip()
    if env_key:
        config["api_key"] = env_key

    env_base = os.environ.get("NOVEL_AI_BASE_URL", "").strip()
    if env_base:
        config["base_url"] = env_base

    env_model = os.environ.get("NOVEL_AI_MODEL", "").strip()
    if env_model:
        config["model"] = env_model

    return config


def save_config(config: dict):
    """保存配置到文件。API Key 加密后落盘，文件内不出现明文。

    - 设置了环境变量 NOVEL_AI_API_KEY：Key 来源是环境变量，文件留空。
    - 否则传入的 api_key 若非空 → Fernet 加密后写入(api_key_enc=true)。
    """
    safe = dict(config)
    env_key = os.environ.get("NOVEL_AI_API_KEY", "").strip()
    key = (safe.get("api_key") or "").strip()

    if env_key:
        safe["api_key"] = ""
        safe["api_key_enc"] = False
    elif key:
        safe["api_key"] = encrypt_api_key(key)
        safe["api_key_enc"] = True
    else:
        safe["api_key"] = ""
        safe["api_key_enc"] = False

    # 兜底 Key：与主 Key 同一套加密落盘逻辑
    fb_key = (safe.get("fallback_api_key") or "").strip()
    if fb_key:
        safe["fallback_api_key"] = encrypt_api_key(fb_key)
        safe["fallback_api_key_enc"] = True
    else:
        safe["fallback_api_key"] = ""
        safe["fallback_api_key_enc"] = False

    # 仅保留已知字段，避免历史残留噪音
    known = {"api_key", "api_key_enc", "base_url", "model", "theme",
             "requirements", "chapters_count", "words_per_chapter",
             "single_chapter_scene", "single_chapter_write",
             "fallback_api_url", "fallback_api_key", "fallback_api_key_enc",
             "fallback_model", "fallback_context_limit", "fallback_proxy"}
    safe = {k: v for k, v in safe.items() if k in known}

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(safe, f, ensure_ascii=False, indent=2)


def mask_api_key(key: str) -> str:
    """脱敏显示 API Key"""
    if not key:
        return "未配置"
    if len(key) > 8:
        return key[:4] + "***" + key[-4:]
    return "***" if len(key) <= 4 else key[:2] + "***"
