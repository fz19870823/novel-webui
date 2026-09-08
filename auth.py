"""
novel-webui 鉴权模块

首次启动强制初始化：创建唯一管理员账号（密码只存 hash，不落明文）。
凭据文件 auth_users.json 与 session 签名密钥 .session_secret 均落在运行期
数据目录（DATA_DIR，容器内为 /data 卷）→ 重启服务/容器后继续生效，
每次访问都要求用同一账号登录。

忘记密码：删除 auth_users.json（容器内 docker compose exec novel-webui
rm /data/auth_users.json）后重启，即可重新走首次初始化。
"""

import json
import os
import secrets
import threading
import time

from werkzeug.security import generate_password_hash, check_password_hash

from config import DATA_DIR

AUTH_FILE = os.path.join(DATA_DIR, "auth_users.json")
SESSION_SECRET_FILE = os.path.join(DATA_DIR, ".session_secret")

MIN_PASSWORD_LEN = 6

_lock = threading.RLock()
_users = None   # 内存缓存: {"username": {"hash": ..., "created_at": ...}}


def _load() -> dict:
    """懒加载用户表。文件缺失/损坏视为空（可重新初始化）。"""
    global _users
    if _users is None:
        try:
            with open(AUTH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            _users = data.get("users") or {}
        except (FileNotFoundError, json.JSONDecodeError, IOError):
            _users = {}
    return _users


def _chmod_600(path: str):
    """尽量收紧权限（Windows 上 chmod 无实际作用，忽略即可）。"""
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _persist():
    """原子落盘（临时文件 + rename），并同步内存缓存。"""
    tmp = AUTH_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "users": _users}, f,
                  ensure_ascii=False, indent=2)
    os.replace(tmp, AUTH_FILE)
    _chmod_600(AUTH_FILE)


def has_users() -> bool:
    return bool(_load())


def create_user(username: str, password: str) -> tuple[bool, str]:
    """创建唯一管理员账号。已存在任意用户则拒绝（首次初始化只能做一次）。

    返回 (ok, err_message)。
    """
    global _users
    username = (username or "").strip()
    if not (2 <= len(username) <= 32) or \
            any(ch.isspace() or ord(ch) < 32 for ch in username):
        return False, "用户名需 2-32 个字符且不含空格"
    password = password or ""
    if len(password) < MIN_PASSWORD_LEN:
        return False, f"密码至少 {MIN_PASSWORD_LEN} 位"
    with _lock:
        if _load():   # 二次检查：并发首设时后到者应被拒
            return False, "已初始化过，请直接登录"
        _users = {username: {
            "hash": generate_password_hash(password),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }}
        _persist()
    return True, ""


def verify_user(username: str, password: str) -> bool:
    """校验账号密码（登录）。失败统一返回 False，不泄露具体原因。"""
    rec = _load().get((username or "").strip())
    if not rec:
        return False
    try:
        return check_password_hash(rec.get("hash", ""), password or "")
    except Exception:
        return False


def session_secret() -> str:
    """返回稳定的 session 签名密钥（首次生成并持久化）。

    密钥不变 → 重启后已登录会话不失效；容器重建（/data 卷保留）同理。
    """
    if os.path.exists(SESSION_SECRET_FILE):
        try:
            with open(SESSION_SECRET_FILE, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            if raw:
                return raw
        except IOError:
            pass
    key = secrets.token_hex(32)
    with open(SESSION_SECRET_FILE, "w", encoding="utf-8") as f:
        f.write(key + "\n")
    _chmod_600(SESSION_SECRET_FILE)
    return key
