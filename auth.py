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
SETUP_TOKEN_FILE = os.path.join(DATA_DIR, ".setup_token")

MIN_PASSWORD_LEN = 6

# 登录失败限流（内存滑动窗口，按来源 IP 计数）
LOGIN_MAX_FAILS = 5          # 窗口内允许的失败次数
LOGIN_WINDOW_SECONDS = 300   # 窗口长度 / 锁定时长

_lock = threading.RLock()
_users = None   # 内存缓存: {"username": {"hash": ..., "created_at": ...}}
_login_fails: dict = {}   # ip -> [失败时间戳, ...]
_login_lock = threading.Lock()


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


def _write_atomic(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    _chmod_600(path)


def setup_token() -> str:
    """首次初始化口令：随机生成并持久化，创建首个账号后失效。

    用途：/api/setup 要求携带该口令。口令只出现在服务启动日志与数据目录
    .setup_token 里，因此即便服务直接暴露公网，陌生访问者也无法抢先注册管理员。
    """
    try:
        with open(SETUP_TOKEN_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if raw:
            return raw
    except (FileNotFoundError, IOError):
        pass
    tok = secrets.token_urlsafe(24)
    _write_atomic(SETUP_TOKEN_FILE, tok + "\n")
    return tok


def verify_setup_token(token: str) -> bool:
    """恒定时间比较初始化口令。"""
    if not token:
        return False
    return secrets.compare_digest(str(token), setup_token())


def _drop_setup_token():
    try:
        os.remove(SETUP_TOKEN_FILE)
    except OSError:
        pass


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
        _drop_setup_token()   # 初始化完成 → 口令作废
    return True, ""


def login_block_remaining(key: str) -> int:
    """返回该来源还需等待的秒数；0 表示未被限制。

    滑动窗口：只统计最近 LOGIN_WINDOW_SECONDS 内的失败次数，
    达到 LOGIN_MAX_FAILS 即锁定，最早一次失败滑出窗口后自动解锁。
    """
    now = time.time()
    with _login_lock:
        fails = [t for t in (_login_fails.get(key) or [])
                 if now - t < LOGIN_WINDOW_SECONDS]
        if fails:
            _login_fails[key] = fails
        else:
            _login_fails.pop(key, None)
        if len(fails) >= LOGIN_MAX_FAILS:
            return max(1, int(LOGIN_WINDOW_SECONDS - (now - fails[0])) + 1)
        return 0


def record_login_failure(key: str):
    with _login_lock:
        _login_fails.setdefault(key, []).append(time.time())


def record_login_success(key: str):
    with _login_lock:
        _login_fails.pop(key, None)


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
