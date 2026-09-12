# novel-webui · 小说远程生成器（WebUI）

基于 Grok/xAI 的四层递进生成管线（设定圣经 → 章节大纲 → 场景分解 → 正文写作），
自带后端服务、前端页面、鉴权与断点体系，**不依赖任何桌面端程序**。

## 核心能力

- **后端常驻**：提交任务后关闭页面也能跑完；生成全程在 worker 线程，Flask 只管状态存取。
- **实时流式**：WebSocket（`/ws`）实时推送正文、日志、待确认项。
- **各层确认不依赖前端**：引擎 `request_confirm()` 阻塞等待，倒计时结束自动确认原文，
  也可由前端在截止前提交「确认(可编辑)/重新生成/取消」。
- **断点续传**：一键继续上次生成，场景分解阶段也能续传。
- **拒答兜底**：主 API 连拒 3 次自动改用兜底模型补写（OpenAI 兼容，可跨机器）；兜底不可用才登记待处理。
- **内置登录鉴权**：首次启动用初始化口令创建管理员，之后按 session 登录（默认 30 天）。

## 快速开始

```bash
python -m venv venv
venv\Scripts\pip install -r requirements.txt      # Windows
# venv/bin/pip install -r requirements.txt        # Linux/macOS

setx NOVEL_AI_API_KEY sk-xxxx                     # Windows（或 export，Linux/macOS）
venv\Scripts\python server.py                     # 启动 → http://127.0.0.1:8000
```

**Windows 一键启动**：双击 `start.bat`（默认绑定 `0.0.0.0:8000` 并自动开浏览器；
可选参数 `start.bat [host] [port] [confirm秒]`；Key 优先读环境变量，其次读 `key.env`）。

**首次启动**：控制台会打印一行初始化口令（`🔐 初始化口令（setup token）：xxxx`，
也落盘在数据目录 `.setup_token`）。打开页面填入口令 + 管理员账号密码即完成初始化，
入口永久关闭、口令文件自动删除——即使服务暴露公网，陌生人也无法抢先注册管理员。

**忘记密码**：删掉数据目录里的 `auth_users.json` 后重启，可重新初始化（会生成新的口令）。

## 项目结构

```
novel-webui/
├── server.py          ← Flask 服务 + 全部 API + 鉴权守卫 + /ws 推送
├── auth.py            ← 初始化口令 / 账号校验 / 登录限流 / session 密钥
├── controller.py      ← 线程安全任务中枢 / 待确认项 / 日志环形缓冲
├── worker.py          ← 后台生成线程（驱动 NovelGenerator）
├── engine.py          ← 生成引擎（四层管线 + 拒答兜底调度）
├── config.py          ← 配置管理（API Key 经 Fernet 加密落盘）
├── state.py           ← 断点管理
├── static/index.html  ← 单页前端（WebSocket 推送）
├── static/login.html  static/setup.html
├── start.bat          key.env.example  requirements.txt
```

运行期数据目录（`NOVEL_DATA_DIR`，默认=代码目录；容器内 `/data`）主要文件：
配置 `novel_generator_config.json`（Key 为 Fernet 密文）、鉴权凭据 `auth_users.json` /
`.session_secret` / `.setup_token`、断点 `novel_resume_state.json` + 分片目录、
拒答列表 `novel_refusals.json`、成品 `<标题>_YYYYMMDD_HHMMSS.txt`。

## Docker 部署

镜像由 GitHub Actions 自动构建推送至 `ghcr.io/fz19870823/novel-webui`。

```bash
docker compose up -d                              # 推荐，成品落命名卷 novel_data
docker compose up -d --build                       # 本地改动后重建
docker compose logs -f novel-webui                 # 首次取初始化口令

# 或裸跑
docker run -d --name novel-webui -p 8000:8000 \
  -e NOVEL_AI_API_KEY=sk-xxxx -v novel_data:/data \
  ghcr.io/fz19870823/novel-webui:latest
```

浏览器打开 `http://主机IP:8000`，先用初始化口令创建管理员。小说、断点、配置、登录凭据
全部在 `/data`，升级镜像不丢数据；仅当删除命名卷才需重新初始化。

## 无审查兜底 API（OpenAI 兼容，可跨机器）

兜底地址按 OpenAI 兼容协议处理，本机或远端自建服务（Ollama / LM Studio / vLLM / one-api 等）都能填。
只填 `192.168.1.50:8000` 也能用，会自动补 `http://` 与 `/v1`。

| 配置键 | 说明 |
|---|---|
| `fallback_api_url` | OpenAI 兼容地址；**留空 = 禁用兜底** |
| `fallback_model` | 兜底模型名，可手填或用「拉取兜底模型」下拉 |
| `fallback_api_key` | 自建服务通常留空；非空同样加密落盘 |
| `fallback_context_limit` | 上下文预算，默认 64000，超限先用主 API 压缩提示词 |
| `fallback_proxy` | `auto`(默认) / `direct` / `system` |

`auto` 会对内网/本机地址直连、公网走系统代理——因为 Windows 上 SDK 默认 `trust_env=True`
会把 `127.0.0.1` 的请求也送给系统代理（Clash 等）导致 502，而部分机器直连公网又不通。
页面「拉取兜底模型」「测试兜底连接」均只针对兜底服务、不会带上主 API Key。

## 反向代理

前端走同源相对地址连接 ws/wss，反代只需正确转发 `/ws` 的 `Upgrade`/`Connection` 头即可。
nginx 参考：

```nginx
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
location /ws {
    proxy_pass http://127.0.0.1:8000/ws;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}
```

Caddy 自动处理 WS 升级，无需额外配置；云端 CDN 需在面板开启 WebSocket。

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/` | 前端页面（未初始化→设置页，未登录→登录页） |
| GET  | `/api/auth_state` | 鉴权状态（公开） |
| POST | `/api/setup` · `/api/login` · `/api/logout` | 初始化 · 登录 · 退出 |
| GET/POST | `/api/config` | 读取 / 保存配置（不含 Key） |
| POST | `/api/test` · `/api/models` | 测试连接 · 拉取模型列表（`local:true` 走兜底服务） |
| POST | `/api/start` · `/api/resume` · `/api/stop` · `/api/confirm` | 提交 · 续传 · 停止 · 应答确认 |
| GET  | `/api/status` · `/api/logs` | 状态 / 增量日志（兼容保留，前端已走 /ws） |
| GET  | `/api/files` · `/api/download/xxx.txt` | 成品列表 · 下载 |
| POST | `/api/files/delete` | 删除成品（`{names:[...]}` → `{deleted,failed}`） |

除 `auth_state` / 登录 / 初始化外，所有页面、`/api/*`、`/ws` 均需登录；
`/ws` 握手还会校验 `Origin` 同源。

## 环境变量

| 变量 | 说明 |
|---|---|
| `NOVEL_AI_API_KEY` | 主 API Key（优先于配置文件，此时配置文件留空） |
| `NOVEL_DATA_DIR` | 运行期数据目录（默认=代码目录，容器内 `/data`） |
| `NOVEL_TRUST_PROXY=1` | 反代后按 `X-Forwarded-For` 真实 IP 做登录限流（默认关闭） |
| `NOVEL_COOKIE_SECURE=1` | HTTPS 部署时给 session cookie 加 `Secure` |

同一来源 IP 5 分钟内连续登录失败 5 次即锁定 5 分钟（429）。
