# novel-webui · 小说远程生成器（WebUI）

基于 Grok/xAI 的四层递进生成管线（设定圣经 → 章节大纲 → 场景分解 → 正文写作 + 自适应优化），
**完全独立的项目**：自带后端服务、前端页面、鉴权与断点体系，不依赖任何桌面端程序。

## 核心能力

| 能力 | 说明 |
|---|---|
| 界面 | 浏览器（本机/局域网/远程均可） |
| 后台运行 | **后端常驻，提交任务后关闭页面也能跑完** |
| 实时流式正文 | WebSocket 实时推送刷新 |
| 各层确认 | 页面弹窗 + 倒计时自动确认 |
| 断点续传 | 一键继续上次生成（**场景分解阶段也能续传**） |
| 拒答兜底 | 连续拒答自动改用**兜底模型**补写（OpenAI 兼容，支持其他机器的自建服务）；兜底不可用才登记待处理项 |
| 访问控制 | **内置登录鉴权**（首次启动设置账号 + 初始化口令） |

## 快速开始

```bash
# 1) 建 venv 并安装
python -m venv venv
venv\Scripts\pip install -r requirements.txt        # Windows
# venv/bin/pip install -r requirements.txt          # Linux/macOS

# 2) 配 API Key（推荐，避免明文落盘）
# Windows:
setx NOVEL_AI_API_KEY sk-xxxx
# 或 Linux/macOS:
export NOVEL_AI_API_KEY=sk-xxxx

# 3) 启动（本机访问）
venv\Scripts\python server.py
# 浏览器打开 http://127.0.0.1:8000
```

### 首次启动（登录鉴权）

服务默认开启**账号密码鉴权**：

1. 首次启动时控制台会打印一行**初始化口令（setup token）**，形如：

   ```
   🔐 初始化口令（setup token）：V1a2b3C4d5E6f7G8h9I0jK
   ```

   该口令同时持久化在数据目录 `.setup_token`（也可用 `docker compose logs novel-webui | grep 初始化口令` 获取）。
2. 打开页面会进入「首次初始化」页 → 填入**口令 + 管理员账号密码**
   （密码只存 hash，不落明文），保存后初始化入口**永久关闭**、口令文件自动删除。
   > 口令的意义：即使服务直接暴露在公网，陌生访问者也**无法抢先注册管理员**。
3. 此后每次访问都需登录；会话默认保持 30 天。
4. 凭据文件 `auth_users.json`、session 密钥 `.session_secret`、初始化口令 `.setup_token`
   都保存在**运行期数据目录**（见下方 `NOVEL_DATA_DIR` / Docker `/data` 卷），重启服务或容器后继续生效。

登录失败限流：同一来源 IP 在 5 分钟内连续失败 5 次即锁定 5 分钟（返回 429）。
若服务在反向代理之后，需设置 `NOVEL_TRUST_PROXY=1` 才会按 `X-Forwarded-For` 里的**真实客户端 IP** 计数，
否则所有请求都算在代理 IP 上（默认关闭——直连暴露时 XFF 可伪造，贸然信任等于免限流）。

忘记密码：删除数据目录里的 `auth_users.json` 后重启服务，可重新走首次初始化
（Docker：`docker compose exec novel-webui rm /data/auth_users.json && docker compose restart novel-webui`；
重启后会生成**新的初始化口令**）。

### 一键启动

Windows 双击 **`start.bat`** 即可：
- 自动 `cd` 到项目目录并复用 `venv`
- 默认绑定 `0.0.0.0:8000`（局域网/远程设备可访问），并自动打开本机浏览器
- 可选参数：`start.bat [host] [port] [confirm秒]`
- API Key 自动读取：优先系统环境变量 `NOVEL_AI_API_KEY`，否则读 `key.env`（复制 `key.env.example` 填入即可）
- 服务运行时保持窗口打开，关闭窗口即停止（初始化口令就在这个窗口里）

### 远程访问

```bash
# 局域网/远程（内置账号鉴权；公网使用请务必设置强密码）
venv\Scripts\python server.py --host 0.0.0.0 --port 8000

# 自定义确认倒计时（秒）
venv\Scripts\python server.py --confirm 10
```

> 服务内置登录鉴权（首次启动需初始化口令 + 设置管理员账号）。仍建议：
> ① 用强密码；② 公网部署叠加反代 TLS；③ 不要把 `auth_users.json` /
> `.session_secret` / `.setup_token` 提交到任何代码仓库或镜像层。

## 项目结构

```
novel-webui/
├── server.py          ← Flask 服务 + 全部 API + 登录鉴权守卫 + /ws 推送
├── auth.py            ← 首次初始化口令 / 账号校验 / 登录限流 / session 密钥持久化
├── controller.py      ← 线程安全任务中枢 / 待确认项 / 日志环形缓冲 / 变更通知
├── worker.py          ← 后台生成线程（驱动 NovelGenerator）
├── engine.py          ← 生成引擎（四层递进管线 + 拒答兜底调度）
├── config.py          ← 配置管理（API Key 经 Fernet 加密落盘，不出现明文）
├── state.py           ← 断点管理（进度文件 + 章节/静态分片，见下）
├── static/index.html  ← 单页前端（WebSocket 推送）
├── static/login.html  ← 登录页
├── static/setup.html  ← 首次初始化页（需初始化口令）
├── start.bat          ← 一键启动（Windows，自动开浏览器）
├── key.env.example    ← API Key 明文文件模板（可选，start.bat 会读取）
├── .gitignore
├── requirements.txt
└── README.md
```

运行期数据目录（`NOVEL_DATA_DIR`，默认=代码目录；容器内 `/data`）内会生成：

```
novel_generator_config.json     配置（主/兜底 API Key 均为 Fernet 密文，环境变量注入时文件留空）
.model_secret                   Fernet 密钥
auth_users.json / .session_secret / .setup_token   鉴权凭据与密钥
novel_resume_state.json         断点进度（小文件，每次保存都写）
novel_resume_static/<hash>.json 断点静态部分（大纲/场景/设定圣经，内容不变则复用）
novel_resume_chapters/<hash>.json 断点章节正文分片（内容寻址）
novel_refusals.json             拒答待处理列表（含发送内容与拒答原文）
<标题>_YYYYMMDD_HHMMSS.txt      生成的小说成品
```

## 设计要点

- **确认不依赖前端**：各层确认由引擎线程 `request_confirm()` 阻塞等待，二选一放行
  ——① 倒计时结束自动确认原文；② 前端在截止前提交 确认(可编辑)/重新生成/取消。
  因此即使没有任何浏览器在线，流程也会自动推进到底。
- **纯后端完成**：生成全程在 `worker` 线程跑，Flask 只做状态存取，前端经 `/ws` 实时推送。
- **API Key 安全**：Key 优先由环境变量 `NOVEL_AI_API_KEY` 注入（此时配置文件留空）；
  页面填写的 Key 经 Fernet 加密后落盘，文件里不出现明文。
- **同一时刻仅一个任务**：/api/start 有并发锁，防重复提交。
- **内置鉴权**：除首次初始化/登录页与对应 4 个 API 外，所有页面、`/api/*`、`/ws`
  均需登录（session cookie）；凭据与签名密钥落在数据目录，重启不丢。
  session cookie 为 `SameSite=Lax + HttpOnly`（HTTPS 部署可加 `NOVEL_COOKIE_SECURE=1`）；
  `/ws` 握手还会校验 `Origin` 同源（防跨站 WebSocket 劫持）。
- **断点分片存储**：layer4 每写完一章都会保存断点。旧实现把全部章节正文 + 场景 + 设定
  塞进单个 JSON 全量重写，写盘量随进度呈 O(N²)（实测 60 章累计 12.4MB）；现改为
  「小进度文件 + 内容寻址的章节/静态分片」，只写变化的部分，同样场景实测 **0.38MB（33×）**，
  且元信息为原子写（临时文件 + rename），崩溃不会留下半截 JSON。
- **实时推送节流**：引擎每个流式 chunk 都会触发变更通知，服务端按最小帧间隔（100ms）合并，
  最多 10 帧/秒，避免逐 token 全量推送；空闲每 15s 发心跳保活。

## 无审查兜底 API（OpenAI 兼容，可跨机器）

主 API 连续拒答 3 次后，自动改用兜底模型补写当前部分：
兜底成功即直接采用兜底产出（不登记拒答）；兜底未配置或失败，才照旧登记进「拒答待处理」列表。

兜底地址按 **OpenAI 兼容协议**处理，可以是本机服务，也可以是**其他机器上的自建服务**：
Ollama / LM Studio / llama.cpp / vLLM / one-api / new-api 等都能直接填。

配置项（页面「API 配置」区，或配置文件同名键）：

| 键 | 说明 |
|---|---|
| `fallback_api_url` | OpenAI 兼容地址，如本机 `http://127.0.0.1:11434/v1`、远端 `http://192.168.1.50:8000/v1`；**留空 = 禁用兜底** |
| `fallback_model` | 兜底模型名（如 `qwen3:14b`），可手动输入或用「拉取兜底模型」下拉选择 |
| `fallback_api_key` | 自建服务通常留空；非空同样 Fernet 加密落盘 |
| `fallback_context_limit` | 兜底服务上下文预算，默认 **64000**（按 CJK 字符 + 英文词近似估算） |
| `fallback_proxy` | 代理模式：`auto`（默认）/ `direct` / `system`，见下 |

- **地址自动规范化**：只填 `192.168.1.50:8000` 也能用 —— 自动补 `http://`，路径为空时自动补 `/v1`。
  显式写了路径的（`/v1`、`/openai/v1`）原样保留。报错信息里出现 404 时会提示检查 `/v1`。
- **代理模式**（`fallback_proxy`）：
  | 值 | 行为 |
  |---|---|
  | `auto`（默认） | 内网/本机地址**直连**（回环、私网 IP、无点短名、`.local/.lan/.internal`，以及解析到私网的内网域名）；公网地址照常走系统代理 |
  | `direct` | 强制直连，绝不走代理 |
  | `system` | 强制遵循系统/环境代理 |
  为什么要分：Windows 上 `getproxies()` 会读到系统代理（如 Clash `127.0.0.1:7890`），SDK 默认
  `trust_env=True` 会把 `127.0.0.1` 的请求也送给代理，代理回连不了本地服务 → 直接 502；
  而公网自建服务在部分机器上直连出网不通，又必须走代理。`auto` 同时覆盖这两种情况。
- **模型列表与主 API 完全独立**：页面「拉取兜底模型」只查兜底服务的 `GET {fallback_api_url}/models`，
  结果填进兜底 Model 的下拉框，不影响主 API 的模型列表；
  该请求走 `local:true`，**不会把主 API Key 发给兜底服务**，空地址会明确报错。
- **超长提示词先压缩**：提示词超过 `fallback_context_limit` 时，先用主 API 压缩
  （保留字数等硬性要求、`@@第N章@@` 格式标记、人物与情节要点），再发送给兜底模型；
  压缩失败或仍超限则放弃兜底，回落到拒答登记。
- 自建模型 prompt eval 慢，兜底调用单独放宽超时（首块 300s / 块间 60s），
  且输出同样过质量闸（空内容或疑似拒答会重试一次）。
- 断点里会记录兜底配置（不含 Key），续传与「拒答待处理」重新提交补写同样生效。
- 页面「测试兜底连接」按钮对应 `POST /api/test` 的 `local:true`（自建服务无 Key 也合法）。

## Docker 部署

镜像由 GitHub Actions 自动构建并推送至 `ghcr.io/fz19870823/novel-webui`
（`latest` = main 分支最新；每次 push 额外打 `<sha>` 标签）。

```bash
# 方式一：docker compose（推荐，成品落命名卷 novel_data）
docker compose up -d            # 首次构建或拉取镜像
docker compose up -d --build    # 本地改动代码后重建
docker compose logs -f novel-webui

# 方式二：裸跑
docker run -d --name novel-webui -p 8000:8000 \
  -e NOVEL_AI_API_KEY=sk-xxxx \
  -v novel_data:/data \
  ghcr.io/fz19870823/novel-webui:latest
```

- 浏览器打开 `http://主机IP:8000`，**首次访问先用初始化口令创建管理员账号**，之后需登录。
  口令获取：`docker compose logs novel-webui | grep 初始化口令`。
- API Key 建议用 `NOVEL_AI_API_KEY` 环境变量注入；页面填写则 Fernet 加密落盘到数据卷。
- 生成的小说、断点、本地配置、**登录凭据**全部持久化在 `/data`（compose 卷 `novel_data`），
  升级镜像不丢数据。查看成品：`docker compose exec novel-webui ls /data`。
- 服务健康检查：`docker inspect --format '{{.State.Health.Status}}' novel-webui`。
- 升级后登录失效的情况：仅当你删掉了命名卷 `novel_data`（连凭据一起没了），
  重新初始化即可；正常 `docker compose pull && docker compose up -d` 升级不受影响。

## 反向代理（WebSocket）

前端实时显示走 **`/ws`** WebSocket（状态/日志/正文/待确认项推送），已移除原来的 GET 轮询。
页面通过**同源相对地址**连接 ws/wss，因此任何反代只要正确转发 `/ws` 的
`Upgrade`/`Connection` 头即可，前端无需改地址（https 下自动用 wss）。

```nginx
# nginx：HTTP 与 WS 走同一 server
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
location /ws {
    proxy_pass http://127.0.0.1:8000/ws;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;      # ← WS 升级关键
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;                    # 服务端每 15s 心跳，这里配长即可
    proxy_send_timeout 3600s;
}
```

```caddy
# Caddy：自动处理 WebSocket Upgrade，无需额外配置
example.com {
    reverse_proxy 127.0.0.1:8000
}
```

> - 云端 CDN（Cloudflare 等）需在面板开启 WebSocket 支持；服务端空闲时会每 15s 发送
>   心跳帧 `{"type":"ping"}`（浏览器回 `pong`），避免反代/中间层因空闲断连。
> - 反代下若要按真实客户端 IP 做登录限流，给容器/进程加 `NOVEL_TRUST_PROXY=1`
>   （仅在反代会**覆盖**而非透传 `X-Forwarded-For` 时才安全）。
> - `/ws` 会校验 `Origin` 与请求 Host 的**主机名**一致（忽略端口与协议，
>   以便 TLS 终结与端口映射场景正常工作）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/` | 前端页面（未初始化→首次设置页，未登录→登录页） |
| GET  | `/setup.html` · `/login.html` | 首次初始化页 · 登录页（公开） |
| GET  | `/api/auth_state` | 鉴权状态 `{setup_required, authed}`（公开） |
| POST | `/api/setup` | 首次初始化（需初始化口令，仅可调用一次） |
| POST | `/api/login` · `/api/logout` | 登录 · 退出（失败超限返回 429） |
| GET  | `/api/config` | 读取配置(不含 Key) |
| POST | `/api/config` | 保存配置 |
| POST | `/api/test` | 测试连接（`local:true` 时测兜底/自建服务，允许无 Key） |
| POST | `/api/models` | 拉取模型列表（`local:true` 时拉兜底/自建服务的模型，不会带上主 API Key） |
| POST | `/api/start` | 提交新生成任务 |
| POST | `/api/resume` | 断点续传 |
| POST | `/api/stop` | 请求停止 |
| POST | `/api/confirm` | 应答待确认项 |
| GET  | `/api/status?content=1` | 状态+确认项+正文尾部（兼容保留，前端已走 /ws） |
| GET  | `/api/logs?since=N` | 增量日志（兼容保留，前端已走 /ws） |
| GET  | `/api/files` | 成品列表 |
| POST | `/api/files/delete` | 删除成品（单个/批量，`{names:[...]}`；返回 `{deleted,failed}`） |
| GET  | `/api/download/xxx.txt` | 下载成品 |

> 除标记「公开」的路径外，其余全部需要登录（session cookie）；`/ws` 未登录在握手阶段即拒绝。
> `/api/files/delete` 只接受成品命名（`标题_时间戳.txt` / `novel_*.txt` / `log_*.txt`），
> 逐个处理并校验解析后路径仍在数据目录内，连同同名 `.bak` 备份一起清理；部分失败不影响其余文件。

## WebSocket 推送协议

前端与 `/ws` 之间的帧（JSON）：

| 方向 | 帧 | 说明 |
|---|---|---|
| 服务端→客户端 | `{"type":"init", status, logs, content, clen}` | 连接建立即推送全量快照 |
| 服务端→客户端 | `{"type":"diff", status?, logs?, seq?, content?, clen?}` | 增量：只含变化的字段 |
| 服务端→客户端 | `{"type":"ping"}` | 空闲 15s 心跳，防反代断连 |
| 客户端→服务端 | `{"type":"pong"}` / `{"type":"ping"}` | 应答心跳 / 探测存活（回 `pong`） |

`content` 是实时正文的**尾部 2000 字**（避免整段重传），`clen` 是**全文长度**——
字数显示请用 `clen`，用 `content.length` 会封顶在 2000。
