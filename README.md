# novel-webui · 小说远程生成器（WebUI）

基于 **novel-ai**（Grok/xAI 四层递进生成管线 + 自适应优化）的**远程可操作 Web 版**。
引擎代码从 novel-ai 复制而来，本仓库为完全独立项目。

## 与桌面版(GUI)的关键差异

| 能力 | PySide6 桌面版 | novel-webui |
|---|---|---|
| 界面 | 桌面窗口 | 浏览器（本机/局域网/远程均可） |
| 后台运行 | 依赖窗口常开 | **后端常驻，提交任务后关闭页面也能跑完** |
| 实时流式正文 | 窗口内 | 页面轮询实时刷新 |
| 各层确认 | 模态对话框 | 页面弹窗 + 倒计时自动确认 |
| 断点续传 | 按钮 | 一键继续上次生成 |

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

### 一键启动

Windows 双击 **`start.bat`** 即可：
- 自动 `cd` 到项目目录并复用 `venv`
- 默认绑定 `0.0.0.0:8000`（局域网/远程设备可访问），并自动打开本机浏览器
- 可选参数：`start.bat [host] [port] [confirm秒]`
- API Key 自动读取：优先系统环境变量 `NOVEL_AI_API_KEY`，否则读 `key.env`（复制 `key.env.example` 填入即可）
- 服务运行时保持窗口打开，关闭窗口即停止

### 远程访问

```bash
# 局域网/远程（注意：无鉴权，仅限可信网络或自行加反代鉴权）
venv\Scripts\python server.py --host 0.0.0.0 --port 8000

# 自定义确认倒计时（秒）
venv\Scripts\python server.py --confirm 10
```

> ⚠️ 暴露到公网前请务必加访问鉴权（如反代 Basic Auth / 内网穿透带口令），
> 防止他人滥用你的 API Key 造成扣费。

## 项目结构

```
novel-webui/
├── server.py          ← Flask 服务 + 全部 API
├── controller.py      ← 线程安全任务中枢 / 待确认项 / 日志环形缓冲
├── worker.py          ← 后台生成线程（驱动 NovelGenerator）
├── engine.py          ← 生成引擎（复制自 novel-ai）
├── config.py          ← 配置管理（复制，Key 不落盘）
├── state.py           ← 断点管理（复制）
├── static/index.html  ← 单页前端
├── start.bat          ← 一键启动（Windows，自动开浏览器）
├── key.env.example    ← API Key 明文文件模板（可选，start.bat 会读取）
├── .gitignore
├── requirements.txt
└── README.md
```

## 设计要点

- **确认不依赖前端**：各层确认由引擎线程 `request_confirm()` 阻塞等待，二选一放行
  ——① 倒计时结束自动确认原文；② 前端在截止前提交 确认(可编辑)/重新生成/取消。
  因此即使没有任何浏览器在线，流程也会自动推进到底。
- **纯后端完成**：生成全程在 `worker` 线程跑，Flask 只做状态存取，前端纯轮询。
- **API Key 安全**：与 novel-ai 一致，Key 不写盘，优先环境变量 `NOVEL_AI_API_KEY`。
- **同一时刻仅一个任务**：/api/start 有并发锁，防重复提交。

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

- 浏览器打开 `http://主机IP:8000`（无鉴权，公网请自行加反代鉴权）。
- API Key 建议用 `NOVEL_AI_API_KEY` 环境变量注入；页面填写则 Fernet 加密落盘到数据卷。
- 生成的小说、断点、本地配置全部持久化在 `/data`（compose 卷 `novel_data`），
  升级镜像不丢数据。查看成品：`docker compose exec novel-webui ls /data`。
- 服务健康检查：`docker inspect --format '{{.State.Health.Status}}' novel-webui`。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/` | 前端页面 |
| GET  | `/api/config` | 读取配置(不含 Key) |
| POST | `/api/config` | 保存配置 |
| POST | `/api/test` | 测试连接 |
| POST | `/api/models` | 拉取模型列表 |
| POST | `/api/start` | 提交新生成任务 |
| POST | `/api/resume` | 断点续传 |
| POST | `/api/stop` | 请求停止 |
| POST | `/api/confirm` | 应答待确认项 |
| GET  | `/api/status?content=1` | 状态+确认项+正文尾部 |
| GET  | `/api/logs?since=N` | 增量日志 |
| GET  | `/api/files` | 成品列表 |
| GET  | `/api/download/xxx.txt` | 下载成品 |
