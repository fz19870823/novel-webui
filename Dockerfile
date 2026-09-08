# syntax=docker/dockerfile:1
# novel-webui — Flask 小说生成 Web 服务
# 构建: docker build -t novel-webui .
# 运行: docker run -d -p 8000:8000 -v novel_data:/data -e NOVEL_AI_API_KEY=sk-xxx novel-webui
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NOVEL_WEBUI_HOST=0.0.0.0 \
    NOVEL_WEBUI_PORT=8000 \
    # 运行期数据（成品小说/断点/本地配置）统一写入 /data 卷，代码层保持只读
    NOVEL_DATA_DIR=/data

WORKDIR /app

# 依赖层单独缓存（requirements 不变则复用镜像层）
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 非 root 运行
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data

COPY --chown=appuser:appuser . .

# /data 为运行期数据卷（compose 里挂 novel_data；裸跑用 -v novel_data:/data）
VOLUME ["/data"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/auth_state', timeout=4).status == 200 else 1)" || exit 1

USER appuser

CMD ["python", "server.py"]
