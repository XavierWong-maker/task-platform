FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# 1) 依赖层：只依赖 pyproject.toml，代码变更不会让这一层失效
COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" > /tmp/requirements.txt \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# 2) 代码层
COPY src ./src

# 3) 非 root 运行；data/ 供 SQLite 与 ETL 输出写入
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/data \
    && chown -R app:app /app/data
USER app

EXPOSE 8000

# 默认角色是 API；dispatch / worker 在 Compose 里覆盖 command
CMD ["uvicorn", "task_platform.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
