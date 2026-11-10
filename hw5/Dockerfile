FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/hw5/app

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project \
    --no-install-package matplotlib --no-install-package pandas \
    --no-install-package duckdb --no-install-package kazoo --no-install-package psutil
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
    --no-install-package matplotlib --no-install-package pandas \
    --no-install-package duckdb --no-install-package kazoo --no-install-package psutil
COPY hw5/app ./hw5/app

USER 10001
ENTRYPOINT ["python", "-m", "k8spairs"]
