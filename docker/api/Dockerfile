# The serving layer: the tributary package with the `api` extra (FastAPI, uvicorn)
# and nothing else (no JVM, no dbt, no Airflow), installed from uv.lock.
FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv

FROM python:3.12.16-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/tributary
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project --extra api
COPY README.md LICENSE ./
COPY tributary ./tributary
# --reinstall-package: see docker/airflow/Dockerfile (uv's cached wheel of a local
# project is keyed on pyproject.toml alone).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --reinstall-package tributary-platform --extra api

RUN useradd --create-home --uid 10001 tributary
ENV PATH=/opt/venv/bin:$PATH \
    TRIBUTARY_CATALOG_DIR=/data/catalog \
    TRIBUTARY_LOG_FORMAT=json \
    TRIBUTARY_API_HOST=0.0.0.0
USER tributary
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]
ENTRYPOINT ["tributary"]
CMD ["serve"]
