# Airflow 3 (scheduler, API server, DAG processor) with dbt, the Trino client and
# the tributary package, all installed from this repository's uv.lock. Not built
# on apache/airflow: that image brings its own constraint set, so the scheduler
# would run different versions of Airflow's dependencies (and of dbt and psycopg)
# than CI tests. Here the environment is exactly `uv sync --locked`.
FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv

FROM python:3.12.16-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/tributary
# Dependencies first, in their own layer: code changes do not reinstall Airflow.
COPY pyproject.toml uv.lock ./
# The uv cache lives in a BuildKit cache mount, not in the image.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project \
        --extra airflow --extra warehouse --extra lakehouse
COPY README.md LICENSE ./
COPY tributary ./tributary
# --reinstall-package: uv keys its cached wheel of a local project on pyproject.toml
# alone, so with the cache mount a rebuild after a code change would install the
# previous build's wheel and the scheduler would run stale code.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --reinstall-package tributary-platform \
        --extra airflow --extra warehouse --extra lakehouse

# The dbt project (sources only: no target/ or logs/ from a developer's machine;
# every run writes its artifacts to a temporary directory) and the DAGs.
COPY warehouse/dbt_project.yml warehouse/profiles.yml ./warehouse/
COPY warehouse/models ./warehouse/models
COPY warehouse/macros ./warehouse/macros
COPY warehouse/seeds ./warehouse/seeds
COPY warehouse/tests ./warehouse/tests
COPY orchestration/dags ./orchestration/dags

RUN useradd --create-home --uid 50000 --gid 0 airflow \
    && mkdir -p /opt/airflow /data/catalog \
    && chown -R airflow:0 /opt/airflow /data/catalog

# The package is installed non-editable, so paths relative to its source tree do
# not exist: point at the copied project explicitly.
ENV PATH=/opt/venv/bin:$PATH \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/tributary/orchestration/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    TRIBUTARY_DBT_PROJECT_DIR=/opt/tributary/warehouse \
    TRIBUTARY_CATALOG_DIR=/data/catalog \
    TRIBUTARY_LOG_FORMAT=json \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

USER airflow
WORKDIR /opt/airflow
ENTRYPOINT ["airflow"]
CMD ["version"]
