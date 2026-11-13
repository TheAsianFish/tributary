# Airflow 3 (scheduler, API server, DAG processor) with dbt, the Trino client,
# PySpark with a Java 21 runtime and the Kafka connector (for the reconciliation
# pass raw_recentchange runs when the stream drops rows), and the tributary
# package, all installed from this repository's uv.lock. Not built
# on apache/airflow: that image brings its own constraint set, so the scheduler
# would run different versions of Airflow's dependencies (and of dbt and psycopg)
# than CI tests. Here the environment is exactly `uv sync --locked`.
FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv

FROM python:3.12.16-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

COPY --from=uv /uv /usr/local/bin/uv
# A headless JRE for local-mode Spark (Spark 4.1 supports Java 17 and 21; this
# Debian release ships 21), curl only for the connector jars below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
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
        --extra airflow --extra warehouse --extra lakehouse --extra spark
# The Kafka source PySpark lacks, pinned exactly as in docker/spark/Dockerfile and
# dropped into PySpark's own jars, so no package is resolved at run time.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/venv/lib/python3.12/site-packages/pyspark/jars; \
    fetch() { curl -fsSL -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
        cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
        0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c; \
    fetch org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
        7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9; \
    fetch org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
        5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef
COPY README.md LICENSE ./
COPY tributary ./tributary
# --reinstall-package: uv keys its cached wheel of a local project on pyproject.toml
# alone, so with the cache mount a rebuild after a code change would install the
# previous build's wheel and the scheduler would run stale code.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --reinstall-package tributary-platform \
        --extra airflow --extra warehouse --extra lakehouse --extra spark

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
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    TRIBUTARY_SPARK_PACKAGES=

USER airflow
WORKDIR /opt/airflow
ENTRYPOINT ["airflow"]
CMD ["version"]
