# The stream job and the producer share this image: the official Spark 4.1.3
# runtime, the Kafka connector and the Iceberg runtime (with its S3 client and
# the PostgreSQL driver for the JDBC catalog) baked into /opt/spark/jars, and
# the tributary package in a Python 3.12 venv (the base image ships Python 3.10).
FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv

FROM apache/spark:4.1.3-python3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211

USER root
ARG MAVEN=https://repo1.maven.org/maven2
# Connectors and their transitive dependencies that the base image lacks, pinned by
# sha256 (each checked against Maven Central's published SHA-1 when it was added).
RUN set -eux; cd /opt/spark/jars; \
    fetch() { curl -fsSL -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
        cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
        0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c; \
    fetch org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
        7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9; \
    fetch org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
        5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef; \
    fetch org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
        d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242; \
    fetch org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
        38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f; \
    fetch org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
        73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --python 3.12
COPY README.md LICENSE ./
COPY tributary ./tributary
RUN uv sync --locked --no-dev --no-editable --python 3.12
COPY data/sample ./data/sample

RUN mkdir -p /data/checkpoints && chown -R spark:spark /data
ENV PATH=/opt/venv/bin:$PATH \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    TRIBUTARY_SPARK_PACKAGES= \
    TRIBUTARY_SAMPLE_PATH=/app/data/sample/recentchange.jsonl.gz \
    TRIBUTARY_CHECKPOINT_DIR=/data/checkpoints/warehouse \
    TRIBUTARY_LAKE_CHECKPOINT_DIR=/data/checkpoints/lake \
    TRIBUTARY_LOG_FORMAT=json

USER spark
ENTRYPOINT ["tributary"]
CMD ["--help"]
