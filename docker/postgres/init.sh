#!/usr/bin/env bash
# Runs once, when the pgdata volume is first initialised (docker-entrypoint-initdb.d).
# The database for Airflow's metadata, the schemas every component expects, and
# the read-only role used by Trino's postgresql catalog (and the agent, from M3).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v reader="${TRIBUTARY_READER_USER:-tributary_reader}" \
    -v reader_password="${TRIBUTARY_READER_PASSWORD:-tributary_reader}" <<'SQL'
CREATE DATABASE airflow;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ops;
-- The Iceberg JDBC catalog keeps its two tables here (Spark and Trino share it).
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;

CREATE ROLE :"reader" LOGIN PASSWORD :'reader_password';
ALTER ROLE :"reader" SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE :"DBNAME" TO :"reader";
GRANT USAGE ON SCHEMA raw, staging, marts, ops TO :"reader";
GRANT SELECT ON ALL TABLES IN SCHEMA raw, staging, marts, ops TO :"reader";
-- Tables the stream job and dbt create later are readable too.
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, staging, marts, ops GRANT SELECT ON TABLES TO :"reader";
SQL
