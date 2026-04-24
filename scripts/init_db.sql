-- Initialise the observability schema and tables on first Postgres startup.
-- This runs automatically via docker-entrypoint-initdb.d.

CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.sla_status (
    dataset_name     TEXT NOT NULL,
    namespace        TEXT NOT NULL,
    state            TEXT NOT NULL,
    last_run_time    TIMESTAMPTZ,
    elapsed_seconds  FLOAT,
    sla_seconds      FLOAT NOT NULL,
    evaluated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_name, namespace)
);

CREATE TABLE IF NOT EXISTS observability.contract_violations (
    id               SERIAL PRIMARY KEY,
    dataset_name     TEXT NOT NULL,
    violation_type   TEXT NOT NULL,
    column_name      TEXT,
    expected         TEXT,
    observed         TEXT,
    null_count       INTEGER,
    run_timestamp    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS observability.alert_delivery_failures (
    id               SERIAL PRIMARY KEY,
    alert_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    attempts         INTEGER NOT NULL,
    last_attempt_at  TIMESTAMPTZ NOT NULL
);
