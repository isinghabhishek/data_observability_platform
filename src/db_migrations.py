"""Warehouse schema migrations for the Data Observability Platform."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def create_observability_schema(engine: Engine) -> None:
    """Create the observability schema and all platform tables if they don't exist.

    Creates:
    - observability schema
    - observability.sla_status (upsert target for SLA_Monitor)
    - observability.contract_violations (append target for Contract_Enforcer)
    - observability.alert_delivery_failures (dead-letter for Alert_Manager)

    Safe to call multiple times — uses CREATE IF NOT EXISTS throughout.
    """
    statements = [
        # Schema
        "CREATE SCHEMA IF NOT EXISTS observability",

        # SLA status table
        """
        CREATE TABLE IF NOT EXISTS observability.sla_status (
            dataset_name     TEXT NOT NULL,
            namespace        TEXT NOT NULL,
            state            TEXT NOT NULL,
            last_run_time    TIMESTAMPTZ,
            elapsed_seconds  FLOAT,
            sla_seconds      FLOAT NOT NULL,
            evaluated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (dataset_name, namespace)
        )
        """,

        # Contract violations table
        """
        CREATE TABLE IF NOT EXISTS observability.contract_violations (
            id               SERIAL PRIMARY KEY,
            dataset_name     TEXT NOT NULL,
            violation_type   TEXT NOT NULL,
            column_name      TEXT,
            expected         TEXT,
            observed         TEXT,
            null_count       INTEGER,
            run_timestamp    TIMESTAMPTZ NOT NULL
        )
        """,

        # Alert delivery failures table
        """
        CREATE TABLE IF NOT EXISTS observability.alert_delivery_failures (
            id               SERIAL PRIMARY KEY,
            alert_type       TEXT NOT NULL,
            payload          JSONB NOT NULL,
            attempts         INTEGER NOT NULL,
            last_attempt_at  TIMESTAMPTZ NOT NULL
        )
        """,
    ]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    logger.info("Observability schema and tables created/verified successfully")
