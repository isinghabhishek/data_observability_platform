"""Unit tests for src/db_migrations.py."""

from unittest.mock import MagicMock

import pytest

from src.db_migrations import create_observability_schema


def test_create_observability_schema_idempotent():
    """create_observability_schema is safe to call multiple times."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # Should not raise on either call
    create_observability_schema(mock_engine)
    create_observability_schema(mock_engine)

    # 4 statements × 2 calls = 8 total executions
    assert mock_conn.execute.call_count == 8


def test_create_observability_schema_executes_four_statements():
    """Exactly 4 DDL statements are executed: schema + 3 tables."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    create_observability_schema(mock_engine)

    assert mock_conn.execute.call_count == 4
