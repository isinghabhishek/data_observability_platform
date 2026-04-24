"""FastAPI app for SLA Monitor HTTP endpoints."""

from __future__ import annotations

import dataclasses
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException

from src.sla_monitor import SLAMonitor

logger = logging.getLogger(__name__)

# Module-level references populated during lifespan startup
_sla_monitor: SLAMonitor | None = None
_scheduler: BackgroundScheduler | None = None


def _load_sla_config(config_path: Path) -> list[dict]:
    """Load dataset SLA config from a YAML file.

    Returns an empty list if the file does not exist.
    """
    if not config_path.exists():
        logger.warning("sla_config.yml not found at %s — using empty config", config_path)
        return []
    with config_path.open() as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("datasets", [])


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Startup / shutdown lifecycle for the FastAPI app."""
    global _sla_monitor, _scheduler

    marquez_url = os.environ.get("MARQUEZ_URL", "http://marquez:5000")
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "data_observability_platform")
    db_url = os.environ.get("WAREHOUSE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    poll_interval = int(os.environ.get("SLA_POLL_INTERVAL_MINUTES", "60"))

    config_path = Path(os.environ.get("SLA_CONFIG_PATH", "sla_config.yml"))
    sla_config = _load_sla_config(config_path)

    _sla_monitor = SLAMonitor(
        marquez_url=marquez_url,
        namespace=namespace,
        sla_config=sla_config,
        db_url=db_url,
    )

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        _sla_monitor.evaluate_all,
        trigger="interval",
        minutes=poll_interval,
        id="evaluate_all",
    )
    _scheduler.start()
    logger.info("APScheduler started — polling every %d minutes", poll_interval)

    yield

    # Shutdown
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("APScheduler stopped")


app = FastAPI(title="SLA Monitor", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/freshness")
def get_all_freshness() -> list[dict[str, Any]]:
    """Return freshness status for all monitored datasets."""
    if _sla_monitor is None:
        return []
    return [dataclasses.asdict(s) for s in _sla_monitor.get_all_statuses()]


@app.get("/freshness/{name:path}")
def get_freshness(name: str) -> dict[str, Any]:
    """Return freshness status for a single dataset, or 404 if not found."""
    if _sla_monitor is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    status = _sla_monitor.get_status(name)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    return dataclasses.asdict(status)
