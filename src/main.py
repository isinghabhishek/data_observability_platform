"""Main entry point for the Data Observability Platform.

Startup sequence:
1. Validate all required environment variables
2. Create observability schema and tables
3. Load SLA config
4. Instantiate all services (SLAMonitor, ContractEnforcer, AlertManager)
5. Wire contract violations → AlertManager
6. Wire SLA breaches → AlertManager
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml
from sqlalchemy import create_engine

from src.config import REQUIRED_VARS, load_config, validate_env
from src.alert_manager import AlertManager
from src.contract_enforcer import ContractEnforcer
from src.db_migrations import create_observability_schema
from src.sla_monitor import SLAMonitor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _load_sla_config(path: Path) -> list[dict]:
    if not path.exists():
        logger.warning("sla_config.yml not found at %s — SLA monitoring disabled", path)
        return []
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("datasets", [])


def build_services(config: dict) -> tuple[SLAMonitor, ContractEnforcer, AlertManager]:
    """Instantiate and return all platform services."""
    db_url = config["WAREHOUSE_URL"]
    engine = create_engine(db_url)

    # Bootstrap schema
    create_observability_schema(engine)

    sla_config = _load_sla_config(Path(os.environ.get("SLA_CONFIG_PATH", "sla_config.yml")))

    sla_monitor = SLAMonitor(
        marquez_url=config["MARQUEZ_URL"],
        namespace=config["OPENLINEAGE_NAMESPACE"],
        sla_config=sla_config,
        db_url=db_url,
    )

    contract_enforcer = ContractEnforcer(db_url=db_url)
    contract_enforcer.load_contracts(Path("contracts"))

    alert_manager = AlertManager(
        channel=config["ALERT_CHANNEL"],
        slack_webhook_url=config.get("SLACK_WEBHOOK_URL"),
        smtp_host=config.get("SMTP_HOST"),
        smtp_port=int(config.get("SMTP_PORT", 587)),
        alert_email_to=config.get("ALERT_EMAIL_TO"),
        suppression_window_hours=float(config.get("ALERT_SUPPRESSION_WINDOW_HOURS", 4)),
        db_url=db_url,
    )

    return sla_monitor, contract_enforcer, alert_manager


def run_contract_checks(contract_enforcer: ContractEnforcer, alert_manager: AlertManager) -> None:
    """Run contract validation and send alerts for any violations."""
    report = contract_enforcer.validate_all()
    for violation in report.violations:
        alert_manager.send_contract_alert(violation)
    if report.violations:
        logger.warning("Contract violations found: %d", len(report.violations))
    else:
        logger.info("All contract checks passed")


def run_sla_checks(sla_monitor: SLAMonitor, alert_manager: AlertManager) -> None:
    """Evaluate SLA freshness and send alerts for breaches."""
    statuses = sla_monitor.evaluate_all()
    for status in statuses:
        if status.state == "SLA_BREACHED":
            from src.models import SLABreach
            breach = SLABreach(
                dataset_name=status.dataset_name,
                namespace=status.namespace,
                freshness_sla_seconds=status.sla_seconds,
                elapsed_seconds=status.elapsed_seconds or 0.0,
                breach_time=status.breach_time,
            )
            alert_manager.send_sla_alert(breach)


def main() -> None:
    config = load_config()
    sla_monitor, contract_enforcer, alert_manager = build_services(config)
    run_contract_checks(contract_enforcer, alert_manager)
    run_sla_checks(sla_monitor, alert_manager)
    logger.info("Platform checks complete")


if __name__ == "__main__":
    main()
