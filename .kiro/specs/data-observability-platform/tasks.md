# Implementation Plan: Data Observability Platform

## Overview

Implement the Data Observability Platform incrementally, starting with the project scaffold and shared infrastructure, then building each observability component in dependency order: Lineage_Tracker → Quality_Engine → SLA_Monitor → Contract_Enforcer → Alert_Manager → Observability_Dashboard → Docker Compose wiring. Property-based tests (Hypothesis) are placed immediately after the logic they validate.

## Tasks

- [x] 1. Project scaffold and shared infrastructure
  - Create directory structure: `src/`, `contracts/`, `tests/`, `tests/unit/`, `tests/property/`, `tests/integration/`, `dbt_project/`, `dbt_project/models/`, `dbt_project/tests/`, `dbt_project/macros/`
  - Create `pyproject.toml` (or `requirements.txt`) with dependencies: `dbt-core`, `dbt-postgres`, `openlineage-dbt`, `elementary-data`, `fastapi`, `uvicorn`, `apscheduler`, `httpx`, `pydantic`, `pyyaml`, `sqlalchemy`, `psycopg2-binary`, `hypothesis`, `pytest`, `pytest-asyncio`
  - Create `.env.example` with all required variables: `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`, `OPENLINEAGE_TRANSPORT_RETRY_ATTEMPTS`, `OPENLINEAGE_TRANSPORT_RETRY_BACKOFF`, `WAREHOUSE_URL`, `ALERT_CHANNEL`, `SLACK_WEBHOOK_URL`, `SMTP_HOST`, `SMTP_PORT`, `ALERT_EMAIL_TO`, `ALERT_SUPPRESSION_WINDOW_HOURS`, `SLA_POLL_INTERVAL_MINUTES`
  - Implement `src/config.py` with `validate_env(required_vars, env_dict)` that returns a list of missing variables and raises `EnvironmentError` with a descriptive message listing each missing variable
  - Write `src/__init__.py` and `tests/__init__.py` stubs
  - _Requirements: 10.2, 10.6_

- [x] 2. Data models (shared types)
  - [x] 2.1 Implement Pydantic and dataclass models in `src/models.py`
    - `ColumnContract(BaseModel)`: `name`, `type`, `nullable=True`, `unique=False`
    - `DataContract(BaseModel)`: `dataset`, `columns: list[ColumnContract]`, `freshness_sla: int | None`
    - `ContractViolation` dataclass: `dataset_name`, `violation_type`, `column_name`, `expected`, `observed`, `null_count`, `run_timestamp`
    - `FreshnessStatus` dataclass: `dataset_name`, `namespace`, `state`, `last_run_time`, `elapsed_seconds`, `sla_seconds`, `warning_threshold`, `breach_time`, `recovery_time`, `breach_duration_seconds`
    - `Anomaly` dataclass: `table_name`, `column_name`, `metric_name`, `observed_value`, `expected_min`, `expected_max`, `std_deviations`, `detection_timestamp`
    - `TestFailure`, `SLABreach` dataclasses for Alert_Manager inputs
    - _Requirements: 5.2, 5.3, 6.2, 6.3, 6.4, 6.5, 4.3_

  - [x]* 2.2 Write property test for DataContract YAML round-trip
    - File: `tests/property/test_contract_yaml_roundtrip.py`
    - Use Hypothesis `@given` with strategies that generate valid `DataContract` objects (random dataset names, 1–10 columns with random names/types/nullability/uniqueness)
    - Serialise each generated contract to YAML via `yaml.dump(contract.model_dump())`, deserialise with `DataContract(**yaml.safe_load(...))`, assert equality of all fields
    - **Property 4: Contract YAML round-trip**
    - **Validates: Requirements 6.1**

- [x] 3. Lineage_Tracker — OpenLineage event construction
  - [x] 3.1 Implement `src/lineage_tracker.py`
    - `build_run_event(job_name, run_id, inputs, outputs, state, error_msg=None) -> dict` — constructs a valid OpenLineage `RunEvent` JSON dict with `eventType`, `eventTime`, `run`, `job`, `inputs`, `outputs` fields
    - `build_column_lineage_facet(column_mappings: dict) -> dict` — constructs a `ColumnLineageDatasetFacet` from a mapping of output column → list of input (namespace, dataset, field) tuples
    - `emit_event(event: dict, marquez_url: str, namespace: str) -> None` — POSTs to `{marquez_url}/api/v1/lineage` using `httpx`, raises on non-2xx
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6_

  - [x]* 3.2 Write property test for lineage event round-trip serialisation
    - File: `tests/property/test_lineage_roundtrip.py`
    - Use Hypothesis to generate random job names (text), run IDs (UUIDs), input/output dataset lists (lists of `{"namespace": str, "name": str}`)
    - Call `build_run_event(...)`, serialise to JSON with `json.dumps`, deserialise with `json.loads`, assert `eventType`, `job.name`, `inputs`, `outputs` are structurally equivalent to inputs
    - **Property 1: Lineage event round-trip**
    - **Validates: Requirements 1.1, 1.2, 2.1, 2.2, 2.3**

- [x] 4. Checkpoint — core models and lineage serialisation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. SLA_Monitor — freshness evaluation logic
  - [x] 5.1 Implement `src/sla_monitor.py`
    - `classify_freshness(elapsed: float, sla: float, warning_threshold: float) -> str` — pure function returning `"FRESH"`, `"WARNING"`, or `"SLA_BREACHED"` per the monotonicity property; raises `ValueError` for non-positive `sla` or `warning_threshold` outside `(0, 1)`
    - `SLAMonitor` class with `__init__(self, marquez_url, namespace, sla_config_path, db_url)`
    - `evaluate_all(self) -> list[FreshnessStatus]` — queries Marquez `GET /api/v1/namespaces/{ns}/datasets/{name}/versions` for each configured dataset, calls `classify_freshness`, writes results to `observability.sla_status`
    - `get_status(self, dataset_name: str) -> FreshnessStatus`
    - `get_all_statuses(self) -> list[FreshnessStatus]`
    - Handle `httpx.ConnectError` by logging a warning and returning `UNKNOWN` state; handle `sqlalchemy.exc.OperationalError` without crashing
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [x]* 5.2 Write property test for freshness state monotonicity
    - File: `tests/property/test_freshness_monotonicity.py`
    - Use Hypothesis `@given(st.floats(min_value=0.001, max_value=1e9), st.floats(min_value=0.001, max_value=1e9), st.floats(min_value=0.01, max_value=0.99))` for `(elapsed, sla, warning_threshold)`
    - Assert: if `elapsed <= sla * warning_threshold` → `FRESH`; if `sla * warning_threshold < elapsed <= sla` → `WARNING`; if `elapsed > sla` → `SLA_BREACHED`
    - Assert the three regions are mutually exclusive (no overlap) and exhaustive (every positive triple maps to exactly one state)
    - **Property 2: Freshness state monotonicity**
    - **Validates: Requirements 5.2, 5.3, 5.5**

  - [x] 5.3 Implement SLA_Monitor FastAPI app in `src/sla_api.py`
    - `GET /freshness` → `list[FreshnessStatus]` (calls `sla_monitor.get_all_statuses()`)
    - `GET /freshness/{name}` → `FreshnessStatus` (calls `sla_monitor.get_status(name)`, returns 404 if not found)
    - Wire APScheduler to call `sla_monitor.evaluate_all()` on `SLA_POLL_INTERVAL_MINUTES` interval (default 60)
    - _Requirements: 5.1, 5.4_

- [x] 6. Contract_Enforcer — YAML loading and violation detection
  - [x] 6.1 Implement `src/contract_enforcer.py`
    - `ContractEnforcer` class with `__init__(self, db_url: str)`
    - `load_contracts(self, contracts_dir: Path) -> list[DataContract]` — reads all `*.yml` files, parses with Pydantic `DataContract`, catches `ValidationError` per file (logs error with file path and field name, skips offending file, continues)
    - `_get_warehouse_schema(self, dataset_name: str) -> list[dict]` — queries `information_schema.columns` for the dataset, returns list of `{"column_name": str, "data_type": str, "is_nullable": str}`
    - `_check_nullability(self, dataset_name: str, column_name: str) -> int` — queries the actual table for `COUNT(*) WHERE column IS NULL`
    - `validate(self, contract: DataContract) -> list[ContractViolation]` — detects `MISSING_COLUMN`, `TYPE_MISMATCH`, `NULLABILITY_VIOLATION`; returns exactly one violation per offending column
    - `validate_all(self) -> ContractComplianceReport` — runs `validate()` for all loaded contracts, aggregates results
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 9.4_

  - [x]* 6.2 Write property test for contract violation completeness
    - File: `tests/property/test_contract_violations.py`
    - Use Hypothesis to generate random `DataContract` objects and random warehouse schemas (lists of column dicts); inject deliberate mismatches: randomly remove columns (→ `MISSING_COLUMN`), change types (→ `TYPE_MISMATCH`), mark non-nullable columns as having nulls (→ `NULLABILITY_VIOLATION`)
    - Call `validate(contract, schema)` (extract pure logic into a testable helper that accepts schema as parameter), assert the returned violation set contains exactly one violation per injected mismatch — no more, no fewer
    - **Property 3: Contract violation completeness**
    - **Validates: Requirements 6.2, 6.3, 6.4, 6.5**

- [x] 7. Checkpoint — SLA and contract logic
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Alert_Manager — suppression, retry, and delivery
  - [x] 8.1 Implement `src/alert_manager.py`
    - `AlertManager` class with `__init__(self, channel, slack_webhook_url, smtp_host, smtp_port, alert_email_to, suppression_window_hours, db_url)`
    - `_is_suppressed(self, alert_key: str, last_sent: datetime | None, window_hours: float) -> bool` — pure suppression check
    - `_deliver_slack(self, payload: dict) -> None` — POSTs to `SLACK_WEBHOOK_URL` via `httpx`
    - `_deliver_email(self, subject: str, body: str) -> None` — sends via `smtplib.SMTP`
    - `_send_with_retry(self, alert_key: str, deliver_fn: Callable, payload: dict, alert_type: str) -> None` — retries up to 3 times with exponential backoff; on exhaustion writes to `observability.alert_delivery_failures`; checks suppression before first attempt
    - `send_quality_alert(self, failure: TestFailure) -> None`
    - `send_anomaly_alert(self, anomaly: Anomaly) -> None`
    - `send_sla_alert(self, breach: SLABreach) -> None`
    - `send_contract_alert(self, violation: ContractViolation) -> None`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

  - [x]* 8.2 Write property test for alert suppression idempotence
    - File: `tests/property/test_alert_suppression.py`
    - Use Hypothesis to generate random alert keys, `last_sent` timestamps, and `window_hours` values; generate sequences of submission timestamps (some within window, some outside)
    - Assert: submissions within the suppression window after `last_sent` return `True` from `_is_suppressed`; submissions after the window return `False`; the suppression check is deterministic (same inputs → same output)
    - **Property 5: Alert suppression idempotence**
    - **Validates: Requirements 7.6**

  - [x]* 8.3 Write property test for retry delivery exhaustion
    - File: `tests/property/test_alert_retry.py`
    - Use Hypothesis to generate random failure sequences (lists of booleans where `True` = attempt fails); mock the delivery function to fail/succeed per the sequence
    - Assert: total attempt count equals `min(3, first_successful_attempt_index + 1)` (or 3 if all fail); if all 3 attempts fail, a delivery failure record is written; if any attempt succeeds, no failure record is written
    - **Property 7: Retry delivery exhaustion**
    - **Validates: Requirements 7.7, 1.5**

- [x] 9. Quality_Engine — dbt project and Elementary integration
  - [x] 9.1 Create dbt project structure
    - Write `dbt_project/dbt_project.yml` with project name, model paths, Elementary package reference, and `on-run-end` hook that queries `elementary.dbt_models` for untested models and logs a warning
    - Write `dbt_project/packages.yml` with `dbt-data-reliability` (Elementary) dependency
    - Write `dbt_project/profiles.yml` template reading all connection parameters from environment variables (no hardcoded credentials)
    - Create `dbt_project/models/staging/`, `dbt_project/models/marts/` directories with placeholder `schema.yml` files that include Elementary test declarations (`volume_anomalies`, `null_count_anomalies`, `dimension_anomalies`) and dbt-native tests (`not_null`, `unique`, `accepted_values`, `relationships`)
    - _Requirements: 3.1, 3.2, 3.5, 9.1, 9.2, 9.3, 10.4_

  - [x] 9.2 Implement anomaly detection helper in `src/anomaly_detector.py`
    - `classify_anomaly(observed: float, history: list[float], threshold_stddevs: float = 3.0) -> bool` — returns `True` if `|observed - mean(history)| > threshold_stddevs * std(history)` AND `len(history) >= 14`; returns `False` if fewer than 14 points (unless volume threshold override is set)
    - `build_anomaly(table_name, column_name, metric_name, observed, history, threshold_stddevs) -> Anomaly | None`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [x]* 9.3 Write property test for anomaly threshold invariant
    - File: `tests/property/test_anomaly_threshold.py`
    - Use Hypothesis to generate random time series of ≥14 floats and a new observation; compute expected classification using the formula directly; assert `classify_anomaly` returns the same result
    - Also generate series of <14 points and assert `classify_anomaly` always returns `False` (no anomaly raised)
    - **Property 6: Anomaly threshold invariant**
    - **Validates: Requirements 4.2, 4.4, 4.5**

- [x] 10. Checkpoint — quality engine and alert manager
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Warehouse schema migrations
  - Implement `src/db_migrations.py` with `create_observability_schema(engine)` that creates (if not exists):
    - `observability.sla_status` table (per design DDL)
    - `observability.contract_violations` table
    - `observability.alert_delivery_failures` table
  - Call `create_observability_schema` at startup of SLA_Monitor and Contract_Enforcer services
  - _Requirements: 9.3, 5.2, 6.6, 7.7_

- [x] 12. Makefile and bootstrap command
  - Create `Makefile` with targets:
    - `setup`: runs `validate_env`, installs dbt deps (`dbt deps`), runs Elementary setup (`dbt run --select elementary`), checks Marquez API health (`GET /api/v1/namespaces`)
    - `run`: invokes `dbt-ol run` then `dbt test` then `edr report generate` then `python -m src.contract_enforcer`
    - `report`: runs `edr report generate` and copies output to `reports/`
    - `test`: runs `pytest tests/unit tests/property`
    - `lint`: runs `ruff` or `flake8`
  - Create `scripts/validate_env.py` that imports and calls `validate_env` from `src/config.py` with all required variable names; exits non-zero on failure
  - _Requirements: 10.5, 10.6, 10.2_

- [x] 13. Docker Compose deployment
  - Create `docker-compose.yml` with services:
    - `postgres`: Postgres 15, exposes port 5432, mounts init SQL for `observability` schema
    - `marquez`: `marquezproject/marquez:latest`, depends on `postgres`, exposes ports 5000 (API) and 3000 (UI), configured via env vars
    - `marquez-web`: Marquez UI on port 3000
    - `sla-monitor`: builds from `Dockerfile.sla_monitor`, depends on `postgres` and `marquez`, exposes port 8080
    - `elementary-report`: nginx serving the Elementary HTML report on port 8081, mounts `reports/` directory
  - Create `Dockerfile.sla_monitor` for the SLA_Monitor FastAPI service
  - Add healthchecks for `postgres` and `marquez` services; set `depends_on` with `condition: service_healthy`
  - _Requirements: 10.1, 10.3_

- [x] 14. Wire all components together
  - [x] 14.1 Create `src/main.py` entry point
    - On startup: call `validate_env`, call `create_observability_schema`, load SLA config from `sla_config.yml`, instantiate `SLAMonitor`, `ContractEnforcer`, `AlertManager`
    - Wire `ContractEnforcer.validate_all()` result to `AlertManager.send_contract_alert()` for each violation
    - Wire `SLAMonitor` breach events to `AlertManager.send_sla_alert()`
    - _Requirements: 6.6, 7.3, 7.4, 10.2_

  - [x] 14.2 Create `src/run_pipeline.py` orchestration script
    - Invokes `dbt-ol run` as a subprocess, captures exit code
    - On success: runs `dbt test`, then `ContractEnforcer.validate_all()`, then `edr report generate`
    - On failure: ensures a `FAIL` lineage event is emitted via `emit_event`
    - Passes quality failures from Elementary results table to `AlertManager.send_quality_alert()`
    - _Requirements: 1.1, 1.2, 3.1, 3.4, 9.5_

  - [x]* 14.3 Write unit tests for `validate_env` and startup wiring
    - File: `tests/unit/test_config.py`
    - Test: all required vars present → no error; one missing var → `EnvironmentError` with var name in message; multiple missing → all listed
    - _Requirements: 10.6_

- [x] 15. Final checkpoint — full integration
  - Ensure all unit and property tests pass (`pytest tests/unit tests/property`)
  - Verify `make setup` completes without error against a running Docker Compose stack
  - Ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- Property tests (Hypothesis) validate universal correctness properties; unit tests validate specific examples and edge cases
- The `classify_freshness` and `classify_anomaly` functions are intentionally pure (no I/O) to make property testing straightforward
- Checkpoints at tasks 4, 7, 10, and 15 ensure incremental validation before building on top
