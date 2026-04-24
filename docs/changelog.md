# Data Observability Platform — Implementation Changelog

A running log of completed tasks, files created or modified, and a summary of changes made in each task.

---

## Task 1 — Project Scaffold and Shared Infrastructure

**Status:** ✅ Completed  
**Requirements covered:** 10.2, 10.6

### Files Created

| File / Directory | Description |
|---|---|
| `src/` | Python source package root |
| `src/__init__.py` | Package stub |
| `src/config.py` | `validate_env()`, `load_config()`, `REQUIRED_VARS` |
| `tests/` | Test root |
| `tests/__init__.py` | Test package stub |
| `tests/unit/__init__.py` | Unit test package stub |
| `tests/property/__init__.py` | Property test package stub |
| `tests/integration/__init__.py` | Integration test package stub |
| `contracts/` | YAML data contract definitions directory |
| `dbt_project/` | dbt project root |
| `dbt_project/models/` | dbt model SQL files |
| `dbt_project/tests/` | dbt custom test SQL files |
| `dbt_project/macros/` | dbt macro files |
| `reports/` | Generated Elementary HTML reports |
| `scripts/` | Utility and bootstrap scripts |
| `pyproject.toml` | Project metadata and all Python dependencies |
| `.env.example` | Template with all 14 required environment variables |
| `Makefile` | Placeholder targets: `setup`, `run`, `test`, `report`, `lint` |
| `README.md` | Project overview, quick-start guide, and stack table |

### Key Changes

- **`src/config.py`** — `validate_env(required_vars, env_dict)` returns a list of missing variable names and raises `EnvironmentError` with a descriptive message listing each missing variable. `load_config()` reads from `os.environ` and calls `validate_env`. `REQUIRED_VARS` lists all 14 required env var names.
- **`pyproject.toml`** — pinned dependencies: `dbt-core>=1.7`, `dbt-postgres>=1.7`, `openlineage-dbt>=1.9`, `elementary-data>=0.14`, `fastapi>=0.110`, `uvicorn>=0.29`, `apscheduler>=3.10`, `httpx>=0.27`, `pydantic>=2.6`, `pyyaml>=6.0`, `sqlalchemy>=2.0`, `psycopg2-binary>=2.9`, `hypothesis>=6.100`, `pytest>=8.0`, `pytest-asyncio>=0.23`.
- **`.env.example`** — all required variables documented with example values: `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`, `OPENLINEAGE_TRANSPORT_RETRY_ATTEMPTS`, `OPENLINEAGE_TRANSPORT_RETRY_BACKOFF`, `WAREHOUSE_URL`, `ALERT_CHANNEL`, `SLACK_WEBHOOK_URL`, `SMTP_HOST`, `SMTP_PORT`, `ALERT_EMAIL_TO`, `ALERT_SUPPRESSION_WINDOW_HOURS`, `SLA_POLL_INTERVAL_MINUTES`, `DBT_PROFILES_DIR`, `MARQUEZ_URL`.

---

<!-- Future completed tasks will be appended below this line -->

## Task 2 — Data Models (Shared Types)

**Status:** ✅ Completed  
**Requirements covered:** 4.3, 5.2, 5.3, 6.1, 6.2, 6.3, 6.4, 6.5

### Files Created

| File | Description |
|---|---|
| `src/models.py` | All shared Pydantic models and dataclasses |
| `tests/property/test_contract_yaml_roundtrip.py` | Property 4 — YAML round-trip test (Hypothesis, 100 examples) |

### Key Changes

- **`src/models.py`**
  - `ColumnContract(BaseModel)` — `name`, `type`, `nullable=True`, `unique=False`
  - `DataContract(BaseModel)` — `dataset`, `columns: list[ColumnContract]`, `freshness_sla: int | None`
  - `FreshnessStatus` dataclass — full freshness state including `state` (`FRESH`/`WARNING`/`SLA_BREACHED`/`UNKNOWN`), `breach_time`, `recovery_time`, `breach_duration_seconds`
  - `ContractViolation` dataclass — `violation_type` (`MISSING_COLUMN`/`TYPE_MISMATCH`/`NULLABILITY_VIOLATION`), column details, `run_timestamp`
  - `Anomaly` dataclass — `metric_name`, `observed_value`, `expected_min`/`expected_max`, `std_deviations`, `detection_timestamp`
  - `TestFailure` dataclass — `model_name`, `test_name`, `failure_count`, `timestamp`, optional `message`
  - `SLABreach` dataclass — `dataset_name`, `namespace`, `freshness_sla_seconds`, `elapsed_seconds`, `breach_time`
  - `ContractComplianceReport` dataclass — `violations` list + `violations_by_dataset()` helper method

- **`tests/property/test_contract_yaml_roundtrip.py`**
  - Hypothesis strategies for random `DataContract` objects (random names, 1–10 columns, typed column types, optional SLA)
  - Serialises to YAML via `yaml.dump(contract.model_dump())`, deserialises back, asserts full field equality
  - 100 examples, all passing — validates Property 4

---

<!-- Future completed tasks will be appended below this line -->

## Task 3 — Lineage_Tracker (OpenLineage Event Construction)

**Status:** ✅ Completed  
**Requirements covered:** 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3

### Files Created

| File | Description |
|---|---|
| `src/lineage_tracker.py` | OpenLineage event construction and delivery with retry logic |
| `tests/property/test_lineage_roundtrip.py` | Property 1 — lineage event JSON round-trip test (Hypothesis, 100 examples) |

### Key Changes

- **`src/lineage_tracker.py`**
  - `build_run_event(job_name, run_id, inputs, outputs, state, namespace, error_msg)` — constructs a fully valid OpenLineage `RunEvent` dict with `eventType`, `eventTime` (UTC ISO 8601), `run` (with optional `errorMessage` facet), `job`, `inputs`, `outputs`, `producer`, `schemaURL`
  - `build_column_lineage_facet(column_mappings)` — constructs a `ColumnLineageDatasetFacet` mapping output columns to their source `(namespace, dataset, field)` tuples; maps `"dataset"` key → `"name"` in `inputFields`
  - `emit_event(event, marquez_url, max_retries=3, backoff_factor=2.0)` — POSTs to `{marquez_url}/api/v1/lineage` via `httpx`; retries up to `max_retries` times on `ConnectError` or `HTTPStatusError` with exponential backoff (`backoff_factor ** attempt` seconds); logs and re-raises after exhaustion

- **`tests/property/test_lineage_roundtrip.py`**
  - Hypothesis strategies for random job names, UUIDs, namespaces, states, dataset lists, and optional error messages
  - Serialises `RunEvent` → JSON → back and asserts structural equivalence of all fields including the optional `errorMessage` facet
  - 100 examples, all passing — validates Property 1

---

<!-- Future completed tasks will be appended below this line -->

## Task 4 — Checkpoint: Core Models and Lineage Serialisation

**Status:** ✅ Completed

### Verification

- Ran `pytest tests/unit tests/property -v` — **2 passed, 0 failures**
  - `test_datacontract_yaml_roundtrip` — 100 Hypothesis examples ✅
  - `test_lineage_event_roundtrip` — 100 Hypothesis examples ✅

### Fixes Applied

- **`src/lineage_tracker.py`** — replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)` to eliminate Python 3.13 deprecation warning. `eventTime` format unchanged (`...Z` suffix).

---

<!-- Future completed tasks will be appended below this line -->

## Task 5 — SLA_Monitor (Freshness Evaluation Logic)

**Status:** ✅ Completed  
**Requirements covered:** 5.1, 5.2, 5.3, 5.4, 5.5, 5.6

### Files Created

| File | Description |
|---|---|
| `src/sla_monitor.py` | `classify_freshness` pure function + `SLAMonitor` class |
| `src/sla_api.py` | FastAPI app with `/freshness` endpoints + APScheduler |
| `sla_config.yml` | Example SLA config for `marts.orders` and `marts.customers` |
| `tests/property/test_freshness_monotonicity.py` | Property 2 — freshness monotonicity test (Hypothesis, 100 examples) + invalid-input tests |

### Key Changes

- **`src/sla_monitor.py`**
  - `classify_freshness(elapsed, sla, warning_threshold=0.8)` — pure function; returns `FRESH` / `WARNING` / `SLA_BREACHED`; raises `ValueError` for `sla <= 0` or `warning_threshold` outside `(0, 1)`
  - `SLAMonitor._get_last_run_time(dataset_name)` — queries Marquez `GET /api/v1/namespaces/{ns}/datasets/{name}/versions`; returns latest `createdAt` as `datetime` or `None`; catches `httpx.ConnectError` gracefully
  - `SLAMonitor._write_status(status)` — upserts into `observability.sla_status` via SQLAlchemy; catches `OperationalError` without crashing
  - `SLAMonitor.evaluate_all()` — evaluates all configured datasets, builds `FreshnessStatus` objects, persists and caches results
  - `SLAMonitor.get_status(name)` / `get_all_statuses()` — return from in-memory cache

- **`src/sla_api.py`**
  - FastAPI app with `asynccontextmanager` lifespan: loads `sla_config.yml`, instantiates `SLAMonitor`, starts `BackgroundScheduler` polling on `SLA_POLL_INTERVAL_MINUTES`
  - `GET /freshness` → all statuses as list of dicts
  - `GET /freshness/{name}` → single status dict or 404

- **`tests/property/test_freshness_monotonicity.py`**
  - Property 2: 100 Hypothesis examples asserting correct region classification and mutual exclusivity of states
  - 2 additional unit tests for `ValueError` on invalid `sla` and `warning_threshold`
  - All 3 tests passing ✅

### Test Results
```
5 passed in 3.32s (all unit + property tests)
```

---

<!-- Future completed tasks will be appended below this line -->

## Task 6 — Contract_Enforcer (YAML Loading and Violation Detection)

**Status:** ✅ Completed  
**Requirements covered:** 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 9.4

### Files Created

| File | Description |
|---|---|
| `src/contract_enforcer.py` | `ContractEnforcer` class + `_validate_contract` pure helper |
| `contracts/orders.yml` | Sample contract for `public.orders` |
| `tests/property/test_contract_violations.py` | Property 3 — violation completeness test (Hypothesis, 100 examples) |

### Key Changes

- **`src/contract_enforcer.py`**
  - `_validate_contract(contract, warehouse_schema, null_counts, run_timestamp)` — module-level pure function; checks columns in order: `MISSING_COLUMN` → `TYPE_MISMATCH` → `NULLABILITY_VIOLATION`; skips later checks if column is missing; returns exactly one violation per offending column
  - `ContractEnforcer.load_contracts(contracts_dir)` — reads all `*.yml` files; catches `yaml.YAMLError` and `pydantic.ValidationError` per file with field-level logging; skips bad files, continues
  - `ContractEnforcer._get_warehouse_schema(dataset_name)` — queries `information_schema.columns`; handles `schema.table` format (defaults to `public`); catches `OperationalError`
  - `ContractEnforcer._check_nullability(dataset_name, column_name)` — `COUNT(*) WHERE col IS NULL`; catches `OperationalError`, returns 0
  - `ContractEnforcer.validate(contract)` — fetches schema + null counts, delegates to `_validate_contract`
  - `ContractEnforcer.validate_all()` — runs all contracts, aggregates into `ContractComplianceReport`, persists each violation to `observability.contract_violations`

- **`contracts/orders.yml`** — sample contract with `order_id` (INTEGER, non-nullable, unique), `customer_id` (INTEGER, non-nullable), `status` (TEXT, nullable), `freshness_sla: 3600`

- **`tests/property/test_contract_violations.py`**
  - Generates random contracts with unique column names; builds clean matching schema; injects disjoint sets of mismatches (missing, type mismatch, nullability)
  - Asserts exact violation sets match injected mismatches — no extras, no missing
  - 100 examples, all passing ✅

### Test Results
```
6 passed in 3.37s (all unit + property tests)
```

---

<!-- Future completed tasks will be appended below this line -->

## Task 7 — Checkpoint: SLA and Contract Logic

**Status:** ✅ Completed

### Verification
- Ran `pytest tests/unit tests/property -v` — **6 passed, 0 failures**
- All Tasks 1–6 verified clean before proceeding to Task 8

---

## Task 8 — Alert_Manager (Suppression, Retry, and Delivery)

**Status:** ✅ Completed  
**Requirements covered:** 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7

### Files Created

| File | Description |
|---|---|
| `src/alert_manager.py` | `AlertManager` class + `_is_suppressed` and `_should_retry` pure helpers |
| `tests/property/test_alert_suppression.py` | Property 5 — suppression idempotence (Hypothesis, 100+50 examples) |
| `tests/property/test_alert_retry.py` | Property 7 — retry exhaustion (Hypothesis, 100 examples) |

### Key Changes

- **`src/alert_manager.py`**
  - `_is_suppressed(alert_key, last_sent, window_hours)` — pure function; returns `True` if `last_sent` is not `None` and elapsed < `window_hours * 3600`; `None` last_sent is never suppressed
  - `_should_retry(attempt, max_attempts)` — pure function; returns `True` if `attempt < max_attempts`
  - `AlertManager._deliver_slack(payload)` — `httpx.post` to Slack webhook, raises `HTTPError` on failure
  - `AlertManager._deliver_email(subject, body)` — `smtplib.SMTP` delivery via `MIMEText`, raises `SMTPException` on failure
  - `AlertManager._send_with_retry(alert_key, deliver_fn, payload, alert_type)` — checks suppression first; retries up to 3 times with `2.0 ** attempt` second sleep; on exhaustion writes to `observability.alert_delivery_failures`; catches `OperationalError` on DB write
  - `AlertManager._dispatch(alert_key, payload, alert_type)` — routes to Slack, email, or both based on `channel` config
  - `send_quality_alert`, `send_anomaly_alert`, `send_sla_alert`, `send_contract_alert` — each builds a keyed payload and calls `_dispatch`

- **`tests/property/test_alert_suppression.py`** — 3 tests: within-window property (100 examples), `None` last_sent unit test, determinism property (50 examples). All passing ✅

- **`tests/property/test_alert_retry.py`** — 2 tests: retry policy property (100 examples), total-attempts simulation (100 examples). All passing ✅

### Test Results
```
11 passed in 4.09s (all unit + property tests)
```

---

<!-- Future completed tasks will be appended below this line -->

## Task 9 — Quality_Engine (dbt Project and Elementary Integration)

**Status:** ✅ Completed  
**Requirements covered:** 3.1, 3.2, 3.5, 4.1, 4.2, 4.3, 4.4, 4.5, 9.1, 9.2, 9.3, 10.4

### Files Created

| File | Description |
|---|---|
| `dbt_project/dbt_project.yml` | dbt project config with `on-run-end` untested-model hook |
| `dbt_project/packages.yml` | Elementary (`>=0.14`) and dbt_utils (`>=1.1`) dependencies |
| `dbt_project/profiles.yml` | Env-var-driven profiles for Postgres, BigQuery, Snowflake — no hardcoded credentials |
| `dbt_project/macros/check_untested_models.sql` | Macro that logs a warning for any model with no tests |
| `dbt_project/models/sources.yml` | Raw source definition for `orders` table |
| `dbt_project/models/staging/stg_orders.sql` | Staging model — cleans raw orders |
| `dbt_project/models/staging/schema.yml` | Staging tests: `not_null`, `unique`, `accepted_values` + Elementary `volume_anomalies`, `null_count_anomalies` |
| `dbt_project/models/marts/marts_orders.sql` | Marts model — final enriched orders |
| `dbt_project/models/marts/schema.yml` | Marts tests: dbt-native + Elementary `volume_anomalies`, `null_count_anomalies`, `dimension_anomalies` |
| `src/anomaly_detector.py` | Pure anomaly detection helpers |
| `tests/property/test_anomaly_threshold.py` | Property 6 — anomaly threshold invariant (Hypothesis, 100 examples) |

### Key Changes

- **`dbt_project/profiles.yml`** — all connection params read from env vars (`DBT_HOST`, `DBT_USER`, `DBT_PASSWORD`, etc.); supports `dev` (Postgres), `bigquery`, and `snowflake` targets via `DBT_TARGET`

- **`dbt_project/macros/check_untested_models.sql`** — `check_untested_models()` macro runs on `on-run-end`; queries `elementary.dbt_models` vs `elementary.dbt_tests` and logs a warning for each untested model

- **`src/anomaly_detector.py`**
  - `_mean(values)` — arithmetic mean, raises `ValueError` on empty list
  - `_std(values, mean)` — population std dev, returns `0.0` for < 2 values
  - `classify_anomaly(observed, history, threshold_stddevs=3.0, volume_threshold=None)`:
    - `len(history) < 14` + no volume_threshold → `False`
    - `len(history) < 14` + volume_threshold → fires if `abs(observed - mean) > volume_threshold`
    - `len(history) >= 14` → fires if `abs(observed - mean) > threshold_stddevs * std`
  - `build_anomaly(...)` → returns `Anomaly` dataclass or `None`

- **`tests/property/test_anomaly_threshold.py`** — 3 tests:
  - Property test (100 examples): ≥14-point series matches `|obs - mean| > k * std` formula exactly
  - Property test (100 examples): <14-point series with no volume_threshold always returns `False`
  - Unit test: volume_threshold override fires correctly with short history
  - All passing ✅

### Test Results
```
14 passed in 6.24s (all unit + property tests)
```

---

<!-- Future completed tasks will be appended below this line -->

## Task 10 — Checkpoint: Quality Engine and Alert Manager

**Status:** ✅ Completed

### Verification
- Ran `pytest tests/unit tests/property -v` — **14 passed, 0 failures**
- All Tasks 1–9 verified clean before proceeding

---

## Task 11 — Warehouse Schema Migrations

**Status:** ✅ Completed  
**Requirements covered:** 9.3, 5.2, 6.6, 7.7

### Files Created

| File | Description |
|---|---|
| `src/db_migrations.py` | `create_observability_schema(engine)` — idempotent DDL for all 3 platform tables |
| `tests/unit/test_db_migrations.py` | 2 unit tests: idempotency + exact statement count |

### Key Changes
- `create_observability_schema(engine)` — executes 4 DDL statements in one transaction: `CREATE SCHEMA IF NOT EXISTS observability` + `sla_status`, `contract_violations`, `alert_delivery_failures` tables
- Called at startup in both `SLAMonitor.__init__` and `ContractEnforcer.__init__`

---

## Task 12 — Makefile and Bootstrap Command

**Status:** ✅ Completed  
**Requirements covered:** 10.2, 10.5, 10.6

### Files Created / Updated

| File | Description |
|---|---|
| `Makefile` | Full implementation: `setup`, `run`, `test`, `report`, `lint`, `validate-env` |
| `scripts/validate_env.py` | Calls `validate_env(REQUIRED_VARS, os.environ)`, exits 0/1 |
| `scripts/check_marquez.py` | GETs `{MARQUEZ_URL}/api/v1/namespaces`, exits 1 on failure |

### Key Changes
- `make setup` — validates env → installs deps → `dbt deps` → Elementary init → Marquez health check
- `make run` — validates env → `python src/run_pipeline.py`
- `make test` — `pytest tests/unit tests/property -v`
- `make report` — `edr report generate` + copy HTML to `reports/`
- `make lint` — `ruff check src tests`

---

## Task 13 — Docker Compose Deployment

**Status:** ✅ Completed  
**Requirements covered:** 10.1, 10.3

### Files Created

| File | Description |
|---|---|
| `docker-compose.yml` | 5 services: postgres, marquez, marquez-web, sla-monitor, elementary-report |
| `Dockerfile.sla_monitor` | Python 3.11-slim image running FastAPI SLA Monitor on port 8080 |
| `scripts/init_db.sql` | Creates observability schema + 3 tables on first Postgres startup |

### Key Changes
- All services use `depends_on: condition: service_healthy` with proper healthchecks
- Postgres mounts `scripts/init_db.sql` via `docker-entrypoint-initdb.d`
- SLA Monitor mounts `sla_config.yml` as read-only volume
- Elementary report served via nginx on port 8081 from `reports/` directory
- `.env.example` updated with `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`

---

## Task 14 — Wire All Components Together

**Status:** ✅ Completed  
**Requirements covered:** 1.1, 1.2, 3.1, 3.4, 6.6, 7.3, 7.4, 9.5, 10.2

### Files Created

| File | Description |
|---|---|
| `src/main.py` | Platform entry point — validates env, bootstraps schema, wires all services |
| `src/run_pipeline.py` | Pipeline orchestration: dbt-ol → dbt test → contracts → edr report |
| `tests/unit/test_config.py` | 5 unit tests for `validate_env` |

### Key Changes
- **`src/main.py`** — `build_services()` instantiates `SLAMonitor`, `ContractEnforcer`, `AlertManager`; `run_contract_checks()` and `run_sla_checks()` wire violations/breaches to `AlertManager`
- **`src/run_pipeline.py`** — runs `dbt-ol run` → on failure emits `FAIL` lineage event and exits; on success runs `dbt test` → queries Elementary results → sends quality alerts → runs contract enforcement → generates edr report
- **`tests/unit/test_config.py`** — covers all-present, one missing, multiple missing, empty-string-as-missing, and custom var list cases

---

## Task 15 — Final Checkpoint: Full Integration

**Status:** ✅ Completed

### Final Test Results
```
21 passed in 5.17s
  Unit tests:     7 (test_config × 5, test_db_migrations × 2)
  Property tests: 14 (all 7 correctness properties validated)
  Failures:       0
  Warnings:       1 (asyncio_mode config — cosmetic only)
```

### Complete Test Inventory

| Test File | Tests | Property |
|---|---|---|
| `tests/unit/test_config.py` | 5 | — |
| `tests/unit/test_db_migrations.py` | 2 | — |
| `tests/property/test_lineage_roundtrip.py` | 1 | Property 1 |
| `tests/property/test_freshness_monotonicity.py` | 3 | Property 2 |
| `tests/property/test_contract_violations.py` | 1 | Property 3 |
| `tests/property/test_contract_yaml_roundtrip.py` | 1 | Property 4 |
| `tests/property/test_alert_suppression.py` | 3 | Property 5 |
| `tests/property/test_anomaly_threshold.py` | 3 | Property 6 |
| `tests/property/test_alert_retry.py` | 2 | Property 7 |

---
