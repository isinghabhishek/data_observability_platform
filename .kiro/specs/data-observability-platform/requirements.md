# Requirements Document

## Introduction

The Data Observability Platform is a portfolio-grade, end-to-end observability system for modern data pipelines. It provides data lineage tracking (table-level and column-level), automated data quality checks, anomaly detection, SLA and freshness monitoring, data contract enforcement, and an observability dashboard with alerting. The platform integrates with a dbt-based transformation layer, a SQL warehouse (Postgres, BigQuery, or Snowflake), Elementary for quality/anomaly detection, OpenLineage for lineage event emission, and Marquez as the lineage metadata store.

This is Project 7 in a data engineering portfolio series, building on the streaming and batch ELT foundations established in Projects 1 and 2.

---

## Glossary

- **Platform**: The Data Observability Platform — the system described in this document.
- **Lineage_Tracker**: The component responsible for capturing and storing data lineage events via OpenLineage and Marquez.
- **Quality_Engine**: The dbt + Elementary component responsible for running data quality checks and detecting anomalies.
- **SLA_Monitor**: The component that evaluates freshness and SLA compliance for monitored datasets.
- **Contract_Enforcer**: The component that validates datasets against declared data contracts.
- **Alert_Manager**: The component that routes and delivers notifications when quality, SLA, or contract violations occur.
- **Observability_Dashboard**: The UI layer (Elementary report or Marquez UI) that visualises lineage, quality results, and SLA status.
- **Data_Contract**: A schema-level agreement (name, type, nullability, uniqueness, freshness SLA) declared for a dataset.
- **Lineage_Event**: An OpenLineage-compliant JSON event describing a dataset input/output relationship for a job run.
- **Anomaly**: A statistical deviation in a dataset metric (row count, null rate, distribution) that exceeds a configured threshold.
- **Freshness_SLA**: The maximum acceptable elapsed time between the last successful load and the current evaluation time for a dataset.
- **Warehouse**: The target SQL data warehouse — Postgres (default), BigQuery, or Snowflake.
- **dbt_Project**: The dbt project containing models, tests, and Elementary configuration that runs against the Warehouse.

---

## Requirements

### Requirement 1: Data Lineage Emission

**User Story:** As a data engineer, I want every dbt model run and pipeline job to emit OpenLineage events, so that I have a complete, auditable record of how data flows through the system.

#### Acceptance Criteria

1. WHEN a dbt model run completes, THE Lineage_Tracker SHALL emit an OpenLineage `RunEvent` containing the job name, run ID, input datasets, output datasets, and run state (`COMPLETE` or `FAIL`).
2. WHEN a dbt model run fails, THE Lineage_Tracker SHALL emit an OpenLineage `RunEvent` with run state `FAIL` and include the error message in the event facet.
3. THE Lineage_Tracker SHALL emit column-level lineage facets for each dbt model that has column-level transformations defined in its metadata.
4. WHEN an OpenLineage event is emitted, THE Lineage_Tracker SHALL deliver it to the Marquez HTTP API within 30 seconds of job completion.
5. IF the Marquez API is unreachable, THEN THE Lineage_Tracker SHALL retry delivery up to 3 times with exponential backoff before logging a delivery failure.
6. THE Lineage_Tracker SHALL assign a consistent namespace to all events belonging to the same dbt project, enabling cross-run lineage stitching in Marquez.

---

### Requirement 2: Lineage Storage and Querying

**User Story:** As a data engineer, I want to query the full upstream and downstream lineage of any dataset, so that I can perform impact analysis before making schema changes.

#### Acceptance Criteria

1. THE Lineage_Tracker SHALL persist all received OpenLineage events in Marquez, retaining them for a minimum of 90 days.
2. WHEN a dataset name is provided, THE Lineage_Tracker SHALL return all direct upstream datasets within 2 seconds via the Marquez REST API.
3. WHEN a dataset name is provided, THE Lineage_Tracker SHALL return all direct downstream datasets within 2 seconds via the Marquez REST API.
4. THE Lineage_Tracker SHALL support column-level lineage queries, returning the source columns that contribute to a given output column.
5. WHEN a lineage graph is requested for a dataset, THE Lineage_Tracker SHALL return a directed acyclic graph (DAG) representation with nodes for datasets and edges for jobs.

---

### Requirement 3: Data Quality Checks

**User Story:** As a data engineer, I want automated data quality checks to run after every dbt model materialisation, so that data issues are caught before they reach downstream consumers.

#### Acceptance Criteria

1. WHEN a dbt model is materialised, THE Quality_Engine SHALL execute all Elementary and dbt-native tests defined for that model.
2. THE Quality_Engine SHALL support the following test types: not-null, uniqueness, accepted values, referential integrity, row count thresholds, and custom SQL assertions.
3. WHEN a quality test fails, THE Quality_Engine SHALL record the failure with the model name, test name, failure count, and timestamp in the Elementary results table.
4. THE Quality_Engine SHALL produce a test results summary report after each full dbt run, listing pass counts, fail counts, and warn counts per model.
5. IF a dbt model has no quality tests defined, THEN THE Quality_Engine SHALL emit a warning in the run log identifying the untested model.

---

### Requirement 4: Anomaly Detection

**User Story:** As a data engineer, I want the platform to automatically detect statistical anomalies in dataset metrics, so that silent data degradation is surfaced without requiring manual threshold configuration for every metric.

#### Acceptance Criteria

1. THE Quality_Engine SHALL compute the following metrics for each monitored table after every load: row count, null rate per column, and distinct value count per column.
2. WHEN a metric value deviates from its historical baseline by more than a configurable number of standard deviations (default: 3), THE Quality_Engine SHALL classify the deviation as an Anomaly.
3. WHEN an Anomaly is detected, THE Quality_Engine SHALL record it with the table name, column name (if applicable), metric name, observed value, expected range, and detection timestamp.
4. THE Quality_Engine SHALL require a minimum of 14 historical data points before activating anomaly detection for a given metric, to avoid false positives during initial data collection.
5. WHERE volume anomaly detection is enabled for a table, THE Quality_Engine SHALL flag row count changes exceeding the configured threshold as Anomalies regardless of historical baseline availability.

---

### Requirement 5: SLA and Freshness Monitoring

**User Story:** As a data consumer, I want to know whether the datasets I depend on have been refreshed within their promised SLA window, so that I can trust the data I am using for analysis.

#### Acceptance Criteria

1. THE SLA_Monitor SHALL evaluate the freshness of every dataset that has a Freshness_SLA configured, on a schedule no less frequent than every 60 minutes.
2. WHEN a dataset's last successful load timestamp exceeds its Freshness_SLA, THE SLA_Monitor SHALL mark the dataset as `SLA_BREACHED` and record the breach time and expected refresh interval.
3. WHEN a dataset transitions from `SLA_BREACHED` to a fresh state, THE SLA_Monitor SHALL record the recovery time and calculate the total breach duration.
4. THE SLA_Monitor SHALL expose a freshness status endpoint that returns the current freshness state (`FRESH`, `WARNING`, `SLA_BREACHED`) for all monitored datasets.
5. WHERE a dataset has a warning threshold configured (default: 80% of SLA window elapsed), THE SLA_Monitor SHALL mark the dataset as `WARNING` before the full SLA breach occurs.
6. IF a dataset has no recorded load events within the Marquez lineage store, THEN THE SLA_Monitor SHALL mark it as `UNKNOWN` and exclude it from SLA breach counts.

---

### Requirement 6: Data Contract Enforcement

**User Story:** As a data platform engineer, I want to declare data contracts for critical datasets and have the platform automatically validate conformance, so that schema drift and breaking changes are detected before they impact consumers.

#### Acceptance Criteria

1. THE Contract_Enforcer SHALL accept data contracts defined in YAML files specifying: dataset name, expected columns (name, data type, nullability), uniqueness constraints, and Freshness_SLA.
2. WHEN a dbt model is materialised, THE Contract_Enforcer SHALL validate the output dataset schema against its declared Data_Contract.
3. WHEN a column defined in a Data_Contract is absent from the materialised dataset, THE Contract_Enforcer SHALL record a `MISSING_COLUMN` violation with the dataset name, column name, and run timestamp.
4. WHEN a column's data type in the materialised dataset differs from the type declared in the Data_Contract, THE Contract_Enforcer SHALL record a `TYPE_MISMATCH` violation.
5. WHEN a nullability constraint is violated (a non-nullable column contains null values), THE Contract_Enforcer SHALL record a `NULLABILITY_VIOLATION` with the column name and null count.
6. THE Contract_Enforcer SHALL produce a contract compliance report after each dbt run, listing all violations grouped by dataset.
7. IF no Data_Contract is declared for a dataset, THEN THE Contract_Enforcer SHALL skip validation for that dataset without raising an error.

---

### Requirement 7: Alerting and Notifications

**User Story:** As a data engineer, I want to receive timely alerts when quality checks fail, anomalies are detected, or SLA breaches occur, so that I can investigate and remediate issues quickly.

#### Acceptance Criteria

1. WHEN a quality test fails, THE Alert_Manager SHALL send a notification within 5 minutes of the failure being recorded.
2. WHEN an Anomaly is detected, THE Alert_Manager SHALL send a notification containing the table name, metric name, observed value, and expected range.
3. WHEN an SLA breach occurs, THE Alert_Manager SHALL send a notification containing the dataset name, Freshness_SLA value, and elapsed time since last refresh.
4. WHEN a Data_Contract violation is recorded, THE Alert_Manager SHALL send a notification containing the dataset name, violation type, and affected column.
5. THE Alert_Manager SHALL support Slack webhook and email as notification channels, configurable per alert type.
6. WHERE alert suppression is configured for a dataset, THE Alert_Manager SHALL suppress duplicate notifications for the same open issue within a configurable suppression window (default: 4 hours).
7. IF an alert delivery attempt fails, THEN THE Alert_Manager SHALL retry delivery up to 3 times before logging a delivery failure.

---

### Requirement 8: Observability Dashboard

**User Story:** As a data engineer or analyst, I want a visual dashboard showing lineage graphs, quality test results, anomaly history, and SLA status, so that I can monitor the health of the data platform at a glance.

#### Acceptance Criteria

1. THE Observability_Dashboard SHALL display the lineage DAG for any selected dataset, showing upstream and downstream nodes up to a configurable depth (default: 3 hops).
2. THE Observability_Dashboard SHALL display the most recent quality test results for each model, including pass/fail/warn status and failure counts.
3. THE Observability_Dashboard SHALL display the current freshness status (`FRESH`, `WARNING`, `SLA_BREACHED`) for all monitored datasets.
4. THE Observability_Dashboard SHALL display a time-series chart of anomaly detections per dataset over the last 30 days.
5. THE Observability_Dashboard SHALL display active Data_Contract violations grouped by dataset and violation type.
6. WHEN a user selects a dataset node in the lineage DAG, THE Observability_Dashboard SHALL display the dataset's quality status, freshness status, and contract compliance status in a detail panel.
7. THE Observability_Dashboard SHALL refresh its data at most every 5 minutes without requiring a manual page reload.

---

### Requirement 9: Warehouse Integration

**User Story:** As a data engineer, I want the platform to integrate with standard SQL warehouses so that it can be adopted regardless of the team's existing warehouse choice.

#### Acceptance Criteria

1. THE Platform SHALL support Postgres, BigQuery, and Snowflake as target Warehouse backends, selectable via configuration.
2. WHEN the Warehouse backend is changed via configuration, THE dbt_Project SHALL apply the correct connection profile without requiring code changes to models or tests.
3. THE Quality_Engine SHALL store Elementary test results and anomaly metrics in a dedicated schema within the configured Warehouse.
4. THE Contract_Enforcer SHALL query dataset schemas directly from the Warehouse information schema to perform contract validation.
5. IF the Warehouse connection is unavailable during a dbt run, THEN THE dbt_Project SHALL fail the run with a descriptive connection error and emit a `FAIL` Lineage_Event.

---

### Requirement 10: Platform Configuration and Deployment

**User Story:** As a data engineer, I want to deploy the entire platform using a single configuration file and a reproducible setup process, so that the platform can be stood up in a new environment quickly.

#### Acceptance Criteria

1. THE Platform SHALL be deployable using Docker Compose for local development, with services for Marquez, the Warehouse (Postgres), and the Elementary reporting UI.
2. THE Platform SHALL expose all environment-specific settings (warehouse credentials, Marquez URL, Slack webhook URL, alert thresholds, SLA values) through a single `.env` file or equivalent secrets manager integration.
3. WHEN the Docker Compose stack is started, THE Platform SHALL be fully operational within 3 minutes on a machine with the required dependencies installed.
4. THE dbt_Project SHALL include a `profiles.yml` template that reads all connection parameters from environment variables, containing no hardcoded credentials.
5. THE Platform SHALL include a `make setup` or equivalent bootstrap command that installs dbt dependencies, runs Elementary setup, and validates the Marquez connection in a single step.
6. IF a required environment variable is missing at startup, THEN THE Platform SHALL log a descriptive error identifying the missing variable and exit with a non-zero status code.
