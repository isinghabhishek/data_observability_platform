# Data Observability Platform

A portfolio-grade, end-to-end observability system for modern dbt-based data pipelines. It layers data lineage tracking, automated quality checks, anomaly detection, SLA monitoring, and data contract enforcement on top of an existing dbt + SQL warehouse stack. All components are wired together through a shared `.env` configuration and deployed via Docker Compose.

## Quick Start

```bash
cp .env.example .env   # fill in your values
make setup             # install deps, init Elementary, validate Marquez
make run               # run dbt-ol, tests, contract enforcement, and report
```

## Stack

| Concern | Technology |
|---|---|
| Lineage emission & storage | OpenLineage (`dbt-ol`) + Marquez |
| Quality checks & anomaly detection | dbt + Elementary (`dbt-data-reliability`) |
| SLA & freshness monitoring | Python APScheduler service |
| Contract enforcement & alerting | Python + Pydantic + FastAPI |
| Deployment | Docker Compose + Makefile |
