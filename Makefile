.PHONY: setup run test report lint validate-env

# ── Bootstrap ──────────────────────────────────────────────────────────────
setup: validate-env
	@echo ">>> Installing Python dependencies..."
	pip install -e ".[dev]" --quiet
	@echo ">>> Installing dbt packages..."
	dbt deps --project-dir dbt_project --profiles-dir dbt_project
	@echo ">>> Initialising Elementary..."
	dbt run --select elementary --project-dir dbt_project --profiles-dir dbt_project
	@echo ">>> Validating Marquez connection..."
	python scripts/check_marquez.py
	@echo ">>> Setup complete."

# ── Pipeline ───────────────────────────────────────────────────────────────
run: validate-env
	@echo ">>> Running dbt-ol pipeline..."
	python src/run_pipeline.py

# ── Quality ────────────────────────────────────────────────────────────────
test:
	pytest tests/unit tests/property -v

# ── Reporting ──────────────────────────────────────────────────────────────
report:
	@echo ">>> Generating Elementary report..."
	edr report generate --project-dir dbt_project --profiles-dir dbt_project
	@echo ">>> Copying report to reports/..."
	python -c "import shutil, glob; [shutil.copy(f, 'reports/') for f in glob.glob('edr_target/*.html')]"

# ── Linting ────────────────────────────────────────────────────────────────
lint:
	ruff check src tests

# ── Env validation ─────────────────────────────────────────────────────────
validate-env:
	python scripts/validate_env.py
