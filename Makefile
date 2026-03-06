.PHONY: dev dev-stop dev-reset test

PID_FILE := .dev.pids
FLASK_PORT := 5001
REDIS_URL := redis://localhost:6379/0
VENV := .venv/bin

# ── Local Development ────────────────────────────────────────────────────────

dev: ## Start Redis + Flask + RQ worker with mock adapters
	@echo "==> Starting local dev environment..."
	@# Start Redis if not already running
	@redis-cli ping > /dev/null 2>&1 || redis-server --daemonize yes
	@echo "  Redis: running"
	@# Run migrations (SQLite by default)
	@$(VENV)/alembic upgrade head 2>&1 | tail -1
	@echo "  Migrations: done"
	@# Seed test data
	@$(VENV)/python scripts/seed_test_data.py 2>&1 | tail -1
	@echo "  Seed data: loaded"
	@# Start Flask in background
	@MOCK_PIPELINE=1 $(VENV)/flask run --port $(FLASK_PORT) --no-reload > .dev-flask.log 2>&1 & echo $$! > $(PID_FILE)
	@echo "  Flask: PID $$(cat $(PID_FILE))"
	@# Start RQ worker in background
	@MOCK_PIPELINE=1 $(VENV)/rq worker --url $(REDIS_URL) > .dev-worker.log 2>&1 & echo $$! >> $(PID_FILE)
	@echo "  Worker: PID $$(tail -1 $(PID_FILE))"
	@echo ""
	@echo "==> Ready: http://localhost:$(FLASK_PORT)"
	@echo "    Logs:  tail -f .dev-flask.log .dev-worker.log"
	@echo "    Stop:  make dev-stop"

dev-stop: ## Stop all dev processes
	@echo "==> Stopping dev environment..."
	@if [ -f $(PID_FILE) ]; then \
		while read pid; do \
			kill $$pid 2>/dev/null && echo "  Killed PID $$pid" || true; \
		done < $(PID_FILE); \
		rm -f $(PID_FILE); \
	fi
	@redis-cli shutdown nosave 2>/dev/null || true
	@rm -f .dev-flask.log .dev-worker.log
	@echo "  Done."

dev-reset: ## Wipe DB + Redis, re-seed, restart
	@echo "==> Resetting dev environment..."
	@$(MAKE) dev-stop
	@rm -f local.db
	@echo "  DB removed"
	@$(MAKE) dev

test: ## Run test suite
	@$(VENV)/pytest
