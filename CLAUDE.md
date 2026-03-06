# Social Analyzer MVP 2

## Local Development

- `make dev` — start local environment (Redis + Flask + RQ worker, mock adapters, seeds test data)
- `make dev-stop` — shut everything down
- `make dev-reset` — wipe DB + Redis and restart fresh
- `make test` — run pytest

No Docker required. Uses SQLite by default, `MOCK_PIPELINE=1` for all adapters. Runs on port 5001 (5000 is taken by macOS AirPlay).

## Testing

- Run full suite: `make test` or `.venv/bin/python -m pytest`
- Tests use in-memory SQLite via conftest.py fixtures
- Mock adapter output matches real InsightIQ API format — see `tests/app/pipeline/test_mock_parity.py`

## Deployment

- Push to `main` triggers Railway auto-deploy (CI handles staging + production)
- Pre-deploy runs `alembic upgrade head` automatically
