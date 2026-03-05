#!/usr/bin/env python
"""Railway cron entry point — runs the enrollment dispatcher directly.

Usage (Railway cron service):
    python scripts/cron_enrollment.py

Accepts optional env overrides:
    ENROLLMENT_FORCE=1   — run even on non-business days
    ENROLLMENT_DRY_RUN=1 — preview mode, no HubSpot writes
"""
import os
import sys
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("cron_enrollment")


def main():
    force = os.getenv("ENROLLMENT_FORCE", "").strip() == "1"
    dry_run = os.getenv("ENROLLMENT_DRY_RUN", "").strip() == "1"

    logger.info("Starting enrollment dispatch (force=%s, dry_run=%s)", force, dry_run)

    from app.services.enrollment_dispatcher import run_enrollment_dispatcher

    result = run_enrollment_dispatcher(force=force, dry_run=dry_run)

    logger.info(
        "Dispatch finished — status=%s enrolled=%s errors=%s",
        result.get("status"),
        result.get("enrolled_count"),
        result.get("error_count"),
    )

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Full result: %s", json.dumps(result, default=str, indent=2))

    # Exit non-zero on error so Railway marks the run as failed
    if result.get("status") == "error":
        sys.exit(1)


if __name__ == "__main__":
    main()
