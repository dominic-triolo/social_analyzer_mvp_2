"""
Notifications — Slack webhook integration for pipeline events.

Notification failure never blocks the pipeline.
"""
import logging
import requests

from app.config import SLACK_WEBHOOK_URL

logger = logging.getLogger('services.notifications')


def notify_run_complete(run):
    """Post run completion summary to Slack."""
    if not SLACK_WEBHOOK_URL:
        return

    try:
        tier = run.tier_distribution or {}
        auto = tier.get('auto_enroll', 0)
        total_scored = run.profiles_scored or 0

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Pipeline Run Completed — {run.platform.capitalize()}",
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Found:* {run.profiles_found or 0}"},
                    {"type": "mrkdwn", "text": f"*Pre-screened:* {run.profiles_pre_screened or 0}"},
                    {"type": "mrkdwn", "text": f"*Scored:* {total_scored}"},
                    {"type": "mrkdwn", "text": f"*Synced:* {run.contacts_synced or 0}"},
                    {"type": "mrkdwn", "text": f"*Auto-Enroll:* {auto}"},
                    {"type": "mrkdwn", "text": f"*Dupes Skipped:* {run.duplicates_skipped or 0}"},
                ]
            },
        ]

        if run.summary:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"_{run.summary}_"}
            })

        if run.actual_cost and run.actual_cost > 0:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Cost: ~${run.actual_cost:.2f}"}]
            })

        requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
        logger.info("Run %s completion notification sent", run.id[:8])

    except Exception:
        logger.error("Failed to send notification for run %s", run.id[:8], exc_info=True)


def notify_run_failed(run):
    """Post run failure alert to Slack."""
    if not SLACK_WEBHOOK_URL:
        return

    try:
        last_error = ''
        if run.errors:
            last_err = run.errors[-1] if isinstance(run.errors, list) else {}
            last_error = last_err.get('message', '') if isinstance(last_err, dict) else str(last_err)

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Pipeline Run FAILED — {run.platform.capitalize()}",
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Stage:* {run.current_stage or 'unknown'}"},
                    {"type": "mrkdwn", "text": f"*Found so far:* {run.profiles_found or 0}"},
                ]
            },
        ]

        if last_error:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:* ```{last_error[:500]}```"}
            })

        requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
        logger.info("Run %s failure notification sent", run.id[:8])

    except Exception:
        logger.error("Failed to send failure notification for run %s", run.id[:8], exc_info=True)
