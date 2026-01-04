"""
Notification module for NRG.

Sends alerts via email, Slack, or other channels when:
- Mode changes (NORMAL -> HALF -> MIN)
- Utilization crosses above 1.0
- Data ingestion fails
"""

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError

from .risk_engine import RiskResult, RiskMode

logger = logging.getLogger(__name__)


class Notifier:
    """Send notifications via configured channels."""

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.enabled = self.config.get("enabled", False)

    def _send_email(self, subject: str, body: str) -> bool:
        """Send email notification via SMTP."""
        email_config = self.config.get("email", {})
        if not email_config:
            return False

        try:
            smtp_server = email_config.get("smtp_server", "smtp.gmail.com")
            smtp_port = email_config.get("smtp_port", 587)
            username = email_config.get("username") or os.environ.get("SMTP_USERNAME")
            password = email_config.get("password") or os.environ.get("SMTP_PASSWORD")
            to_addr = email_config.get("to")

            if not all([username, password, to_addr]):
                logger.warning("Email config incomplete, skipping")
                return False

            msg = MIMEMultipart()
            msg["From"] = username
            msg["To"] = to_addr
            msg["Subject"] = f"[NRG] {subject}"
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)

            logger.info(f"Sent email notification: {subject}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def _send_slack(self, message: str) -> bool:
        """Send Slack notification via webhook."""
        slack_config = self.config.get("slack", {})
        webhook_url = slack_config.get("webhook_url") or os.environ.get("SLACK_WEBHOOK_URL")

        if not webhook_url:
            return False

        try:
            payload = json.dumps({"text": message}).encode("utf-8")
            req = Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urlopen(req, timeout=10) as response:
                if response.status == 200:
                    logger.info("Sent Slack notification")
                    return True
        except URLError as e:
            logger.error(f"Failed to send Slack notification: {e}")

        return False

    def notify(self, title: str, message: str):
        """Send notification via all configured channels."""
        if not self.enabled:
            logger.debug("Notifications disabled")
            return

        # Try email
        if "email" in self.config or os.environ.get("SMTP_USERNAME"):
            self._send_email(title, message)

        # Try Slack
        if "slack" in self.config or os.environ.get("SLACK_WEBHOOK_URL"):
            slack_msg = f"*{title}*\n```{message}```"
            self._send_slack(slack_msg)

    def notify_mode_change(self, result: RiskResult):
        """Send notification for mode change."""
        if not result.mode_changed:
            return

        title = f"Mode Change: {result.old_mode.value} -> {result.mode.value}"
        message = f"""Risk Mode Change Alert

Previous Mode: {result.old_mode.value}
New Mode: {result.mode.value}
Risk Scale: {result.risk_scale:.0%}

Account Status:
  Equity: ${result.equity:,.2f}
  Peak: ${result.peak:,.2f}
  Drawdown: {result.drawdown:.2%}

This change affects position sizing and thesis budgets.
"""
        self.notify(title, message)

    def notify_utilization_breach(self, result: RiskResult):
        """Send notification for utilization breaches."""
        breaches = [t for t in result.thesis_results if t.utilization > 1.0]
        if not breaches:
            return

        title = f"Utilization Breach: {len(breaches)} thesis(es)"
        lines = ["Thesis utilization exceeds budget:\n"]
        for t in breaches:
            lines.append(f"  {t.name}: {t.utilization:.0%} utilization")
            lines.append(f"    Action: {t.action}")
            lines.append(f"    Reduce by: ${t.reduce_amount:,.0f}")
            lines.append("")

        self.notify(title, "\n".join(lines))

    def notify_data_failure(self, broker: str, error: str):
        """Send notification for data ingestion failure."""
        title = f"Data Ingestion Failed: {broker}"
        message = f"""Broker data ingestion failed

Broker: {broker}
Error: {error}

The risk run will continue with partial data (DEGRADED status).
Please check credentials and data sources.
"""
        self.notify(title, message)

    def send_daily_summary(self, result: RiskResult, summary: str):
        """Send optional daily summary notification."""
        if not self.config.get("daily_summary", False):
            return

        self.notify(f"Daily Summary - {result.mode.value}", summary)
