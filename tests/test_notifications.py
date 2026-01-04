"""Tests for the notifications module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.notifications import Notifier
from src.risk_engine import RiskResult, RiskMode, ThesisResult, ThesisStatus
from src.connectors.base import Position, InstrumentType


@pytest.fixture
def sample_risk_result():
    """Create a sample RiskResult for testing."""
    return RiskResult(
        timestamp=datetime.now(),
        equity=100000.0,
        peak=110000.0,
        drawdown=-0.09,
        mode=RiskMode.NORMAL,
        risk_scale=1.0,
        thesis_results=[
            ThesisResult(
                name="Test_Thesis",
                mv=30000.0,
                stress_pct=0.30,
                budget_pct=0.10,
                worst_loss=9000.0,
                budget_dollars=10000.0,
                utilization=0.9,
                action=None,
                reduce_amount=0,
                target_mv=30000.0,
                status=ThesisStatus.ACTIVE,
                falsifier="Test",
                positions=[]
            )
        ],
        positions=[],
        status="OK",
        broker_statuses={"TestBroker": "OK"},
        actions=[],
        mode_changed=False,
        old_mode=None
    )


@pytest.fixture
def mode_changed_result(sample_risk_result):
    """Create a RiskResult with mode change."""
    sample_risk_result.mode_changed = True
    sample_risk_result.old_mode = RiskMode.NORMAL
    sample_risk_result.mode = RiskMode.HALF
    sample_risk_result.risk_scale = 0.5
    return sample_risk_result


@pytest.fixture
def breach_result(sample_risk_result):
    """Create a RiskResult with utilization breach."""
    sample_risk_result.thesis_results = [
        ThesisResult(
            name="Breached_Thesis",
            mv=50000.0,
            stress_pct=0.30,
            budget_pct=0.10,
            worst_loss=15000.0,
            budget_dollars=10000.0,
            utilization=1.5,
            action="REDUCE $16,667",
            reduce_amount=16667.0,
            target_mv=33333.0,
            status=ThesisStatus.ACTIVE,
            falsifier="Test",
            positions=[]
        )
    ]
    return sample_risk_result


class TestNotifierInit:
    """Test Notifier initialization."""

    def test_init_default(self):
        """Test default initialization."""
        notifier = Notifier()
        assert notifier.enabled is False
        assert notifier.config == {}

    def test_init_with_config(self):
        """Test initialization with config."""
        config = {"enabled": True, "email": {"to": "test@example.com"}}
        notifier = Notifier(config)
        assert notifier.enabled is True
        assert "email" in notifier.config

    def test_init_disabled(self):
        """Test initialization with disabled notifications."""
        notifier = Notifier({"enabled": False})
        assert notifier.enabled is False


class TestNotify:
    """Test the notify method."""

    def test_notify_disabled(self):
        """Test that notify does nothing when disabled."""
        notifier = Notifier({"enabled": False})
        # Should not raise any errors
        notifier.notify("Test", "Message")

    def test_notify_enabled_no_channels(self):
        """Test notify when enabled but no channels configured."""
        notifier = Notifier({"enabled": True})
        # Should not raise any errors
        notifier.notify("Test", "Message")

    @patch.object(Notifier, '_send_email')
    def test_notify_calls_email(self, mock_email):
        """Test that notify calls email when configured."""
        notifier = Notifier({"enabled": True, "email": {"to": "test@example.com"}})
        notifier.notify("Test", "Message")
        mock_email.assert_called_once()

    @patch.object(Notifier, '_send_slack')
    def test_notify_calls_slack(self, mock_slack):
        """Test that notify calls Slack when configured."""
        notifier = Notifier({"enabled": True, "slack": {"webhook_url": "http://test"}})
        notifier.notify("Test", "Message")
        mock_slack.assert_called_once()


class TestSendEmail:
    """Test email sending."""

    def test_send_email_no_config(self):
        """Test email returns False with no config."""
        notifier = Notifier({})
        assert notifier._send_email("Test", "Message") is False

    def test_send_email_incomplete_config(self):
        """Test email returns False with incomplete config."""
        notifier = Notifier({"email": {"smtp_server": "smtp.test.com"}})
        assert notifier._send_email("Test", "Message") is False

    @patch('smtplib.SMTP')
    def test_send_email_success(self, mock_smtp):
        """Test successful email sending."""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        config = {
            "email": {
                "smtp_server": "smtp.test.com",
                "smtp_port": 587,
                "username": "user@test.com",
                "password": "password",
                "to": "recipient@test.com"
            }
        }
        notifier = Notifier(config)
        result = notifier._send_email("Test Subject", "Test Body")

        assert result is True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once()
        mock_server.send_message.assert_called_once()

    @patch('smtplib.SMTP')
    def test_send_email_failure(self, mock_smtp):
        """Test email failure handling."""
        mock_smtp.return_value.__enter__.side_effect = Exception("SMTP Error")

        config = {
            "email": {
                "username": "user@test.com",
                "password": "password",
                "to": "recipient@test.com"
            }
        }
        notifier = Notifier(config)
        result = notifier._send_email("Test", "Message")

        assert result is False


class TestSendSlack:
    """Test Slack sending."""

    def test_send_slack_no_webhook(self):
        """Test Slack returns False with no webhook."""
        notifier = Notifier({})
        assert notifier._send_slack("Message") is False

    @patch('src.notifications.urlopen')
    def test_send_slack_success(self, mock_urlopen):
        """Test successful Slack notification."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        config = {"slack": {"webhook_url": "https://hooks.slack.com/test"}}
        notifier = Notifier(config)
        result = notifier._send_slack("Test message")

        assert result is True
        mock_urlopen.assert_called_once()

    @patch('src.notifications.urlopen')
    def test_send_slack_failure(self, mock_urlopen):
        """Test Slack failure handling."""
        from urllib.error import URLError
        mock_urlopen.side_effect = URLError("Connection failed")

        config = {"slack": {"webhook_url": "https://hooks.slack.com/test"}}
        notifier = Notifier(config)
        result = notifier._send_slack("Test message")

        assert result is False


class TestNotifyModeChange:
    """Test mode change notifications."""

    def test_notify_mode_change_no_change(self, sample_risk_result):
        """Test no notification when mode hasn't changed."""
        notifier = Notifier({"enabled": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.notify_mode_change(sample_risk_result)
            mock_notify.assert_not_called()

    def test_notify_mode_change_with_change(self, mode_changed_result):
        """Test notification sent when mode changes."""
        notifier = Notifier({"enabled": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.notify_mode_change(mode_changed_result)
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert "NORMAL" in call_args[0][0]
            assert "HALF" in call_args[0][0]


class TestNotifyUtilizationBreach:
    """Test utilization breach notifications."""

    def test_notify_breach_no_breaches(self, sample_risk_result):
        """Test no notification when no breaches."""
        notifier = Notifier({"enabled": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.notify_utilization_breach(sample_risk_result)
            mock_notify.assert_not_called()

    def test_notify_breach_with_breach(self, breach_result):
        """Test notification sent for utilization breach."""
        notifier = Notifier({"enabled": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.notify_utilization_breach(breach_result)
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert "Breach" in call_args[0][0]


class TestNotifyDataFailure:
    """Test data failure notifications."""

    def test_notify_data_failure(self):
        """Test data failure notification."""
        notifier = Notifier({"enabled": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.notify_data_failure("Schwab", "Connection timeout")
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert "Schwab" in call_args[0][0]


class TestSendDailySummary:
    """Test daily summary notifications."""

    def test_daily_summary_disabled(self, sample_risk_result):
        """Test no summary when disabled."""
        notifier = Notifier({"enabled": True, "daily_summary": False})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.send_daily_summary(sample_risk_result, "Summary text")
            mock_notify.assert_not_called()

    def test_daily_summary_enabled(self, sample_risk_result):
        """Test summary sent when enabled."""
        notifier = Notifier({"enabled": True, "daily_summary": True})
        with patch.object(notifier, 'notify') as mock_notify:
            notifier.send_daily_summary(sample_risk_result, "Summary text")
            mock_notify.assert_called_once()
