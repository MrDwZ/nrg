"""Tests for the main module."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime

from src.connectors.base import AccountData, Position, InstrumentType
from src.risk_engine import RiskResult, RiskMode, ThesisResult, ThesisStatus


class TestLoadConfig:
    """Test configuration loading."""

    def test_load_config_success(self, temp_dir):
        """Test successful config loading."""
        config_dir = temp_dir / "config"
        config_dir.mkdir()
        (config_dir / "account.yaml").write_text("""
timezone: America/Los_Angeles
drawdown_x: 0.15
""")

        from src.main import load_config
        config = load_config(str(config_dir))

        assert config["timezone"] == "America/Los_Angeles"
        assert config["drawdown_x"] == 0.15

    def test_load_config_missing_file(self, temp_dir):
        """Test config loading with missing file."""
        from src.main import load_config
        config = load_config(str(temp_dir / "nonexistent"))

        assert config == {}


class TestCollectBrokerData:
    """Test broker data collection."""

    @patch('src.main.SchwabConnector')
    @patch('src.main.FidelityCSVConnector')
    def test_collect_both_success(self, mock_fidelity, mock_schwab):
        """Test collecting data from both brokers."""
        # Mock Schwab
        mock_schwab_instance = MagicMock()
        mock_schwab_instance.connect.return_value = True
        mock_schwab_instance.get_all_accounts_data.return_value = [
            AccountData(
                broker="Schwab",
                account_id="S123",
                equity=50000.0,
                cash=10000.0,
                positions=[],
                status="OK"
            )
        ]
        mock_schwab.return_value = mock_schwab_instance

        # Mock Fidelity
        mock_fidelity_instance = MagicMock()
        mock_fidelity_instance.connect.return_value = True
        mock_fidelity_instance.get_all_accounts_data.return_value = [
            AccountData(
                broker="Fidelity",
                account_id="F456",
                equity=50000.0,
                cash=10000.0,
                positions=[],
                status="OK"
            )
        ]
        mock_fidelity_instance.get_csv_file_info.return_value = {"file": "test.csv"}
        mock_fidelity.return_value = mock_fidelity_instance

        from src.main import collect_broker_data
        accounts, statuses = collect_broker_data()

        assert len(accounts) == 2
        assert statuses["Schwab"] == "OK"
        assert statuses["Fidelity"] == "OK"

    @patch('src.main.SchwabConnector')
    @patch('src.main.FidelityCSVConnector')
    def test_collect_schwab_fails(self, mock_fidelity, mock_schwab):
        """Test collection when Schwab fails."""
        mock_schwab_instance = MagicMock()
        mock_schwab_instance.connect.return_value = False
        mock_schwab.return_value = mock_schwab_instance

        mock_fidelity_instance = MagicMock()
        mock_fidelity_instance.connect.return_value = False
        mock_fidelity.return_value = mock_fidelity_instance

        from src.main import collect_broker_data
        accounts, statuses = collect_broker_data()

        assert len(accounts) == 0
        assert statuses["Schwab"] == "CONNECT_FAILED"
        assert statuses["Fidelity"] == "NO_CSV_FILE"

    @patch('src.main.SchwabConnector')
    @patch('src.main.FidelityCSVConnector')
    def test_collect_schwab_exception(self, mock_fidelity, mock_schwab):
        """Test collection when Schwab throws exception."""
        mock_schwab.side_effect = Exception("API Error")

        mock_fidelity_instance = MagicMock()
        mock_fidelity_instance.connect.return_value = False
        mock_fidelity.return_value = mock_fidelity_instance

        from src.main import collect_broker_data
        accounts, statuses = collect_broker_data()

        assert "ERROR" in statuses["Schwab"]

    @patch('src.main.SchwabConnector')
    @patch('src.main.FidelityCSVConnector')
    def test_collect_fidelity_exception(self, mock_fidelity, mock_schwab):
        """Test collection when Fidelity throws exception."""
        mock_schwab_instance = MagicMock()
        mock_schwab_instance.connect.return_value = False
        mock_schwab.return_value = mock_schwab_instance

        mock_fidelity.side_effect = Exception("CSV Error")

        from src.main import collect_broker_data
        accounts, statuses = collect_broker_data()

        assert "ERROR" in statuses["Fidelity"]


class TestRun:
    """Test the run function."""

    @patch('src.main.SheetsWriter')
    @patch('src.main.Storage')
    @patch('src.main.RiskEngine')
    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_success(self, mock_config, mock_collect, mock_engine,
                         mock_storage, mock_sheets):
        """Test successful run."""
        mock_config.return_value = {"notifications": {"enabled": False}}
        mock_collect.return_value = (
            [AccountData(
                broker="Test",
                account_id="123",
                equity=100000.0,
                cash=50000.0,
                positions=[
                    Position(
                        broker="Test",
                        account_id="123",
                        symbol="AAPL",
                        instrument_type=InstrumentType.STOCK,
                        qty=100,
                        multiplier=1.0,
                        price=150.0,
                        mv=15000.0
                    )
                ],
                status="OK"
            )],
            {"Test": "OK"}
        )

        mock_result = RiskResult(
            timestamp=datetime.now(),
            equity=100000.0,
            peak=100000.0,
            drawdown=0.0,
            mode=RiskMode.NORMAL,
            risk_scale=1.0,
            thesis_results=[],
            positions=[],
            status="OK",
            broker_statuses={"Test": "OK"},
            actions=[],
            mode_changed=False,
            old_mode=None
        )
        mock_engine_instance = MagicMock()
        mock_engine_instance.compute.return_value = mock_result
        mock_engine_instance.format_summary.return_value = "Summary"
        mock_engine.return_value = mock_engine_instance

        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        mock_sheets_instance = MagicMock()
        mock_sheets_instance.write_all.return_value = True
        mock_sheets.return_value = mock_sheets_instance

        from src.main import run
        result = run(dry_run=False, skip_sheets=False)

        assert result == 0
        mock_engine_instance.compute.assert_called_once()

    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_no_accounts(self, mock_config, mock_collect):
        """Test run with no account data."""
        mock_config.return_value = {}
        mock_collect.return_value = ([], {"Schwab": "FAILED"})

        from src.main import run
        result = run()

        assert result == 1

    @patch('src.main.RiskEngine')
    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_engine_error(self, mock_config, mock_collect, mock_engine):
        """Test run with risk engine error."""
        mock_config.return_value = {}
        mock_collect.return_value = (
            [AccountData(
                broker="Test",
                account_id="123",
                equity=0,  # Zero equity will cause error
                cash=0,
                positions=[],
                status="OK"
            )],
            {"Test": "OK"}
        )

        mock_engine_instance = MagicMock()
        mock_engine_instance.compute.side_effect = ValueError("Equity error")
        mock_engine.return_value = mock_engine_instance

        from src.main import run
        result = run()

        assert result == 1

    @patch('src.main.SheetsWriter')
    @patch('src.main.Storage')
    @patch('src.main.RiskEngine')
    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_dry_run(self, mock_config, mock_collect, mock_engine,
                         mock_storage, mock_sheets):
        """Test dry run skips sheets."""
        mock_config.return_value = {}
        mock_collect.return_value = (
            [AccountData(
                broker="Test",
                account_id="123",
                equity=100000.0,
                cash=50000.0,
                positions=[],
                status="OK"
            )],
            {"Test": "OK"}
        )

        mock_result = RiskResult(
            timestamp=datetime.now(),
            equity=100000.0,
            peak=100000.0,
            drawdown=0.0,
            mode=RiskMode.NORMAL,
            risk_scale=1.0,
            thesis_results=[],
            positions=[],
            status="OK",
            broker_statuses={},
            actions=[],
            mode_changed=False,
            old_mode=None
        )
        mock_engine_instance = MagicMock()
        mock_engine_instance.compute.return_value = mock_result
        mock_engine_instance.format_summary.return_value = "Summary"
        mock_engine.return_value = mock_engine_instance

        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        from src.main import run
        result = run(dry_run=True)

        assert result == 0
        mock_sheets.assert_not_called()

    @patch('src.main.SheetsWriter')
    @patch('src.main.Storage')
    @patch('src.main.RiskEngine')
    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_sheets_error(self, mock_config, mock_collect, mock_engine,
                              mock_storage, mock_sheets):
        """Test run handles sheets error gracefully."""
        mock_config.return_value = {}
        mock_collect.return_value = (
            [AccountData(
                broker="Test",
                account_id="123",
                equity=100000.0,
                cash=50000.0,
                positions=[],
                status="OK"
            )],
            {"Test": "OK"}
        )

        mock_result = RiskResult(
            timestamp=datetime.now(),
            equity=100000.0,
            peak=100000.0,
            drawdown=0.0,
            mode=RiskMode.NORMAL,
            risk_scale=1.0,
            thesis_results=[],
            positions=[],
            status="OK",
            broker_statuses={},
            actions=[],
            mode_changed=False,
            old_mode=None
        )
        mock_engine_instance = MagicMock()
        mock_engine_instance.compute.return_value = mock_result
        mock_engine_instance.format_summary.return_value = "Summary"
        mock_engine.return_value = mock_engine_instance

        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        mock_sheets.side_effect = Exception("Sheets API Error")

        from src.main import run
        result = run(dry_run=False, skip_sheets=False)

        # Should still succeed even with sheets error
        assert result == 0

    @patch('src.main.Notifier')
    @patch('src.main.Storage')
    @patch('src.main.RiskEngine')
    @patch('src.main.collect_broker_data')
    @patch('src.main.load_config')
    def test_run_with_mode_change(self, mock_config, mock_collect, mock_engine,
                                   mock_storage, mock_notifier):
        """Test run with mode change notification."""
        mock_config.return_value = {"notifications": {"enabled": True}}
        mock_collect.return_value = (
            [AccountData(
                broker="Test",
                account_id="123",
                equity=100000.0,
                cash=50000.0,
                positions=[],
                status="OK"
            )],
            {"Test": "OK"}
        )

        mock_result = RiskResult(
            timestamp=datetime.now(),
            equity=100000.0,
            peak=120000.0,
            drawdown=-0.17,
            mode=RiskMode.HALF,
            risk_scale=0.5,
            thesis_results=[],
            positions=[],
            status="OK",
            broker_statuses={},
            actions=[],
            mode_changed=True,
            old_mode=RiskMode.NORMAL
        )
        mock_engine_instance = MagicMock()
        mock_engine_instance.compute.return_value = mock_result
        mock_engine_instance.format_summary.return_value = "Summary"
        mock_engine.return_value = mock_engine_instance

        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        mock_notifier_instance = MagicMock()
        mock_notifier.return_value = mock_notifier_instance

        from src.main import run
        result = run(dry_run=True, skip_sheets=True)

        assert result == 0
        mock_notifier_instance.notify_mode_change.assert_called_once()


class TestMain:
    """Test the main CLI entry point."""

    @patch('src.main.run')
    @patch('sys.argv', ['main.py'])
    def test_main_default(self, mock_run):
        """Test main with default arguments."""
        mock_run.return_value = 0

        from src.main import main
        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        mock_run.assert_called_once_with(dry_run=False, skip_sheets=False)

    @patch('src.main.run')
    @patch('sys.argv', ['main.py', '--dry-run'])
    def test_main_dry_run(self, mock_run):
        """Test main with dry-run flag."""
        mock_run.return_value = 0

        from src.main import main
        with pytest.raises(SystemExit):
            main()

        mock_run.assert_called_once_with(dry_run=True, skip_sheets=False)

    @patch('src.main.run')
    @patch('sys.argv', ['main.py', '--no-sheets'])
    def test_main_no_sheets(self, mock_run):
        """Test main with no-sheets flag."""
        mock_run.return_value = 0

        from src.main import main
        with pytest.raises(SystemExit):
            main()

        mock_run.assert_called_once_with(dry_run=False, skip_sheets=True)

    @patch('src.main.run')
    @patch('sys.argv', ['main.py', '--verbose'])
    def test_main_verbose(self, mock_run):
        """Test main with verbose flag."""
        mock_run.return_value = 0

        from src.main import main
        with pytest.raises(SystemExit):
            main()
