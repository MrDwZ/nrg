"""Tests for the risk engine module."""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.risk_engine import (
    RiskEngine,
    RiskMode,
    ThesisStatus,
    ThesisConfig,
    RiskResult,
)
from src.connectors.base import AccountData, Position, InstrumentType


class TestRiskModeComputation:
    """Test risk mode computation based on drawdown."""

    def test_normal_mode_no_drawdown(self, temp_dir, sample_config_dir):
        """Test NORMAL mode when no drawdown."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        mode, scale = engine._compute_mode(0.0)
        assert mode == RiskMode.NORMAL
        assert scale == 1.0

    def test_normal_mode_small_drawdown(self, temp_dir, sample_config_dir):
        """Test NORMAL mode with drawdown less than X."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        # X = 0.12, so -0.10 should still be NORMAL
        mode, scale = engine._compute_mode(-0.10)
        assert mode == RiskMode.NORMAL
        assert scale == 1.0

    def test_half_mode_at_threshold(self, temp_dir, sample_config_dir):
        """Test HALF mode when drawdown equals X."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        # X = 0.12, drawdown = -0.12 means HALF mode
        mode, scale = engine._compute_mode(-0.12)
        assert mode == RiskMode.HALF
        assert scale == 0.5

    def test_half_mode_between_thresholds(self, temp_dir, sample_config_dir):
        """Test HALF mode when -2X < drawdown <= -X."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        # X = 0.12, so -0.18 is between -0.24 and -0.12
        mode, scale = engine._compute_mode(-0.18)
        assert mode == RiskMode.HALF
        assert scale == 0.5

    def test_min_mode_at_double_threshold(self, temp_dir, sample_config_dir):
        """Test MIN mode when drawdown equals 2X."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        # X = 0.12, so -0.24 = -2X triggers MIN mode
        mode, scale = engine._compute_mode(-0.24)
        assert mode == RiskMode.MIN
        assert scale == 0.2

    def test_min_mode_severe_drawdown(self, temp_dir, sample_config_dir):
        """Test MIN mode with severe drawdown."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )
        mode, scale = engine._compute_mode(-0.40)
        assert mode == RiskMode.MIN
        assert scale == 0.2


class TestThesisUtilization:
    """Test thesis utilization calculations."""

    def test_utilization_under_budget(self, temp_dir, sample_config_dir):
        """Test utilization when within budget."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        # MV = 30000 (AAPL + MSFT), stress = 30%, budget = 10%
        # WorstLoss = 30000 * 0.30 = 9000
        # Budget$ = 100000 * 0.10 * 1.0 = 10000
        # Utilization = 9000 / 10000 = 0.9

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=100000,
            cash=70000,
            positions=[
                Position(
                    broker="Test", account_id="123", symbol="AAPL",
                    instrument_type=InstrumentType.STOCK,
                    qty=100, multiplier=1.0, price=150.0, mv=15000.0
                ),
                Position(
                    broker="Test", account_id="123", symbol="MSFT",
                    instrument_type=InstrumentType.STOCK,
                    qty=50, multiplier=1.0, price=300.0, mv=15000.0
                ),
            ],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value=None):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute([account_data])

        test_thesis = next(
            (t for t in result.thesis_results if t.name == "Test_Thesis"),
            None
        )
        assert test_thesis is not None
        assert test_thesis.mv == 30000.0
        assert test_thesis.worst_loss == 9000.0  # 30000 * 0.30
        assert test_thesis.budget_dollars == 10000.0  # 100000 * 0.10 * 1.0
        assert test_thesis.utilization == 0.9
        assert test_thesis.action is None

    def test_utilization_over_budget_triggers_reduce(self, temp_dir, sample_config_dir):
        """Test that utilization > 1 triggers REDUCE action."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        # Large position that exceeds budget
        # MV = 50000, stress = 30%, budget = 10%
        # WorstLoss = 50000 * 0.30 = 15000
        # Budget$ = 100000 * 0.10 * 1.0 = 10000
        # Utilization = 15000 / 10000 = 1.5

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=100000,
            cash=50000,
            positions=[
                Position(
                    broker="Test", account_id="123", symbol="AAPL",
                    instrument_type=InstrumentType.STOCK,
                    qty=250, multiplier=1.0, price=200.0, mv=50000.0
                ),
            ],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value=None):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute([account_data])

        test_thesis = next(
            (t for t in result.thesis_results if t.name == "Test_Thesis"),
            None
        )
        assert test_thesis is not None
        assert test_thesis.utilization == 1.5
        assert test_thesis.action is not None
        assert "REDUCE" in test_thesis.action

        # TargetMV = Budget$ / stress_pct = 10000 / 0.30 = 33333.33
        # Reduce$ = MV - TargetMV = 50000 - 33333.33 = 16666.67
        assert test_thesis.target_mv == pytest.approx(33333.33, rel=0.01)
        assert test_thesis.reduce_amount == pytest.approx(16666.67, rel=0.01)

    def test_broken_thesis_triggers_exit(self, temp_dir, sample_config_dir):
        """Test that BROKEN thesis status triggers EXIT action."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=100000,
            cash=90000,
            positions=[
                Position(
                    broker="Test", account_id="123", symbol="BROKEN",
                    instrument_type=InstrumentType.STOCK,
                    qty=100, multiplier=1.0, price=100.0, mv=10000.0
                ),
            ],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value=None):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute([account_data])

        broken_thesis = next(
            (t for t in result.thesis_results if t.name == "Broken_Thesis"),
            None
        )
        assert broken_thesis is not None
        assert broken_thesis.action == "EXIT"
        assert broken_thesis.reduce_amount == 10000.0
        assert broken_thesis.target_mv == 0


class TestSymbolMapping:
    """Test symbol to thesis mapping."""

    def test_exact_match_mapping(self, temp_dir, sample_config_dir):
        """Test exact symbol match mapping."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        thesis, weight = engine._map_position_to_thesis("AAPL")
        assert thesis == "Test_Thesis"
        assert weight == 1.0

    def test_unmapped_symbol(self, temp_dir, sample_config_dir):
        """Test unmapped symbol goes to _UNMAPPED."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        thesis, weight = engine._map_position_to_thesis("RANDOM_SYMBOL")
        assert thesis == "_UNMAPPED"
        assert weight == 1.0


class TestModeChange:
    """Test mode change detection."""

    def test_mode_change_detected(self, temp_dir, sample_config_dir):
        """Test that mode change is detected and flagged."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=88000,  # 12% drawdown from 100k peak
            cash=88000,
            positions=[],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value="NORMAL"):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_mode_change') as mock_mode_change:
                        with patch.object(engine.storage, 'save_thesis_metrics'):
                            with patch.object(engine.storage, 'save_positions'):
                                result = engine.compute([account_data])

        assert result.mode_changed is True
        assert result.old_mode == RiskMode.NORMAL
        assert result.mode == RiskMode.HALF
        mock_mode_change.assert_called_once()


class TestRiskScaleImpact:
    """Test that risk scale affects budget calculations."""

    def test_half_mode_reduces_budget(self, temp_dir, sample_config_dir):
        """Test that HALF mode reduces effective budget."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        # Create position that would be under budget in NORMAL mode
        # but over budget in HALF mode
        # MV = 20000, stress = 30%
        # WorstLoss = 20000 * 0.30 = 6000
        # NORMAL Budget$ = 100000 * 0.10 * 1.0 = 10000 -> Util = 0.6
        # HALF Budget$ = 88000 * 0.10 * 0.5 = 4400 -> Util = 1.36

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=88000,  # 12% drawdown triggers HALF mode
            cash=68000,
            positions=[
                Position(
                    broker="Test", account_id="123", symbol="AAPL",
                    instrument_type=InstrumentType.STOCK,
                    qty=100, multiplier=1.0, price=200.0, mv=20000.0
                ),
            ],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value="HALF"):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute([account_data])

        assert result.mode == RiskMode.HALF
        assert result.risk_scale == 0.5

        test_thesis = next(
            (t for t in result.thesis_results if t.name == "Test_Thesis"),
            None
        )
        assert test_thesis is not None
        # Budget$ = 88000 * 0.10 * 0.5 = 4400
        assert test_thesis.budget_dollars == pytest.approx(4400.0, rel=0.01)
        # Utilization = 6000 / 4400 = 1.36
        assert test_thesis.utilization > 1.0
        assert test_thesis.action is not None


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_zero_equity_raises_error(self, temp_dir, sample_config_dir):
        """Test that zero equity raises an error."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=0,
            cash=0,
            positions=[],
            status="OK"
        )

        with pytest.raises(ValueError, match="Equity cannot be computed"):
            engine.compute([account_data])

    def test_degraded_status_with_partial_data(self, temp_dir, sample_config_dir):
        """Test degraded status when one broker fails."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        accounts = [
            AccountData(
                broker="Good",
                account_id="123",
                equity=100000,
                cash=100000,
                positions=[],
                status="OK"
            ),
            AccountData(
                broker="Bad",
                account_id="456",
                equity=0,
                cash=0,
                positions=[],
                status="ERROR",
                error_message="Connection failed"
            ),
        ]

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value=None):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute(accounts)

        assert result.status == "DEGRADED"
        assert result.equity == 100000  # Only good broker's equity


class TestSummaryFormat:
    """Test summary formatting."""

    def test_format_summary_contains_key_info(self, temp_dir, sample_config_dir):
        """Test that summary contains all key information."""
        engine = RiskEngine(
            config_dir=str(sample_config_dir),
            data_dir=str(temp_dir / "data")
        )

        account_data = AccountData(
            broker="Test",
            account_id="123",
            equity=100000,
            cash=85000,
            positions=[
                Position(
                    broker="Test", account_id="123", symbol="AAPL",
                    instrument_type=InstrumentType.STOCK,
                    qty=100, multiplier=1.0, price=150.0, mv=15000.0
                ),
            ],
            status="OK"
        )

        with patch.object(engine.storage, 'get_peak', return_value=100000):
            with patch.object(engine.storage, 'get_last_mode', return_value=None):
                with patch.object(engine.storage, 'save_equity_snapshot'):
                    with patch.object(engine.storage, 'save_thesis_metrics'):
                        with patch.object(engine.storage, 'save_positions'):
                            result = engine.compute([account_data])

        summary = engine.format_summary(result)

        assert "Equity" in summary
        assert "Peak" in summary
        assert "Drawdown" in summary
        assert "Mode" in summary
        assert "NORMAL" in summary
        assert "Test_Thesis" in summary
