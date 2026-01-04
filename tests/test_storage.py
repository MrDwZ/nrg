"""Tests for the storage module."""

import pytest
from datetime import datetime, timedelta

from src.storage import (
    Storage,
    EquitySnapshot,
    ThesisMetric,
    PositionRecord,
)


class TestStorageInitialization:
    """Test storage initialization and schema creation."""

    def test_creates_database_file(self, temp_db):
        """Test that database file is created."""
        storage = Storage(temp_db)
        from pathlib import Path
        assert Path(temp_db).exists()

    def test_creates_tables(self, temp_db):
        """Test that all required tables are created."""
        storage = Storage(temp_db)
        conn = storage._get_conn()
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "equity_history" in tables
        assert "mode_history" in tables
        assert "thesis_metrics" in tables
        assert "positions" in tables
        assert "run_log" in tables


class TestEquitySnapshots:
    """Test equity snapshot operations."""

    def test_save_and_retrieve_peak(self, temp_db):
        """Test saving equity snapshot and retrieving peak."""
        storage = Storage(temp_db)

        # Save first snapshot
        snapshot1 = EquitySnapshot(
            timestamp=datetime.now(),
            equity=100000,
            peak=100000,
            drawdown=0.0,
            mode="NORMAL",
            risk_scale=1.0,
            status="OK"
        )
        storage.save_equity_snapshot(snapshot1)

        # Peak should be 100000
        assert storage.get_peak() == 100000

        # Save snapshot with higher peak
        snapshot2 = EquitySnapshot(
            timestamp=datetime.now(),
            equity=110000,
            peak=110000,
            drawdown=0.0,
            mode="NORMAL",
            risk_scale=1.0,
            status="OK"
        )
        storage.save_equity_snapshot(snapshot2)

        # Peak should now be 110000
        assert storage.get_peak() == 110000

    def test_get_last_mode(self, temp_db):
        """Test retrieving last mode."""
        storage = Storage(temp_db)

        # Initially no mode
        assert storage.get_last_mode() is None

        # Save snapshot
        snapshot = EquitySnapshot(
            timestamp=datetime.now(),
            equity=100000,
            peak=100000,
            drawdown=0.0,
            mode="NORMAL",
            risk_scale=1.0,
            status="OK"
        )
        storage.save_equity_snapshot(snapshot)

        assert storage.get_last_mode() == "NORMAL"

        # Save another with different mode
        snapshot2 = EquitySnapshot(
            timestamp=datetime.now() + timedelta(hours=1),
            equity=88000,
            peak=100000,
            drawdown=-0.12,
            mode="HALF",
            risk_scale=0.5,
            status="OK"
        )
        storage.save_equity_snapshot(snapshot2)

        assert storage.get_last_mode() == "HALF"


class TestModeHistory:
    """Test mode change history."""

    def test_save_mode_change(self, temp_db):
        """Test saving mode change events."""
        storage = Storage(temp_db)

        storage.save_mode_change(
            timestamp=datetime.now(),
            old_mode="NORMAL",
            new_mode="HALF",
            equity=88000,
            drawdown=-0.12
        )

        # Verify saved by querying directly
        conn = storage._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mode_history")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 1
        assert rows[0]["old_mode"] == "NORMAL"
        assert rows[0]["new_mode"] == "HALF"


class TestThesisMetrics:
    """Test thesis metrics operations."""

    def test_save_thesis_metrics(self, temp_db):
        """Test saving thesis metrics."""
        storage = Storage(temp_db)

        metrics = [
            ThesisMetric(
                timestamp=datetime.now(),
                thesis="Test_Thesis",
                mv=50000,
                stress_pct=0.30,
                budget_pct=0.10,
                worst_loss=15000,
                budget_dollars=10000,
                utilization=1.5,
                action="REDUCE $16667",
                status="ACTIVE"
            ),
            ThesisMetric(
                timestamp=datetime.now(),
                thesis="Index_Core",
                mv=30000,
                stress_pct=0.20,
                budget_pct=0.05,
                worst_loss=6000,
                budget_dollars=5000,
                utilization=1.2,
                action="REDUCE $5000",
                status="ACTIVE"
            ),
        ]

        storage.save_thesis_metrics(metrics)

        # Verify saved
        conn = storage._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM thesis_metrics ORDER BY thesis")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 2
        assert rows[0]["thesis"] == "Index_Core"
        assert rows[1]["thesis"] == "Test_Thesis"

    def test_get_thesis_history(self, temp_db):
        """Test retrieving thesis history."""
        storage = Storage(temp_db)

        # Save metrics for multiple days
        for i in range(5):
            metric = ThesisMetric(
                timestamp=datetime.now() - timedelta(days=i),
                thesis="Test_Thesis",
                mv=50000 - i * 1000,
                stress_pct=0.30,
                budget_pct=0.10,
                worst_loss=15000 - i * 300,
                budget_dollars=10000,
                utilization=1.5 - i * 0.1,
                action=None,
                status="ACTIVE"
            )
            storage.save_thesis_metrics([metric])

        history = storage.get_thesis_history("Test_Thesis", days=30)
        assert len(history) == 5


class TestPositionRecords:
    """Test position snapshot operations."""

    def test_save_positions(self, temp_db):
        """Test saving position snapshots."""
        storage = Storage(temp_db)

        positions = [
            PositionRecord(
                timestamp=datetime.now(),
                broker="Schwab",
                account_id="12345",
                symbol="AAPL",
                instrument_type="STOCK",
                qty=100,
                multiplier=1.0,
                price=150.0,
                mv=15000.0,
                currency="USD",
                thesis="Tech_Growth",
                notes=None
            ),
            PositionRecord(
                timestamp=datetime.now(),
                broker="Fidelity",
                account_id="67890",
                symbol="SPY",
                instrument_type="ETF",
                qty=50,
                multiplier=1.0,
                price=400.0,
                mv=20000.0,
                currency="USD",
                thesis="Index_Core",
                notes="Core holding"
            ),
        ]

        storage.save_positions(positions)

        # Verify saved
        latest = storage.get_latest_positions()
        assert len(latest) == 2

    def test_get_latest_positions_sorted_by_mv(self, temp_db):
        """Test that latest positions are sorted by market value."""
        storage = Storage(temp_db)

        positions = [
            PositionRecord(
                timestamp=datetime.now(),
                broker="Test",
                account_id="123",
                symbol="SMALL",
                instrument_type="STOCK",
                qty=10,
                multiplier=1.0,
                price=10.0,
                mv=100.0,
                currency="USD",
                thesis="Test",
                notes=None
            ),
            PositionRecord(
                timestamp=datetime.now(),
                broker="Test",
                account_id="123",
                symbol="LARGE",
                instrument_type="STOCK",
                qty=100,
                multiplier=1.0,
                price=100.0,
                mv=10000.0,
                currency="USD",
                thesis="Test",
                notes=None
            ),
        ]

        storage.save_positions(positions)

        latest = storage.get_latest_positions()
        assert latest[0]["symbol"] == "LARGE"  # Larger MV first
        assert latest[1]["symbol"] == "SMALL"


class TestRunLog:
    """Test run logging operations."""

    def test_log_run(self, temp_db):
        """Test logging a run execution."""
        storage = Storage(temp_db)

        storage.log_run(
            timestamp=datetime.now(),
            status="OK",
            message="Completed successfully",
            brokers_status={"Schwab": "OK", "Fidelity": "OK"},
            duration_seconds=5.23
        )

        # Verify logged
        conn = storage._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM run_log")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 1
        assert rows[0]["status"] == "OK"
        assert rows[0]["duration_seconds"] == pytest.approx(5.23)


class TestEquityHistory:
    """Test equity history retrieval."""

    def test_get_equity_history(self, temp_db):
        """Test retrieving equity history."""
        storage = Storage(temp_db)

        # Save snapshots for multiple days
        for i in range(10):
            snapshot = EquitySnapshot(
                timestamp=datetime.now() - timedelta(days=i),
                equity=100000 - i * 1000,
                peak=100000,
                drawdown=-i * 0.01,
                mode="NORMAL" if i < 5 else "HALF",
                risk_scale=1.0 if i < 5 else 0.5,
                status="OK"
            )
            storage.save_equity_snapshot(snapshot)

        history = storage.get_equity_history(days=365)
        assert len(history) == 10

        # Should be sorted descending by timestamp
        assert history[0]["equity"] == 100000  # Most recent first
