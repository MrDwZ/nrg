"""Pytest fixtures for NRG tests."""

import os
import tempfile
from pathlib import Path
from datetime import datetime

import pytest

# Set up test environment
os.environ.setdefault("FIDELITY_CSV_DIR", "/tmp/test_fidelity")


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_config_dir(temp_dir):
    """Create sample configuration files."""
    config_dir = temp_dir / "config"
    config_dir.mkdir()

    # account.toml
    (config_dir / "account.toml").write_text("""
timezone = "America/Los_Angeles"
drawdown_x = 0.12

[risk_scale]
NORMAL = 1.0
HALF = 0.5
MIN = 0.2
""")

    # thesis.toml
    (config_dir / "thesis.toml").write_text("""
[theses.Test_Thesis]
stress_pct = 0.30
budget_pct = 0.10
status = "ACTIVE"
falsifier = "Test invalidation"

[theses.Broken_Thesis]
stress_pct = 0.25
budget_pct = 0.05
status = "BROKEN"
falsifier = "Already broken"

[theses._UNMAPPED]
stress_pct = 0.25
budget_pct = 0.02
status = "ACTIVE"
falsifier = "N/A"
""")

    # mapping.csv
    (config_dir / "mapping.csv").write_text("""symbol_pattern,thesis,weight
AAPL,Test_Thesis,1.0
MSFT,Test_Thesis,1.0
BROKEN,Broken_Thesis,1.0
""")

    return config_dir


@pytest.fixture
def sample_positions():
    """Create sample position data."""
    from src.connectors.base import Position, InstrumentType

    return [
        Position(
            broker="TestBroker",
            account_id="12345",
            symbol="AAPL",
            instrument_type=InstrumentType.STOCK,
            qty=100,
            multiplier=1.0,
            price=150.0,
            mv=15000.0,
        ),
        Position(
            broker="TestBroker",
            account_id="12345",
            symbol="MSFT",
            instrument_type=InstrumentType.STOCK,
            qty=50,
            multiplier=1.0,
            price=300.0,
            mv=15000.0,
        ),
        Position(
            broker="TestBroker",
            account_id="12345",
            symbol="UNKNOWN",
            instrument_type=InstrumentType.STOCK,
            qty=10,
            multiplier=1.0,
            price=100.0,
            mv=1000.0,
        ),
    ]


@pytest.fixture
def sample_account_data(sample_positions):
    """Create sample account data."""
    from src.connectors.base import AccountData

    return AccountData(
        broker="TestBroker",
        account_id="12345",
        equity=100000.0,
        cash=69000.0,
        positions=sample_positions,
        status="OK",
    )


@pytest.fixture
def temp_db(temp_dir):
    """Create a temporary database."""
    db_path = temp_dir / "test.db"
    return str(db_path)
