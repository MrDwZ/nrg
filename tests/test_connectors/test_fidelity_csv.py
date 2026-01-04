"""Tests for the Fidelity CSV connector."""

import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from src.connectors.fidelity_csv import FidelityCSVConnector
from src.connectors.base import InstrumentType


class TestFidelityConnectorInit:
    """Test Fidelity connector initialization."""

    def test_init_with_default_dir(self):
        """Test init with default directory."""
        connector = FidelityCSVConnector()
        assert "fidelity" in str(connector.csv_dir).lower()

    def test_init_with_custom_dir(self, temp_dir):
        """Test init with custom directory."""
        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        assert connector.csv_dir == temp_dir

    def test_broker_name(self):
        """Test broker name property."""
        connector = FidelityCSVConnector()
        assert connector.broker_name == "Fidelity"


class TestFindLatestCSV:
    """Test finding the latest CSV file."""

    def test_no_csv_files(self, temp_dir):
        """Test when no CSV files exist."""
        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        assert connector._find_latest_csv() is None

    def test_finds_csv_file(self, temp_dir):
        """Test finding a CSV file."""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text("Symbol,Quantity\nAAPL,100")

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        found = connector._find_latest_csv()

        assert found == csv_file

    def test_finds_newest_csv(self, temp_dir):
        """Test that the newest CSV is found."""
        import time

        old_file = temp_dir / "old.csv"
        old_file.write_text("Symbol,Quantity\nAAPL,50")
        time.sleep(0.1)

        new_file = temp_dir / "new.csv"
        new_file.write_text("Symbol,Quantity\nAAPL,100")

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        found = connector._find_latest_csv()

        assert found == new_file

    def test_directory_not_exists(self, temp_dir):
        """Test when directory doesn't exist."""
        connector = FidelityCSVConnector(csv_dir=str(temp_dir / "nonexistent"))
        assert connector._find_latest_csv() is None


class TestParseNumber:
    """Test number parsing from various formats."""

    def test_parse_simple_number(self):
        """Test parsing simple number."""
        connector = FidelityCSVConnector()
        assert connector._parse_number("100") == 100.0

    def test_parse_with_commas(self):
        """Test parsing number with commas."""
        connector = FidelityCSVConnector()
        assert connector._parse_number("1,234.56") == 1234.56

    def test_parse_with_dollar_sign(self):
        """Test parsing number with dollar sign."""
        connector = FidelityCSVConnector()
        assert connector._parse_number("$1,234.56") == 1234.56

    def test_parse_negative_parentheses(self):
        """Test parsing negative number in parentheses."""
        connector = FidelityCSVConnector()
        assert connector._parse_number("($500.00)") == -500.0

    def test_parse_empty_string(self):
        """Test parsing empty string."""
        connector = FidelityCSVConnector()
        assert connector._parse_number("") == 0.0
        assert connector._parse_number("--") == 0.0
        assert connector._parse_number("n/a") == 0.0


class TestInstrumentTypeDetection:
    """Test instrument type detection."""

    def test_detect_cash(self):
        """Test detecting cash/money market."""
        connector = FidelityCSVConnector()

        assert connector._determine_instrument_type(
            "SPAXX", "FIDELITY GOVERNMENT MONEY MARKET", ""
        ) == InstrumentType.CASH

        assert connector._determine_instrument_type(
            "CORE", "CORE CASH", ""
        ) == InstrumentType.CASH

    def test_detect_option_from_description(self):
        """Test detecting options from description."""
        connector = FidelityCSVConnector()

        assert connector._determine_instrument_type(
            "AAPL240119C150", "AAPL CALL 150 01/19/24", ""
        ) == InstrumentType.OPTION

    def test_detect_option_from_symbol(self):
        """Test detecting options from symbol pattern."""
        connector = FidelityCSVConnector()

        assert connector._determine_instrument_type(
            "AAPL240119C00150000", "", ""
        ) == InstrumentType.OPTION

    def test_detect_etf(self):
        """Test detecting ETFs."""
        connector = FidelityCSVConnector()

        assert connector._determine_instrument_type(
            "SPY", "SPDR S&P 500 ETF", "ETF"
        ) == InstrumentType.ETF

    def test_default_to_stock(self):
        """Test defaulting to stock."""
        connector = FidelityCSVConnector()

        assert connector._determine_instrument_type(
            "AAPL", "APPLE INC", ""
        ) == InstrumentType.STOCK


class TestParseCSV:
    """Test CSV parsing."""

    def test_parse_basic_csv(self, temp_dir):
        """Test parsing a basic positions CSV."""
        csv_content = """Account Number,Symbol,Description,Quantity,Last Price,Current Value
12345,AAPL,APPLE INC,100,$150.00,"$15,000.00"
12345,MSFT,MICROSOFT CORP,50,$300.00,"$15,000.00"
12345,SPAXX,FIDELITY GOVERNMENT MONEY,5000,$1.00,"$5,000.00"
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        assert len(accounts) == 1
        account = accounts[0]
        assert account.account_id == "12345"
        assert len(account.positions) == 3

        # Check AAPL position
        aapl = next(p for p in account.positions if p.symbol == "AAPL")
        assert aapl.qty == 100
        assert aapl.mv == 15000.0
        assert aapl.instrument_type == InstrumentType.STOCK

        # Check cash position
        cash = next(p for p in account.positions if p.symbol == "SPAXX")
        assert cash.instrument_type == InstrumentType.CASH

        # Check equity is sum of MVs
        assert account.equity == 35000.0
        assert account.cash == 5000.0

    def test_parse_multiple_accounts(self, temp_dir):
        """Test parsing CSV with multiple accounts."""
        csv_content = """Account Number,Symbol,Quantity,Last Price,Current Value
12345,AAPL,100,150.00,15000.00
67890,MSFT,50,300.00,15000.00
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        assert len(accounts) == 2
        account_ids = [a.account_id for a in accounts]
        assert "12345" in account_ids
        assert "67890" in account_ids

    def test_parse_handles_missing_columns(self, temp_dir):
        """Test parsing with minimal columns."""
        csv_content = """Symbol,Quantity,Current Value
AAPL,100,15000
MSFT,50,15000
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        assert len(accounts) == 1
        assert accounts[0].account_id == "default"
        assert len(accounts[0].positions) == 2

    def test_parse_skips_total_rows(self, temp_dir):
        """Test that total/summary rows are skipped."""
        csv_content = """Symbol,Quantity,Current Value
AAPL,100,15000.00
Total,,30000.00
Account Total,,30000.00
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        assert len(accounts[0].positions) == 1
        assert accounts[0].positions[0].symbol == "AAPL"

    def test_parse_handles_header_in_middle(self, temp_dir):
        """Test parsing when header is not on first line."""
        csv_content = """Fidelity Positions Export
Date: 2024-01-15

Symbol,Quantity,Current Value
AAPL,100,15000.00
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        assert len(accounts) == 1
        assert len(accounts[0].positions) == 1

    def test_parse_computes_price_from_value(self, temp_dir):
        """Test that price is computed from value if missing."""
        csv_content = """Symbol,Quantity,Current Value
AAPL,100,15000.00
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector._parse_csv(csv_file)

        position = accounts[0].positions[0]
        assert position.price == 150.0  # 15000 / 100


class TestConnect:
    """Test connection/CSV discovery."""

    def test_connect_success(self, temp_dir):
        """Test successful connection when CSV exists."""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text("Symbol,Quantity\nAAPL,100")

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        assert connector.connect() is True
        assert connector._latest_file == csv_file

    def test_connect_failure(self, temp_dir):
        """Test connection failure when no CSV exists."""
        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        assert connector.connect() is False


class TestGetAccounts:
    """Test account retrieval."""

    def test_get_accounts(self, temp_dir):
        """Test getting account list."""
        csv_content = """Account Number,Symbol,Quantity,Current Value
12345,AAPL,100,15000
67890,MSFT,50,15000
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        accounts = connector.get_accounts()

        assert "12345" in accounts
        assert "67890" in accounts


class TestGetAccountData:
    """Test individual account data retrieval."""

    def test_get_account_data(self, temp_dir):
        """Test getting specific account data."""
        csv_content = """Account Number,Symbol,Quantity,Current Value
12345,AAPL,100,15000
67890,MSFT,50,15000
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        data = connector.get_account_data("12345")

        assert data.account_id == "12345"
        assert len(data.positions) == 1
        assert data.positions[0].symbol == "AAPL"

    def test_get_account_data_not_found(self, temp_dir):
        """Test getting data for non-existent account."""
        csv_content = """Account Number,Symbol,Quantity,Current Value
12345,AAPL,100,15000
"""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text(csv_content)

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        data = connector.get_account_data("99999")

        assert data.status == "ERROR"
        assert "not found" in data.error_message.lower()


class TestGetCSVFileInfo:
    """Test CSV file info retrieval."""

    def test_get_csv_file_info(self, temp_dir):
        """Test getting CSV file metadata."""
        csv_file = temp_dir / "positions.csv"
        csv_file.write_text("Symbol,Quantity\nAAPL,100")

        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        connector.connect()
        info = connector.get_csv_file_info()

        assert info["status"] == "ok"
        assert info["file"] == "positions.csv"
        assert "modified" in info

    def test_get_csv_file_info_no_file(self, temp_dir):
        """Test file info when no file exists."""
        connector = FidelityCSVConnector(csv_dir=str(temp_dir))
        info = connector.get_csv_file_info()

        assert info["status"] == "no_file"
