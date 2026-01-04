"""Tests for the Google Sheets writer module."""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.sheets_writer import SheetsWriter
from src.risk_engine import RiskResult, RiskMode, ThesisResult, ThesisStatus
from src.connectors.base import Position, InstrumentType


@pytest.fixture
def sample_risk_result():
    """Create a sample risk result for testing."""
    return RiskResult(
        timestamp=datetime(2024, 1, 15, 18, 0, 0),
        equity=100000.0,
        peak=105000.0,
        drawdown=-0.0476,
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
                reduce_amount=0.0,
                target_mv=30000.0,
                status=ThesisStatus.ACTIVE,
                falsifier="Test invalidation condition",
                positions=[]
            ),
            ThesisResult(
                name="Over_Budget",
                mv=50000.0,
                stress_pct=0.25,
                budget_pct=0.05,
                worst_loss=12500.0,
                budget_dollars=5000.0,
                utilization=2.5,
                action="REDUCE $30000",
                reduce_amount=30000.0,
                target_mv=20000.0,
                status=ThesisStatus.ACTIVE,
                falsifier="Over budget thesis",
                positions=[]
            ),
        ],
        positions=[
            Position(
                broker="TestBroker",
                account_id="12345",
                symbol="AAPL",
                instrument_type=InstrumentType.STOCK,
                qty=100,
                multiplier=1.0,
                price=150.0,
                mv=15000.0,
                thesis="Test_Thesis"
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
                thesis="Test_Thesis"
            ),
        ],
        status="OK",
        broker_statuses={"TestBroker:12345": "OK"},
        actions=["Over_Budget: REDUCE $30000 (Util=250%)"],
        mode_changed=False,
        old_mode=None
    )


class TestSheetsWriterInit:
    """Test SheetsWriter initialization."""

    def test_init_with_defaults(self):
        """Test init with default values."""
        writer = SheetsWriter()
        assert writer.credentials_path is not None
        assert writer.sheet_id is None  # No env var set

    def test_init_with_custom_paths(self):
        """Test init with custom paths."""
        writer = SheetsWriter(
            credentials_path="/custom/creds.json",
            sheet_id="custom_sheet_id"
        )
        assert writer.credentials_path == "/custom/creds.json"
        assert writer.sheet_id == "custom_sheet_id"

    def test_init_from_env_vars(self):
        """Test init reads from environment variables."""
        with patch.dict("os.environ", {
            "GOOGLE_SHEETS_CREDENTIALS": "/env/creds.json",
            "GOOGLE_SHEETS_ID": "env_sheet_id"
        }):
            writer = SheetsWriter()
            assert writer.credentials_path == "/env/creds.json"
            assert writer.sheet_id == "env_sheet_id"


class TestWriteAccount:
    """Test writing account data."""

    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_account(self, mock_service, mock_ensure, mock_write,
                           sample_risk_result):
        """Test writing account sheet."""
        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_account(sample_risk_result)

        mock_ensure.assert_called_once_with("Account")
        mock_write.assert_called_once()

        # Check data format
        call_args = mock_write.call_args
        range_name = call_args[0][0]
        values = call_args[0][1]

        assert range_name == "Account!A1:G2"
        assert values[0] == ["DateTime", "Equity", "Peak", "Drawdown",
                            "Mode", "RiskScale", "Status"]
        assert values[1][1] == 100000.0
        assert values[1][4] == "NORMAL"


class TestWriteThesis:
    """Test writing thesis data."""

    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_clear_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_thesis(self, mock_service, mock_ensure, mock_clear,
                          mock_write, sample_risk_result):
        """Test writing thesis sheet."""
        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_thesis(sample_risk_result)

        mock_ensure.assert_called_once_with("Thesis")
        mock_clear.assert_called_once_with("Thesis!A:J")
        mock_write.assert_called_once()

        # Check data format
        call_args = mock_write.call_args
        values = call_args[0][1]

        # Header row
        assert "Thesis" in values[0]
        assert "Utilization" in values[0]

        # Data rows (2 theses)
        assert len(values) == 3
        assert values[1][0] == "Test_Thesis"
        assert values[2][0] == "Over_Budget"


class TestWritePositions:
    """Test writing positions data."""

    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_clear_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_positions(self, mock_service, mock_ensure, mock_clear,
                             mock_write, sample_risk_result):
        """Test writing positions sheet."""
        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_positions(sample_risk_result)

        mock_ensure.assert_called_once_with("Positions")
        mock_clear.assert_called_once_with("Positions!A:I")
        mock_write.assert_called_once()

        # Check data format
        call_args = mock_write.call_args
        values = call_args[0][1]

        # Header + 2 positions
        assert len(values) == 3
        assert values[0] == ["Broker", "Account", "Symbol", "Type", "Qty",
                            "Price", "MV", "Thesis", "Notes"]

        # Check position data
        assert values[1][2] == "AAPL"
        assert values[2][2] == "MSFT"


class TestWriteSnapshot:
    """Test writing snapshot data."""

    @patch.object(SheetsWriter, "_append_row")
    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_snapshot_with_headers(self, mock_service, mock_ensure,
                                         mock_write, mock_append,
                                         sample_risk_result):
        """Test writing snapshot with headers."""
        # Mock getting existing values (empty)
        mock_sheets = MagicMock()
        mock_values = MagicMock()
        mock_values.get.return_value.execute.return_value = {"values": [[]]}
        mock_sheets.spreadsheets.return_value.values.return_value = mock_values
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_snapshot(sample_risk_result)

        mock_ensure.assert_called_once_with("Snapshots")
        mock_write.assert_called_once()  # Headers written
        mock_append.assert_called_once()  # Row appended

    @patch.object(SheetsWriter, "_append_row")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_snapshot_existing_headers(self, mock_service, mock_ensure,
                                              mock_append, sample_risk_result):
        """Test writing snapshot when headers already exist."""
        expected_headers = [
            "DateTime", "Equity", "Peak", "Drawdown", "Mode", "RiskScale",
            "Status", "TopThesis", "TopUtil", "NumActions", "ActionSummary"
        ]

        mock_sheets = MagicMock()
        mock_values = MagicMock()
        mock_values.get.return_value.execute.return_value = {
            "values": [expected_headers]
        }
        mock_sheets.spreadsheets.return_value.values.return_value = mock_values
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_snapshot(sample_risk_result)

        # Only append called, no write for headers
        mock_append.assert_called_once()


class TestWriteAll:
    """Test writing all data at once."""

    @patch.object(SheetsWriter, "write_snapshot")
    @patch.object(SheetsWriter, "write_positions")
    @patch.object(SheetsWriter, "write_thesis")
    @patch.object(SheetsWriter, "write_account")
    def test_write_all_success(self, mock_account, mock_thesis,
                               mock_positions, mock_snapshot,
                               sample_risk_result):
        """Test successful write_all."""
        writer = SheetsWriter(sheet_id="test_sheet")
        result = writer.write_all(sample_risk_result)

        assert result is True
        mock_account.assert_called_once_with(sample_risk_result)
        mock_thesis.assert_called_once_with(sample_risk_result)
        mock_positions.assert_called_once_with(sample_risk_result)
        mock_snapshot.assert_called_once_with(sample_risk_result)

    def test_write_all_no_sheet_id(self, sample_risk_result):
        """Test write_all skips when no sheet ID configured."""
        writer = SheetsWriter(sheet_id=None)
        result = writer.write_all(sample_risk_result)

        assert result is False

    @patch.object(SheetsWriter, "write_account")
    def test_write_all_handles_error(self, mock_account, sample_risk_result):
        """Test write_all handles errors gracefully."""
        mock_account.side_effect = Exception("API Error")

        writer = SheetsWriter(sheet_id="test_sheet")
        result = writer.write_all(sample_risk_result)

        assert result is False


class TestEnsureSheetExists:
    """Test sheet creation logic."""

    @patch.object(SheetsWriter, "_get_service")
    def test_creates_sheet_if_missing(self, mock_service):
        """Test that sheet is created if it doesn't exist."""
        mock_sheets = MagicMock()

        # Mock getting existing sheets
        mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
            "sheets": [{"properties": {"title": "ExistingSheet"}}]
        }

        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer._ensure_sheet_exists("NewSheet")

        # Should call batchUpdate to create sheet
        mock_sheets.spreadsheets.return_value.batchUpdate.assert_called_once()

    @patch.object(SheetsWriter, "_get_service")
    def test_skips_if_sheet_exists(self, mock_service):
        """Test that no action if sheet already exists."""
        mock_sheets = MagicMock()

        mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
            "sheets": [{"properties": {"title": "Account"}}]
        }

        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer._ensure_sheet_exists("Account")

        # Should NOT call batchUpdate
        mock_sheets.spreadsheets.return_value.batchUpdate.assert_not_called()


class TestDataFormatting:
    """Test data formatting for sheets."""

    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_thesis_utilization_format(self, mock_service, mock_ensure,
                                        mock_write, sample_risk_result):
        """Test that utilization is formatted correctly."""
        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_thesis(sample_risk_result)

        call_args = mock_write.call_args
        values = call_args[0][1]

        # Find Over_Budget thesis (should have 2.5 utilization)
        over_budget_row = None
        for row in values[1:]:
            if row[0] == "Over_Budget":
                over_budget_row = row
                break

        assert over_budget_row is not None
        # Utilization should be numeric, not formatted
        assert over_budget_row[6] == 2.5

    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_action_empty_when_none(self, mock_service, mock_ensure,
                                     mock_write, sample_risk_result):
        """Test that action is empty string when None."""
        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_thesis(sample_risk_result)

        call_args = mock_write.call_args
        values = call_args[0][1]

        # Find Test_Thesis (should have no action)
        test_thesis_row = None
        for row in values[1:]:
            if row[0] == "Test_Thesis":
                test_thesis_row = row
                break

        assert test_thesis_row is not None
        assert test_thesis_row[7] == ""  # Action column should be empty string


class TestGetService:
    """Test Google Sheets service initialization."""

    @patch('src.sheets_writer.build')
    @patch('src.sheets_writer.Credentials')
    def test_get_service_success(self, mock_creds, mock_build):
        """Test successful service initialization."""
        mock_creds.from_service_account_file.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        writer = SheetsWriter(
            credentials_path="/test/creds.json",
            sheet_id="test_sheet"
        )
        service = writer._get_service()

        assert service is not None
        mock_creds.from_service_account_file.assert_called_once()
        mock_build.assert_called_once()

    @patch('src.sheets_writer.build')
    @patch('src.sheets_writer.Credentials')
    def test_get_service_caches_service(self, mock_creds, mock_build):
        """Test that service is cached after first call."""
        mock_creds.from_service_account_file.return_value = MagicMock()
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        writer = SheetsWriter(
            credentials_path="/test/creds.json",
            sheet_id="test_sheet"
        )
        service1 = writer._get_service()
        service2 = writer._get_service()

        assert service1 is service2
        # Should only be called once due to caching
        assert mock_build.call_count == 1

    @patch('src.sheets_writer.Credentials')
    def test_get_service_failure(self, mock_creds):
        """Test service initialization failure."""
        mock_creds.from_service_account_file.side_effect = Exception("Auth failed")

        writer = SheetsWriter(
            credentials_path="/invalid/creds.json",
            sheet_id="test_sheet"
        )

        with pytest.raises(Exception):
            writer._get_service()


class TestAPIOperations:
    """Test low-level API operations."""

    @patch.object(SheetsWriter, "_get_service")
    def test_write_range_success(self, mock_service):
        """Test successful range write."""
        mock_sheets = MagicMock()
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer._write_range("Sheet1!A1:B2", [["a", "b"], [1, 2]])

        mock_sheets.spreadsheets.return_value.values.return_value.update.assert_called_once()

    @patch.object(SheetsWriter, "_get_service")
    def test_write_range_http_error(self, mock_service):
        """Test write range handles HTTP error."""
        from googleapiclient.errors import HttpError
        mock_sheets = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 403
        mock_sheets.spreadsheets.return_value.values.return_value.update.return_value.execute.side_effect = HttpError(mock_resp, b"Forbidden")
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")

        with pytest.raises(HttpError):
            writer._write_range("Sheet1!A1", [["test"]])

    @patch.object(SheetsWriter, "_get_service")
    def test_append_row_success(self, mock_service):
        """Test successful row append."""
        mock_sheets = MagicMock()
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer._append_row("Sheet1", ["value1", "value2"])

        mock_sheets.spreadsheets.return_value.values.return_value.append.assert_called_once()

    @patch.object(SheetsWriter, "_get_service")
    def test_append_row_http_error(self, mock_service):
        """Test append row handles HTTP error."""
        from googleapiclient.errors import HttpError
        mock_sheets = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_sheets.spreadsheets.return_value.values.return_value.append.return_value.execute.side_effect = HttpError(mock_resp, b"Server Error")
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")

        with pytest.raises(HttpError):
            writer._append_row("Sheet1", ["test"])

    @patch.object(SheetsWriter, "_get_service")
    def test_clear_range_success(self, mock_service):
        """Test successful range clear."""
        mock_sheets = MagicMock()
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer._clear_range("Sheet1!A:Z")

        mock_sheets.spreadsheets.return_value.values.return_value.clear.assert_called_once()

    @patch.object(SheetsWriter, "_get_service")
    def test_clear_range_http_error(self, mock_service):
        """Test clear range handles HTTP error."""
        from googleapiclient.errors import HttpError
        mock_sheets = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 404
        mock_sheets.spreadsheets.return_value.values.return_value.clear.return_value.execute.side_effect = HttpError(mock_resp, b"Not Found")
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")

        with pytest.raises(HttpError):
            writer._clear_range("InvalidSheet!A:Z")

    @patch.object(SheetsWriter, "_get_service")
    def test_ensure_sheet_exists_http_error(self, mock_service):
        """Test ensure sheet exists handles HTTP error."""
        from googleapiclient.errors import HttpError
        mock_sheets = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 403
        mock_sheets.spreadsheets.return_value.get.return_value.execute.side_effect = HttpError(mock_resp, b"Forbidden")
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")

        with pytest.raises(HttpError):
            writer._ensure_sheet_exists("TestSheet")


class TestSnapshotHttpError:
    """Test snapshot writing with HTTP errors."""

    @patch.object(SheetsWriter, "_append_row")
    @patch.object(SheetsWriter, "_write_range")
    @patch.object(SheetsWriter, "_ensure_sheet_exists")
    @patch.object(SheetsWriter, "_get_service")
    def test_write_snapshot_get_headers_error(self, mock_service, mock_ensure,
                                               mock_write, mock_append,
                                               sample_risk_result):
        """Test snapshot handles error when getting existing headers."""
        from googleapiclient.errors import HttpError
        mock_sheets = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 404
        mock_sheets.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = HttpError(mock_resp, b"Not Found")
        mock_service.return_value = mock_sheets

        writer = SheetsWriter(sheet_id="test_sheet")
        writer.write_snapshot(sample_risk_result)

        # Should still write headers since get failed
        mock_write.assert_called_once()
        mock_append.assert_called_once()
