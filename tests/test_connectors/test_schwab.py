"""Tests for the Schwab API connector."""

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.connectors.schwab import SchwabConnector
from src.connectors.base import InstrumentType


class TestSchwabConnectorInit:
    """Test Schwab connector initialization."""

    def test_init_reads_env_vars(self):
        """Test that init reads from environment variables."""
        with patch.dict("os.environ", {
            "SCHWAB_CLIENT_ID": "test_client",
            "SCHWAB_CLIENT_SECRET": "test_secret",
            "SCHWAB_REFRESH_TOKEN": "test_refresh",
        }):
            connector = SchwabConnector()
            assert connector.client_id == "test_client"
            assert connector.client_secret == "test_secret"
            assert connector.refresh_token == "test_refresh"

    def test_broker_name(self):
        """Test broker name property."""
        connector = SchwabConnector()
        assert connector.broker_name == "Schwab"


class TestTokenHandling:
    """Test OAuth token handling."""

    def test_load_cached_token_no_file(self, temp_dir):
        """Test loading cached token when file doesn't exist."""
        with patch.dict("os.environ", {
            "SCHWAB_TOKEN_FILE": str(temp_dir / "nonexistent.json"),
        }):
            connector = SchwabConnector()
            assert connector._load_cached_token() is False

    def test_load_cached_token_expired(self, temp_dir):
        """Test loading cached token when expired."""
        token_file = temp_dir / "token.json"
        expired_time = datetime.now() - timedelta(hours=1)
        token_file.write_text(json.dumps({
            "access_token": "expired_token",
            "expiry": expired_time.isoformat()
        }))

        with patch.dict("os.environ", {
            "SCHWAB_TOKEN_FILE": str(token_file),
        }):
            connector = SchwabConnector()
            connector.token_file = str(token_file)
            assert connector._load_cached_token() is False

    def test_load_cached_token_valid(self, temp_dir):
        """Test loading valid cached token."""
        token_file = temp_dir / "token.json"
        future_time = datetime.now() + timedelta(hours=1)
        token_file.write_text(json.dumps({
            "access_token": "valid_token",
            "expiry": future_time.isoformat()
        }))

        with patch.dict("os.environ", {
            "SCHWAB_TOKEN_FILE": str(token_file),
        }):
            connector = SchwabConnector()
            connector.token_file = str(token_file)
            assert connector._load_cached_token() is True
            assert connector.access_token == "valid_token"

    def test_save_token(self, temp_dir):
        """Test saving token to cache file."""
        token_file = temp_dir / "token.json"

        connector = SchwabConnector()
        connector.token_file = str(token_file)
        connector._save_token("new_token", 3600, "new_refresh")

        assert token_file.exists()
        saved = json.loads(token_file.read_text())
        assert saved["access_token"] == "new_token"
        assert connector.access_token == "new_token"
        assert connector.refresh_token == "new_refresh"

    @patch("requests.post")
    def test_refresh_access_token_success(self, mock_post, temp_dir):
        """Test successful token refresh."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "refreshed_token",
            "expires_in": 1800,
            "refresh_token": "new_refresh_token"
        }
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {
            "SCHWAB_CLIENT_ID": "client",
            "SCHWAB_CLIENT_SECRET": "secret",
            "SCHWAB_REFRESH_TOKEN": "refresh",
            "SCHWAB_TOKEN_FILE": str(temp_dir / "token.json"),
        }):
            connector = SchwabConnector()
            connector.token_file = str(temp_dir / "token.json")
            result = connector._refresh_access_token()

            assert result is True
            assert connector.access_token == "refreshed_token"

    @patch("requests.post")
    def test_refresh_access_token_failure(self, mock_post):
        """Test failed token refresh."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Invalid credentials"
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {
            "SCHWAB_CLIENT_ID": "client",
            "SCHWAB_CLIENT_SECRET": "secret",
            "SCHWAB_REFRESH_TOKEN": "bad_refresh",
        }):
            connector = SchwabConnector()
            result = connector._refresh_access_token()

            assert result is False


class TestConnect:
    """Test connection establishment."""

    def test_connect_missing_credentials(self):
        """Test connect fails with missing credentials."""
        with patch.dict("os.environ", {}, clear=True):
            connector = SchwabConnector()
            connector.client_id = None
            connector.client_secret = None
            connector.refresh_token = None
            assert connector.connect() is False

    @patch.object(SchwabConnector, "_ensure_valid_token")
    def test_connect_success(self, mock_ensure_token):
        """Test successful connection."""
        mock_ensure_token.return_value = True

        with patch.dict("os.environ", {
            "SCHWAB_CLIENT_ID": "client",
            "SCHWAB_CLIENT_SECRET": "secret",
            "SCHWAB_REFRESH_TOKEN": "refresh",
        }):
            connector = SchwabConnector()
            assert connector.connect() is True


class TestGetAccounts:
    """Test account listing."""

    @patch.object(SchwabConnector, "_request")
    def test_get_accounts_success(self, mock_request):
        """Test successful account listing."""
        mock_request.return_value = [
            {"accountNumber": "12345"},
            {"accountNumber": "67890"}
        ]

        connector = SchwabConnector()
        accounts = connector.get_accounts()

        assert accounts == ["12345", "67890"]
        mock_request.assert_called_once_with(
            "GET", "/trader/v1/accounts/accountNumbers"
        )

    @patch.object(SchwabConnector, "_request")
    def test_get_accounts_caches_result(self, mock_request):
        """Test that account list is cached."""
        mock_request.return_value = [{"accountNumber": "12345"}]

        connector = SchwabConnector()
        connector.get_accounts()
        connector.get_accounts()

        # Should only call API once
        assert mock_request.call_count == 1

    @patch.object(SchwabConnector, "_request")
    def test_get_accounts_handles_error(self, mock_request):
        """Test error handling in account listing."""
        mock_request.side_effect = Exception("API Error")

        connector = SchwabConnector()
        accounts = connector.get_accounts()

        assert accounts == []


class TestGetAccountData:
    """Test account data fetching."""

    @patch.object(SchwabConnector, "_request")
    def test_get_account_data_success(self, mock_request):
        """Test successful account data fetch."""
        mock_request.return_value = {
            "securitiesAccount": {
                "currentBalances": {
                    "liquidationValue": 100000,
                    "cashBalance": 50000
                },
                "positions": [
                    {
                        "instrument": {
                            "symbol": "AAPL",
                            "assetType": "EQUITY"
                        },
                        "longQuantity": 100,
                        "shortQuantity": 0,
                        "marketValue": 15000
                    },
                    {
                        "instrument": {
                            "symbol": "SPY",
                            "assetType": "ETF"
                        },
                        "longQuantity": 50,
                        "shortQuantity": 0,
                        "marketValue": 20000
                    }
                ]
            }
        }

        connector = SchwabConnector()
        data = connector.get_account_data("12345")

        assert data.broker == "Schwab"
        assert data.account_id == "12345"
        assert data.equity == 100000
        assert data.cash == 50000
        assert data.status == "OK"
        assert len(data.positions) == 2

        # Check first position
        aapl = data.positions[0]
        assert aapl.symbol == "AAPL"
        assert aapl.instrument_type == InstrumentType.STOCK
        assert aapl.qty == 100
        assert aapl.mv == 15000

        # Check second position
        spy = data.positions[1]
        assert spy.symbol == "SPY"
        assert spy.instrument_type == InstrumentType.ETF

    @patch.object(SchwabConnector, "_request")
    def test_get_account_data_with_options(self, mock_request):
        """Test parsing options positions."""
        mock_request.return_value = {
            "securitiesAccount": {
                "currentBalances": {
                    "liquidationValue": 50000,
                    "cashBalance": 40000
                },
                "positions": [
                    {
                        "instrument": {
                            "symbol": "AAPL 240119C150",
                            "assetType": "OPTION"
                        },
                        "longQuantity": 10,
                        "shortQuantity": 0,
                        "marketValue": 5000
                    }
                ]
            }
        }

        connector = SchwabConnector()
        data = connector.get_account_data("12345")

        option = data.positions[0]
        assert option.instrument_type == InstrumentType.OPTION
        assert option.multiplier == 100.0

    @patch.object(SchwabConnector, "_request")
    def test_get_account_data_handles_error(self, mock_request):
        """Test error handling in account data fetch."""
        mock_request.side_effect = Exception("API Error")

        connector = SchwabConnector()
        data = connector.get_account_data("12345")

        assert data.status == "ERROR"
        assert data.equity == 0
        assert "API Error" in data.error_message


class TestAPIRequest:
    """Test the internal API request method."""

    @patch("requests.request")
    @patch.object(SchwabConnector, "_ensure_valid_token")
    def test_request_with_retry_on_401(self, mock_ensure, mock_request):
        """Test that 401 triggers token refresh and retry."""
        mock_ensure.return_value = True

        # First call returns 401, second succeeds
        response_401 = MagicMock()
        response_401.status_code = 401

        response_ok = MagicMock()
        response_ok.status_code = 200
        response_ok.json.return_value = {"data": "success"}
        response_ok.raise_for_status = MagicMock()

        mock_request.side_effect = [response_401, response_ok]

        with patch.object(SchwabConnector, "_refresh_access_token", return_value=True):
            connector = SchwabConnector()
            connector.access_token = "token"
            result = connector._request("GET", "/test")

        assert result == {"data": "success"}
        assert mock_request.call_count == 2

    @patch("requests.request")
    @patch.object(SchwabConnector, "_ensure_valid_token")
    def test_request_rate_limit_handling(self, mock_ensure, mock_request):
        """Test rate limit (429) handling."""
        mock_ensure.return_value = True

        response_429 = MagicMock()
        response_429.status_code = 429
        response_429.headers = {"Retry-After": "1"}

        response_ok = MagicMock()
        response_ok.status_code = 200
        response_ok.json.return_value = {"data": "success"}
        response_ok.raise_for_status = MagicMock()

        mock_request.side_effect = [response_429, response_ok]

        with patch("time.sleep"):  # Don't actually sleep in tests
            connector = SchwabConnector()
            connector.access_token = "token"
            result = connector._request("GET", "/test")

        assert result == {"data": "success"}
