"""
Schwab API Connector for NRG.

Uses OAuth 2.0 authentication with token refresh handling.
Requires environment variables:
- SCHWAB_CLIENT_ID: OAuth client ID (app key)
- SCHWAB_CLIENT_SECRET: OAuth client secret
- SCHWAB_REFRESH_TOKEN: Refresh token from initial OAuth flow
- SCHWAB_TOKEN_FILE: (optional) Path to store token cache
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import requests

from .base import BaseConnector, AccountData, Position, InstrumentType

logger = logging.getLogger(__name__)


class SchwabConnector(BaseConnector):
    """Schwab API connector with OAuth token handling."""

    BASE_URL = "https://api.schwabapi.com"
    TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"

    def __init__(self):
        self.client_id = os.environ.get("SCHWAB_CLIENT_ID")
        self.client_secret = os.environ.get("SCHWAB_CLIENT_SECRET")
        self.refresh_token = os.environ.get("SCHWAB_REFRESH_TOKEN")
        self.token_file = os.environ.get("SCHWAB_TOKEN_FILE", "data/.schwab_token.json")

        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        self._account_ids: list[str] = []

    @property
    def broker_name(self) -> str:
        return "Schwab"

    def _load_cached_token(self) -> bool:
        """Load token from cache file if valid."""
        token_path = Path(self.token_file)
        if not token_path.exists():
            return False

        try:
            with open(token_path) as f:
                data = json.load(f)

            expiry = datetime.fromisoformat(data["expiry"])
            if expiry > datetime.now() + timedelta(minutes=5):
                self.access_token = data["access_token"]
                self.token_expiry = expiry
                if "refresh_token" in data:
                    self.refresh_token = data["refresh_token"]
                logger.info("Loaded cached Schwab token")
                return True
        except Exception as e:
            logger.warning(f"Failed to load cached token: {e}")

        return False

    def _save_token(self, access_token: str, expires_in: int,
                    refresh_token: Optional[str] = None):
        """Save token to cache file."""
        self.access_token = access_token
        self.token_expiry = datetime.now() + timedelta(seconds=expires_in)

        token_path = Path(self.token_file)
        token_path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "access_token": access_token,
            "expiry": self.token_expiry.isoformat()
        }
        if refresh_token:
            data["refresh_token"] = refresh_token
            self.refresh_token = refresh_token

        with open(token_path, "w") as f:
            json.dump(data, f)

        logger.info("Saved Schwab token to cache")

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token."""
        if not self.refresh_token:
            logger.error("No refresh token available")
            return False

        try:
            response = requests.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                auth=(self.client_id, self.client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Token refresh failed: {response.status_code} - {response.text}")
                return False

            data = response.json()
            self._save_token(
                data["access_token"],
                data.get("expires_in", 1800),
                data.get("refresh_token")
            )
            return True

        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            return False

    def _ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token."""
        # Check if current token is still valid
        if self.access_token and self.token_expiry:
            if self.token_expiry > datetime.now() + timedelta(minutes=5):
                return True

        # Try to load from cache
        if self._load_cached_token():
            return True

        # Refresh the token
        return self._refresh_access_token()

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an authenticated API request with retry."""
        if not self._ensure_valid_token():
            raise Exception("Failed to obtain valid Schwab token")

        url = f"{self.BASE_URL}{endpoint}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"

        for attempt in range(3):
            try:
                response = requests.request(
                    method, url, headers=headers, timeout=30, **kwargs
                )

                if response.status_code == 401:
                    # Token expired, refresh and retry
                    logger.warning("Token expired, refreshing...")
                    if self._refresh_access_token():
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        continue
                    raise Exception("Token refresh failed")

                if response.status_code == 429:
                    # Rate limited
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout, attempt {attempt + 1}/3")
                if attempt == 2:
                    raise

        raise Exception("Max retries exceeded")

    def connect(self) -> bool:
        """Establish connection by validating/refreshing token."""
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            logger.error("Missing Schwab credentials in environment variables")
            return False

        return self._ensure_valid_token()

    def get_accounts(self) -> list[str]:
        """Get list of account numbers/IDs."""
        if self._account_ids:
            return self._account_ids

        try:
            data = self._request("GET", "/trader/v1/accounts/accountNumbers")
            self._account_ids = [acc["accountNumber"] for acc in data]
            logger.info(f"Found {len(self._account_ids)} Schwab accounts")
            return self._account_ids
        except Exception as e:
            logger.error(f"Failed to get accounts: {e}")
            return []

    def get_account_data(self, account_id: str) -> AccountData:
        """Fetch account equity and positions."""
        try:
            # Get account with positions
            data = self._request(
                "GET",
                f"/trader/v1/accounts/{account_id}",
                params={"fields": "positions"}
            )

            account = data.get("securitiesAccount", data)

            # Extract equity (net liquidation value)
            balances = account.get("currentBalances", {})
            equity = balances.get("liquidationValue", 0)
            cash = balances.get("cashBalance", 0)

            # Parse positions
            positions = []
            for pos in account.get("positions", []):
                instrument = pos.get("instrument", {})
                symbol = instrument.get("symbol", "UNKNOWN")
                asset_type = instrument.get("assetType", "EQUITY")

                # Determine instrument type
                if asset_type == "OPTION":
                    inst_type = InstrumentType.OPTION
                    multiplier = 100.0
                elif asset_type == "CASH_EQUIVALENT":
                    inst_type = InstrumentType.CASH
                    multiplier = 1.0
                elif asset_type == "ETF":
                    inst_type = InstrumentType.ETF
                    multiplier = 1.0
                else:
                    inst_type = InstrumentType.STOCK
                    multiplier = 1.0

                qty = pos.get("longQuantity", 0) - pos.get("shortQuantity", 0)
                price = pos.get("marketValue", 0) / (qty * multiplier) if qty != 0 else 0
                mv = pos.get("marketValue", 0)

                positions.append(Position(
                    broker="Schwab",
                    account_id=account_id,
                    symbol=symbol,
                    instrument_type=inst_type,
                    qty=qty,
                    multiplier=multiplier,
                    price=price,
                    mv=mv,
                    notes=f"assetType={asset_type}"
                ))

            return AccountData(
                broker="Schwab",
                account_id=account_id,
                equity=equity,
                cash=cash,
                positions=positions,
                status="OK"
            )

        except Exception as e:
            logger.error(f"Failed to get account data for {account_id}: {e}")
            return AccountData(
                broker="Schwab",
                account_id=account_id,
                equity=0,
                cash=0,
                positions=[],
                status="ERROR",
                error_message=str(e)
            )
