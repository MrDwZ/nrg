"""Base connector interface for broker integrations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class InstrumentType(Enum):
    STOCK = "STOCK"
    ETF = "ETF"
    OPTION = "OPTION"
    CASH = "CASH"
    OTHER = "OTHER"


@dataclass
class Position:
    """Normalized position schema."""
    broker: str
    account_id: str
    symbol: str
    instrument_type: InstrumentType
    qty: float
    multiplier: float  # 1 for stocks, 100 for options by default
    price: float  # mark/last price
    mv: float  # market value = qty * price * multiplier
    currency: str = "USD"
    thesis: str = "_UNMAPPED"
    notes: Optional[str] = None


@dataclass
class AccountData:
    """Account data from a broker."""
    broker: str
    account_id: str
    equity: float  # Net liquidation value
    cash: float
    positions: list[Position]
    status: str = "OK"  # OK, ERROR, PARTIAL
    error_message: Optional[str] = None


class BaseConnector(ABC):
    """Abstract base class for broker connectors."""

    @property
    @abstractmethod
    def broker_name(self) -> str:
        """Return the broker name identifier."""
        pass

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection / authenticate with the broker.
        Returns True if successful.
        """
        pass

    @abstractmethod
    def get_accounts(self) -> list[str]:
        """Return list of account IDs."""
        pass

    @abstractmethod
    def get_account_data(self, account_id: str) -> AccountData:
        """Fetch account equity and positions for a specific account."""
        pass

    def get_all_accounts_data(self) -> list[AccountData]:
        """Fetch data for all accounts."""
        accounts = []
        for account_id in self.get_accounts():
            try:
                data = self.get_account_data(account_id)
                accounts.append(data)
            except Exception as e:
                accounts.append(AccountData(
                    broker=self.broker_name,
                    account_id=account_id,
                    equity=0,
                    cash=0,
                    positions=[],
                    status="ERROR",
                    error_message=str(e)
                ))
        return accounts
