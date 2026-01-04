"""
Fidelity CSV Connector for NRG.

Parses exported position CSV files from Fidelity website.
Watches a configured directory for the latest CSV file.

Environment variables:
- FIDELITY_CSV_DIR: Directory path where Fidelity CSV files are placed
"""

import os
import csv
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from .base import BaseConnector, AccountData, Position, InstrumentType

logger = logging.getLogger(__name__)


class FidelityCSVConnector(BaseConnector):
    """Fidelity connector that parses exported CSV position files."""

    # Common Fidelity CSV column names (may vary by export type)
    COLUMN_MAPPINGS = {
        "symbol": ["Symbol", "symbol", "SYMBOL"],
        "description": ["Description", "description", "Security Description"],
        "quantity": ["Quantity", "quantity", "Shares", "shares", "Qty"],
        "price": ["Last Price", "Current Value", "Price", "price", "Last"],
        "value": ["Current Value", "Market Value", "Value", "value"],
        "account": ["Account Number", "Account", "account", "Account Name/Number"],
        "type": ["Type", "Security Type", "Asset Class"],
        "cost_basis": ["Cost Basis Total", "Cost Basis", "Total Cost Basis"],
    }

    def __init__(self, csv_dir: Optional[str] = None):
        self.csv_dir = Path(csv_dir or os.environ.get("FIDELITY_CSV_DIR", "data/fidelity"))
        self._latest_file: Optional[Path] = None
        self._parsed_data: Optional[list[AccountData]] = None

    @property
    def broker_name(self) -> str:
        return "Fidelity"

    def _find_latest_csv(self) -> Optional[Path]:
        """Find the most recent CSV file in the watched directory."""
        if not self.csv_dir.exists():
            logger.warning(f"Fidelity CSV directory does not exist: {self.csv_dir}")
            return None

        csv_files = list(self.csv_dir.glob("*.csv")) + list(self.csv_dir.glob("*.CSV"))
        if not csv_files:
            logger.warning(f"No CSV files found in {self.csv_dir}")
            return None

        # Sort by modification time, newest first
        csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        latest = csv_files[0]
        logger.info(f"Found latest Fidelity CSV: {latest.name}")
        return latest

    def _normalize_column(self, header: str, mapping_key: str) -> bool:
        """Check if a header matches any of the known column names."""
        return header.strip() in self.COLUMN_MAPPINGS.get(mapping_key, [])

    def _find_column(self, headers: list[str], mapping_key: str) -> Optional[int]:
        """Find the column index for a given mapping key."""
        for i, h in enumerate(headers):
            if self._normalize_column(h, mapping_key):
                return i
        return None

    def _parse_number(self, value: str) -> float:
        """Parse a number from various formats ($1,234.56 -> 1234.56)."""
        if not value or value.strip() in ["", "--", "n/a", "N/A"]:
            return 0.0
        # Remove $, commas, and whitespace
        cleaned = re.sub(r"[$,\s]", "", value.strip())
        # Handle parentheses for negative numbers
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = "-" + cleaned[1:-1]
        try:
            return float(cleaned)
        except ValueError:
            logger.warning(f"Could not parse number: {value}")
            return 0.0

    def _determine_instrument_type(self, symbol: str, description: str,
                                   type_str: str) -> InstrumentType:
        """Determine instrument type from available info."""
        symbol_upper = symbol.upper()
        desc_upper = description.upper() if description else ""
        type_upper = type_str.upper() if type_str else ""

        # Cash/money market
        if any(x in desc_upper for x in ["MONEY MARKET", "CASH", "SPAXX", "FDRXX"]):
            return InstrumentType.CASH
        if symbol_upper in ["SPAXX", "FDRXX", "CORE", "CASH"]:
            return InstrumentType.CASH

        # Options (usually have special symbols or descriptions)
        if any(x in desc_upper for x in ["CALL", "PUT", "OPTION"]):
            return InstrumentType.OPTION
        # Option symbols often contain numbers in middle
        if re.match(r"[A-Z]+\d{6}[CP]\d+", symbol_upper):
            return InstrumentType.OPTION

        # ETF detection
        if "ETF" in type_upper or "ETF" in desc_upper:
            return InstrumentType.ETF

        return InstrumentType.STOCK

    def _parse_csv(self, file_path: Path) -> list[AccountData]:
        """Parse a Fidelity positions CSV file."""
        accounts_data: dict[str, AccountData] = {}

        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                # Try to detect the CSV format
                content = f.read()

            # Handle potential multi-section Fidelity exports
            lines = content.strip().split("\n")

            # Find the header row (look for common column names)
            header_idx = 0
            for i, line in enumerate(lines):
                if any(col in line for col in ["Symbol", "Quantity", "Current Value"]):
                    header_idx = i
                    break

            # Parse CSV from header row
            reader = csv.reader(lines[header_idx:])
            headers = next(reader, [])

            # Find column indices
            symbol_col = self._find_column(headers, "symbol")
            qty_col = self._find_column(headers, "quantity")
            price_col = self._find_column(headers, "price")
            value_col = self._find_column(headers, "value")
            account_col = self._find_column(headers, "account")
            type_col = self._find_column(headers, "type")
            desc_col = self._find_column(headers, "description")

            if symbol_col is None:
                raise ValueError("Could not find Symbol column in CSV")

            for row in reader:
                if len(row) <= symbol_col:
                    continue

                symbol = row[symbol_col].strip()
                if not symbol or symbol.lower() in ["total", "account total", ""]:
                    continue

                # Get account ID
                account_id = row[account_col].strip() if account_col and len(row) > account_col else "default"
                # Clean up account ID (remove extra info)
                account_id = account_id.split()[0] if account_id else "default"

                # Get quantity
                qty = self._parse_number(row[qty_col]) if qty_col and len(row) > qty_col else 0

                # Get price and value
                price = self._parse_number(row[price_col]) if price_col and len(row) > price_col else 0
                mv = self._parse_number(row[value_col]) if value_col and len(row) > value_col else 0

                # If we have value but not price, calculate price
                if mv and not price and qty:
                    price = mv / qty

                # If we have price but not value
                if price and qty and not mv:
                    mv = price * qty

                # Get type and description
                type_str = row[type_col].strip() if type_col and len(row) > type_col else ""
                description = row[desc_col].strip() if desc_col and len(row) > desc_col else ""

                inst_type = self._determine_instrument_type(symbol, description, type_str)
                multiplier = 100.0 if inst_type == InstrumentType.OPTION else 1.0

                # Create position
                position = Position(
                    broker="Fidelity",
                    account_id=account_id,
                    symbol=symbol,
                    instrument_type=inst_type,
                    qty=qty,
                    multiplier=multiplier,
                    price=price,
                    mv=mv,
                    notes=description[:100] if description else None
                )

                # Add to account
                if account_id not in accounts_data:
                    accounts_data[account_id] = AccountData(
                        broker="Fidelity",
                        account_id=account_id,
                        equity=0,
                        cash=0,
                        positions=[],
                        status="OK"
                    )

                accounts_data[account_id].positions.append(position)

                # Track cash separately
                if inst_type == InstrumentType.CASH:
                    accounts_data[account_id].cash += mv

            # Calculate equity for each account (sum of MVs)
            for account in accounts_data.values():
                account.equity = sum(p.mv for p in account.positions)

            return list(accounts_data.values())

        except Exception as e:
            logger.error(f"Failed to parse Fidelity CSV: {e}")
            return [AccountData(
                broker="Fidelity",
                account_id="unknown",
                equity=0,
                cash=0,
                positions=[],
                status="ERROR",
                error_message=str(e)
            )]

    def connect(self) -> bool:
        """Check if CSV directory exists and has files."""
        self._latest_file = self._find_latest_csv()
        if self._latest_file:
            logger.info(f"Fidelity connector ready: {self._latest_file}")
            return True
        return False

    def get_accounts(self) -> list[str]:
        """Get list of account IDs from parsed CSV."""
        if self._parsed_data is None:
            if not self._latest_file:
                self._latest_file = self._find_latest_csv()
            if self._latest_file:
                self._parsed_data = self._parse_csv(self._latest_file)
            else:
                return []

        return [acc.account_id for acc in self._parsed_data]

    def get_account_data(self, account_id: str) -> AccountData:
        """Get account data for a specific account."""
        if self._parsed_data is None:
            if not self._latest_file:
                self._latest_file = self._find_latest_csv()
            if self._latest_file:
                self._parsed_data = self._parse_csv(self._latest_file)

        if self._parsed_data:
            for acc in self._parsed_data:
                if acc.account_id == account_id:
                    return acc

        return AccountData(
            broker="Fidelity",
            account_id=account_id,
            equity=0,
            cash=0,
            positions=[],
            status="ERROR",
            error_message="Account not found in CSV"
        )

    def get_all_accounts_data(self) -> list[AccountData]:
        """Get all account data from the CSV."""
        if self._parsed_data is None:
            if not self._latest_file:
                self._latest_file = self._find_latest_csv()
            if self._latest_file:
                self._parsed_data = self._parse_csv(self._latest_file)
            else:
                return []

        return self._parsed_data or []

    def get_csv_file_info(self) -> dict:
        """Get info about the current CSV file being used."""
        if not self._latest_file:
            return {"status": "no_file"}

        stat = self._latest_file.stat()
        return {
            "status": "ok",
            "file": self._latest_file.name,
            "path": str(self._latest_file),
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "size_bytes": stat.st_size
        }
