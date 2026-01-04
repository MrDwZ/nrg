"""
Google Sheets Writer for NRG.

Writes risk data to Google Sheets with stable schema for dashboard consumption.

Environment variables:
- GOOGLE_SHEETS_CREDENTIALS: Path to service account JSON file
- GOOGLE_SHEETS_ID: ID of the target Google Sheet
"""

import os
import logging
from datetime import datetime
from typing import Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .risk_engine import RiskResult, ThesisResult
from .connectors.base import Position

logger = logging.getLogger(__name__)


class SheetsWriter:
    """Google Sheets writer with stable schema for NRG dashboard."""

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Sheet tab names
    ACCOUNT_SHEET = "Account"
    THESIS_SHEET = "Thesis"
    POSITIONS_SHEET = "Positions"
    SNAPSHOTS_SHEET = "Snapshots"

    def __init__(self, credentials_path: Optional[str] = None,
                 sheet_id: Optional[str] = None):
        self.credentials_path = credentials_path or os.environ.get(
            "GOOGLE_SHEETS_CREDENTIALS", "config/google_credentials.json"
        )
        self.sheet_id = sheet_id or os.environ.get("GOOGLE_SHEETS_ID")
        self._service = None

    def _get_service(self):
        """Initialize Google Sheets API service."""
        if self._service:
            return self._service

        try:
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=self.SCOPES
            )
            self._service = build("sheets", "v4", credentials=creds)
            return self._service
        except Exception as e:
            logger.error(f"Failed to initialize Sheets API: {e}")
            raise

    def _ensure_sheet_exists(self, sheet_name: str):
        """Create a sheet tab if it doesn't exist."""
        service = self._get_service()

        try:
            # Get existing sheets
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=self.sheet_id
            ).execute()

            existing_sheets = [
                s["properties"]["title"]
                for s in spreadsheet.get("sheets", [])
            ]

            if sheet_name not in existing_sheets:
                request = {
                    "addSheet": {
                        "properties": {"title": sheet_name}
                    }
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=self.sheet_id,
                    body={"requests": [request]}
                ).execute()
                logger.info(f"Created sheet: {sheet_name}")

        except HttpError as e:
            logger.error(f"Error checking/creating sheet {sheet_name}: {e}")
            raise

    def _write_range(self, range_name: str, values: list[list],
                    value_input_option: str = "USER_ENTERED"):
        """Write values to a range in the sheet."""
        service = self._get_service()

        try:
            body = {"values": values}
            service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()
        except HttpError as e:
            logger.error(f"Error writing to {range_name}: {e}")
            raise

    def _append_row(self, sheet_name: str, values: list):
        """Append a row to a sheet."""
        service = self._get_service()

        try:
            body = {"values": [values]}
            service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,
                range=f"{sheet_name}!A:A",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()
        except HttpError as e:
            logger.error(f"Error appending to {sheet_name}: {e}")
            raise

    def _clear_range(self, range_name: str):
        """Clear a range in the sheet."""
        service = self._get_service()

        try:
            service.spreadsheets().values().clear(
                spreadsheetId=self.sheet_id,
                range=range_name,
                body={}
            ).execute()
        except HttpError as e:
            logger.error(f"Error clearing {range_name}: {e}")
            raise

    def write_account(self, result: RiskResult):
        """Write account data to the Account sheet."""
        self._ensure_sheet_exists(self.ACCOUNT_SHEET)

        # Fixed header row
        headers = ["DateTime", "Equity", "Peak", "Drawdown", "Mode", "RiskScale", "Status"]

        values = [
            headers,
            [
                result.timestamp.isoformat(),
                result.equity,
                result.peak,
                result.drawdown,
                result.mode.value,
                result.risk_scale,
                result.status
            ]
        ]

        self._write_range(f"{self.ACCOUNT_SHEET}!A1:G2", values)
        logger.info("Updated Account sheet")

    def write_thesis(self, result: RiskResult):
        """Write thesis data to the Thesis sheet."""
        self._ensure_sheet_exists(self.THESIS_SHEET)

        # Fixed header row
        headers = [
            "Thesis", "MV", "Stress%", "Budget%", "WorstLoss",
            "Budget$", "Utilization", "Action", "Status", "Falsifier"
        ]

        values = [headers]
        for t in result.thesis_results:
            values.append([
                t.name,
                t.mv,
                t.stress_pct,
                t.budget_pct,
                t.worst_loss,
                t.budget_dollars,
                t.utilization,
                t.action or "",
                t.status.value,
                t.falsifier
            ])

        # Clear existing data and write new
        self._clear_range(f"{self.THESIS_SHEET}!A:J")
        self._write_range(f"{self.THESIS_SHEET}!A1", values)
        logger.info(f"Updated Thesis sheet with {len(result.thesis_results)} rows")

    def write_positions(self, result: RiskResult):
        """Write positions to the Positions sheet."""
        self._ensure_sheet_exists(self.POSITIONS_SHEET)

        # Fixed header row
        headers = [
            "Broker", "Account", "Symbol", "Type", "Qty",
            "Price", "MV", "Thesis", "Notes"
        ]

        values = [headers]
        for p in result.positions:
            values.append([
                p.broker,
                p.account_id,
                p.symbol,
                p.instrument_type.value,
                p.qty,
                p.price,
                p.mv,
                p.thesis,
                p.notes or ""
            ])

        # Clear and write
        self._clear_range(f"{self.POSITIONS_SHEET}!A:I")
        self._write_range(f"{self.POSITIONS_SHEET}!A1", values)
        logger.info(f"Updated Positions sheet with {len(result.positions)} rows")

    def write_snapshot(self, result: RiskResult):
        """Append a snapshot row to the Snapshots sheet (append-only)."""
        self._ensure_sheet_exists(self.SNAPSHOTS_SHEET)

        # Check if headers exist
        service = self._get_service()
        try:
            response = service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=f"{self.SNAPSHOTS_SHEET}!A1:L1"
            ).execute()
            existing = response.get("values", [[]])
        except HttpError:
            existing = [[]]

        headers = [
            "DateTime", "Equity", "Peak", "Drawdown", "Mode", "RiskScale",
            "Status", "TopThesis", "TopUtil", "NumActions", "ActionSummary"
        ]

        # Write headers if not present
        if not existing or existing[0] != headers:
            self._write_range(f"{self.SNAPSHOTS_SHEET}!A1:K1", [headers])

        # Build snapshot row
        top_thesis = result.thesis_results[0] if result.thesis_results else None
        action_summary = "; ".join(result.actions[:3]) if result.actions else ""

        row = [
            result.timestamp.isoformat(),
            result.equity,
            result.peak,
            result.drawdown,
            result.mode.value,
            result.risk_scale,
            result.status,
            top_thesis.name if top_thesis else "",
            top_thesis.utilization if top_thesis else 0,
            len(result.actions),
            action_summary[:200]  # Truncate
        ]

        self._append_row(self.SNAPSHOTS_SHEET, row)
        logger.info("Appended snapshot row")

    def write_all(self, result: RiskResult):
        """Write all data to the Google Sheet."""
        if not self.sheet_id:
            logger.warning("No Google Sheet ID configured, skipping sheets write")
            return False

        try:
            self.write_account(result)
            self.write_thesis(result)
            self.write_positions(result)
            self.write_snapshot(result)
            logger.info("Successfully updated Google Sheet")
            return True
        except Exception as e:
            logger.error(f"Failed to write to Google Sheet: {e}")
            return False
