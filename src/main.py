#!/usr/bin/env python3
"""
NRG - Narrative Risk Guard

Main entry point for the risk control system.

Usage:
    python -m src.main [--intraday] [--dry-run] [--no-sheets]

Environment Variables:
    SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET, SCHWAB_REFRESH_TOKEN
    FIDELITY_CSV_DIR
    GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from .connectors import SchwabConnector, FidelityCSVConnector
from .connectors.base import AccountData
from .risk_engine import RiskEngine
from .sheets_writer import SheetsWriter
from .notifications import Notifier
from .storage import Storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/nrg.log")
    ]
)
logger = logging.getLogger("nrg")


def load_config(config_dir: str = "config") -> dict:
    """Load account configuration."""
    config_path = Path(config_dir) / "account.toml"
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        logger.warning(f"Could not load config: {e}")
        return {}


def collect_broker_data() -> tuple[list[AccountData], dict[str, str]]:
    """Collect data from all configured brokers."""
    all_accounts: list[AccountData] = []
    broker_statuses: dict[str, str] = {}

    # Try Schwab
    try:
        schwab = SchwabConnector()
        if schwab.connect():
            accounts = schwab.get_all_accounts_data()
            all_accounts.extend(accounts)
            broker_statuses["Schwab"] = "OK"
            logger.info(f"Schwab: loaded {len(accounts)} accounts")
        else:
            broker_statuses["Schwab"] = "CONNECT_FAILED"
            logger.warning("Schwab: connection failed")
    except Exception as e:
        broker_statuses["Schwab"] = f"ERROR: {e}"
        logger.error(f"Schwab error: {e}")

    # Try Fidelity CSV
    try:
        fidelity = FidelityCSVConnector()
        if fidelity.connect():
            accounts = fidelity.get_all_accounts_data()
            all_accounts.extend(accounts)
            broker_statuses["Fidelity"] = "OK"
            csv_info = fidelity.get_csv_file_info()
            logger.info(f"Fidelity: loaded {len(accounts)} accounts from {csv_info.get('file', 'unknown')}")
        else:
            broker_statuses["Fidelity"] = "NO_CSV_FILE"
            logger.warning("Fidelity: no CSV file found")
    except Exception as e:
        broker_statuses["Fidelity"] = f"ERROR: {e}"
        logger.error(f"Fidelity error: {e}")

    return all_accounts, broker_statuses


def run(dry_run: bool = False, skip_sheets: bool = False) -> int:
    """Execute a single risk computation run."""
    start_time = time.time()
    timestamp = datetime.now()
    logger.info("=" * 60)
    logger.info(f"NRG Run Starting: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Load config
    config = load_config()
    notification_config = config.get("notifications", {})
    notifier = Notifier(notification_config)

    # Collect broker data
    accounts, broker_statuses = collect_broker_data()

    # Check for broker failures
    for broker, status in broker_statuses.items():
        if status not in ["OK", "NO_CSV_FILE"]:
            notifier.notify_data_failure(broker, status)

    if not accounts:
        logger.error("No account data available from any broker")
        return 1

    # Initialize and run risk engine
    try:
        engine = RiskEngine()
        result = engine.compute(accounts)
    except ValueError as e:
        logger.error(f"Risk computation failed: {e}")
        return 1

    # Print summary
    summary = engine.format_summary(result)
    print(summary)

    # Log run
    duration = time.time() - start_time
    storage = Storage()
    storage.log_run(timestamp, result.status, "Completed", broker_statuses, duration)

    # Send notifications
    if result.mode_changed:
        notifier.notify_mode_change(result)

    breaches = [t for t in result.thesis_results if t.utilization > 1.0]
    if breaches:
        notifier.notify_utilization_breach(result)

    # Write to Google Sheets
    if not skip_sheets and not dry_run:
        try:
            writer = SheetsWriter()
            if writer.write_all(result):
                logger.info("Google Sheets updated successfully")
            else:
                logger.warning("Google Sheets update skipped (not configured)")
        except Exception as e:
            logger.error(f"Google Sheets error: {e}")

    logger.info(f"Run completed in {duration:.2f}s")
    return 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="NRG - Narrative Risk Guard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m src.main              # Normal daily run
    python -m src.main --dry-run    # Compute but don't write to sheets
    python -m src.main --no-sheets  # Skip Google Sheets update
        """
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute risk but don't write to external services"
    )
    parser.add_argument(
        "--no-sheets",
        action="store_true",
        help="Skip Google Sheets update"
    )
    parser.add_argument(
        "--intraday",
        action="store_true",
        help="Run in intraday mode (currently same as daily)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)

    sys.exit(run(
        dry_run=args.dry_run,
        skip_sheets=args.no_sheets
    ))


if __name__ == "__main__":
    main()
