"""
Storage module for NRG - SQLite persistence for equity history, mode history,
thesis metrics, and position snapshots.
"""

import sqlite3
import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict


@dataclass
class EquitySnapshot:
    timestamp: datetime
    equity: float
    peak: float
    drawdown: float
    mode: str
    risk_scale: float
    status: str  # OK, DEGRADED


@dataclass
class ThesisMetric:
    timestamp: datetime
    thesis: str
    mv: float
    stress_pct: float
    budget_pct: float
    worst_loss: float
    budget_dollars: float
    utilization: float
    action: Optional[str]
    status: str


@dataclass
class PositionRecord:
    timestamp: datetime
    broker: str
    account_id: str
    symbol: str
    instrument_type: str
    qty: float
    multiplier: float
    price: float
    mv: float
    currency: str
    thesis: str
    notes: Optional[str]


class Storage:
    """SQLite-based storage for NRG data."""

    def __init__(self, db_path: str = "data/nrg.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database schema."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Equity history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                equity REAL NOT NULL,
                peak REAL NOT NULL,
                drawdown REAL NOT NULL,
                mode TEXT NOT NULL,
                risk_scale REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Mode history table (tracks mode changes)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mode_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                old_mode TEXT,
                new_mode TEXT NOT NULL,
                equity REAL NOT NULL,
                drawdown REAL NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Thesis daily metrics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS thesis_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                thesis TEXT NOT NULL,
                mv REAL NOT NULL,
                stress_pct REAL NOT NULL,
                budget_pct REAL NOT NULL,
                worst_loss REAL NOT NULL,
                budget_dollars REAL NOT NULL,
                utilization REAL NOT NULL,
                action TEXT,
                status TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Position snapshots
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                broker TEXT NOT NULL,
                account_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                qty REAL NOT NULL,
                multiplier REAL NOT NULL,
                price REAL NOT NULL,
                mv REAL NOT NULL,
                currency TEXT NOT NULL,
                thesis TEXT NOT NULL,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Run log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                brokers_status TEXT,
                duration_seconds REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_equity_timestamp
            ON equity_history(timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_thesis_timestamp
            ON thesis_metrics(timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_positions_timestamp
            ON positions(timestamp)
        """)

        conn.commit()
        conn.close()

    def get_peak(self) -> float:
        """Get the historical peak equity value."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(peak) as peak FROM equity_history")
        row = cursor.fetchone()
        conn.close()
        return row["peak"] if row and row["peak"] else 0.0

    def get_last_mode(self) -> Optional[str]:
        """Get the most recent mode."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mode FROM equity_history
            ORDER BY timestamp DESC LIMIT 1
        """)
        row = cursor.fetchone()
        conn.close()
        return row["mode"] if row else None

    def save_equity_snapshot(self, snapshot: EquitySnapshot):
        """Save an equity snapshot."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO equity_history
            (timestamp, equity, peak, drawdown, mode, risk_scale, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.timestamp.isoformat(),
            snapshot.equity,
            snapshot.peak,
            snapshot.drawdown,
            snapshot.mode,
            snapshot.risk_scale,
            snapshot.status
        ))
        conn.commit()
        conn.close()

    def save_mode_change(self, timestamp: datetime, old_mode: Optional[str],
                         new_mode: str, equity: float, drawdown: float):
        """Record a mode change event."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mode_history
            (timestamp, old_mode, new_mode, equity, drawdown)
            VALUES (?, ?, ?, ?, ?)
        """, (
            timestamp.isoformat(),
            old_mode,
            new_mode,
            equity,
            drawdown
        ))
        conn.commit()
        conn.close()

    def save_thesis_metrics(self, metrics: list[ThesisMetric]):
        """Save thesis metrics for a run."""
        conn = self._get_conn()
        cursor = conn.cursor()
        for m in metrics:
            cursor.execute("""
                INSERT INTO thesis_metrics
                (timestamp, thesis, mv, stress_pct, budget_pct, worst_loss,
                 budget_dollars, utilization, action, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                m.timestamp.isoformat(),
                m.thesis,
                m.mv,
                m.stress_pct,
                m.budget_pct,
                m.worst_loss,
                m.budget_dollars,
                m.utilization,
                m.action,
                m.status
            ))
        conn.commit()
        conn.close()

    def save_positions(self, positions: list[PositionRecord]):
        """Save position snapshot."""
        conn = self._get_conn()
        cursor = conn.cursor()
        for p in positions:
            cursor.execute("""
                INSERT INTO positions
                (timestamp, broker, account_id, symbol, instrument_type, qty,
                 multiplier, price, mv, currency, thesis, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                p.timestamp.isoformat(),
                p.broker,
                p.account_id,
                p.symbol,
                p.instrument_type,
                p.qty,
                p.multiplier,
                p.price,
                p.mv,
                p.currency,
                p.thesis,
                p.notes
            ))
        conn.commit()
        conn.close()

    def log_run(self, timestamp: datetime, status: str, message: str,
                brokers_status: dict, duration_seconds: float):
        """Log a run execution."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO run_log
            (timestamp, status, message, brokers_status, duration_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (
            timestamp.isoformat(),
            status,
            message,
            json.dumps(brokers_status),
            duration_seconds
        ))
        conn.commit()
        conn.close()

    def get_equity_history(self, days: int = 365) -> list[dict]:
        """Get equity history for the last N days."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM equity_history
            WHERE date(timestamp) >= date('now', ?)
            ORDER BY timestamp DESC
        """, (f"-{days} days",))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows

    def get_thesis_history(self, thesis: str, days: int = 30) -> list[dict]:
        """Get thesis metrics history."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM thesis_metrics
            WHERE thesis = ? AND date(timestamp) >= date('now', ?)
            ORDER BY timestamp DESC
        """, (thesis, f"-{days} days"))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows

    def get_latest_positions(self) -> list[dict]:
        """Get the most recent position snapshot."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM positions
            WHERE date(timestamp) = (SELECT MAX(date(timestamp)) FROM positions)
            ORDER BY mv DESC
        """)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows
