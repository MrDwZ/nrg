"""
Risk Engine for NRG.

Core logic for computing:
- Account mode (NORMAL/HALF/MIN) based on drawdown
- Thesis utilization and required reductions
"""

import csv
import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml

from .connectors.base import AccountData, Position, InstrumentType
from .storage import Storage, EquitySnapshot, ThesisMetric, PositionRecord

logger = logging.getLogger(__name__)


class RiskMode(Enum):
    NORMAL = "NORMAL"
    HALF = "HALF"
    MIN = "MIN"


class ThesisStatus(Enum):
    ACTIVE = "ACTIVE"
    WATCH = "WATCH"
    BROKEN = "BROKEN"


@dataclass
class ThesisConfig:
    """Configuration for a thesis."""
    name: str
    stress_pct: float
    budget_pct: float
    status: ThesisStatus
    falsifier: str
    time_window_end: Optional[str] = None


@dataclass
class ThesisResult:
    """Computed risk metrics for a thesis."""
    name: str
    mv: float
    stress_pct: float
    budget_pct: float
    worst_loss: float
    budget_dollars: float
    utilization: float
    action: Optional[str]
    reduce_amount: float
    target_mv: float
    status: ThesisStatus
    falsifier: str
    positions: list[Position] = field(default_factory=list)


@dataclass
class RiskResult:
    """Complete risk engine computation result."""
    timestamp: datetime
    equity: float
    peak: float
    drawdown: float
    mode: RiskMode
    risk_scale: float
    thesis_results: list[ThesisResult]
    positions: list[Position]
    status: str  # OK, DEGRADED
    broker_statuses: dict[str, str]
    actions: list[str]
    mode_changed: bool
    old_mode: Optional[RiskMode]


class RiskEngine:
    """Main risk engine for computing account and thesis risk state."""

    def __init__(self, config_dir: str = "config", data_dir: str = "data"):
        self.config_dir = Path(config_dir)
        self.data_dir = Path(data_dir)
        self.storage = Storage(str(self.data_dir / "nrg.db"))

        # Load configurations
        self.account_config = self._load_account_config()
        self.thesis_configs = self._load_thesis_config()
        self.mappings = self._load_mappings()

    def _load_account_config(self) -> dict:
        """Load account configuration from YAML."""
        config_path = self.config_dir / "account.yaml"
        try:
            with open(config_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Could not load account.yaml: {e}, using defaults")
            return {
                "drawdown_x": 0.12,
                "risk_scale": {"NORMAL": 1.0, "HALF": 0.5, "MIN": 0.2}
            }

    def _load_thesis_config(self) -> dict[str, ThesisConfig]:
        """Load thesis configurations from YAML."""
        config_path = self.config_dir / "thesis.yaml"
        configs = {}

        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)

            for name, cfg in data.get("theses", {}).items():
                configs[name] = ThesisConfig(
                    name=name,
                    stress_pct=cfg.get("stress_pct", 0.25),
                    budget_pct=cfg.get("budget_pct", 0.05),
                    status=ThesisStatus(cfg.get("status", "ACTIVE")),
                    falsifier=cfg.get("falsifier", "N/A"),
                    time_window_end=cfg.get("time_window_end")
                )
        except Exception as e:
            logger.warning(f"Could not load thesis.yaml: {e}")
            # Add default unmapped thesis
            configs["_UNMAPPED"] = ThesisConfig(
                name="_UNMAPPED",
                stress_pct=0.25,
                budget_pct=0.02,
                status=ThesisStatus.ACTIVE,
                falsifier="N/A"
            )

        return configs

    def _load_mappings(self) -> list[dict]:
        """Load symbol to thesis mappings from CSV."""
        mappings = []
        mapping_path = self.config_dir / "mapping.csv"

        try:
            with open(mapping_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mappings.append({
                        "pattern": row.get("symbol_pattern", "").strip(),
                        "thesis": row.get("thesis", "_UNMAPPED").strip(),
                        "weight": float(row.get("weight", 1.0))
                    })
        except Exception as e:
            logger.warning(f"Could not load mapping.csv: {e}")

        return mappings

    def _map_position_to_thesis(self, symbol: str) -> tuple[str, float]:
        """Map a symbol to a thesis based on mapping rules."""
        for mapping in self.mappings:
            pattern = mapping["pattern"]
            # Support exact match or wildcard/regex
            if pattern == symbol:
                return mapping["thesis"], mapping["weight"]
            # Try as regex pattern
            try:
                if re.match(f"^{pattern}$", symbol, re.IGNORECASE):
                    return mapping["thesis"], mapping["weight"]
            except re.error:
                pass

        return "_UNMAPPED", 1.0

    def _compute_mode(self, drawdown: float) -> tuple[RiskMode, float]:
        """Compute risk mode based on drawdown."""
        x = self.account_config.get("drawdown_x", 0.12)
        risk_scales = self.account_config.get("risk_scale", {})

        if drawdown > -x:
            mode = RiskMode.NORMAL
        elif drawdown > -2 * x:
            mode = RiskMode.HALF
        else:
            mode = RiskMode.MIN

        scale = risk_scales.get(mode.value, 1.0)
        return mode, scale

    def compute(self, accounts_data: list[AccountData]) -> RiskResult:
        """Run the full risk computation."""
        timestamp = datetime.now()
        actions = []
        broker_statuses = {}

        # Aggregate equity and positions across all accounts
        total_equity = 0.0
        all_positions: list[Position] = []
        status = "OK"

        for acc in accounts_data:
            broker_statuses[f"{acc.broker}:{acc.account_id}"] = acc.status
            if acc.status == "OK":
                total_equity += acc.equity
                all_positions.extend(acc.positions)
            else:
                status = "DEGRADED"
                logger.warning(f"Broker {acc.broker} account {acc.account_id} "
                             f"status: {acc.status} - {acc.error_message}")

        if total_equity <= 0:
            logger.error("Cannot compute risk: equity is zero or negative")
            raise ValueError("Equity cannot be computed reliably")

        # Get historical peak and compute drawdown
        historical_peak = self.storage.get_peak()
        peak = max(historical_peak, total_equity)
        drawdown = (total_equity - peak) / peak if peak > 0 else 0.0

        # Compute mode and check for mode change
        mode, risk_scale = self._compute_mode(drawdown)
        old_mode_str = self.storage.get_last_mode()
        old_mode = RiskMode(old_mode_str) if old_mode_str else None
        mode_changed = old_mode is not None and old_mode != mode

        if mode_changed:
            actions.append(f"MODE CHANGE: {old_mode.value} -> {mode.value}")
            self.storage.save_mode_change(timestamp, old_mode_str, mode.value,
                                         total_equity, drawdown)

        if mode != RiskMode.NORMAL:
            actions.append(f"Account in {mode.value} mode - risk scaled to {risk_scale:.0%}")

        # Map positions to theses
        for pos in all_positions:
            thesis, weight = self._map_position_to_thesis(pos.symbol)
            pos.thesis = thesis

        # Aggregate by thesis
        thesis_mvs: dict[str, float] = {}
        thesis_positions: dict[str, list[Position]] = {}

        for pos in all_positions:
            thesis = pos.thesis
            if thesis not in thesis_mvs:
                thesis_mvs[thesis] = 0.0
                thesis_positions[thesis] = []
            thesis_mvs[thesis] += pos.mv
            thesis_positions[thesis].append(pos)

        # Compute thesis metrics
        thesis_results: list[ThesisResult] = []

        for thesis_name, mv in thesis_mvs.items():
            config = self.thesis_configs.get(thesis_name)
            if not config:
                # Use defaults for unmapped
                config = ThesisConfig(
                    name=thesis_name,
                    stress_pct=0.25,
                    budget_pct=0.02,
                    status=ThesisStatus.ACTIVE,
                    falsifier="N/A"
                )

            # Check for short options - flag as unsupported risk
            has_short_options = any(
                p.instrument_type == InstrumentType.OPTION and p.qty < 0
                for p in thesis_positions.get(thesis_name, [])
            )
            if has_short_options:
                actions.append(f"WARNING: {thesis_name} has short options - "
                             "risk may be understated (UNSUPPORTED_RISK)")

            # Compute risk metrics
            worst_loss = abs(mv) * config.stress_pct
            budget_dollars = total_equity * config.budget_pct * risk_scale
            utilization = worst_loss / budget_dollars if budget_dollars > 0 else float('inf')

            # Determine action
            action = None
            reduce_amount = 0.0
            target_mv = mv

            if config.status == ThesisStatus.BROKEN:
                action = "EXIT"
                reduce_amount = mv
                target_mv = 0
                actions.append(f"{thesis_name}: EXIT (thesis BROKEN)")
            elif utilization > 1.0:
                target_mv = budget_dollars / config.stress_pct
                reduce_amount = mv - target_mv
                action = f"REDUCE ${reduce_amount:,.0f}"
                actions.append(f"{thesis_name}: {action} (Util={utilization:.1%})")

            thesis_results.append(ThesisResult(
                name=thesis_name,
                mv=mv,
                stress_pct=config.stress_pct,
                budget_pct=config.budget_pct,
                worst_loss=worst_loss,
                budget_dollars=budget_dollars,
                utilization=utilization,
                action=action,
                reduce_amount=reduce_amount,
                target_mv=target_mv,
                status=config.status,
                falsifier=config.falsifier,
                positions=thesis_positions.get(thesis_name, [])
            ))

        # Sort by utilization descending
        thesis_results.sort(key=lambda t: t.utilization, reverse=True)

        # Save to storage
        self._save_results(timestamp, total_equity, peak, drawdown, mode,
                          risk_scale, status, thesis_results, all_positions)

        return RiskResult(
            timestamp=timestamp,
            equity=total_equity,
            peak=peak,
            drawdown=drawdown,
            mode=mode,
            risk_scale=risk_scale,
            thesis_results=thesis_results,
            positions=all_positions,
            status=status,
            broker_statuses=broker_statuses,
            actions=actions,
            mode_changed=mode_changed,
            old_mode=old_mode
        )

    def _save_results(self, timestamp: datetime, equity: float, peak: float,
                     drawdown: float, mode: RiskMode, risk_scale: float,
                     status: str, thesis_results: list[ThesisResult],
                     positions: list[Position]):
        """Save computation results to storage."""
        # Save equity snapshot
        self.storage.save_equity_snapshot(EquitySnapshot(
            timestamp=timestamp,
            equity=equity,
            peak=peak,
            drawdown=drawdown,
            mode=mode.value,
            risk_scale=risk_scale,
            status=status
        ))

        # Save thesis metrics
        thesis_metrics = [
            ThesisMetric(
                timestamp=timestamp,
                thesis=t.name,
                mv=t.mv,
                stress_pct=t.stress_pct,
                budget_pct=t.budget_pct,
                worst_loss=t.worst_loss,
                budget_dollars=t.budget_dollars,
                utilization=t.utilization,
                action=t.action,
                status=t.status.value
            )
            for t in thesis_results
        ]
        self.storage.save_thesis_metrics(thesis_metrics)

        # Save positions
        position_records = [
            PositionRecord(
                timestamp=timestamp,
                broker=p.broker,
                account_id=p.account_id,
                symbol=p.symbol,
                instrument_type=p.instrument_type.value,
                qty=p.qty,
                multiplier=p.multiplier,
                price=p.price,
                mv=p.mv,
                currency=p.currency,
                thesis=p.thesis,
                notes=p.notes
            )
            for p in positions
        ]
        self.storage.save_positions(position_records)

    def format_summary(self, result: RiskResult) -> str:
        """Format a human-readable summary."""
        lines = [
            "=" * 60,
            f"NRG Daily Risk Summary - {result.timestamp.strftime('%Y-%m-%d %H:%M PT')}",
            "=" * 60,
            "",
            "ACCOUNT STATUS",
            "-" * 40,
            f"  Equity:     ${result.equity:>15,.2f}",
            f"  Peak:       ${result.peak:>15,.2f}",
            f"  Drawdown:   {result.drawdown:>15.2%}",
            f"  Mode:       {result.mode.value:>15}",
            f"  Risk Scale: {result.risk_scale:>15.0%}",
            f"  Status:     {result.status:>15}",
            "",
        ]

        if result.mode_changed:
            lines.append(f"  *** MODE CHANGED: {result.old_mode.value} -> {result.mode.value} ***")
            lines.append("")

        lines.extend([
            "THESIS UTILIZATION",
            "-" * 40,
            f"  {'Thesis':<20} {'MV':>12} {'Util':>8} {'Action':<15}",
            "-" * 60,
        ])

        for t in result.thesis_results:
            action_str = t.action or ""
            util_str = f"{t.utilization:.0%}" if t.utilization < 100 else ">9999%"
            lines.append(f"  {t.name:<20} ${t.mv:>10,.0f} {util_str:>8} {action_str:<15}")

        if result.actions:
            lines.extend([
                "",
                "ACTIONS REQUIRED",
                "-" * 40,
            ])
            for action in result.actions:
                lines.append(f"  * {action}")

        lines.extend([
            "",
            "=" * 60,
        ])

        return "\n".join(lines)
