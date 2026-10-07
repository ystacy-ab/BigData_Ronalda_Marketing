"""Single source of truth: naming, allowed values and metric parameters.

Every notebook imports this module (via notebooks/00_config.py), so changing a
name or a rule here changes it everywhere.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

PROJECT = "tpch_mkt"
LAYERS = ("bronze", "silver", "gold", "ops")

# Load order matters for foreign keys: parents first.
SOURCE_TABLES = ("region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem")

# ---- Allowed values: derived by profiling bronze (see notebooks/04_validation.py, "Profiling") ----
ALLOWED_MARKET_SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
ALLOWED_ORDER_STATUS = ("F", "O", "P")
ALLOWED_ORDER_PRIORITY = ("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
ALLOWED_SHIP_MODES = ("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")
ALLOWED_RETURN_FLAGS = ("A", "N", "R")
ALLOWED_LINE_STATUS = ("F", "O")
DISCOUNT_RANGE = (0.0, 0.10)
TAX_RANGE = (0.0, 0.08)

# ---- Metric parameters (definitions are in README -> "Metric definitions") ----
RETENTION_WINDOW_MONTHS = 3
ALERT_ACTIVATION_DROP_PP = 2.0      # alert if activation rate falls by > 2 percentage points
ALERT_NEW_CUSTOMERS_DROP = 0.30     # alert if new customers fall by > 30% quarter over quarter
ALERT_NEW_CUSTOMERS_MIN_BASE = 100  # ...but only when the previous quarter had at least this many

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def _check_ident(value: str, what: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"Invalid {what}: {value!r} (only letters, digits and _ allowed)")
    return value


@dataclass(frozen=True)
class LayerNames:
    """`<catalog>.<env>_tpch_mkt_<layer>` -- portable across workspaces and environments."""

    catalog: str
    env: str

    def __post_init__(self) -> None:
        _check_ident(self.catalog, "catalog")
        _check_ident(self.env, "env")

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
        return f"{self.catalog}.{self.env}_{PROJECT}_{layer}"

    @property
    def bronze(self) -> str:
        return self.schema("bronze")

    @property
    def silver(self) -> str:
        return self.schema("silver")

    @property
    def gold(self) -> str:
        return self.schema("gold")

    @property
    def ops(self) -> str:
        return self.schema("ops")


def validate_source(source: str) -> str:
    parts = source.split(".")
    if len(parts) not in (2, 3):
        raise ValueError(f"Source must be <catalog>.<schema>, got {source!r}")
    for p in parts:
        _check_ident(p, "source")
    return source


def sql_in_list(values: tuple[str, ...]) -> str:
    """('A', 'B') -> "'A', 'B'" with quotes escaped."""
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)
