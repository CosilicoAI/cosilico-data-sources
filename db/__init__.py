"""
Database layer for targets and calibration data.

Uses SQLModel (SQLAlchemy + Pydantic) with SQLite for local storage.
Design follows policyengine-us-data patterns.
"""

from .schema import (
    DataSource,
    Jurisdiction,
    Stratum,
    StratumConstraint,
    Target,
    TargetType,
    get_engine,
    get_session,
    init_db,
)
from .etl_soi import load_soi_targets
from .etl_snap import load_snap_targets
from .etl_hmrc import load_hmrc_targets

__all__ = [
    # Schema
    "DataSource",
    "Jurisdiction",
    "Stratum",
    "StratumConstraint",
    "Target",
    "TargetType",
    "get_engine",
    "get_session",
    "init_db",
    # ETL
    "load_soi_targets",
    "load_snap_targets",
    "load_hmrc_targets",
]
