"""
Database layer for targets and calibration data.

Uses SQLModel (SQLAlchemy + Pydantic) with SQLite for local storage.
Design follows policyengine-us-data patterns.
"""

from .schema import Stratum, StratumConstraint, Target, get_engine, init_db

__all__ = ["Stratum", "StratumConstraint", "Target", "get_engine", "init_db"]
