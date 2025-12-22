"""Tests for BLS (Bureau of Labor Statistics) employment ETL."""

import tempfile
from pathlib import Path

import pytest
from sqlmodel import Session, select

from db.schema import (
    DataSource,
    Jurisdiction,
    Stratum,
    Target,
    TargetType,
    init_db,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_bls.db"
        engine = init_db(db_path)
        yield engine


class TestBlsETL:
    """Tests for BLS employment ETL loader."""

    def test_load_creates_labor_force_stratum(self, temp_db):
        """Loading BLS data should create labor force stratum."""
        from db.etl_bls import load_bls_targets

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            assert labor_force is not None
            assert labor_force.jurisdiction == Jurisdiction.US

    def test_load_creates_employment_count(self, temp_db):
        """Loading BLS should create total employment target."""
        from db.etl_bls import load_bls_targets, BLS_DATA

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            employed = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "employed")
                .where(Target.period == 2023)
            ).first()

            assert employed is not None
            assert employed.value == BLS_DATA[2023]["employed"]
            assert employed.target_type == TargetType.COUNT
            assert employed.source == DataSource.BLS

    def test_load_creates_unemployment_count(self, temp_db):
        """Loading BLS should create unemployment count target."""
        from db.etl_bls import load_bls_targets, BLS_DATA

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            unemployed = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "unemployed")
            ).first()

            assert unemployed is not None
            assert unemployed.value == BLS_DATA[2023]["unemployed"]

    def test_load_creates_unemployment_rate(self, temp_db):
        """Loading BLS should create unemployment rate target."""
        from db.etl_bls import load_bls_targets, BLS_DATA

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            rate = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "unemployment_rate")
            ).first()

            assert rate is not None
            assert rate.value == BLS_DATA[2023]["unemployment_rate"]
            assert rate.target_type == TargetType.RATE

    def test_load_creates_labor_force_participation(self, temp_db):
        """Loading BLS should create LFPR target."""
        from db.etl_bls import load_bls_targets, BLS_DATA

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            lfpr = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "labor_force_participation_rate")
            ).first()

            assert lfpr is not None
            assert lfpr.value == BLS_DATA[2023]["labor_force_participation_rate"]
            assert lfpr.target_type == TargetType.RATE

    def test_load_creates_median_wage(self, temp_db):
        """Loading BLS should create median wage target."""
        from db.etl_bls import load_bls_targets, BLS_DATA

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            wage = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "median_weekly_earnings")
            ).first()

            assert wage is not None
            assert wage.value == BLS_DATA[2023]["median_weekly_earnings"]
            assert wage.target_type == TargetType.AMOUNT

    def test_load_multiple_years(self, temp_db):
        """Loading multiple years should create targets for each."""
        from db.etl_bls import load_bls_targets

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2022, 2023])

            labor_force = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).first()

            targets = session.exec(
                select(Target)
                .where(Target.stratum_id == labor_force.id)
                .where(Target.variable == "employed")
            ).all()

            years = {t.period for t in targets}
            assert years == {2022, 2023}

    def test_load_idempotent(self, temp_db):
        """Loading twice should not duplicate strata."""
        from db.etl_bls import load_bls_targets

        with Session(temp_db) as session:
            load_bls_targets(session, years=[2023])
            load_bls_targets(session, years=[2023])

            labor_force_strata = session.exec(
                select(Stratum).where(Stratum.name == "US Labor Force")
            ).all()

            assert len(labor_force_strata) == 1
