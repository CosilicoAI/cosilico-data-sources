"""Tests for SSA (Social Security) ETL."""

import tempfile
from pathlib import Path

import pytest
from sqlmodel import Session, select

from db.schema import (
    DataSource,
    Stratum,
    Target,
    TargetType,
    init_db,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ssa.db"
        engine = init_db(db_path)
        yield engine


class TestSsaETL:
    """Tests for SSA ETL loader."""

    def test_load_creates_oasdi_stratum(self, temp_db):
        """Loading SSA data should create OASDI beneficiary stratum."""
        from db.etl_ssa import load_ssa_targets

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            oasdi = session.exec(
                select(Stratum).where(Stratum.name == "US OASDI Beneficiaries")
            ).first()

            assert oasdi is not None

    def test_load_creates_beneficiary_count(self, temp_db):
        """Loading SSA should create total beneficiary count target."""
        from db.etl_ssa import load_ssa_targets, SSA_DATA

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            oasdi = session.exec(
                select(Stratum).where(Stratum.name == "US OASDI Beneficiaries")
            ).first()

            count = session.exec(
                select(Target)
                .where(Target.stratum_id == oasdi.id)
                .where(Target.variable == "oasdi_beneficiaries")
                .where(Target.period == 2023)
            ).first()

            assert count is not None
            assert count.value == SSA_DATA[2023]["total_beneficiaries"]
            assert count.target_type == TargetType.COUNT
            assert count.source == DataSource.SSA

    def test_load_creates_benefit_payments(self, temp_db):
        """Loading SSA should create total benefit payment target."""
        from db.etl_ssa import load_ssa_targets, SSA_DATA

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            oasdi = session.exec(
                select(Stratum).where(Stratum.name == "US OASDI Beneficiaries")
            ).first()

            payments = session.exec(
                select(Target)
                .where(Target.stratum_id == oasdi.id)
                .where(Target.variable == "oasdi_benefits")
            ).first()

            assert payments is not None
            assert payments.value == SSA_DATA[2023]["total_benefits"]
            assert payments.target_type == TargetType.AMOUNT

    def test_load_creates_ssi_stratum(self, temp_db):
        """Loading SSA should create SSI recipient stratum."""
        from db.etl_ssa import load_ssa_targets

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            ssi = session.exec(
                select(Stratum).where(Stratum.name == "US SSI Recipients")
            ).first()

            assert ssi is not None

    def test_load_creates_ssi_targets(self, temp_db):
        """Loading SSA should create SSI recipient and payment targets."""
        from db.etl_ssa import load_ssa_targets, SSA_DATA

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            ssi = session.exec(
                select(Stratum).where(Stratum.name == "US SSI Recipients")
            ).first()

            count = session.exec(
                select(Target)
                .where(Target.stratum_id == ssi.id)
                .where(Target.variable == "ssi_recipients")
            ).first()

            assert count is not None
            assert count.value == SSA_DATA[2023]["ssi"]["recipients"]

    def test_load_creates_retired_worker_stratum(self, temp_db):
        """Loading SSA should create retired worker stratum."""
        from db.etl_ssa import load_ssa_targets

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            retired = session.exec(
                select(Stratum).where(Stratum.name == "US Retired Workers")
            ).first()

            assert retired is not None

    def test_retired_worker_avg_benefit(self, temp_db):
        """Retired worker average benefit should be correct."""
        from db.etl_ssa import load_ssa_targets, SSA_DATA

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])

            retired = session.exec(
                select(Stratum).where(Stratum.name == "US Retired Workers")
            ).first()

            avg_benefit = session.exec(
                select(Target)
                .where(Target.stratum_id == retired.id)
                .where(Target.variable == "oasdi_avg_monthly_benefit")
            ).first()

            assert avg_benefit is not None
            expected = SSA_DATA[2023]["retired_workers"]["avg_monthly_benefit"]
            assert avg_benefit.value == expected

    def test_load_multiple_years(self, temp_db):
        """Loading multiple years should create targets for each."""
        from db.etl_ssa import load_ssa_targets

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2022, 2023])

            oasdi = session.exec(
                select(Stratum).where(Stratum.name == "US OASDI Beneficiaries")
            ).first()

            targets = session.exec(
                select(Target)
                .where(Target.stratum_id == oasdi.id)
                .where(Target.variable == "oasdi_beneficiaries")
            ).all()

            years = {t.period for t in targets}
            assert years == {2022, 2023}

    def test_load_idempotent(self, temp_db):
        """Loading twice should not duplicate data."""
        from db.etl_ssa import load_ssa_targets

        with Session(temp_db) as session:
            load_ssa_targets(session, years=[2023])
            load_ssa_targets(session, years=[2023])

            oasdi_strata = session.exec(
                select(Stratum).where(Stratum.name == "US OASDI Beneficiaries")
            ).all()

            assert len(oasdi_strata) == 1
