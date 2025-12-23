"""Tests for Census population ETL."""

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
        db_path = Path(tmpdir) / "test_census.db"
        engine = init_db(db_path)
        yield engine


class TestCensusPopulationETL:
    """Tests for Census population ETL loader."""

    def test_load_creates_national_stratum(self, temp_db):
        """Loading Census data should create a US population stratum."""
        from db.etl_census import load_census_targets

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).first()

            assert national is not None
            assert national.jurisdiction == Jurisdiction.US

    def test_load_creates_total_population(self, temp_db):
        """Loading Census should create total population target."""
        from db.etl_census import load_census_targets, CENSUS_DATA

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).first()

            pop_target = session.exec(
                select(Target)
                .where(Target.stratum_id == national.id)
                .where(Target.variable == "population")
                .where(Target.period == 2023)
            ).first()

            assert pop_target is not None
            assert pop_target.value == CENSUS_DATA[2023]["total_population"]
            assert pop_target.target_type == TargetType.COUNT
            assert pop_target.source == DataSource.CENSUS_ACS

    def test_load_creates_age_group_strata(self, temp_db):
        """Loading Census should create age group strata."""
        from db.etl_census import load_census_targets

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            age_strata = session.exec(
                select(Stratum).where(Stratum.stratum_group_id == "age_groups")
            ).all()

            # Should have 18 age groups (5-year brackets)
            assert len(age_strata) == 18

    def test_load_creates_state_strata(self, temp_db):
        """Loading Census should create state population strata."""
        from db.etl_census import load_census_targets, CENSUS_DATA

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            state_strata = session.exec(
                select(Stratum).where(Stratum.stratum_group_id == "state_population")
            ).all()

            # Should have strata for states in the data
            expected_states = len(CENSUS_DATA[2023].get("states", {}))
            assert len(state_strata) == expected_states

    def test_state_population_correct(self, temp_db):
        """State population targets should have correct values."""
        from db.etl_census import load_census_targets, CENSUS_DATA

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            ca_stratum = session.exec(
                select(Stratum).where(Stratum.name == "California Population")
            ).first()

            assert ca_stratum is not None

            ca_pop = session.exec(
                select(Target)
                .where(Target.stratum_id == ca_stratum.id)
                .where(Target.variable == "population")
            ).first()

            assert ca_pop.value == CENSUS_DATA[2023]["states"]["CA"]["population"]

    def test_load_creates_household_targets(self, temp_db):
        """Loading Census should create household count targets."""
        from db.etl_census import load_census_targets, CENSUS_DATA

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).first()

            hh_target = session.exec(
                select(Target)
                .where(Target.stratum_id == national.id)
                .where(Target.variable == "household_count")
            ).first()

            assert hh_target is not None
            assert hh_target.value == CENSUS_DATA[2023]["households"]

    def test_load_multiple_years(self, temp_db):
        """Loading multiple years should create targets for each."""
        from db.etl_census import load_census_targets

        with Session(temp_db) as session:
            load_census_targets(session, years=[2022, 2023])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).first()

            targets = session.exec(
                select(Target)
                .where(Target.stratum_id == national.id)
                .where(Target.variable == "population")
            ).all()

            years = {t.period for t in targets}
            assert years == {2022, 2023}

    def test_load_idempotent(self, temp_db):
        """Loading twice should not duplicate data."""
        from db.etl_census import load_census_targets

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])
            load_census_targets(session, years=[2023])

            national_strata = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).all()

            assert len(national_strata) == 1

    def test_age_group_population_sums_to_total(self, temp_db):
        """Age group populations should approximately sum to total."""
        from db.etl_census import load_census_targets

        with Session(temp_db) as session:
            load_census_targets(session, years=[2023])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US Population")
            ).first()

            total_pop = session.exec(
                select(Target)
                .where(Target.stratum_id == national.id)
                .where(Target.variable == "population")
                .where(Target.period == 2023)
            ).first()

            age_strata = session.exec(
                select(Stratum).where(Stratum.stratum_group_id == "age_groups")
            ).all()

            age_sum = 0
            for stratum in age_strata:
                target = session.exec(
                    select(Target)
                    .where(Target.stratum_id == stratum.id)
                    .where(Target.variable == "population")
                    .where(Target.period == 2023)
                ).first()
                if target:
                    age_sum += target.value

            # Should be within 1% (accounting for rounding)
            assert abs(age_sum - total_pop.value) / total_pop.value < 0.01
