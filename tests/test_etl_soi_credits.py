"""Tests for state-level SOI tax credits ETL."""

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
from db.etl_soi_credits import (
    load_soi_credits_targets,
    SOI_CREDITS_DATA,
    STATE_FIPS,
    SOURCE_URLS,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_soi_credits.db"
        engine = init_db(db_path)
        yield engine


class TestSoiCreditsETL:
    """Tests for state-level SOI credits ETL loader."""

    def test_load_soi_credits_creates_national_stratum(self, temp_db):
        """Loading credit data should create/reference a national stratum."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US All Filers")
            ).first()

            assert national is not None
            assert national.stratum_group_id == "national"

    def test_load_soi_credits_creates_state_strata(self, temp_db):
        """Loading credit data should create state-level strata."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            state_strata = session.exec(
                select(Stratum).where(Stratum.stratum_group_id == "soi_states")
            ).all()

            # Should have strata for all 50 states + DC
            expected_states = len(STATE_FIPS)
            assert len(state_strata) == expected_states
            assert expected_states == 51  # 50 states + DC

    def test_load_eitc_claims_targets(self, temp_db):
        """Loading credits should create EITC claims count targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            ca_stratum = session.exec(
                select(Stratum).where(Stratum.name == "CA All Filers")
            ).first()

            assert ca_stratum is not None

            eitc_claims_target = session.exec(
                select(Target)
                .where(Target.stratum_id == ca_stratum.id)
                .where(Target.variable == "eitc_claims")
                .where(Target.period == 2021)
            ).first()

            assert eitc_claims_target is not None
            expected = SOI_CREDITS_DATA[2021]["CA"]["eitc_claims"]
            assert eitc_claims_target.value == expected
            assert eitc_claims_target.target_type == TargetType.COUNT
            assert eitc_claims_target.source == DataSource.IRS_SOI

    def test_load_eitc_amount_targets(self, temp_db):
        """Loading credits should create EITC amount targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            tx_stratum = session.exec(
                select(Stratum).where(Stratum.name == "TX All Filers")
            ).first()

            eitc_amount_target = session.exec(
                select(Target)
                .where(Target.stratum_id == tx_stratum.id)
                .where(Target.variable == "eitc_amount")
                .where(Target.period == 2021)
            ).first()

            assert eitc_amount_target is not None
            expected = SOI_CREDITS_DATA[2021]["TX"]["eitc_amount"]
            assert eitc_amount_target.value == expected
            assert eitc_amount_target.target_type == TargetType.AMOUNT
            assert eitc_amount_target.source_url == SOURCE_URLS["eitc"]

    def test_load_ctc_claims_targets(self, temp_db):
        """Loading credits should create CTC claims count targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            fl_stratum = session.exec(
                select(Stratum).where(Stratum.name == "FL All Filers")
            ).first()

            ctc_claims_target = session.exec(
                select(Target)
                .where(Target.stratum_id == fl_stratum.id)
                .where(Target.variable == "ctc_claims")
                .where(Target.period == 2021)
            ).first()

            assert ctc_claims_target is not None
            expected = SOI_CREDITS_DATA[2021]["FL"]["ctc_claims"]
            assert ctc_claims_target.value == expected
            assert ctc_claims_target.target_type == TargetType.COUNT

    def test_load_ctc_amount_targets(self, temp_db):
        """Loading credits should create CTC amount targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            ny_stratum = session.exec(
                select(Stratum).where(Stratum.name == "NY All Filers")
            ).first()

            ctc_amount_target = session.exec(
                select(Target)
                .where(Target.stratum_id == ny_stratum.id)
                .where(Target.variable == "ctc_amount")
                .where(Target.period == 2021)
            ).first()

            assert ctc_amount_target is not None
            expected = SOI_CREDITS_DATA[2021]["NY"]["ctc_amount"]
            assert ctc_amount_target.value == expected
            assert ctc_amount_target.target_type == TargetType.AMOUNT

    def test_load_actc_claims_targets(self, temp_db):
        """Loading credits should create ACTC claims count targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            ga_stratum = session.exec(
                select(Stratum).where(Stratum.name == "GA All Filers")
            ).first()

            actc_claims_target = session.exec(
                select(Target)
                .where(Target.stratum_id == ga_stratum.id)
                .where(Target.variable == "actc_claims")
                .where(Target.period == 2021)
            ).first()

            assert actc_claims_target is not None
            expected = SOI_CREDITS_DATA[2021]["GA"]["actc_claims"]
            assert actc_claims_target.value == expected
            assert actc_claims_target.target_type == TargetType.COUNT

    def test_load_actc_amount_targets(self, temp_db):
        """Loading credits should create ACTC amount targets."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            pa_stratum = session.exec(
                select(Stratum).where(Stratum.name == "PA All Filers")
            ).first()

            actc_amount_target = session.exec(
                select(Target)
                .where(Target.stratum_id == pa_stratum.id)
                .where(Target.variable == "actc_amount")
                .where(Target.period == 2021)
            ).first()

            assert actc_amount_target is not None
            expected = SOI_CREDITS_DATA[2021]["PA"]["actc_amount"]
            assert actc_amount_target.value == expected
            assert actc_amount_target.target_type == TargetType.AMOUNT

    def test_load_soi_credits_stratum_has_state_constraint(self, temp_db):
        """State strata should have state_fips constraint."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            oh_stratum = session.exec(
                select(Stratum).where(Stratum.name == "OH All Filers")
            ).first()

            # Check constraints include state_fips
            state_constraint = None
            for constraint in oh_stratum.constraints:
                if constraint.variable == "state_fips":
                    state_constraint = constraint
                    break

            assert state_constraint is not None
            assert state_constraint.operator == "=="
            assert state_constraint.value == STATE_FIPS["OH"]

    def test_load_soi_credits_stratum_has_parent(self, temp_db):
        """State strata should have national stratum as parent."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            national = session.exec(
                select(Stratum).where(Stratum.name == "US All Filers")
            ).first()

            il_stratum = session.exec(
                select(Stratum).where(Stratum.name == "IL All Filers")
            ).first()

            assert il_stratum.parent_id == national.id

    def test_load_soi_credits_idempotent(self, temp_db):
        """Loading credits twice should not duplicate data."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])
            load_soi_credits_targets(session, years=[2021])

            ca_strata = session.exec(
                select(Stratum).where(Stratum.name == "CA All Filers")
            ).all()

            # Should only have one CA stratum
            assert len(ca_strata) == 1

    def test_all_states_loaded(self, temp_db):
        """All 50 states + DC should be loaded with 6 credit targets each."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            for state_abbrev in STATE_FIPS.keys():
                stratum = session.exec(
                    select(Stratum).where(Stratum.name == f"{state_abbrev} All Filers")
                ).first()

                assert stratum is not None, f"Missing stratum for {state_abbrev}"

                # Each state should have 6 targets (eitc_claims, eitc_amount,
                # ctc_claims, ctc_amount, actc_claims, actc_amount)
                targets = session.exec(
                    select(Target).where(Target.stratum_id == stratum.id)
                ).all()

                assert len(targets) == 6, f"Expected 6 targets for {state_abbrev}, got {len(targets)}"

    def test_target_source_metadata(self, temp_db):
        """Targets should have correct source metadata."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            ca_stratum = session.exec(
                select(Stratum).where(Stratum.name == "CA All Filers")
            ).first()

            eitc_target = session.exec(
                select(Target)
                .where(Target.stratum_id == ca_stratum.id)
                .where(Target.variable == "eitc_claims")
            ).first()

            assert eitc_target.source == DataSource.IRS_SOI
            assert eitc_target.source_table == "EITC Statistics"
            assert "earned-income-tax-credit" in eitc_target.source_url

            ctc_target = session.exec(
                select(Target)
                .where(Target.stratum_id == ca_stratum.id)
                .where(Target.variable == "ctc_claims")
            ).first()

            assert ctc_target.source == DataSource.IRS_SOI
            assert ctc_target.source_table == "State Data FY"
            assert "state-data" in ctc_target.source_url

    def test_national_totals_are_reasonable(self, temp_db):
        """National totals should match expected ranges."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            # Sum all EITC claims across states
            all_eitc_claims = sum(
                state_data["eitc_claims"]
                for state_data in SOI_CREDITS_DATA[2021].values()
            )
            # Should be around 25M
            assert 20_000_000 < all_eitc_claims < 30_000_000

            # Sum all EITC amounts across states
            all_eitc_amount = sum(
                state_data["eitc_amount"]
                for state_data in SOI_CREDITS_DATA[2021].values()
            )
            # Should be around $60B (actual was ~$57B in 2021, allowing margin)
            assert 50_000_000_000 < all_eitc_amount < 75_000_000_000

            # Sum all CTC claims across states
            all_ctc_claims = sum(
                state_data["ctc_claims"]
                for state_data in SOI_CREDITS_DATA[2021].values()
            )
            # Should be around 35M
            assert 30_000_000 < all_ctc_claims < 45_000_000

            # Sum all ACTC amounts across states
            all_actc_amount = sum(
                state_data["actc_amount"]
                for state_data in SOI_CREDITS_DATA[2021].values()
            )
            # Should be around $34B
            assert 25_000_000_000 < all_actc_amount < 40_000_000_000

    def test_large_states_have_higher_claims(self, temp_db):
        """Larger states should have more claims than smaller states."""
        with Session(temp_db) as session:
            load_soi_credits_targets(session, years=[2021])

            # California (large) vs Wyoming (small)
            ca_data = SOI_CREDITS_DATA[2021]["CA"]
            wy_data = SOI_CREDITS_DATA[2021]["WY"]

            assert ca_data["eitc_claims"] > wy_data["eitc_claims"]
            assert ca_data["ctc_claims"] > wy_data["ctc_claims"]
            assert ca_data["eitc_amount"] > wy_data["eitc_amount"]

            # Texas (large) vs Vermont (small)
            tx_data = SOI_CREDITS_DATA[2021]["TX"]
            vt_data = SOI_CREDITS_DATA[2021]["VT"]

            assert tx_data["eitc_claims"] > vt_data["eitc_claims"]
            assert tx_data["ctc_claims"] > vt_data["ctc_claims"]
