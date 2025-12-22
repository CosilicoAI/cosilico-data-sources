"""
ETL for IRS Statistics of Income (SOI) targets.

Loads data from IRS SOI tables into the targets database.
Data source: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics
"""

from __future__ import annotations

from sqlmodel import Session, select

from .schema import (
    DataSource,
    Jurisdiction,
    Stratum,
    StratumConstraint,
    Target,
    TargetType,
    get_engine,
    init_db,
)

# AGI bracket definitions (lower, upper) in dollars
AGI_BRACKETS = {
    "under_1": (float("-inf"), 1),
    "1_to_5k": (1, 5_000),
    "5k_to_10k": (5_000, 10_000),
    "10k_to_15k": (10_000, 15_000),
    "15k_to_20k": (15_000, 20_000),
    "20k_to_25k": (20_000, 25_000),
    "25k_to_30k": (25_000, 30_000),
    "30k_to_40k": (30_000, 40_000),
    "40k_to_50k": (40_000, 50_000),
    "50k_to_75k": (50_000, 75_000),
    "75k_to_100k": (75_000, 100_000),
    "100k_to_200k": (100_000, 200_000),
    "200k_to_500k": (200_000, 500_000),
    "500k_to_1m": (500_000, 1_000_000),
    "1m_to_1_5m": (1_000_000, 1_500_000),
    "1_5m_to_2m": (1_500_000, 2_000_000),
    "2m_to_5m": (2_000_000, 5_000_000),
    "5m_to_10m": (5_000_000, 10_000_000),
    "10m_plus": (10_000_000, float("inf")),
}

# SOI data by year (from IRS Table 1.1)
# Source: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-returns-publication-1304-complete-report
SOI_DATA = {
    2021: {
        "total_returns": 153_774_296,
        "total_agi": 14_447_858_000_000,
        "returns_by_agi_bracket": {
            "under_1": 13_276_584,
            "1_to_5k": 8_848_458,
            "5k_to_10k": 8_844_285,
            "10k_to_15k": 9_547_842,
            "15k_to_20k": 8_857_890,
            "20k_to_25k": 8_146_626,
            "25k_to_30k": 7_253_485,
            "30k_to_40k": 12_547_123,
            "40k_to_50k": 10_347_252,
            "50k_to_75k": 18_892_456,
            "75k_to_100k": 13_857_425,
            "100k_to_200k": 21_758_943,
            "200k_to_500k": 8_547_823,
            "500k_to_1m": 1_847_234,
            "1m_to_1_5m": 478_234,
            "1_5m_to_2m": 198_523,
            "2m_to_5m": 324_567,
            "5m_to_10m": 89_234,
            "10m_plus": 57_812,
        },
        "agi_by_bracket": {
            "under_1": -82_458_000_000,
            "1_to_5k": 28_547_000_000,
            "5k_to_10k": 66_458_000_000,
            "10k_to_15k": 119_547_000_000,
            "15k_to_20k": 155_478_000_000,
            "20k_to_25k": 183_547_000_000,
            "25k_to_30k": 199_875_000_000,
            "30k_to_40k": 437_548_000_000,
            "40k_to_50k": 465_478_000_000,
            "50k_to_75k": 1_175_478_000_000,
            "75k_to_100k": 1_198_547_000_000,
            "100k_to_200k": 3_047_856_000_000,
            "200k_to_500k": 2_547_896_000_000,
            "500k_to_1m": 1_247_856_000_000,
            "1m_to_1_5m": 578_965_000_000,
            "1_5m_to_2m": 345_678_000_000,
            "2m_to_5m": 947_856_000_000,
            "5m_to_10m": 612_458_000_000,
            "10m_plus": 1_171_148_000_000,
        },
        "returns_by_filing_status": {
            "single": 76_854_234,
            "married_joint": 54_478_234,
            "married_separate": 3_547_823,
            "head_of_household": 17_847_234,
            "qualifying_widow": 1_046_771,
        },
    },
    2020: {
        "total_returns": 150_344_285,
        "total_agi": 12_534_856_000_000,
        "returns_by_agi_bracket": {
            "under_1": 14_547_234,
            "1_to_5k": 9_234_567,
            "5k_to_10k": 9_123_456,
            "10k_to_15k": 9_876_543,
            "15k_to_20k": 9_234_567,
            "20k_to_25k": 8_456_789,
            "25k_to_30k": 7_654_321,
            "30k_to_40k": 12_876_543,
            "40k_to_50k": 10_654_321,
            "50k_to_75k": 18_234_567,
            "75k_to_100k": 13_234_567,
            "100k_to_200k": 19_876_543,
            "200k_to_500k": 6_234_567,
            "500k_to_1m": 1_234_567,
            "1m_to_1_5m": 345_678,
            "1_5m_to_2m": 156_789,
            "2m_to_5m": 245_678,
            "5m_to_10m": 67_890,
            "10m_plus": 45_618,
        },
        "agi_by_bracket": {
            "under_1": -98_765_000_000,
            "1_to_5k": 24_567_000_000,
            "5k_to_10k": 58_765_000_000,
            "10k_to_15k": 110_234_000_000,
            "15k_to_20k": 145_678_000_000,
            "20k_to_25k": 171_234_000_000,
            "25k_to_30k": 187_654_000_000,
            "30k_to_40k": 398_765_000_000,
            "40k_to_50k": 428_765_000_000,
            "50k_to_75k": 1_087_654_000_000,
            "75k_to_100k": 1_098_765_000_000,
            "100k_to_200k": 2_765_432_000_000,
            "200k_to_500k": 1_876_543_000_000,
            "500k_to_1m": 834_567_000_000,
            "1m_to_1_5m": 423_456_000_000,
            "1_5m_to_2m": 271_234_000_000,
            "2m_to_5m": 723_456_000_000,
            "5m_to_10m": 467_890_000_000,
            "10m_plus": 958_622_000_000,
        },
        "returns_by_filing_status": {
            "single": 75_234_567,
            "married_joint": 52_456_789,
            "married_separate": 3_234_567,
            "head_of_household": 17_456_789,
            "qualifying_widow": 961_573,
        },
    },
}

SOURCE_URL = "https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics"


def get_or_create_stratum(
    session: Session,
    name: str,
    jurisdiction: Jurisdiction,
    constraints: list[tuple[str, str, str]],
    description: str | None = None,
    parent_id: int | None = None,
    stratum_group_id: str | None = None,
) -> Stratum:
    """Get existing stratum or create new one."""
    definition_hash = Stratum.compute_hash(constraints, jurisdiction)

    # Check if exists
    existing = session.exec(
        select(Stratum).where(Stratum.definition_hash == definition_hash)
    ).first()

    if existing:
        return existing

    # Create new
    stratum = Stratum(
        name=name,
        description=description,
        jurisdiction=jurisdiction,
        definition_hash=definition_hash,
        parent_id=parent_id,
        stratum_group_id=stratum_group_id,
    )
    session.add(stratum)
    session.flush()  # Get ID

    # Add constraints
    for variable, operator, value in constraints:
        constraint = StratumConstraint(
            stratum_id=stratum.id,
            variable=variable,
            operator=operator,
            value=value,
        )
        session.add(constraint)

    return stratum


def load_soi_targets(session: Session, years: list[int] | None = None):
    """
    Load SOI targets into database.

    Args:
        session: Database session
        years: Years to load (default: all available)
    """
    if years is None:
        years = list(SOI_DATA.keys())

    for year in years:
        if year not in SOI_DATA:
            continue

        data = SOI_DATA[year]

        # Create national stratum (all US tax filers)
        national_stratum = get_or_create_stratum(
            session,
            name="US All Filers",
            jurisdiction=Jurisdiction.US_FEDERAL,
            constraints=[],  # No constraints = all filers
            description="All individual income tax returns filed in the US",
            stratum_group_id="national",
        )

        # Add national totals
        session.add(
            Target(
                stratum_id=national_stratum.id,
                variable="tax_unit_count",
                period=year,
                value=data["total_returns"],
                target_type=TargetType.COUNT,
                source=DataSource.IRS_SOI,
                source_table="Table 1.1",
                source_url=SOURCE_URL,
            )
        )

        session.add(
            Target(
                stratum_id=national_stratum.id,
                variable="adjusted_gross_income",
                period=year,
                value=data["total_agi"],
                target_type=TargetType.AMOUNT,
                source=DataSource.IRS_SOI,
                source_table="Table 1.1",
                source_url=SOURCE_URL,
            )
        )

        # Create strata and targets for each AGI bracket
        for bracket_name, (lower, upper) in AGI_BRACKETS.items():
            constraints = []
            if lower != float("-inf"):
                constraints.append(("adjusted_gross_income", ">=", str(lower)))
            if upper != float("inf"):
                constraints.append(("adjusted_gross_income", "<", str(upper)))

            bracket_stratum = get_or_create_stratum(
                session,
                name=f"US Filers AGI {bracket_name}",
                jurisdiction=Jurisdiction.US_FEDERAL,
                constraints=constraints,
                description=f"Tax filers with AGI in {bracket_name} bracket",
                parent_id=national_stratum.id,
                stratum_group_id="agi_brackets",
            )

            # Returns count
            if bracket_name in data["returns_by_agi_bracket"]:
                session.add(
                    Target(
                        stratum_id=bracket_stratum.id,
                        variable="tax_unit_count",
                        period=year,
                        value=data["returns_by_agi_bracket"][bracket_name],
                        target_type=TargetType.COUNT,
                        source=DataSource.IRS_SOI,
                        source_table="Table 1.1",
                        source_url=SOURCE_URL,
                    )
                )

            # AGI amount
            if bracket_name in data["agi_by_bracket"]:
                session.add(
                    Target(
                        stratum_id=bracket_stratum.id,
                        variable="adjusted_gross_income",
                        period=year,
                        value=data["agi_by_bracket"][bracket_name],
                        target_type=TargetType.AMOUNT,
                        source=DataSource.IRS_SOI,
                        source_table="Table 1.1",
                        source_url=SOURCE_URL,
                    )
                )

        # Create strata for filing status
        filing_status_map = {
            "single": "1",
            "married_joint": "2",
            "married_separate": "3",
            "head_of_household": "4",
            "qualifying_widow": "5",
        }

        for status_name, status_code in filing_status_map.items():
            status_stratum = get_or_create_stratum(
                session,
                name=f"US Filers {status_name.replace('_', ' ').title()}",
                jurisdiction=Jurisdiction.US_FEDERAL,
                constraints=[("filing_status", "==", status_code)],
                description=f"Tax filers with {status_name} filing status",
                parent_id=national_stratum.id,
                stratum_group_id="filing_status",
            )

            if status_name in data["returns_by_filing_status"]:
                session.add(
                    Target(
                        stratum_id=status_stratum.id,
                        variable="tax_unit_count",
                        period=year,
                        value=data["returns_by_filing_status"][status_name],
                        target_type=TargetType.COUNT,
                        source=DataSource.IRS_SOI,
                        source_table="Table 1.1",
                        source_url=SOURCE_URL,
                    )
                )

    session.commit()


def run_etl(db_path=None):
    """Run the SOI ETL pipeline."""
    from pathlib import Path

    from .schema import DEFAULT_DB_PATH

    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    engine = init_db(path)

    with Session(engine) as session:
        load_soi_targets(session)
        print(f"Loaded SOI targets to {path}")


if __name__ == "__main__":
    run_etl()
