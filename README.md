# cosilico-microdata-sources

Canonical documentation of microdata sources and their mappings to statute-defined concepts.

## Purpose

This repository documents how variables in surveys and administrative samples map to legal definitions in tax and benefit statutes. It bridges the gap between:

- **Data sources** (CPS, ACS, IRS PUF, UK FRS, etc.)
- **Legal concepts** (wages per IRC § 61(a)(1), SNAP benefits per 7 USC § 2017)

## Structure

```
cosilico-microdata-sources/
├── us/                          # United States sources
│   ├── census/                  # Census Bureau surveys
│   │   ├── cps-asec/           # Current Population Survey ASEC
│   │   └── acs/                # American Community Survey
│   └── irs/                    # IRS administrative data
│       └── puf/                # Public Use File
├── uk/                          # United Kingdom sources
│   ├── ons/                    # Office for National Statistics
│   │   └── frs/                # Family Resources Survey
│   └── hmrc/                   # HM Revenue & Customs
│       └── spi/                # Survey of Personal Incomes
└── cross-national/              # Multi-country sources
    └── lis/                    # Luxembourg Income Study
```

## Variable Schema

Each variable file (YAML) contains:

```yaml
variable: WSAL_VAL              # Source variable name
source: cps-asec                # Source identifier
entity: person                  # person, household, tax_unit
period: year                    # year, month
dtype: money                    # money, count, rate, boolean, category

# Documentation reference
documentation:
  url: "https://www2.census.gov/..."
  section: "Person Income Variables"
  question: "How much did you earn from wages and salaries?"

# Concept mapping
concept: wages_and_salaries
definition: "Gross wages, salaries, tips before deductions"

# Statute mappings (can span jurisdictions)
maps_to:
  - jurisdiction: us
    statute: "26 USC § 61(a)(1)"
    variable: wages  # cosilico-us variable
    coverage: full
    notes: "Direct mapping to gross wages"

  - jurisdiction: us-ca
    statute: "CA Rev & Tax Code § 17071"
    variable: wages
    coverage: full

# Known limitations
gaps:
  - component: tips_underreporting
    impact: medium
    notes: "Cash tips systematically underreported in surveys"
```

## Usage

These mappings are consumed by:
- `cosilico-microdata` - to build calibrated datasets
- `cosilico-us`, `cosilico-uk` - for variable documentation
- Validation tools - to verify coverage

## Contributing

1. Add variable YAML file in appropriate source directory
2. Include documentation URL from official source
3. Map to all relevant jurisdiction statutes
4. Document known gaps and coverage issues
