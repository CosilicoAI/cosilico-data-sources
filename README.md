# microsources

Canonical documentation of microdata sources and their mappings to statute-defined concepts.

## Purpose

This repository documents how variables in surveys and administrative samples map to legal definitions in tax and benefit statutes. It bridges the gap between:

- **Data sources** (CPS ASEC, IRS PUF, UK FRS, etc.)
- **Legal concepts** (wages per 26 USC § 61(a)(1), SNAP per 7 USC § 2017, UC per WRA 2012)

## Current Coverage

| Source | Variables | Description |
|--------|-----------|-------------|
| US CPS ASEC | 56 | Census household survey (income, benefits, demographics) |
| US IRS PUF | 33 | Tax return sample (income, deductions, credits) |
| UK FRS | 29 | DWP household survey (income, benefits, housing) |
| **Total** | **118** | |

## Structure

```
microsources/
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
entity: person                  # person, household, tax_unit, spm_unit
period: year                    # year, month, week, point_in_time
dtype: money                    # money, count, rate, boolean, category

documentation:
  url: "https://www2.census.gov/..."
  section: "Person Income Variables"

concept: wages_and_salaries
definition: "Gross wages, salaries, tips before deductions"

# Statute mappings across jurisdictions
maps_to:
  - jurisdiction: us
    statute: "26 USC § 61(a)(1)"
    variable: wages
    coverage: full

  - jurisdiction: us-ca
    statute: "CA Rev & Tax Code § 17071"
    variable: wages
    coverage: full

# Known limitations
gaps:
  - component: tips_underreporting
    impact: medium  # critical, high, medium, low
    notes: "Cash tips systematically underreported"
```

## Related Repositories

- **cosilico-microdata** - Builds calibrated datasets using these mappings
- **cosilico-us** / **cosilico-uk** - Statute encodings that reference these concepts
- **targets** (planned) - Administrative totals for calibration (SOI, HMRC stats)

## Contributing

1. Add variable YAML file in appropriate source directory
2. Include documentation URL from official source
3. Map to all relevant jurisdiction statutes
4. Document known gaps with impact assessment
