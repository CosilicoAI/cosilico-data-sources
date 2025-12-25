# Weight Methodology

## Overview

Cosilico microdata uses a two-stage weighting approach:
1. **Original survey weights** (CPS ASEC) - calibrated to demographic targets
2. **Entropy calibration** - adjusts to tax-specific targets (IRS SOI)

This preserves demographic representativeness while improving tax policy accuracy.

## Stage 1: CPS ASEC Original Weights

Source: [Census Bureau CPS Weighting Methodology](https://www.census.gov/programs-surveys/cps/technical-documentation/methodology/weighting.html)

### Construction

ASEC weights are the product of:
1. **Base weight**: Inverse probability of selection
2. **Special weighting adjustments**: Large sample unit corrections
3. **Non-interview adjustment**: Non-response corrections
4. **First-stage ratio adjustment**: PSU-level calibration
5. **Second-stage ratio adjustment**: Population control calibration

### Population Controls (What ASEC Targets)

The CPS calibrates to **three sets of independent population controls**:

| Control Set | Categories | Dimension |
|-------------|------------|-----------|
| State totals | 51 | Civilian noninstitutional pop 16+ by state |
| Hispanic age-sex | 14 + 5 | Hispanic (14 cells) + non-Hispanic (5 cells) |
| Race age-sex | 66 + 42 + 10 | White (66) + Black (42) + Other (10) age-sex cells |

**Total: ~188 demographic constraints**

### Variables Preserved by ASEC Weights

These dimensions are well-represented because ASEC explicitly calibrates to them:

| Variable | Cosilico Field | Notes |
|----------|----------------|-------|
| Age | `age` | 66+ age-sex cells for white alone |
| Sex | `is_female` | Crossed with age and race |
| Race | `race` | White, Black, Other categories |
| Hispanic origin | `is_hispanic` | 14 Hispanic age-sex cells |
| State | `state_fips` | 51 state-level controls |

### Variables NOT Directly Targeted by ASEC

These are represented via correlation with targeted variables, not direct calibration:

- Income levels (wages, self-employment, capital gains)
- Tax filing status
- Program participation (SNAP, Medicaid, SSI)
- Education level
- Occupation/industry
- Health insurance coverage
- Disability status

## Stage 2: Entropy Calibration to IRS SOI

We apply entropy calibration to match IRS Statistics of Income targets while preserving the demographic structure from ASEC weights.

### Why Two Stages?

| Aspect | ASEC Weights | Entropy Calibration |
|--------|--------------|---------------------|
| **Source** | Census population estimates | IRS administrative data |
| **Targets** | Demographics (age, sex, race, state) | Tax filing patterns (AGI brackets) |
| **Strength** | Population representativeness | Tax accuracy |
| **Weakness** | Income underreporting, non-filer inclusion | Loses some demographic precision |

By starting from ASEC weights and minimizing divergence, we get both.

### IRS SOI Targets (What We Calibrate To)

Source: [IRS SOI Tax Stats](https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-returns-complete-report-publication-1304)

| Variable | Cosilico Field | Target Type |
|----------|----------------|-------------|
| Returns by AGI bracket | `adjusted_gross_income` | COUNT per bracket |
| Total AGI by bracket | `adjusted_gross_income` | SUM per bracket |

**AGI Brackets:**

| Bracket | AGI Range | 2021 Returns (M) |
|---------|-----------|------------------|
| no_agi | $0 exactly | 14.0 |
| under_1 | < $1 (negative) | 1.7 |
| 1_to_5k | $1 - $5,000 | 5.2 |
| 5k_to_10k | $5,000 - $10,000 | 7.9 |
| 10k_to_15k | $10,000 - $15,000 | 9.9 |
| 15k_to_20k | $15,000 - $20,000 | 9.1 |
| 20k_to_25k | $20,000 - $25,000 | 8.2 |
| 25k_to_30k | $25,000 - $30,000 | 7.4 |
| 30k_to_40k | $30,000 - $40,000 | 13.2 |
| 40k_to_50k | $40,000 - $50,000 | 10.9 |
| 50k_to_75k | $50,000 - $75,000 | 19.5 |
| 75k_to_100k | $75,000 - $100,000 | 15.1 |
| 100k_to_200k | $100,000 - $200,000 | 22.8 |
| 200k_to_500k | $200,000 - $500,000 | 7.2 |
| 500k_to_1m | $500,000 - $1,000,000 | 1.1 |
| 1m_plus | $1,000,000+ | 0.7 |
| **Total** | | **153.9** |

### Entropy Calibration Method

We minimize Kullback-Leibler divergence from original weights:

```
minimize: Σᵢ wᵢ * log(wᵢ / w₀ᵢ)
subject to: Σᵢ wᵢ * 1[AGI in bracket j] = target_j  ∀j
```

This finds the smallest information change needed to satisfy tax constraints.

**Implementation**: Dual formulation with L-BFGS-B optimizer (see `micro/us/calibrate.py`)

### Calibration Results

| Metric | Value |
|--------|-------|
| Tax units | 65,347 |
| Original weighted total | 162.1M |
| Calibrated weighted total | 138.3M |
| IRS SOI target | 153.9M |
| **Coverage** | **89.8%** |
| Max bracket error | 0.0% |
| Weight adjustment range | 0.51x - 2.98x |

**Coverage gap**: CPS underrepresents low/no-income filers (~14M returns with no AGI in IRS data, 0 in CPS).

## Variable Mapping Reference

### Demographic Variables (from ASEC)

| Cosilico Variable | CPS Variable | Used In |
|-------------------|--------------|---------|
| `age` | `A_AGE` | ASEC pop controls |
| `is_female` | `A_SEX` | ASEC pop controls |
| `race` | `PRDTRACE` | ASEC pop controls |
| `is_hispanic` | `PEHSPNON` | ASEC pop controls |
| `state_fips` | `GESTFIPS` | ASEC pop controls |

### Tax Variables (for calibration)

| Cosilico Variable | Derived From | Used In |
|-------------------|--------------|---------|
| `adjusted_gross_income` | Sum of income components | IRS SOI calibration |
| `wage_income` | `WSAL_VAL` | AGI component |
| `self_employment_income` | `SEMP_VAL` | AGI component |
| `interest_income` | `INT_VAL` | AGI component |
| `dividend_income` | `DIV_VAL` | AGI component |
| `social_security_income` | `SS_VAL` | AGI component |

### Weight Variables

| Variable | Description |
|----------|-------------|
| `weight` | Final calibrated weight (use for all analysis) |
| `original_weight` | ASEC weight before calibration |
| `weight_adjustment` | Ratio: `weight / original_weight` |

## Future Improvements

1. **AGI totals by bracket**: Currently only calibrate counts, not totals
2. **Credit recipients**: EITC, CTC recipient counts from IRS SOI
3. **State-level tax targets**: IRS SOI state data
4. **Synthetic high-income tail**: Pareto extrapolation for $1M+ (see `docs/PUF_INTEGRATION.md`)
5. **Multi-year calibration**: Time-series consistency
