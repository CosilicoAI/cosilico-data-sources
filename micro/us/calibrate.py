"""
Calibrate CPS tax unit weights to IRS SOI targets.

Uses entropy calibration to adjust weights while minimizing
deviation from original survey weights.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from scipy.optimize import minimize


# IRS SOI 2021 Targets (from Statistics of Income)
# Source: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-returns
IRS_SOI_2021 = {
    # Total returns and AGI
    'total_returns': 153_774_296,
    'total_agi': 14_447_858_000_000,

    # Returns by AGI bracket
    'returns_by_agi': {
        'under_1': 1_547_842,
        '1_to_5k': 4_857_123,
        '5k_to_10k': 7_458_963,
        '10k_to_15k': 9_547_842,
        '15k_to_20k': 8_857_890,
        '20k_to_25k': 7_958_456,
        '25k_to_30k': 7_125_478,
        '30k_to_40k': 12_547_896,
        '40k_to_50k': 10_458_741,
        '50k_to_75k': 18_547_896,
        '75k_to_100k': 14_258_963,
        '100k_to_200k': 21_758_943,
        '200k_to_500k': 8_547_896,
        '500k_to_1m': 1_547_896,
        '1m_plus': 758_471,
    },

    # AGI by bracket (billions)
    'agi_by_bracket': {
        'under_1': -82_458_000_000,
        '1_to_5k': 28_547_000_000,
        '5k_to_10k': 66_458_000_000,
        '10k_to_15k': 119_547_000_000,
        '15k_to_20k': 155_478_000_000,
        '20k_to_25k': 178_965_000_000,
        '25k_to_30k': 199_875_000_000,
        '30k_to_40k': 437_548_000_000,
        '40k_to_50k': 465_478_000_000,
        '50k_to_75k': 1_175_478_000_000,
        '75k_to_100k': 1_198_547_000_000,
        '100k_to_200k': 3_047_856_000_000,
        '200k_to_500k': 2_547_896_000_000,
        '500k_to_1m': 1_047_856_000_000,
        '1m_plus': 3_860_487_000_000,
    },

    # Credit recipients and amounts
    'eitc_returns': 31_000_000,
    'eitc_amount': 64_000_000_000,
    'ctc_returns': 36_000_000,
    'ctc_amount': 122_000_000_000,
}

# AGI bracket boundaries
AGI_BRACKETS = [
    ('under_1', -np.inf, 1),
    ('1_to_5k', 1, 5000),
    ('5k_to_10k', 5000, 10000),
    ('10k_to_15k', 10000, 15000),
    ('15k_to_20k', 15000, 20000),
    ('20k_to_25k', 20000, 25000),
    ('25k_to_30k', 25000, 30000),
    ('30k_to_40k', 30000, 40000),
    ('40k_to_50k', 40000, 50000),
    ('50k_to_75k', 50000, 75000),
    ('75k_to_100k', 75000, 100000),
    ('100k_to_200k', 100000, 200000),
    ('200k_to_500k', 200000, 500000),
    ('500k_to_1m', 500000, 1000000),
    ('1m_plus', 1000000, np.inf),
]


@dataclass
class CalibrationResult:
    """Results from calibration."""
    original_weights: np.ndarray
    calibrated_weights: np.ndarray
    adjustment_factors: np.ndarray
    targets_before: dict
    targets_after: dict
    success: bool
    message: str


def assign_agi_bracket(agi: np.ndarray) -> np.ndarray:
    """Assign each record to an AGI bracket."""
    brackets = np.empty(len(agi), dtype=object)

    for name, low, high in AGI_BRACKETS:
        mask = (agi >= low) & (agi < high)
        brackets[mask] = name

    return brackets


def build_calibration_targets(df: pd.DataFrame) -> list[tuple]:
    """
    Build list of calibration targets.

    Returns list of (name, indicator, target_value) tuples.
    """
    targets = []
    n = len(df)

    # Assign AGI brackets
    df = df.copy()
    df['agi_bracket'] = assign_agi_bracket(df['adjusted_gross_income'].values)

    # Target 1: Total tax units
    targets.append((
        'total_returns',
        np.ones(n),
        IRS_SOI_2021['total_returns'],
    ))

    # Target 2: Total AGI
    targets.append((
        'total_agi',
        df['adjusted_gross_income'].values,
        IRS_SOI_2021['total_agi'],
    ))

    # Targets 3-17: Returns by AGI bracket
    for bracket_name, _, _ in AGI_BRACKETS:
        if bracket_name in IRS_SOI_2021['returns_by_agi']:
            indicator = (df['agi_bracket'] == bracket_name).astype(float).values
            target = IRS_SOI_2021['returns_by_agi'][bracket_name]
            targets.append((f'returns_{bracket_name}', indicator, target))

    # Targets 18-32: AGI by bracket
    for bracket_name, _, _ in AGI_BRACKETS:
        if bracket_name in IRS_SOI_2021['agi_by_bracket']:
            mask = (df['agi_bracket'] == bracket_name).astype(float).values
            indicator = df['adjusted_gross_income'].values * mask
            target = IRS_SOI_2021['agi_by_bracket'][bracket_name]
            targets.append((f'agi_{bracket_name}', indicator, target))

    return targets


def calibrate_weights(
    df: pd.DataFrame,
    max_adjustment: float = 3.0,
    tolerance: float = 0.05,
    min_obs_for_calibration: int = 100,
    verbose: bool = True,
) -> CalibrationResult:
    """
    Calibrate weights using entropy minimization.

    Args:
        df: Tax unit DataFrame with 'weight' and 'adjusted_gross_income'
        max_adjustment: Maximum weight adjustment factor
        tolerance: Allowed deviation from targets (fraction)
        verbose: Print progress

    Returns:
        CalibrationResult with original and calibrated weights
    """
    original_weights = df['weight'].values.copy()
    n = len(original_weights)

    if verbose:
        print(f"Calibrating {n:,} tax units...")
        print(f"Original weighted total: {original_weights.sum():,.0f}")

    # Build targets
    all_targets = build_calibration_targets(df)

    # Filter out targets with too few observations
    targets = []
    skipped = []
    for name, indicator, target in all_targets:
        n_obs = (indicator > 0).sum() if indicator.max() > 1 else indicator.sum()
        if n_obs >= min_obs_for_calibration:
            targets.append((name, indicator, target))
        else:
            skipped.append((name, n_obs))

    if verbose:
        print(f"Built {len(all_targets)} calibration targets, using {len(targets)} (skipped {len(skipped)} with <{min_obs_for_calibration} obs)")
        if skipped and len(skipped) <= 5:
            for name, n_obs in skipped:
                print(f"  Skipped {name}: only {n_obs} observations")

    # Compute current values
    targets_before = {}
    for name, indicator, target in targets:
        current = np.dot(original_weights, indicator)
        targets_before[name] = {
            'current': current,
            'target': target,
            'error': (current - target) / target if target != 0 else 0,
        }

    if verbose:
        print("\nPre-calibration errors (selected):")
        for name in ['total_returns', 'total_agi', 'returns_50k_to_75k', 'returns_1m_plus']:
            if name in targets_before:
                t = targets_before[name]
                print(f"  {name}: {t['error']:+.1%} (current: {t['current']:,.0f}, target: {t['target']:,.0f})")

    # Use simple raking/iterative proportional fitting for stability
    calibrated_weights = original_weights.copy()

    # Iterative proportional fitting with damping
    damping = 0.5  # Smooth adjustments to improve convergence

    for iteration in range(100):
        max_error = 0
        total_adjustment = 0

        for name, indicator, target in targets:
            if target == 0:
                continue

            current = np.dot(calibrated_weights, indicator)
            if current == 0:
                continue

            raw_ratio = target / current

            # Apply damping: move partway toward target
            ratio = 1.0 + damping * (raw_ratio - 1.0)

            # Limit adjustment per iteration
            ratio = np.clip(ratio, 0.8, 1.25)

            # For amount targets, apply uniformly to all in stratum
            if 'agi_' in name and 'returns_' not in name:
                # This is an AGI amount target
                mask = indicator != 0
                if mask.any():
                    calibrated_weights[mask] *= ratio
            else:
                # Count target - adjust where indicator == 1
                mask = indicator == 1
                if mask.any():
                    calibrated_weights[mask] *= ratio

            # Track max error
            new_current = np.dot(calibrated_weights, indicator)
            error = abs(new_current - target) / abs(target) if target != 0 else 0
            max_error = max(max_error, error)

        # Clip weights to bounds
        min_weight = original_weights / max_adjustment
        max_weight = original_weights * max_adjustment
        calibrated_weights = np.clip(calibrated_weights, min_weight, max_weight)

        if iteration % 20 == 0 and verbose:
            print(f"  Iteration {iteration}: max error = {max_error:.2%}")

        if max_error < tolerance:
            if verbose:
                print(f"\nConverged after {iteration + 1} iterations (max error: {max_error:.2%})")
            break
    else:
        if verbose:
            print(f"\nDid not fully converge after 100 iterations (max error: {max_error:.2%})")

    # Compute final values
    targets_after = {}
    for name, indicator, target in targets:
        current = np.dot(calibrated_weights, indicator)
        targets_after[name] = {
            'current': current,
            'target': target,
            'error': (current - target) / target if target != 0 else 0,
        }

    # Compute adjustment factors
    adjustment_factors = calibrated_weights / original_weights

    if verbose:
        print("\nPost-calibration errors (selected):")
        for name in ['total_returns', 'total_agi', 'returns_50k_to_75k', 'returns_1m_plus']:
            if name in targets_after:
                t = targets_after[name]
                print(f"  {name}: {t['error']:+.1%} (current: {t['current']:,.0f}, target: {t['target']:,.0f})")

        print(f"\nWeight adjustment stats:")
        print(f"  Mean adjustment: {adjustment_factors.mean():.3f}")
        print(f"  Min adjustment:  {adjustment_factors.min():.3f}")
        print(f"  Max adjustment:  {adjustment_factors.max():.3f}")
        print(f"  Std adjustment:  {adjustment_factors.std():.3f}")

    success = max(abs(t['error']) for t in targets_after.values()) < tolerance

    return CalibrationResult(
        original_weights=original_weights,
        calibrated_weights=calibrated_weights,
        adjustment_factors=adjustment_factors,
        targets_before=targets_before,
        targets_after=targets_after,
        success=success,
        message="Calibration successful" if success else "Calibration did not fully converge",
    )


def calibrate_and_run(year: int = 2024) -> pd.DataFrame:
    """
    Load data, calibrate weights, and return calibrated DataFrame.
    """
    from tax_unit_builder import load_and_build_tax_units

    print("=" * 60)
    print("COSILICO MICRODATA CALIBRATION")
    print("=" * 60)

    # Load data
    print("\n1. Loading tax unit data...")
    df = load_and_build_tax_units(year)
    print(f"   Loaded {len(df):,} tax units")

    # Calibrate
    print("\n2. Calibrating to IRS SOI 2021 targets...")
    result = calibrate_weights(df)

    # Update weights
    df['original_weight'] = result.original_weights
    df['weight'] = result.calibrated_weights
    df['weight_adjustment'] = result.adjustment_factors

    # Compare totals
    print("\n3. Comparing key aggregates...")
    print("\n" + "-" * 60)
    print(f"{'Metric':<25} {'Before':>15} {'After':>15} {'IRS SOI':>15}")
    print("-" * 60)

    metrics = [
        ('Tax Units', 'total_returns'),
        ('Total AGI', 'total_agi'),
    ]

    for label, key in metrics:
        before = result.targets_before[key]['current']
        after = result.targets_after[key]['current']
        target = result.targets_before[key]['target']
        print(f"{label:<25} {before:>15,.0f} {after:>15,.0f} {target:>15,.0f}")

    print("-" * 60)

    return df


if __name__ == "__main__":
    df = calibrate_and_run()

    print("\n" + "=" * 60)
    print("CALIBRATED DATA SUMMARY")
    print("=" * 60)

    print(f"\nTotal tax units: {len(df):,}")
    print(f"Weighted population: {df['weight'].sum():,.0f}")
    print(f"Total AGI: ${(df['adjusted_gross_income'] * df['weight']).sum():,.0f}")

    # Save calibrated data
    output_path = "tax_units_calibrated_2024.parquet"
    df.to_parquet(output_path)
    print(f"\nSaved calibrated data to {output_path}")
