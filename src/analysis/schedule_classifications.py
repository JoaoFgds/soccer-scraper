"""Schedule-group classifications shared by the paper analyses."""

import pandas as pd


MAGNITUDE_COLUMNS = (
    "G_type_0.200",
    "G_type_0.300",
    "G_type_0.400",
)
SIGNIFICANCE_LEVELS = (0.10, 0.05)

GROUP_LABELS = {
    "balanced": "G0",
    "unbalanced_weak": "G-",
    "unbalanced_strong": "G+",
}


def iter_schedule_classifications(
    schedule_balance: pd.DataFrame,
):
    """Yield each configured output-folder name and its schedule groups."""
    for column in MAGNITUDE_COLUMNS:
        yield column, schedule_balance[column]

    for significance_level in SIGNIFICANCE_LEVELS:
        significant = schedule_balance["P_value"].le(significance_level)
        groups = pd.Series("balanced", index=schedule_balance.index, dtype="object")
        groups = groups.mask(
            significant & schedule_balance["G"].lt(0),
            "unbalanced_weak",
        )
        groups = groups.mask(
            significant & schedule_balance["G"].gt(0),
            "unbalanced_strong",
        )
        yield f"significance_{significance_level:.2f}", groups
