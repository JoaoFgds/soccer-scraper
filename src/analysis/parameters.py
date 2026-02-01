"""
Centralized analysis parameters for plot scripts.

This module defines threshold configurations that can be used across
multiple analysis scripts. Add new thresholds here to have them
automatically picked up by scripts using the --all flag.

Usage in scripts:
    from src.analysis.parameters import G_TYPE_THRESHOLDS, P_VALUE_THRESHOLDS
"""

# G-type coefficient thresholds
# These correspond to pre-computed G_type_X.XXX columns in the data
# Lower values = more permissive (more teams classified as unbalanced)
# Higher values = more strict (fewer teams classified as unbalanced)
G_TYPE_THRESHOLDS = [
    "0.200",
    "0.250",
    "0.300",
    "0.350",
    "0.400",
]

# P-value thresholds for Spearman correlation significance
# Lower values = more strict (only highly significant imbalances)
# Higher values = more permissive
P_VALUE_THRESHOLDS = [
    0.10,
]

# Default thresholds (used when no --all flag is provided)
DEFAULT_G_TYPE_THRESHOLD = "0.300"
DEFAULT_P_VALUE_THRESHOLD = 0.10
