"""Small shared style for report-ready analysis figures."""

import logging
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.figure import Figure


COLORS = {
    "blue": "#1565c0",
    "red": "#c62828",
    "orange": "#e08214",
    "purple": "#6a51a3",
    "muted_red": "#d98a8a",
}


def apply_plot_style() -> None:
    """Apply the repository's Palatino-based report style."""
    logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
    mpl.rcParams.update(
        {
            "font.family": [
                "Palatino",
                "Palatino Linotype",
                "TeX Gyre Pagella",
                "P052",
                "URW Palladio L",
                "serif",
            ],
            "font.size": 11,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.alpha": 0.25,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.fontsize": 9.5,
            "legend.framealpha": 0.95,
            "savefig.dpi": 200,
            "savefig.bbox": "tight",
        }
    )


def save_figure(figure: Figure, output_path: Path) -> None:
    """Save and close one figure using the shared export settings."""
    figure.savefig(output_path)
    plt.close(figure)
