"""Shared, stable visual identities for benchmark engines.

Colors come from a color-vision-friendly palette.  Markers and hatches provide
redundant identification so plots remain readable in grayscale and when two
colors look similar on a projector.
"""

import math


_COMPACT = False


def set_compact(enabled: bool = True) -> None:
    """Enable the shared-legend compact layout used by ``plot.py``."""
    global _COMPACT
    _COMPACT = enabled


def apply_compact_layout(fig) -> None:
    """Remove the overall title and use one prominent shared figure legend.

    This is deliberately applied immediately before saving.  Plotting modules
    can keep expressing their normal standalone layout while ``plot.py
    --compact`` supplies a consistent presentation across every input format.
    Multi-panel headings are retained because they identify overview panels.
    """
    if not _COMPACT:
        return

    overall_title = fig._suptitle.get_text() if fig._suptitle is not None else ""
    is_overview = "overview" in overall_title.lower()
    if fig._suptitle is not None:
        fig._suptitle.remove()
        fig._suptitle = None

    if is_overview:
        # Overview defaults favor comfortable on-screen inspection.  Compact
        # output is intended for papers, where a narrower canvas and less
        # vertical space per panel are easier to place without downscaling all
        # text along with the figure.
        width, height = fig.get_size_inches()
        fig.set_size_inches(width * 0.70, height * 0.54, forward=True)
    active_axes = [ax for ax in fig.axes if ax.get_visible() and ax.axison]
    entries = []
    for ax in active_axes:
        # A one-panel axes title is the plot's overall title.  In an overview,
        # axes titles are necessary panel names and should remain visible.
        if len(active_axes) == 1:
            ax.set_title("")
        # Shared x-axes hide non-bottom tick labels by default.  Compact
        # overviews still need the measurement values on every active panel.
        ax.tick_params(axis="x", which="both", labelbottom=True)
        legend = ax.get_legend()
        if legend is not None:
            handles = getattr(
                legend, "legend_handles", getattr(legend, "legendHandles", []),
            )
            entries.extend(zip(handles, (text.get_text() for text in legend.get_texts())))
            legend.remove()
        else:
            entries.extend(zip(*ax.get_legend_handles_labels()))

    for legend in list(fig.legends):
        handles = getattr(
            legend, "legend_handles", getattr(legend, "legendHandles", []),
        )
        entries.extend(zip(handles, (text.get_text() for text in legend.get_texts())))
        legend.remove()

    unique = {}
    for handle, label in entries:
        if label and not label.startswith("_"):
            unique.setdefault(label, handle)

    if not unique:
        fig.tight_layout()
        return

    ncol = min(5, len(unique))
    rows = math.ceil(len(unique) / ncol)
    top = max(0.76, 0.998 - 0.055 * rows)
    fig.legend(
        unique.values(), unique.keys(), loc="upper center",
        bbox_to_anchor=(0.5, 0.985), ncol=ncol, frameon=False,
        fontsize=11, columnspacing=1.4, handletextpad=0.6,
        handlelength=2.2,
    )
    fig.tight_layout(rect=(0, 0, 1, top))

ENGINE_ORDER = [
    "batstore", "leanstore", "wiredtiger", "postgres", "umbra", "vweaver_ermia",
    "vweaver_ermia_frugal", "libmdbx",
]

ENGINE_LABELS = {
    "batstore": "BatStore",
    "leanstore": "LeanStore",
    "wiredtiger": "WiredTiger",
    "postgres": "PostgreSQL",
    "umbra": "Umbra",
    "vweaver_ermia": "vWeaver/ERMIA",
    "vweaver_ermia_frugal": "Frugal/ERMIA",
    "libmdbx": "libmdbx",
}

# Okabe-Ito-derived colors, with black reserved for libmdbx.
ENGINE_COLORS = {
    "batstore": "#222222",             # near black; emphasized primary system
    "leanstore": "#0072B2",            # blue
    "wiredtiger": "#E69F00",           # orange
    "postgres": "#D55E00",             # vermillion
    "umbra": "#F0E442",                # yellow
    "vweaver_ermia": "#CC79A7",        # reddish purple
    "vweaver_ermia_frugal": "#56B4E9", # sky blue
    "libmdbx": "#009E73",              # bluish green
}

ENGINE_MARKERS = {
    "batstore": "v",
    "leanstore": "s",
    "wiredtiger": "^",
    "postgres": "D",
    "umbra": "*",
    "vweaver_ermia": "P",
    "vweaver_ermia_frugal": "X",
    "libmdbx": "o",
}

ENGINE_HATCHES = {
    "batstore": "//",
    "leanstore": "\\\\",
    "wiredtiger": "xx",
    "postgres": "..",
    "umbra": "**",
    "vweaver_ermia": "++",
    "vweaver_ermia_frugal": "oo",
    "libmdbx": "--",
}

LATENCY_PERCENTILE_STYLES = {
    "p50": {"color": "#0072B2", "marker": "o", "linestyle": "-", "hatch": "//"},
    "p95": {"color": "#E69F00", "marker": "s", "linestyle": "--", "hatch": "xx"},
    "p99": {"color": "#D55E00", "marker": "D", "linestyle": "-.", "hatch": ".."},
}


def engine_sort_key(name: str) -> int:
    return ENGINE_ORDER.index(name) if name in ENGINE_ORDER else len(ENGINE_ORDER)


def engine_line_style(engine: str) -> dict:
    """Matplotlib kwargs for an engine line, including an unknown fallback."""
    return {
        "color": ENGINE_COLORS.get(engine, "#777777"),
        "marker": ENGINE_MARKERS.get(engine, "o"),
        "markersize": 7 if engine == "batstore" else 6,
        "markeredgecolor": "white",
        "markeredgewidth": 0.7,
        "linewidth": 2.3 if engine == "batstore" else 1.5,
        "zorder": 3 if engine == "batstore" else 2,
    }


def latency_line_style(percentile: str) -> dict:
    """Stable line style for a latency percentile."""
    style = LATENCY_PERCENTILE_STYLES[percentile]
    return {
        "color": style["color"],
        "marker": style["marker"],
        "linestyle": style["linestyle"],
        "markersize": 6,
        "markeredgecolor": "white",
        "markeredgewidth": 0.7,
        "linewidth": 1.8,
    }


def measurement_values(values) -> list[int]:
    """Return sorted, unique non-negative integer measurement values."""
    return sorted({int(value) for value in values if value >= 0})


def measurement_positions(values, axis_values) -> list[int]:
    """Map measurements to evenly spaced categorical positions."""
    position_of = {value: position for position, value in enumerate(axis_values)}
    return [position_of[int(value)] for value in values]


def set_measurement_axis(ax, values, xlabel: str) -> None:
    """Use equal spacing while labeling ticks with the real measured values."""
    values = measurement_values(values)
    if not values:
        return
    ax.set_xscale("linear")
    ax.set_xticks(range(len(values)))
    ax.set_xticklabels([str(value) for value in values])
    ax.minorticks_off()
    ax.set_xlabel(xlabel)
    ax.grid(alpha=0.3)
