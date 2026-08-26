"""Shared, stable visual identities for benchmark engines.

Colors come from a color-vision-friendly palette.  Markers and hatches provide
redundant identification so plots remain readable in grayscale and when two
colors look similar on a projector.
"""

ENGINE_ORDER = [
    "batstore", "leanstore", "wiredtiger", "postgres", "vweaver_ermia",
    "vweaver_ermia_frugal", "libmdbx",
]

ENGINE_LABELS = {
    "batstore": "BatStore",
    "leanstore": "LeanStore",
    "wiredtiger": "WiredTiger",
    "postgres": "PostgreSQL",
    "vweaver_ermia": "vWeaver/ERMIA",
    "vweaver_ermia_frugal": "Frugal/ERMIA",
    "libmdbx": "libmdbx",
}

# Okabe-Ito-derived colors, with black reserved for libmdbx.
ENGINE_COLORS = {
    "batstore": "#009E73",             # bluish green
    "leanstore": "#0072B2",            # blue
    "wiredtiger": "#E69F00",           # orange
    "postgres": "#D55E00",             # vermillion
    "vweaver_ermia": "#CC79A7",        # reddish purple
    "vweaver_ermia_frugal": "#56B4E9", # sky blue
    "libmdbx": "#222222",              # near black
}

ENGINE_MARKERS = {
    "batstore": "o",
    "leanstore": "s",
    "wiredtiger": "^",
    "postgres": "D",
    "vweaver_ermia": "P",
    "vweaver_ermia_frugal": "X",
    "libmdbx": "v",
}

ENGINE_HATCHES = {
    "batstore": "//",
    "leanstore": "\\\\",
    "wiredtiger": "xx",
    "postgres": "..",
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
        "markersize": 6,
        "markeredgecolor": "white",
        "markeredgewidth": 0.7,
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
