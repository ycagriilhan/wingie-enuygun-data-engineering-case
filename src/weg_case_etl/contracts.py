from __future__ import annotations

from pathlib import Path


REQUIRED_SOURCE_FILES: tuple[str, ...] = (
    "airports.parquet",
    "booking_10k.parquet",
    "provider.parquet",
    "search_500k.parquet",
)

COMMAND_ORDER: tuple[str, ...] = (
    "profile",
    "extract-upload",
    "load-raw",
    "transform",
    "dq",
)


def required_source_paths(source_dir: Path) -> dict[str, Path]:
    return {name: source_dir / name for name in REQUIRED_SOURCE_FILES}
