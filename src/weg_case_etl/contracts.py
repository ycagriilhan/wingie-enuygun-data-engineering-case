from __future__ import annotations

from pathlib import Path


REQUIRED_SOURCE_FILES: tuple[str, ...] = (
    "airports.parquet",
    "booking_10k.parquet",
    "provider.parquet",
    "search_500k.parquet",
)

SOURCE_FILE_CLASSIFICATION: dict[str, dict[str, str]] = {
    "airports.parquet": {"dataset": "airport_reference", "domain": "reference"},
    "provider.parquet": {"dataset": "provider", "domain": "reference"},
    "booking_10k.parquet": {"dataset": "booking", "domain": "fact"},
    "search_500k.parquet": {"dataset": "search", "domain": "fact"},
}

COMMAND_ORDER: tuple[str, ...] = (
    "profile",
    "extract-upload",
    "load-raw",
    "transform",
    "dq",
)


def required_source_paths(source_dir: Path) -> dict[str, Path]:
    return {name: source_dir / name for name in REQUIRED_SOURCE_FILES}


def classify_source_file(file_name: str) -> dict[str, str]:
    if file_name not in SOURCE_FILE_CLASSIFICATION:
        return {"dataset": Path(file_name).stem, "domain": "unknown"}
    return SOURCE_FILE_CLASSIFICATION[file_name]
