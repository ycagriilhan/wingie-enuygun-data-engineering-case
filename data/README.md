# Data Directory Contract

This project uses one canonical source-data location:

- `data/source/airports.parquet`
- `data/source/booking_10k.parquet`
- `data/source/provider.parquet`
- `data/source/search_500k.parquet`

Rules for future phases:

- Treat `data/source/` as immutable input files.
- Write all generated outputs to non-source folders such as `data/landing/`, `data/raw/`, `data/staging/`, and `data/mart/`.
- Keep source parquet files versioned in Git for reproducible local runs.
