from __future__ import annotations

from typing import Any

import pandas as pd
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from otel_rlm_logsearch.config import CatalogConfig


def connect_catalog(config: CatalogConfig) -> Catalog:
    """Connect to an Iceberg catalog using the given configuration."""
    if config.catalog_type == "rest":
        return RestCatalog(
            name=config.catalog_name,
            warehouse=config.warehouse,
            uri=config.uri,
            token=config.properties.get("token", ""),
        )
    return load_catalog(
        name=config.catalog_name,
        **{
            "type": config.catalog_type,
            "uri": config.uri,
            "warehouse": config.warehouse,
            **config.properties,
        },
    )


def load_logs(
    catalog: Catalog,
    table_name: str,
    time_range: tuple[str, str] | None = None,
    severity_filter: str | None = None,
    service_filter: str | None = None,
    row_limit: int = 50_000,
) -> pd.DataFrame:
    """Load OTel log records from an Iceberg table into a DataFrame.

    Uses Iceberg predicate pushdown for efficient filtering on Parquet files.
    """
    table = catalog.load_table(table_name)

    filters = []
    if time_range:
        start, end = time_range
        filters.append(GreaterThanOrEqual("timestamp", start))
        filters.append(LessThan("timestamp", end))

    row_filter = (
        And(*filters) if len(filters) > 1 else (filters[0] if filters else None)
    )

    scan_kwargs: dict[str, Any] = {}
    if row_filter:
        scan_kwargs["row_filter"] = row_filter
    if row_limit:
        scan_kwargs["limit"] = row_limit

    scan = table.scan(**scan_kwargs)
    df = scan.to_pandas()

    # Apply post-scan filters (these columns may not support pushdown)
    if severity_filter:
        df = df[df["severity_text"] == severity_filter]
    if service_filter:
        df = df[df["service_name"] == service_filter]

    return df


def list_tables(catalog: Catalog, namespace: str = "otel") -> list[str]:
    """List available tables in the given namespace."""
    try:
        return [f"{ns}.{name}" for ns, name in catalog.list_tables(namespace)]
    except Exception:
        return []
