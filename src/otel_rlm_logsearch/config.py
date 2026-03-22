from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class CatalogConfig:
    catalog_name: str = "default"
    catalog_type: str = "sql"
    uri: str = "sqlite:///./warehouse.db"
    warehouse: str = "./warehouse"
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class AppConfig:
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    model: str = "openrouter/moonshotai/kimi-k2.5"
    max_iterations: int = 15
    table: str = "otel.logs"
    time_range: tuple[str, str] | None = None
    row_limit: int = 50_000

    @classmethod
    def from_env(cls, **overrides) -> AppConfig:
        """Build config from environment variables, with explicit overrides taking priority."""
        catalog = CatalogConfig(
            catalog_name=os.environ.get("OTEL_LOGSEARCH_CATALOG_NAME", "default"),
            catalog_type=os.environ.get("OTEL_LOGSEARCH_CATALOG_TYPE", "sql"),
            uri=os.environ.get(
                "OTEL_LOGSEARCH_CATALOG_URI", "sqlite:///./warehouse.db"
            ),
            warehouse=os.environ.get("OTEL_LOGSEARCH_WAREHOUSE", "./warehouse"),
        )
        config = cls(
            catalog=catalog,
            model=os.environ.get("OTEL_LOGSEARCH_MODEL", cls.model),
            table=os.environ.get("OTEL_LOGSEARCH_TABLE", cls.table),
        )
        # Apply explicit overrides
        for key, value in overrides.items():
            if value is not None:
                if key == "token":
                    config.catalog.properties["token"] = value
                elif hasattr(config.catalog, key):
                    setattr(config.catalog, key, value)
                elif hasattr(config, key):
                    setattr(config, key, value)
        return config
