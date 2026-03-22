"""SandboxSerializable DataFrame wrapper for OTel log data.

Wraps a pandas DataFrame and implements the SandboxSerializable protocol
so it can be injected into the RLM sandbox as a native variable.
The DataFrame is serialized as base64-encoded Parquet for efficient,
type-preserving transfer into the Deno/Pyodide sandbox.
"""

from __future__ import annotations

import base64
from typing import Any

from dspy import SandboxSerializable


class DataFrame(SandboxSerializable):
    """DataFrame wrapper with RLM sandbox support and OTel-aware previews."""

    def __init__(self, data: Any):
        type_name = type(data).__name__
        type_module = getattr(type(data), "__module__", "")
        if type_module.startswith("pandas") and type_name == "DataFrame":
            self.data = data
        elif isinstance(data, DataFrame):
            self.data = data.data
        else:
            raise TypeError(f"Expected pandas DataFrame, got {type_name}")

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or name == "data":
            raise AttributeError(name)
        return getattr(self.data, name)

    # SandboxSerializable protocol

    def sandbox_setup(self) -> str:
        return (
            "import pandas as pd\nimport pyarrow\nimport base64\nimport io\nimport json"
        )

    def to_sandbox(self) -> bytes:
        return base64.b64encode(self.data.to_parquet(index=False))

    def sandbox_assignment(self, var_name: str, data_expr: str) -> str:
        return (
            f"{var_name} = pd.read_parquet(io.BytesIO(base64.b64decode({data_expr})))"
        )

    def rlm_preview(self, max_chars: int = 1000) -> str:
        """Generate an OTel-aware preview of the DataFrame."""
        df = self.data
        lines = [f"DataFrame: {df.shape[0]:,} rows x {df.shape[1]} columns"]

        # Time range
        if "timestamp" in df.columns and len(df) > 0:
            lines.append(
                f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}"
            )

        # Severity distribution
        if "severity_text" in df.columns:
            counts = df["severity_text"].value_counts()
            dist = ", ".join(f"{k}: {v:,}" for k, v in counts.items())
            lines.append(f"Severity: {dist}")

        # Top services
        if "service_name" in df.columns:
            top = df["service_name"].value_counts().head(5)
            svc = ", ".join(f"{k} ({v:,})" for k, v in top.items())
            lines.append(f"Services: {svc}")

        # Columns
        lines.append("\nColumns:")
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = int(df[col].isna().sum())
            null_info = f" ({null_count:,} nulls)" if null_count > 0 else ""
            lines.append(f"  {col}: {dtype}{null_info}")

        # Sample rows
        if len(df) > 0:
            lines.extend(["", "Sample (first 3 rows):", df.head(3).to_string()])

        preview = "\n".join(lines)
        return preview[:max_chars] + "..." if len(preview) > max_chars else preview

    def __repr__(self) -> str:
        return f"DataFrame({self.data.shape[0]} rows x {self.data.shape[1]} cols)"
