"""MCP server exposing OTel log search as a single tool."""

from __future__ import annotations

import asyncio

from mcp.server.fastmcp import Context, FastMCP

from otel_rlm_logsearch.pyodide_setup import ensure_pyodide_packages

mcp = FastMCP("otel-logsearch")


@mcp.tool()
async def query(
    question: str,
    ctx: Context,
    table: str | None = None,
    model: str | None = None,
    max_rows: int = 5000,
    time_range: str | None = None,
) -> str:
    """Ask a natural language question about OpenTelemetry logs stored in Apache Iceberg.

    Catalog connection is configured via environment variables:
    OTEL_LOGSEARCH_CATALOG_TYPE, OTEL_LOGSEARCH_CATALOG_URI,
    OTEL_LOGSEARCH_WAREHOUSE, OTEL_LOGSEARCH_TABLE.
    """
    from otel_rlm_logsearch.config import AppConfig
    from otel_rlm_logsearch.solver import solve

    await ctx.info("Configuring catalog connection...")

    parsed_range = None
    if time_range:
        parts = time_range.split("/")
        if len(parts) == 2:
            parsed_range = (parts[0], parts[1])

    config = AppConfig.from_env(
        table=table,
        row_limit=max_rows,
        time_range=parsed_range,
    )
    if model:
        config.model = model

    await ctx.info(
        f"Loading logs and starting RLM analysis (up to {max_rows:,} rows)..."
    )
    result = await asyncio.to_thread(solve, config, question)

    output = f"Answer: {result['answer']}"
    if result["evidence"]:
        output += f"\n\nEvidence: {result['evidence']}"
    output += f"\n\nRows analyzed: {result['rows_analyzed']:,}"
    return output


def main() -> None:
    ensure_pyodide_packages()
    mcp.run()


if __name__ == "__main__":
    main()
