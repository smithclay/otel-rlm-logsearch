"""CLI entry point for otel-logsearch."""

from __future__ import annotations

import click
from rich.console import Console
from rich.panel import Panel

console = Console()


@click.group()
def main() -> None:
    """Natural language search over OpenTelemetry logs in Apache Iceberg."""


@main.command()
@click.argument("question")
@click.option("--table", "-t", default="otel.logs", help="Iceberg table name")
@click.option("--catalog-type", default=None, help="Catalog type (sql, rest)")
@click.option("--catalog-uri", default=None, help="Catalog URI")
@click.option("--warehouse", default=None, help="Warehouse path")
@click.option("--token", default=None, help="Catalog auth token (for REST catalogs)")
@click.option("--model", "-m", default=None, help="LLM model name")
@click.option("--max-rows", default=50_000, type=int, help="Max rows to load")
@click.option("--max-iterations", default=15, type=int, help="Max RLM iterations")
@click.option(
    "--time-range", default=None, help="Time range filter (start/end ISO timestamps)"
)
@click.option(
    "--verbose", "-v", is_flag=True, help="Show RLM iterations (reasoning + code)"
)
def query(
    question: str,
    table: str,
    catalog_type: str | None,
    catalog_uri: str | None,
    warehouse: str | None,
    token: str | None,
    model: str | None,
    max_rows: int,
    max_iterations: int,
    time_range: str | None,
    verbose: bool,
) -> None:
    """Ask a natural language question about your OTel logs."""
    from otel_rlm_logsearch.config import AppConfig
    from otel_rlm_logsearch.pyodide_setup import ensure_pyodide_packages
    from otel_rlm_logsearch.solver import solve

    ensure_pyodide_packages()

    parsed_range = None
    if time_range:
        parts = time_range.split("/")
        if len(parts) == 2:
            parsed_range = (parts[0], parts[1])

    config = AppConfig.from_env(
        table=table,
        catalog_type=catalog_type,
        uri=catalog_uri,
        warehouse=warehouse,
        token=token,
        row_limit=max_rows,
        max_iterations=max_iterations,
        time_range=parsed_range,
    )
    if model:
        config.model = model

    console.print(f"[bold]Querying[/bold] {table} ({max_rows:,} row limit)")
    console.print(f"[dim]Question: {question}[/dim]\n")

    result = solve(config, question, verbose=verbose)

    console.print(Panel(result["answer"], title="Answer", border_style="green"))
    if result["evidence"]:
        console.print(Panel(result["evidence"], title="Evidence", border_style="blue"))
    console.print(f"\n[dim]Rows analyzed: {result['rows_analyzed']:,}[/dim]")


@main.command()
@click.option("--catalog-type", default="sql", help="Catalog type (sql, rest)")
@click.option("--catalog-uri", default="sqlite:///./warehouse.db", help="Catalog URI")
@click.option("--warehouse", default="./warehouse", help="Warehouse path")
@click.option("--token", default=None, help="Catalog auth token (for REST catalogs)")
@click.option("--namespace", "-n", default="otel", help="Namespace to list")
def tables(
    catalog_type: str,
    catalog_uri: str,
    warehouse: str,
    token: str | None,
    namespace: str,
) -> None:
    """List available Iceberg tables."""
    from otel_rlm_logsearch.catalog import connect_catalog, list_tables
    from otel_rlm_logsearch.config import CatalogConfig

    properties = {}
    if token:
        properties["token"] = token
    config = CatalogConfig(
        catalog_type=catalog_type,
        uri=catalog_uri,
        warehouse=warehouse,
        properties=properties,
    )
    catalog = connect_catalog(config)
    found = list_tables(catalog, namespace)

    if found:
        for t in found:
            console.print(f"  {t}")
    else:
        console.print(f"[yellow]No tables found in namespace '{namespace}'[/yellow]")
