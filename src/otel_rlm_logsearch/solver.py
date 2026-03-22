"""RLM solver that ties catalog loading, DataFrame wrapping, and execution together."""

from __future__ import annotations

import logging

import dspy
from rich.console import Console
from rich.logging import RichHandler

from otel_rlm_logsearch.catalog import connect_catalog, load_logs
from otel_rlm_logsearch.config import AppConfig
from otel_rlm_logsearch.dataframe import DataFrame
from otel_rlm_logsearch.signature import OTelLogAnalysis


def solve(config: AppConfig, question: str, verbose: bool = False) -> dict:
    """Run the RLM to answer a question about OTel logs.

    Returns a dict with keys: answer, evidence, rows_analyzed.
    """
    if verbose:
        # Route DSPy's RLM iteration logs through Rich, replacing default handlers
        console = Console(stderr=True)
        handler = RichHandler(
            console=console,
            show_path=False,
            show_time=False,
            markup=True,
        )
        rlm_logger = logging.getLogger("dspy.predict.rlm")
        rlm_logger.handlers.clear()
        rlm_logger.setLevel(logging.INFO)
        rlm_logger.addHandler(handler)
        rlm_logger.propagate = False

    dspy.configure(lm=dspy.LM(config.model))

    catalog = connect_catalog(config.catalog)
    df = load_logs(
        catalog,
        config.table,
        time_range=config.time_range,
        row_limit=config.row_limit,
    )

    wrapped = DataFrame(df)
    rlm = dspy.RLM(
        OTelLogAnalysis,
        max_iterations=config.max_iterations,
        verbose=verbose,
    )
    result = rlm(logs=wrapped, question=question)

    return {
        "answer": result.answer,
        "evidence": result.evidence,
        "rows_analyzed": len(df),
    }
