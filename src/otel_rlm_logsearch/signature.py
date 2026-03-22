"""DSPy Signature for OTel log analysis."""

from __future__ import annotations

import dspy

from otel_rlm_logsearch.dataframe import DataFrame


class OTelLogAnalysis(dspy.Signature):
    """Analyze OpenTelemetry log data to answer operational questions.

    You have access to a pandas DataFrame called `logs` containing OpenTelemetry log records.
    Key columns include: timestamp, severity_text, severity_number, body, service_name,
    trace_id, span_id, resource_attributes (JSON string), log_attributes (JSON string).

    JSON attribute columns can be parsed with json.loads() to access nested fields.
    Use pandas to filter, aggregate, and analyze the data.
    """

    logs: DataFrame = dspy.InputField(
        desc="DataFrame of OTel log records with columns: timestamp, severity_text, "
        "severity_number, body, service_name, resource_attributes (JSON), "
        "log_attributes (JSON), trace_id, span_id"
    )
    question: str = dspy.InputField(desc="Natural language question about the logs")
    answer: str = dspy.OutputField(
        desc="Direct answer to the question, with supporting data"
    )
    evidence: str = dspy.OutputField(
        desc="Key log entries or aggregations that support the answer"
    )
