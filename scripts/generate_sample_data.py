"""Generate sample OTel log data in a local Iceberg catalog.

Produces realistic OTel logs matching the otlp2records schema and writes them
to a SQLite-backed Iceberg catalog for development and testing.

Usage:
    uv run python scripts/generate_sample_data.py --rows 10000 --hours 24
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

import click
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

SERVICES = [
    {
        "name": "api-gateway",
        "namespace": "production",
        "instance_id": "api-gw-01",
        "log_templates": {
            "INFO": [
                "GET /api/v1/users 200 {}ms",
                "POST /api/v1/orders 201 {}ms",
                "GET /api/v1/health 200 {}ms",
                "GET /api/v1/products 200 {}ms",
            ],
            "WARN": [
                "GET /api/v1/users 429 rate limit exceeded",
                "Request timeout after {}ms to upstream user-service",
                "Connection pool nearing capacity: {}/100",
            ],
            "ERROR": [
                "POST /api/v1/payments 502 Bad Gateway",
                "GET /api/v1/orders 503 Service Unavailable",
                "Upstream connection refused: payment-service:8080",
            ],
            "DEBUG": [
                "Routing request to backend pool: user-service",
                "TLS handshake completed in {}ms",
            ],
        },
    },
    {
        "name": "user-service",
        "namespace": "production",
        "instance_id": "user-svc-01",
        "log_templates": {
            "INFO": [
                "User {} authenticated successfully",
                "Profile updated for user {}",
                "Session created for user {}",
                "Database query completed in {}ms",
            ],
            "WARN": [
                "Slow database query: {}ms (threshold: 500ms)",
                "Cache miss for user profile {}",
                "Retry attempt {}/3 for database connection",
            ],
            "ERROR": [
                "Failed to authenticate user {}: invalid credentials",
                "Database connection pool exhausted",
                "Unhandled exception in user profile handler",
            ],
            "DEBUG": [
                "Cache hit for user session {}",
                "Preparing SQL query for user lookup",
            ],
        },
    },
    {
        "name": "payment-service",
        "namespace": "production",
        "instance_id": "pay-svc-01",
        "log_templates": {
            "INFO": [
                "Payment {} processed successfully: ${}",
                "Refund {} initiated for order {}",
                "Payment gateway health check: OK",
            ],
            "WARN": [
                "Payment gateway response slow: {}ms",
                "Retry payment {} attempt {}/3",
                "Idempotency key collision detected",
            ],
            "ERROR": [
                "Payment {} failed: gateway timeout",
                "Payment {} declined: insufficient funds",
                "Payment gateway connection refused",
                "Critical: payment reconciliation mismatch for order {}",
            ],
            "DEBUG": [
                "Encrypting payment payload for gateway",
                "Validating payment request schema",
            ],
        },
    },
    {
        "name": "notification-service",
        "namespace": "production",
        "instance_id": "notif-svc-01",
        "log_templates": {
            "INFO": [
                "Email sent to {} for order {}",
                "Push notification delivered to device {}",
                "SMS sent to {} successfully",
            ],
            "WARN": [
                "Email delivery delayed: queue depth {}",
                "Push notification rate limit: {} per minute",
            ],
            "ERROR": [
                "Failed to send email to {}: SMTP connection refused",
                "Push notification failed: invalid device token {}",
            ],
            "DEBUG": [
                "Rendering email template: order_confirmation",
                "Queueing notification for batch delivery",
            ],
        },
    },
    {
        "name": "auth-service",
        "namespace": "production",
        "instance_id": "auth-svc-01",
        "log_templates": {
            "INFO": [
                "Token issued for user {} (expires: {}s)",
                "Token refreshed for user {}",
                "OAuth callback processed for provider {}",
            ],
            "WARN": [
                "Token near expiry for user {} ({}s remaining)",
                "Multiple failed login attempts for user {} from {}",
            ],
            "ERROR": [
                "Token validation failed: expired",
                "OAuth provider {} returned error: {}",
                "Brute force detected: {} failed attempts from IP {}",
            ],
            "DEBUG": [
                "JWT signature verified successfully",
                "Loading user permissions from cache",
            ],
        },
    },
]

SEVERITY_WEIGHTS = {"INFO": 0.60, "WARN": 0.20, "ERROR": 0.15, "DEBUG": 0.05}
SEVERITY_NUMBERS = {"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}


def _random_id() -> str:
    return uuid.uuid4().hex[:16]


def _fill_template(template: str) -> str:
    """Fill {} placeholders with random values."""
    result = template
    while "{}" in result:
        placeholder = random.choice(
            [
                str(random.randint(1, 9999)),
                str(random.randint(10, 5000)),
                _random_id()[:8],
            ]
        )
        result = result.replace("{}", placeholder, 1)
    return result


def generate_logs(num_rows: int, hours: int) -> pa.Table:
    """Generate a PyArrow table of OTel log records."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    # Pre-generate trace groups (5-20 logs per trace)
    trace_groups: list[tuple[str, str, str]] = []
    for _ in range(num_rows // 10):
        trace_id = uuid.uuid4().hex
        service = random.choice(SERVICES)
        spans = random.randint(5, 20)
        for _ in range(spans):
            span_id = uuid.uuid4().hex[:16]
            trace_groups.append((trace_id, span_id, service["name"]))

    # Pad to exact row count
    while len(trace_groups) < num_rows:
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        service = random.choice(SERVICES)
        trace_groups.append((trace_id, span_id, service["name"]))
    trace_groups = trace_groups[:num_rows]
    random.shuffle(trace_groups)

    # Create error spike pattern: payment-service errors cluster around hour 3
    spike_start = start + timedelta(hours=3)
    spike_end = spike_start + timedelta(minutes=30)

    timestamps = []
    observed_timestamps = []
    trace_ids = []
    span_ids = []
    service_names = []
    service_namespaces = []
    service_instance_ids = []
    severity_numbers = []
    severity_texts = []
    bodies = []
    resource_attrs = []
    scope_names = []
    scope_versions = []
    scope_attrs = []
    log_attrs = []

    service_map = {s["name"]: s for s in SERVICES}

    for i, (trace_id, span_id, svc_name) in enumerate(trace_groups):
        svc = service_map[svc_name]

        # Generate timestamp
        ts = start + timedelta(seconds=random.uniform(0, hours * 3600))

        # Error spike for payment-service
        if svc_name == "payment-service" and spike_start <= ts <= spike_end:
            severity = "ERROR" if random.random() < 0.7 else "WARN"
        else:
            severity = random.choices(
                list(SEVERITY_WEIGHTS.keys()),
                weights=list(SEVERITY_WEIGHTS.values()),
            )[0]

        templates = svc["log_templates"][severity]
        body = _fill_template(random.choice(templates))

        timestamps.append(ts)
        observed_timestamps.append(int(ts.timestamp() * 1000) + random.randint(0, 10))
        trace_ids.append(trace_id)
        span_ids.append(span_id)
        service_names.append(svc_name)
        service_namespaces.append(svc["namespace"])
        service_instance_ids.append(svc["instance_id"])
        severity_numbers.append(SEVERITY_NUMBERS[severity])
        severity_texts.append(severity)
        bodies.append(body)
        resource_attrs.append(
            json.dumps(
                {
                    "service.name": svc_name,
                    "service.namespace": svc["namespace"],
                    "service.instance.id": svc["instance_id"],
                    "host.name": f"host-{random.randint(1, 5):02d}",
                    "deployment.environment": "production",
                }
            )
        )
        scope_names.append("otel-rlm-logsearch")
        scope_versions.append("0.1.0")
        scope_attrs.append(json.dumps({}))
        log_attrs.append(
            json.dumps(
                {
                    "log.file.name": f"{svc_name}.log",
                    "log.file.path": f"/var/log/{svc_name}/{svc_name}.log",
                }
            )
        )

    return pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.timestamp("ms", tz="UTC")),
            "observed_timestamp": pa.array(observed_timestamps, type=pa.int64()),
            "trace_id": pa.array(trace_ids, type=pa.string()),
            "span_id": pa.array(span_ids, type=pa.string()),
            "service_name": pa.array(service_names, type=pa.string()),
            "service_namespace": pa.array(service_namespaces, type=pa.string()),
            "service_instance_id": pa.array(service_instance_ids, type=pa.string()),
            "severity_number": pa.array(severity_numbers, type=pa.int32()),
            "severity_text": pa.array(severity_texts, type=pa.string()),
            "body": pa.array(bodies, type=pa.string()),
            "resource_attributes": pa.array(resource_attrs, type=pa.string()),
            "scope_name": pa.array(scope_names, type=pa.string()),
            "scope_version": pa.array(scope_versions, type=pa.string()),
            "scope_attributes": pa.array(scope_attrs, type=pa.string()),
            "log_attributes": pa.array(log_attrs, type=pa.string()),
        },
    )


ICEBERG_SCHEMA = Schema(
    NestedField(1, "timestamp", TimestamptzType(), required=False),
    NestedField(2, "observed_timestamp", LongType(), required=False),
    NestedField(3, "trace_id", StringType(), required=False),
    NestedField(4, "span_id", StringType(), required=False),
    NestedField(5, "service_name", StringType(), required=False),
    NestedField(6, "service_namespace", StringType(), required=False),
    NestedField(7, "service_instance_id", StringType(), required=False),
    NestedField(8, "severity_number", IntegerType(), required=False),
    NestedField(9, "severity_text", StringType(), required=False),
    NestedField(10, "body", StringType(), required=False),
    NestedField(11, "resource_attributes", StringType(), required=False),
    NestedField(12, "scope_name", StringType(), required=False),
    NestedField(13, "scope_version", StringType(), required=False),
    NestedField(14, "scope_attributes", StringType(), required=False),
    NestedField(15, "log_attributes", StringType(), required=False),
)


@click.command()
@click.option(
    "--rows", default=10_000, type=int, help="Number of log records to generate"
)
@click.option("--hours", default=24, type=int, help="Hours of log data to span")
@click.option("--warehouse", default="./warehouse", help="Warehouse directory")
@click.option("--catalog-uri", default="sqlite:///./warehouse.db", help="Catalog URI")
def main(rows: int, hours: int, warehouse: str, catalog_uri: str) -> None:
    """Generate sample OTel logs into a local Iceberg catalog."""
    click.echo(f"Generating {rows:,} log records spanning {hours} hours...")

    arrow_table = generate_logs(rows, hours)
    click.echo(f"Generated {arrow_table.num_rows:,} records in memory")

    catalog = SqlCatalog(
        "default",
        **{"uri": catalog_uri, "warehouse": warehouse},
    )

    # Create namespace if needed
    try:
        catalog.create_namespace("otel")
    except Exception:
        pass  # namespace may already exist

    # Create or replace table
    table_name = "otel.logs"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass

    table = catalog.create_table(table_name, schema=ICEBERG_SCHEMA)
    table.append(arrow_table)

    click.echo(f"Wrote {arrow_table.num_rows:,} records to {table_name}")
    click.echo(f"Catalog: {catalog_uri}")
    click.echo(f"Warehouse: {warehouse}")


if __name__ == "__main__":
    main()
