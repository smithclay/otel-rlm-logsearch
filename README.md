# otel-rlm-logsearch

Natural language search over OpenTelemetry logs stored in Apache Iceberg using [DSPy's Recursive Language Model (RLM)](https://dspy.ai/). Inspired by Kevin Madura's [A Data Scientist RLM That Lives in Your Program](https://kmad.ai/A-Data-Analysis-Agent-That-Lives-in-Your-Program).

Instead of writing SQL or grep patterns, ask questions in plain English. The RLM iteratively writes and executes Python code in a sandboxed REPL, exploring your log data without filling up the context window until it finds the answer. No MCPs or subagents needed.

## Why?

Searching over huge log sources with lots of tool calls fill up the context window fast: RLMs avoid this and delegate more of the reasoning to the LLM itself. You also can get excellent results with smaller and cheaper models vs frontier models.

## How it works

```
Question ──> DSPy RLM ──> Sandboxed Python REPL (Pyodide/WASM)
                │                    │
                │              pandas DataFrame
                │              (from Iceberg/Parquet)
                │                    │
                └── iterate ◄────────┘
                     (reason, write code, execute, repeat)
                            │
                        Answer + Evidence
```

1. Logs are loaded from an [Apache Iceberg](https://iceberg.apache.org/) table (Parquet format) into a pandas DataFrame
2. The DataFrame is serialized into a [Pyodide/WASM sandbox](https://pyodide.org/) via the [`SandboxSerializable`](https://github.com/kmad/dspy/tree/sandbox-serializable) protocol
3. The LLM iteratively reasons about the question, writes Python code, executes it in the sandbox, and observes the output
4. After several iterations, it submits a structured answer with supporting evidence

The log schema follows [otlp2records](https://github.com/smithclay/otlp2records) — a Rust library that converts OTLP data to Arrow RecordBatches. Attribute columns (`resource_attributes`, `log_attributes`, `scope_attributes`) are JSON-encoded strings, and `service_name` is a top-level column.

## Quickstart

**Prerequisites:** Python 3.10+, [uv](https://docs.astral.sh/uv/), [Deno](https://docs.deno.com/runtime/getting_started/installation/)

### Quick start

```bash
# Set your API key
export OPENROUTER_API_KEY=your-key-here

# Run directly from GitHub (Pyodide wheels are auto-downloaded on first run)
uvx --from "git+https://github.com/smithclay/otel-rlm-logsearch" otel-logsearch \
    query "What services are generating the most errors?" \
    --table default.logs \
    --catalog-type rest \
    --catalog-uri "https://catalog.cloudflarestorage.com/<account-id>/<bucket>" \
    --warehouse "<account-id>_<bucket>" \
    --token "$R2_TOKEN" \
    --max-rows 5000 \
    -v
```

## CLI Usage

### Query logs

```bash
uv run otel-logsearch query "What caused the error spike around 3am?" \
    --table otel.logs \
    --max-rows 5000 \
    --model openrouter/moonshotai/kimi-k2.5
```

Options:
- `--table`, `-t` — Iceberg table name (default: `otel.logs`)
- `--catalog-type` — Catalog type: `sql` or `rest` (default: `sql`)
- `--catalog-uri` — Catalog connection URI (default: `sqlite:///./warehouse.db`)
- `--warehouse` — Warehouse path (default: `./warehouse`)
- `--token` — Auth token for REST catalogs (e.g. Cloudflare R2)
- `--model`, `-m` — LLM model via [litellm](https://docs.litellm.ai/docs/providers) (default: `openrouter/moonshotai/kimi-k2.5`)
- `--max-rows` — Maximum rows to load (default: 50,000)
- `--max-iterations` — Maximum RLM iterations (default: 15)
- `--time-range` — ISO timestamp filter (`start/end`)
- `--verbose`, `-v` — Show RLM iterations (reasoning + code)

### Query a Cloudflare R2 Data Catalog

Logs written to R2 via [otlp2pipeline](https://github.com/smithclay/otlp2pipeline) are stored as Iceberg tables and can be queried directly:

```bash
# The R2 Token needs access to R2 Data Catalog with 'edit' permissions
uv run otel-logsearch query "What services are generating the most errors?" \
    --table default.logs \
    --catalog-type rest \
    --catalog-uri "https://catalog.cloudflarestorage.com/<account-id>/<bucket>" \
    --warehouse "<account-id>_<bucket>" \
    --token "$R2_TOKEN" \
    --max-rows 5000 \
    -v
```

```
╭──────────────────────────────── Answer ────────────────────────────────╮
│ The 'claude-code' service generates errors primarily through its      │
│ Read tool component (17 errors), followed by WebFetch (4 errors),     │
│ with additional API-level errors (14 errors not associated with       │
│ specific tools).                                                      │
╰───────────────────────────────────────────────────────────────────────╯
╭─────────────────────────────── Evidence ───────────────────────────────╮
│ Analysis of 5,000 OpenTelemetry log records revealed:                 │
│                                                                       │
│ 1. All logs originate from service 'claude-code' (5,000 logs)         │
│ 2. Errors identified by success="false" in log_attributes (21 logs)   │
│    and event.name="api_error" (14 logs)                               │
│ 3. Error distribution by component:                                   │
│    - Read tool: 17 errors / 772 calls = 2.20% error rate              │
│    - WebFetch tool: 4 errors / 98 calls = 4.08% error rate            │
│    - API-level errors: 14 errors with no tool association             │
│ 4. 16 other tools (Bash, Edit, Grep, Write, etc.) had 0 errors       │
╰───────────────────────────────────────────────────────────────────────╯
```

The RLM discovered the schema on its own — severity fields were empty in this dataset, so it adapted by finding errors via `success="false"` and `event.name="api_error"` in the JSON-encoded `log_attributes` column.

### List tables

```bash
uv run otel-logsearch tables --namespace otel

# For R2 Data Catalog:
uv run otel-logsearch tables \
    --catalog-type rest \
    --catalog-uri "https://catalog.cloudflarestorage.com/<account-id>/<bucket>" \
    --warehouse "<account-id>_<bucket>" \
    --token "$R2_TOKEN" \
    -n default
```

## Configuration

Settings are loaded from CLI flags, environment variables, or defaults:

| Env var | Description |
|---------|-------------|
| `OPENROUTER_API_KEY` | OpenRouter API key |
| `ANTHROPIC_API_KEY` | Anthropic API key (if using Anthropic models) |
| `OTEL_LOGSEARCH_MODEL` | Default model |
| `OTEL_LOGSEARCH_TABLE` | Default table name |
| `OTEL_LOGSEARCH_CATALOG_URI` | Catalog URI |
| `OTEL_LOGSEARCH_WAREHOUSE` | Warehouse path |
| `OTEL_LOGSEARCH_TOKEN` | Auth token for REST catalogs (e.g. Cloudflare R2) |

## Use as MCP Server

Run as an MCP server with a single `query` tool — no cloning required. Pyodide wheels are auto-downloaded on first startup.

```bash
uvx --from "git+https://github.com/smithclay/otel-rlm-logsearch[mcp]" otel-logsearch-mcp
```

Add to Claude Desktop or Claude Code (`claude mcp add`):

```json
{
  "mcpServers": {
    "otel-logsearch": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/smithclay/otel-rlm-logsearch[mcp]", "otel-logsearch-mcp"],
      "env": {
        "OPENROUTER_API_KEY": "your-key-here",
        "OTEL_LOGSEARCH_CATALOG_URI": "https://catalog.cloudflarestorage.com/<account-id>/<bucket>",
        "OTEL_LOGSEARCH_WAREHOUSE": "<account-id>_<bucket>",
        "OTEL_LOGSEARCH_TABLE": "default.logs",
        "OTEL_LOGSEARCH_TOKEN": "your-r2-token"
      }
    }
  }
}
```

## Log schema

Records follow the [otlp2records](https://github.com/smithclay/otlp2records) schema:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | timestamp(ms, UTC) | Log occurrence time |
| `observed_timestamp` | int64 | Observation time (ms) |
| `trace_id` | string | Hex trace ID |
| `span_id` | string | Hex span ID |
| `service_name` | string | Service name (top-level) |
| `service_namespace` | string | Service namespace |
| `service_instance_id` | string | Instance ID |
| `severity_number` | int32 | Severity (1-24) |
| `severity_text` | string | DEBUG / INFO / WARN / ERROR |
| `body` | string | Log message |
| `resource_attributes` | string | JSON-encoded resource metadata |
| `log_attributes` | string | JSON-encoded log attributes |
| `scope_name` | string | Instrumentation scope |
| `scope_version` | string | Scope version |
| `scope_attributes` | string | JSON-encoded scope metadata |

## Sample data

The `generate_sample_data.py` script creates realistic OTel logs with:

- 5 services: `api-gateway`, `user-service`, `payment-service`, `notification-service`, `auth-service`
- Severity distribution: ~60% INFO, 20% WARN, 15% ERROR, 5% DEBUG
- Correlated trace/span IDs (5-20 log entries per trace)
- Error spike pattern in `payment-service` around hour 3
- Writes to a local SQLite-backed Iceberg catalog

```bash
uv run python scripts/generate_sample_data.py --rows 50000 --hours 48
```

## How the RLM works

The [Recursive Language Model](https://kmad.ai/A-Data-Analysis-Agent-That-Lives-in-Your-Program) pattern differs from typical LLM agents:

- **Direct variable access** — The DataFrame exists natively in the sandbox as a pandas variable, not as serialized text in the prompt
- **Iterative exploration** — The model writes code, sees output, and refines its approach across multiple iterations
- **Type-safe outputs** — The DSPy Signature enforces structured `answer` + `evidence` fields

This project uses a [DSPy fork](https://github.com/kmad/dspy/tree/sandbox-serializable) that adds the `SandboxSerializable` protocol, enabling DataFrames to be serialized as Parquet into the Deno/Pyodide WASM sandbox.
