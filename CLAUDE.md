# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

`mcp-opensearch` is a FastMCP server that wraps a `smolagents` `CodeAgent` as a single MCP tool. It enables natural-language queries against an OpenSearch cluster (specifically WMS job data) and HTCondor job logs, bridging Claude's MCP protocol to OpenSearch via a multi-step reasoning agent.

## Installation & Running

```bash
pip install -e .          # editable install
pip install -e ".[dev]"   # editable install with test dependencies
mcp-opensearch            # run the server directly
```

Run tests with:
```bash
pytest tests/
```

## Configuration

Two config sources are required at runtime:

1. **Claude API** — read from `~/.claude/settings.json` (`ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`). Model resolution order: (1) `ANTHROPIC_DEFAULT_MODEL` env var, (2) `claude-sonnet-4-6` hardcoded default, (3) `ANTHROPIC_DEFAULT_SONNET_MODEL` from `~/.claude/settings.json` env block (overrides step 2 if set).

2. **OpenSearch credentials** — read from `~/.config/opensearch/opensearch.json` by default. Override path with `OPENSEARCH_SETTINGS` env var.

## Architecture

```
MCP Client (e.g. Claude Code)
    │
    └── run_query(query: str)           ← single MCP tool (agent_server.py)
            │
            └── CodeAgent (smolagents) ← lazy-loaded singleton, uses Claude as LLM
                    │
                    ├── failed_job_log_summaries  ← error summaries from HTCondor logs (ExitCode != 0)
                    ├── retried_job_log_summaries ← error summaries from retried job logs (NumJobStarts > 1)
                    ├── AggregationTool           ← date_histogram, terms, stats, and other aggs
                    ├── FlexibleSearchTool        ← exact/terms/range queries against OpenSearch
                    ├── GetIndexInfoTool          ← disk usage, shard count, health status
                    └── GetIndexMappingsTool      ← index field mappings
```

**`mcp_opensearch/agent_server.py`** — FastMCP server setup, lazy-loads the `CodeAgent` singleton on first call, defines the `run_query` MCP tool.

**`mcp_opensearch/opensearch_tools.py`** — Four `smolagents` Tool subclasses. `FlexibleSearchTool` builds DSL queries supporting exact match, set membership (`terms`), and range filters combined with AND logic. `AggregationTool` runs OpenSearch aggregation queries (e.g. `date_histogram`, `terms`, `stats`) with `size=0`. All tool calls are tracked via a decorator that appends counts to `tool_call_counts.json` in the server's working directory (`Path.cwd()` at import time).

**`mcp_opensearch/log_tools.py`** — Two `smolagents` `@tool`-decorated functions for HTCondor log analysis. Uses `htc_job_history.get_os_job_info()` to query OpenSearch for all matching jobs (no sampling), then streams every log file to extract and normalize exception types. Results are tabulated by `bps_job_label` with occurrence counts, rates, and representative traceback examples.

**`mcp_opensearch/utils.py`** — `track_calls(tool_name)` decorator factory. Increments per-tool call counts in `tool_call_counts.json` using an atomic write (rename from `.tmp`). Handles corrupt JSON gracefully.

## Tool Details

### failed_job_log_summaries — `(job_batch_id, index='htcondor-history-v1', max_examples=3, max_jobs=500)`

Reads up to `max_jobs` log files per `bps_job_label` (random sample when exceeded), groups errors by normalized exception type, and returns tabulated counts with examples. Returns `dict[bps_job_label -> {"total_jobs": int, "sampled_jobs": int, "errors": {error_key: {"count": int, "rate": float, "examples": list[str]}}}]` where `rate = count / sampled_jobs`.

### retried_job_log_summaries — `(job_batch_id, index='htcondor-history-v1', max_examples=3, max_jobs=500)`

Same structure as above but filters for `NumJobStarts > 1` and reads the second-to-last log file per job (`last_log_index=-2`) to capture the failing retry attempt.

### Sampling and log parsing internals (`log_tools.py`)

Each failed quantum generates two ERROR blocks in the log file:
1. `single_quantum_executor.py:316` — single-line with inline `Exception ExcType: msg`
2. `mp_graph_executor.py:720` — notification + full chained traceback

`_extract_errors()` streams the file, capping each ERROR block at `_MAX_BLOCK_LINES=200` lines. `_parse_block()` extracts the exception type from the inline form first, falling back to the last exception-shaped line in the traceback. Both blocks produce the same normalized key, so `job_log_summaries` deduplicates per file by keeping only the longest block (the full traceback) as the example and counting each key once per job.

Exception message normalization strips UUIDs, absolute paths, bare integers, and LSST Python class paths (`lsst.x.y.ClassName` → `<lsst_class>`), then truncates to 200 chars. `MPGraphExecutorError` (the outermost wrapper) is always discarded.

### AggregationTool — `run_aggregation(index, aggs, query=None)`

Runs an OpenSearch aggregation with `size=0` (no raw hits returned). `aggs` is a raw OpenSearch aggregation DSL dict. `query` is an optional filter using the same format as `FlexibleSearchTool`; omitting it aggregates over all documents. Returns `{"aggregations": {...}, "error": ""}`.

Example `aggs` for jobs per day:
```python
{"jobs_per_day": {"date_histogram": {"field": "creationtime", "calendar_interval": "day"}}}
```

### FlexibleSearchTool — `flexible_search(index, query, limit=10, scan=False)`

`query` is a dict where each key is a field name and the value determines the clause type:

| Value type | DSL clause |
|---|---|
| `str` / `int` / `float` | `{"match": {field: value}}` |
| `list` | `{"terms": {field: [...]}}` |
| `dict` with any of `gte`/`lte`/`gt`/`lt` | `{"range": {field: {...}}}` |

All clauses are combined with `bool.must` (AND). Returns `{"result": [...], "total": N, "error": ""}`.

When `scan=True`, uses `opensearchpy.helpers.scan` (scroll API) to page through result sets larger than 10,000 hits. `limit` still caps the total returned.

### GetIndexInfoTool — `get_index_info(index="*")`

Returns disk usage, shard count, and health status via `cat.indices()`.

### GetIndexMappingsTool — `get_index_mappings(index="*")`

Returns the full mappings object for one or more indexes.

## OpenSearch Client

`get_opensearch_client(wms="panda")` in `opensearch_tools.py` initializes the client at module import time using credentials from the settings file. The `wms` parameter selects the credentials key (defaults to `"panda"`). HTTP compression and SSL are enabled; hostname verification is relaxed (`ssl_assert_hostname=False`).

## Agent Instructions

The `CodeAgent` is initialized with:
- **Authorized imports**: `json`, `pandas`
- **Instructions**: directs the agent to use `htcondor-history-v1` for HTCondor jobs and `panda-<year>-<month>` indexes for PanDA jobs. For log investigation, the agent is instructed to call `failed_job_log_summaries` or `retried_job_log_summaries`, then examine the returned error messages per `bps_job_label` and report patterns.
- `run_query` catches all exceptions and returns them as `"Error: ..."` strings rather than raising.

## Tests

Unit tests live in `tests/test_opensearch_tools.py` (34 tests). They cover DSL building, scan/non-scan execution paths, aggregation query construction, the `track_calls` decorator, and the `Tool.forward` wrappers. All external calls are mocked via `unittest.mock.patch` — no live OpenSearch connection is needed.

## Requirements

- Python >= 3.10
- Runtime dependencies: `mcp`, `smolagents`, `opensearch-py`, `htc_job_history`, `numpy`
- Dev dependencies: `pytest` (install with `pip install -e ".[dev]"`)

## MCP Registration

To use as a Claude Code MCP server, add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "mcp-opensearch": {
      "command": "mcp-opensearch"
    }
  }
}
```
