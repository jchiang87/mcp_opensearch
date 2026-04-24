# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

`mcp-opensearch` is a FastMCP server that wraps a `smolagents` `CodeAgent` as a single MCP tool. It enables natural-language queries against an OpenSearch cluster (specifically WMS job data), bridging Claude's MCP protocol to OpenSearch via a multi-step reasoning agent.

## Installation & Running

```bash
pip install -e .          # editable install
mcp-opensearch            # run the server directly
```

There are no tests or linting configurations in this project.

## Configuration

Two config sources are required at runtime:

1. **Claude API** — read from `~/.claude/settings.json` (`ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`). Model defaults to `claude-sonnet-4-6` but can be overridden with `ANTHROPIC_DEFAULT_SONNET_MODEL` or `ANTHROPIC_DEFAULT_MODEL`.

2. **OpenSearch credentials** — read from `~/.config/opensearch/opensearch.json` by default. Override path with `OPENSEARCH_SETTINGS` env var.

## Architecture

```
MCP Client (e.g. Claude Code)
    │
    └── run_query(query: str)           ← single MCP tool (agent_server.py)
            │
            └── CodeAgent (smolagents) ← lazy-loaded singleton, uses Claude as LLM
                    │
                    ├── FlexibleSearchTool   ← exact/terms/range queries against OpenSearch
                    ├── GetIndexInfoTool     ← disk usage, shard count, health status
                    └── GetIndexMappingsTool ← index field mappings
```

**`mcp_opensearch/agent_server.py`** — FastMCP server setup, lazy-loads the `CodeAgent` singleton on first call, defines the `run_query` MCP tool.

**`mcp_opensearch/opensearch_tools.py`** — Three `smolagents` Tool subclasses. `FlexibleSearchTool` builds DSL queries supporting exact match, set membership (`terms`), and range filters combined with AND logic. All tool calls are tracked via a decorator that appends counts to `work/tool_call_counts.json`.

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
