# mcp-opensearch

FastMCP server that exposes a [smolagents](https://github.com/huggingface/smolagents) `CodeAgent` as a single MCP tool for natural-language OpenSearch queries.

## Requirements

- Python >= 3.10
- Access to an OpenSearch cluster
- An OpenAI-compatible API endpoint (e.g. Claude via Anthropic)

## Installation

```bash
pip install .
```

Or in editable mode for development:

```bash
pip install -e .
```

This installs the `mcp-opensearch` CLI command.

## Configuration

Two configuration files are required before running the server.

### 1. Model settings (`~/.claude/settings.json`)

The server reads model credentials from your Claude Code settings file. Ensure the `env` section contains:

```json
{
  "env": {
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "claude-sonnet-4-6",
    "ANTHROPIC_BASE_URL": "https://api.anthropic.com/v1",
    "ANTHROPIC_AUTH_TOKEN": "<your-api-key>"
  }
}
```

### 2. OpenSearch settings (`~/.config/opensearch/opensearch.json`)

```json
{
  "panda": {
    "host": "<opensearch-host>",
    "port": <opensearch-port>,
    "secret": "<password>"
  }
}
```

The path can be overridden with the `OPENSEARCH_SETTINGS` environment variable:

```bash
export OPENSEARCH_SETTINGS=/path/to/opensearch.json
```

## Usage

### As a standalone server (stdio transport)

```bash
mcp-opensearch
```

### Register with Claude Code

Add to your Claude Code MCP configuration (e.g. `~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "opensearch": {
      "command": "mcp-opensearch"
    }
  }
}
```

Once registered, the `run_query` tool is available. Example queries:

- *"How many finished jobs are there for task 12345?"*
- *"Show me recent failed jobs for user jchiang."*
- *"What jobs ran in the last hour?"*

## MCP tool

| Tool | Description |
|------|-------------|
| `run_query(query: str)` | Run a natural-language query against the OpenSearch agent. Returns a plain-text summary. |
