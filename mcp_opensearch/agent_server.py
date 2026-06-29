"""FastMCP server that exposes a smolagents CodeAgent as a single MCP tool.

The agent has access to an OpenSearch flexible search tool, enabling
natural-language queries about WMS job data stored in OpenSearch.

Run:
    python agent_server.py      # stdio transport

"""
from __future__ import annotations

import os
import json
import logging
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from smolagents import CodeAgent, OpenAIServerModel
from .log_tools import failed_job_log_summaries, retried_job_log_summaries
from .opensearch_tools import (
    AggregationTool,
    FlexibleSearchTool,
    GetIndexInfoTool,
    GetIndexMappingsTool,
)

logging.getLogger("smolagents").setLevel(logging.FATAL)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _get_model() -> OpenAIServerModel:
    settings = json.loads(
        (Path("~/.claude/settings.json").expanduser()).read_text()
    )
    env = settings["env"]
    default_model_id = os.environ.get("ANTHROPIC_DEFAULT_MODEL",
                                      "claude-sonnet-4-6")
    model_id = env.get("ANTHROPIC_DEFAULT_SONNET_MODEL", default_model_id)
    return OpenAIServerModel(
        model_id=model_id,
        api_base=env["ANTHROPIC_BASE_URL"],
        api_key=env["ANTHROPIC_AUTH_TOKEN"],
    )


# ---------------------------------------------------------------------------
# Agent (lazy singleton)
# ---------------------------------------------------------------------------

_agent: CodeAgent | None = None


def _get_agent() -> CodeAgent:
    global _agent
    if _agent is not None:
        return _agent

    tools = [
        failed_job_log_summaries,
        retried_job_log_summaries,
        AggregationTool(),
        FlexibleSearchTool(),
        GetIndexInfoTool(),
        GetIndexMappingsTool(),
    ]

    _agent = CodeAgent(
        model=_get_model(),
        tools=tools,
        additional_authorized_imports=["json", "pandas"],
        name="opensearch_agent",
        description=("Searches and manages WMS job data in an "
                     "OpenSearch cluster."),
        instructions="""
        Search WMS job records in OpenSearch for the user-requested
        information.  For HTCondor jobs, use the htcondor-history-v1
        index, while for PanDA jobs, use the indexes of the form
        panda-<year>-<month>.  Answer concisely with relevant job
        counts, statuses, and summaries.

        When asked to investigate log files for failed or retried jobs,
        perform the following workflow:
        1. For failed jobs, call the tool
           `failed_job_log_summaries(JobBatchId, nsamp=50)`, adjusting
           the value of nsamp, if requested.
        2. For retried jobs, call the tool
           `retried_job_log_summaries(JobBatchId, nsamp=100)`, adjusting
           the value of nsamp, if requested.
        3. The keys of the returned dict will be the bps_job_labels that
           have failed jobsm and the values of the dict will be summaries of
           the error messages extracted from the log files.
        4. For each bps_job_label, examine the error messages and flag any
           patterns.
        5. Provide a concise summary of any failure patterns, aggregated by
           bps_job_label.
        """,
        verbosity_level=1,
    )
    return _agent


# ---------------------------------------------------------------------------
# FastMCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "opensearch-agent",
    instructions=(
        "A CodeAgent that queries WMS job data in OpenSearch. "
        "Pass a natural-language query to run_query and receive a summary."
    ),
)


@mcp.tool()
def run_query(query: str) -> str:
    """Run a natural-language query against the OpenSearch agent.

    The agent has access to OpenSearch tools for searching job metadata,
    fetching logs, and indexing or updating documents.

    Args:
        query: Natural-language question about OpenSearch job data, e.g.
            'How many completed jobs are there for task 12345?',
            'Show me recent failed jobs for user jchiang.',
            'What does the log at /path/to/log.txt say?'
    """
    try:
        result = _get_agent().run(query)
        return str(result)
    except Exception as exc:
        return f"Error: {exc}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Run the agent MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    run()
