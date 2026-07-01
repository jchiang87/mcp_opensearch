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
        1. For failed jobs, call `failed_job_log_summaries(job_batch_id)`.
           For retried jobs, call `retried_job_log_summaries(job_batch_id)`.
        2. The returned dict maps bps_job_label to a summary containing:
           - "total_jobs" / "sampled_jobs": job counts
           - "errors": dict keyed by normalized exception type, each with:
             - "count" and "rate": occurrence frequency
             - "examples": list of {"text", "call_chain", "exception_chain"}
               - "call_chain": list of "filename:funcname" for lsst-stack frames
               - "exception_chain": ordered list of exception types/messages
        3. For each bps_job_label, examine the error patterns and report:
           - The dominant error type(s) with count and rate
           - For infrastructure errors (timeouts, I/O, network): identify
             which storage backends or services are affected
           - Use "call_chain" to identify which pipeline component(s) raised
             the error (useful context for where in the code the failure occurs)
           - Use "exception_chain" to trace root causes through chained
             exceptions
        4. IMPORTANT: For scientific or algorithmic failures (e.g. numerical
           errors, insufficient data, degenerate matrices), report the
           exception type and the call_chain entry point, but do NOT make
           algorithmic recommendations. These failures require domain expertise
           in the specific algorithm and its implementation that cannot be
           inferred from log output alone.
        5. Provide a concise summary of failure patterns aggregated by
           bps_job_label, distinguishing infrastructure failures (retriable)
           from scientific/numerical failures (not retriable by infrastructure
           fixes).
        6. IMPORTANT: Always report EVERY bps_job_label present in the
           returned dict. Never silently omit or merge tasks, even if their
           error patterns look similar to another task or their job counts are
           large. Each task must appear in the output with its total_jobs,
           sampled_jobs, and error breakdown.
        """,
        verbosity_level=0,
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
