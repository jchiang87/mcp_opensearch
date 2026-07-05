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
from .log_tools import retried_job_log_summaries
from .report_tools import format_log_summary_report, write_report
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


def _get_agent(verbosity_level: int = 0) -> CodeAgent:
    global _agent
    if _agent is not None:
        return _agent

    tools = [
        retried_job_log_summaries,
        format_log_summary_report,
        write_report,
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

        When asked to investigate job logs for a batch, perform the following
        workflow:

        STEP 1 — Fetch the log summary:
          Call `retried_job_log_summaries(job_batch_id)`. This targets jobs
          with NumJobStarts > 1, reading the second-to-last log to capture
          failing retry attempts. It surfaces transient infrastructure failures
          that would be invisible if only finally-failed jobs were examined.
          IMPORTANT: always use this tool for log access — never locate or
          read log files directly via the filesystem.

        STEP 2 — Render the structural skeleton:
          Call `format_log_summary_report(summary, job_batch_id, log_type="retried")`.
          This produces four labelled
          sections: REPORT HEADER, TASKS WITH NO ERRORS, PER-TASK ERROR DATA,
          and SUMMARY TABLE.

        STEP 3 — Assemble the final report in this fixed order:
          a. [paste REPORT HEADER section verbatim]
          b. OVERVIEW — a short paragraph summarising how many tasks were
             analysed, how many had errors, and the top-level picture.
          c. [paste TASKS WITH NO ERRORS section verbatim]
          d. [write a section header called "FAILURE ANALYSES" enclosed with
             "================================================================"
              line delimiters.]
          e. Failure sections — one section per failure category. Standard
             categories are "INFRASTRUCTURE FAILURES (RETRIABLE)" and
             "SCIENTIFIC / ALGORITHMIC FAILURES (NOT RETRIABLE)". Add extra
             categories for anomalies that don't fit either (e.g.
             "CONFIGURATION ERRORS", "UNCLASSIFIED FAILURES"). For each
             category:
             - Write a brief narrative: root cause, affected services or
               components, and whether the pattern is novel or unexpected.
             - For infrastructure failures, note the error time range and flag
               it as useful for correlating with known incidents or maintenance
               windows. If timestamps are absent, note that timing is
               unavailable.
             - For scientific/algorithmic failures, report the exception type
               and call_chain entry point but do NOT make algorithmic
               recommendations — these require domain expertise beyond what
               log output alone can provide.
             - For each affected task, paste its per-task block verbatim from
               the PER-TASK ERROR DATA section.
             - For each error type, provide the file path to the example error
               log file.
             - If an error's host_counts show that failures are concentrated on
               one or a few nodes (e.g. >50% of errors from a single host),
               flag this as a potential bad-node issue worth investigating.
             - If a task's per-task block shows non-zero signal kills or memory
               pressure, include a "RESOURCE EXHAUSTION" section for that task.
          f. [paste SUMMARY TABLE section verbatim]
          g. KEY FINDINGS — bullet list of the most important observations,
             distinguishing retriable from non-retriable failures and flagging
             any anomalies.

        STEP 4 — Write the report to a file:
          Call `write_report(report_text=<assembled_report>, job_batch_id=<id>)`.
          This writes the file and returns its absolute path. Confirm the path
          in your final response.

        IMPORTANT — coverage and fidelity:
          - Every bps_job_label in "tasks" must appear in the report. Never
            silently omit or merge tasks, even if their error patterns look
            similar or their job counts are large.
          - Paste the formatter's sections verbatim; do not reformat or
            summarise them. The consistent layout is intentional.
        """,
        verbosity_level=verbosity_level,
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
