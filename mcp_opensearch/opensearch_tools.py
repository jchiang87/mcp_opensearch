import os
import json
import functools
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from opensearchpy import OpenSearch
from smolagents import Tool


__all__ = (
    "FlexibleSearchTool",
)


STATS_FILE = Path.cwd() / "tool_call_counts.json"


def track_calls(tool_name: str):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                counts = (json.loads(STATS_FILE.read_text())
                          if STATS_FILE.exists() else {})
            except (json.JSONDecodeError, OSError):
                counts = {}
            counts[tool_name] = counts.get(tool_name, 0) + 1
            tmp = STATS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(counts, indent=2))
            os.replace(tmp, STATS_FILE)
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def get_opensearch_client(wms="panda"):
    settings_file = Path(
        os.environ.get(
            "OPENSEARCH_SETTINGS",
            "~/.config/opensearch/opensearch.json"
        )
    )
    settings = json.loads((settings_file.expanduser()).read_text())
    config = settings[wms.lower()]
    return OpenSearch(hosts=[{'host': config["host"], 'port': config["port"]}],
                      http_compress=True, http_auth=(wms, config["secret"]),
                      use_ssl=True, verify_certs=True,
                      ssl_assert_hostname=False, ssl_show_warn=False)

OPENSEARCH_CLIENT = get_opensearch_client()


def _flexible_search(
    index: str,
    query: Dict[str, Any],
    limit: int = 10,
) -> Dict[str, Any]:
    """Helper for flexible OpenSearch queries supporting
    exact match, set membership, and range filters.

    Each key in `query` maps to a field name. The value determines the
    clause type:
      - str/int/float  -> exact match:     {"match": {field: value}}
      - list           -> set membership:  {"terms": {field: [...]}}
      - dict with any of gte/lte/gt/lt keys -> range: {"range": {field: {...}}}
    """
    if not query:
        return {
            "result": [],
            "total": 0,
            "error": "query must contain at least one filter."
        }

    RANGE_KEYS = {"gte", "lte", "gt", "lt"}
    must_clauses = []
    for field, value in query.items():
        if isinstance(value, list):
            must_clauses.append({"terms": {field: value}})
        elif isinstance(value, dict) and RANGE_KEYS.intersection(value.keys()):
            must_clauses.append({"range": {field: value}})
        else:
            must_clauses.append({"match": {field: value}})

    query_body = {"query": {"bool": {"must": must_clauses}}}

    try:
        response = OPENSEARCH_CLIENT.search(
            index=index,
            body=query_body,
            size=limit
        )
    except Exception as e:
        return {
            "result": [],
            "total": 0,
            "error": f"OpenSearch Error: {type(e).__name__} - {e}"
        }

    results = [hit.get("_source", {}) for hit in
               response.get("hits", {}).get("hits", [])]

    return {"result": results, "total": len(results), "error": ""}


class FlexibleSearchTool(Tool):
    name = "flexible_search"
    description = (
        "Perform a flexible OpenSearch search with exact match, "
        "set membership, and range queries combined with AND logic. "
        "Returns a JSON object with 'result' (list of documents), 'total', "
        "and 'error'."
    )
    inputs = {
        "index": {
            "type": "string",
            "description": ("OpenSearch index to query "
                            "(e.g. 'panda_prod_test-2026-04')."),
        },
        "query": {
            "type": "object",
            "description": (
                "Filter clauses as a dict. Scalar value → exact match; "
                "list → set membership (terms); "
                "dict with gte/lte/gt/lt → range. "
                "Example: {\"jobstatus\": \"finished\","
                "\"starttime\": {\"gte\": \"2026-04-14\"}}."
            ),
        },
        "limit": {
            "type": "integer",
            "description": ("Maximum number of results to return. "
                            "Default is 10."),
            "nullable": True,
        },
    }
    output_type = "string"

    @track_calls("flexible_search")
    def forward(
        self,
        index: str,
        query: Dict[str, Any],
        limit: int = 10,
    ) -> str:
        result = _flexible_search(index, query, limit)
        return json.dumps(result)
