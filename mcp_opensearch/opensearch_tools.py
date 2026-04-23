import os
import json
import functools
from pathlib import Path
from typing import Any
from opensearchpy import OpenSearch
from smolagents import Tool


__all__ = (
    "FlexibleSearchTool",
    "GetIndexInfoTool",
    "GetIndexMappingsTool",
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


def _get_index_info(index: str="*") -> list[dict[str, Any]]:
    return OPENSEARCH_CLIENT.cat.indices(index=index, format="json")


class GetIndexInfoTool(Tool):
    name = "get_index_info"
    description = (
        "Get information related to indexes: how much disk space "
        "they are using, how many shards they have, their health status, "
        "and so on."
    )
    inputs = {
        "index": {
            "type": "string",
            "description": ("OpenSearch index to query "
                            "(e.g. 'panda_prod_test-2026-04')."),
            "nullable": True,
        },
    }
    output_type = "array"

    @track_calls("get_index_info")
    def forward(
        self,
        index: str = "*",
    ) -> list[dict[str, Any]]:
        return _get_index_info(index=index)


def _get_index_mappings(index: str="*") -> dict[str: dict[str: dict[str: Any]]]:
    return OPENSEARCH_CLIENT.indices.get_mapping(index=index)


class GetIndexMappingsTool(Tool):
    name = "get_index_mappings"
    description = (
        "Get mappings for one or more indexes, e.g., for properties, "
        "dynamic_templates, date_detection, numeric_detection, and so on."
    )
    inputs = {
        "index": {
            "type": "string",
            "description": ("OpenSearch index to query "
                            "(e.g. 'panda_prod_test-2026-04')."),
            "nullable": True,
        },
    }
    output_type = "object"

    @track_calls("get_index_mappings")
    def forward(
        self,
        index: str = "*",
    ) -> list[dict[str, Any]]:
        return _get_index_mappings(index=index)


def _flexible_search(
    index: str,
    query: dict[str, Any],
    limit: int = 10,
) -> dict[str, Any]:
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
        query: dict[str, Any],
        limit: int = 10,
    ) -> str:
        result = _flexible_search(index, query, limit)
        return json.dumps(result)
