import os
import json
import functools
from pathlib import Path
from typing import Any
from opensearchpy import OpenSearch, helpers
from smolagents import Tool


__all__ = (
    "AggregationTool",
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
    scan: bool = False,
) -> dict[str, Any]:
    """Helper for flexible OpenSearch queries supporting
    exact match, set membership, and range filters.

    Each key in `query` maps to a field name. The value determines the
    clause type:
      - str/int/float  -> exact match:     {"match": {field: value}}
      - list           -> set membership:  {"terms": {field: [...]}}
      - dict with any of gte/lte/gt/lt keys -> range: {"range": {field: {...}}}

    When scan=True, uses the scroll API via helpers.scan to page through
    results beyond the 10,000-hit OpenSearch limit. limit still caps the
    total returned.
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
        if scan:
            results = []
            for hit in helpers.scan(
                OPENSEARCH_CLIENT,
                query=query_body,
                index=index,
                size=1000,
            ):
                results.append(hit.get("_source", {}))
                if len(results) >= limit:
                    break
        else:
            response = OPENSEARCH_CLIENT.search(
                index=index,
                body=query_body,
                size=limit,
            )
            results = [hit.get("_source", {}) for hit in
                       response.get("hits", {}).get("hits", [])]
    except Exception as e:
        return {
            "result": [],
            "total": 0,
            "error": f"OpenSearch Error: {type(e).__name__} - {e}"
        }

    return {"result": results, "total": len(results), "error": ""}


def _run_aggregation(
    index: str,
    aggs: dict[str, Any],
    query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    RANGE_KEYS = {"gte", "lte", "gt", "lt"}
    if query:
        must_clauses = []
        for field, value in query.items():
            if isinstance(value, list):
                must_clauses.append({"terms": {field: value}})
            elif isinstance(value, dict) and RANGE_KEYS.intersection(value.keys()):
                must_clauses.append({"range": {field: value}})
            else:
                must_clauses.append({"match": {field: value}})
        query_block = {"bool": {"must": must_clauses}}
    else:
        query_block = {"match_all": {}}

    body = {"size": 0, "query": query_block, "aggs": aggs}

    try:
        response = OPENSEARCH_CLIENT.search(index=index, body=body)
    except Exception as e:
        return {"aggregations": {}, "error": f"OpenSearch Error: {type(e).__name__} - {e}"}

    return {"aggregations": response.get("aggregations", {}), "error": ""}


class AggregationTool(Tool):
    name = "run_aggregation"
    description = (
        "Run an OpenSearch aggregation query (e.g. date_histogram, terms, stats, "
        "cardinality) against an index. Uses size=0 so no raw documents are returned — "
        "only the aggregation results. Optionally filter the documents included in the "
        "aggregation using the same query format as flexible_search."
    )
    inputs = {
        "index": {
            "type": "string",
            "description": "OpenSearch index to query (e.g. 'panda_prod_test-2026-04').",
        },
        "aggs": {
            "type": "object",
            "description": (
                "OpenSearch aggregation DSL as a dict. "
                "Example for jobs per day: "
                "{\"jobs_per_day\": {\"date_histogram\": {\"field\": \"creationtime\", "
                "\"calendar_interval\": \"day\"}}}."
            ),
        },
        "query": {
            "type": "object",
            "description": (
                "Optional filter using the same format as flexible_search: "
                "scalar → exact match, list → terms, dict with gte/lte/gt/lt → range. "
                "If omitted, aggregates over all documents in the index."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    @track_calls("run_aggregation")
    def forward(
        self,
        index: str,
        aggs: dict[str, Any],
        query: dict[str, Any] | None = None,
    ) -> str:
        result = _run_aggregation(index, aggs, query)
        return json.dumps(result)


class FlexibleSearchTool(Tool):
    name = "flexible_search"
    description = (
        "Perform a flexible OpenSearch search with exact match, "
        "set membership, and range queries combined with AND logic. "
        "Returns a JSON object with 'result' (list of documents), 'total', "
        "and 'error'. Set scan=True to page through more than 10,000 results "
        "using the scroll API."
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
        "scan": {
            "type": "boolean",
            "description": (
                "If True, use the scroll API to retrieve more than 10,000 "
                "results. limit still caps the total returned. "
                "Default is False."
            ),
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
        scan: bool = False,
    ) -> str:
        result = _flexible_search(index, query, limit, scan)
        return json.dumps(result)
