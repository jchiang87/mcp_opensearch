import json
import pytest
from unittest.mock import MagicMock, patch

import mcp_opensearch.opensearch_tools as ost


def make_search_response(docs):
    return {
        "hits": {
            "hits": [{"_source": d} for d in docs],
            "total": {"value": len(docs)},
        }
    }


# ---------------------------------------------------------------------------
# _flexible_search — DSL building
# ---------------------------------------------------------------------------

class TestFlexibleSearchDSL:
    def _must_clauses(self, query, limit=10):
        mock_client = MagicMock()
        mock_client.search.return_value = make_search_response([])
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            ost._flexible_search("test-index", query, limit=limit)
        return mock_client.search.call_args[1]["body"]["query"]["bool"]["must"]

    def test_scalar_produces_match_clause(self):
        must = self._must_clauses({"jobstatus": "finished"})
        assert {"match": {"jobstatus": "finished"}} in must

    def test_int_scalar_produces_match_clause(self):
        must = self._must_clauses({"pandaid": 12345})
        assert {"match": {"pandaid": 12345}} in must

    def test_list_produces_terms_clause(self):
        must = self._must_clauses({"computingsite": ["SLAC", "LANCS"]})
        assert {"terms": {"computingsite": ["SLAC", "LANCS"]}} in must

    def test_dict_with_gte_lte_produces_range_clause(self):
        must = self._must_clauses({"creationtime": {"gte": "2026-04-13", "lte": "2026-04-30"}})
        assert {"range": {"creationtime": {"gte": "2026-04-13", "lte": "2026-04-30"}}} in must

    def test_dict_with_gt_lt_produces_range_clause(self):
        must = self._must_clauses({"cpuconsumptiontime": {"gt": 0, "lt": 3600}})
        assert {"range": {"cpuconsumptiontime": {"gt": 0, "lt": 3600}}} in must

    def test_mixed_query_produces_all_clauses(self):
        must = self._must_clauses({
            "jobstatus": "finished",
            "computingsite": ["SLAC", "LANCS"],
            "creationtime": {"gte": "2026-04-13"},
        })
        assert len(must) == 3

    def test_limit_passed_to_search(self):
        mock_client = MagicMock()
        mock_client.search.return_value = make_search_response([])
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            ost._flexible_search("test-index", {"jobstatus": "finished"}, limit=42)
        assert mock_client.search.call_args[1]["size"] == 42

    def test_empty_query_returns_error_without_calling_opensearch(self):
        mock_client = MagicMock()
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            result = ost._flexible_search("test-index", {})
        assert result["error"] != ""
        assert result["total"] == 0
        mock_client.search.assert_not_called()


# ---------------------------------------------------------------------------
# _flexible_search — execution paths
# ---------------------------------------------------------------------------

class TestFlexibleSearchExecution:
    def test_returns_sources_from_hits(self):
        docs = [{"pandaid": 1}, {"pandaid": 2}]
        mock_client = MagicMock()
        mock_client.search.return_value = make_search_response(docs)
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            result = ost._flexible_search("idx", {"jobstatus": "finished"})
        assert result["result"] == docs
        assert result["total"] == 2
        assert result["error"] == ""

    def test_opensearch_exception_returns_error(self):
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("connection refused")
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            result = ost._flexible_search("idx", {"jobstatus": "finished"})
        assert result["error"].startswith("OpenSearch Error:")
        assert result["result"] == []
        assert result["total"] == 0

    def test_scan_calls_helpers_scan_not_search(self):
        docs = [{"pandaid": i} for i in range(5)]
        mock_helpers = MagicMock()
        mock_helpers.scan.return_value = iter({"_source": d} for d in docs)
        mock_client = MagicMock()
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client), \
             patch.object(ost, "helpers", mock_helpers):
            result = ost._flexible_search("idx", {"jobstatus": "finished"}, limit=10, scan=True)
        mock_helpers.scan.assert_called_once()
        mock_client.search.assert_not_called()
        assert result["total"] == 5

    def test_scan_passes_query_body_and_index(self):
        mock_helpers = MagicMock()
        mock_helpers.scan.return_value = iter([])
        with patch.object(ost, "OPENSEARCH_CLIENT", MagicMock()), \
             patch.object(ost, "helpers", mock_helpers):
            ost._flexible_search("my-index", {"jobstatus": "finished"}, scan=True)
        call_kwargs = mock_helpers.scan.call_args
        assert call_kwargs[1]["index"] == "my-index"
        body = call_kwargs[1]["query"]
        assert "bool" in body["query"]

    def test_scan_respects_limit(self):
        docs = [{"pandaid": i} for i in range(20)]
        mock_helpers = MagicMock()
        mock_helpers.scan.return_value = iter({"_source": d} for d in docs)
        with patch.object(ost, "OPENSEARCH_CLIENT", MagicMock()), \
             patch.object(ost, "helpers", mock_helpers):
            result = ost._flexible_search("idx", {"jobstatus": "finished"}, limit=7, scan=True)
        assert result["total"] == 7

    def test_scan_exception_returns_error(self):
        mock_helpers = MagicMock()
        mock_helpers.scan.side_effect = Exception("scroll error")
        with patch.object(ost, "OPENSEARCH_CLIENT", MagicMock()), \
             patch.object(ost, "helpers", mock_helpers):
            result = ost._flexible_search("idx", {"jobstatus": "finished"}, scan=True)
        assert result["error"].startswith("OpenSearch Error:")
        assert result["result"] == []


# ---------------------------------------------------------------------------
# _run_aggregation
# ---------------------------------------------------------------------------

SAMPLE_AGGS = {
    "jobs_per_day": {
        "date_histogram": {"field": "creationtime", "calendar_interval": "day"}
    }
}


class TestRunAggregation:
    def _call(self, query=None, aggs_response=None):
        mock_client = MagicMock()
        mock_client.search.return_value = {"aggregations": aggs_response or {}}
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            result = ost._run_aggregation("test-index", SAMPLE_AGGS, query)
        return mock_client, result

    def test_no_query_uses_match_all(self):
        client, _ = self._call()
        body = client.search.call_args[1]["body"]
        assert body["query"] == {"match_all": {}}

    def test_size_is_zero(self):
        client, _ = self._call()
        body = client.search.call_args[1]["body"]
        assert body["size"] == 0

    def test_aggs_block_passed_through(self):
        client, _ = self._call()
        body = client.search.call_args[1]["body"]
        assert body["aggs"] == SAMPLE_AGGS

    def test_with_scalar_query_builds_match_filter(self):
        client, _ = self._call(query={"jobstatus": "finished"})
        body = client.search.call_args[1]["body"]
        assert body["query"] == {"bool": {"must": [{"match": {"jobstatus": "finished"}}]}}

    def test_with_list_query_builds_terms_filter(self):
        client, _ = self._call(query={"computingsite": ["SLAC", "LANCS"]})
        body = client.search.call_args[1]["body"]
        assert {"terms": {"computingsite": ["SLAC", "LANCS"]}} in body["query"]["bool"]["must"]

    def test_with_range_query_builds_range_filter(self):
        client, _ = self._call(query={"creationtime": {"gte": "2026-04-13"}})
        body = client.search.call_args[1]["body"]
        assert {"range": {"creationtime": {"gte": "2026-04-13"}}} in body["query"]["bool"]["must"]

    def test_returns_aggregations(self):
        buckets = {"buckets": [{"key_as_string": "2026-04-13", "doc_count": 100}]}
        _, result = self._call(aggs_response={"jobs_per_day": buckets})
        assert result["aggregations"]["jobs_per_day"] == buckets
        assert result["error"] == ""

    def test_exception_returns_error(self):
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("timeout")
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client):
            result = ost._run_aggregation("idx", SAMPLE_AGGS)
        assert result["error"].startswith("OpenSearch Error:")
        assert result["aggregations"] == {}


# ---------------------------------------------------------------------------
# track_calls decorator
# ---------------------------------------------------------------------------

class TestTrackCalls:
    def test_creates_stats_file_on_first_call(self, tmp_path):
        stats_file = tmp_path / "counts.json"
        with patch.object(ost, "STATS_FILE", stats_file):
            @ost.track_calls("mytool")
            def noop():
                return 42
            noop()
        data = json.loads(stats_file.read_text())
        assert data["mytool"] == 1

    def test_increments_on_subsequent_calls(self, tmp_path):
        stats_file = tmp_path / "counts.json"
        with patch.object(ost, "STATS_FILE", stats_file):
            @ost.track_calls("mytool")
            def noop():
                return 42
            noop()
            noop()
            noop()
        data = json.loads(stats_file.read_text())
        assert data["mytool"] == 3

    def test_multiple_tools_tracked_independently(self, tmp_path):
        stats_file = tmp_path / "counts.json"
        with patch.object(ost, "STATS_FILE", stats_file):
            @ost.track_calls("tool_a")
            def a():
                pass

            @ost.track_calls("tool_b")
            def b():
                pass

            a()
            a()
            b()
        data = json.loads(stats_file.read_text())
        assert data["tool_a"] == 2
        assert data["tool_b"] == 1

    def test_handles_corrupt_json_gracefully(self, tmp_path):
        stats_file = tmp_path / "counts.json"
        stats_file.write_text("not valid json{{{")
        with patch.object(ost, "STATS_FILE", stats_file):
            @ost.track_calls("mytool")
            def noop():
                pass
            noop()
        data = json.loads(stats_file.read_text())
        assert data["mytool"] == 1

    def test_preserves_wrapped_function_return_value(self, tmp_path):
        stats_file = tmp_path / "counts.json"
        with patch.object(ost, "STATS_FILE", stats_file):
            @ost.track_calls("mytool")
            def returns_value():
                return {"answer": 42}
            assert returns_value() == {"answer": 42}

    def test_preserves_function_name(self):
        @ost.track_calls("mytool")
        def my_original_function():
            pass
        assert my_original_function.__name__ == "my_original_function"


# ---------------------------------------------------------------------------
# FlexibleSearchTool.forward
# ---------------------------------------------------------------------------

class TestFlexibleSearchToolForward:
    def test_returns_json_string(self, tmp_path):
        docs = [{"pandaid": 1}]
        mock_client = MagicMock()
        mock_client.search.return_value = make_search_response(docs)
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            output = ost.FlexibleSearchTool().forward("idx", {"jobstatus": "finished"})
        parsed = json.loads(output)
        assert parsed["result"] == docs
        assert parsed["total"] == 1

    def test_scan_parameter_forwarded(self, tmp_path):
        mock_internal = MagicMock(return_value={"result": [], "total": 0, "error": ""})
        with patch.object(ost, "_flexible_search", mock_internal), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            ost.FlexibleSearchTool().forward("idx", {"jobstatus": "finished"}, limit=5, scan=True)
        mock_internal.assert_called_once_with("idx", {"jobstatus": "finished"}, 5, True)

    def test_error_surfaced_in_json_output(self, tmp_path):
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("boom")
        with patch.object(ost, "OPENSEARCH_CLIENT", mock_client), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            output = ost.FlexibleSearchTool().forward("idx", {"jobstatus": "finished"})
        parsed = json.loads(output)
        assert "OpenSearch Error:" in parsed["error"]


# ---------------------------------------------------------------------------
# AggregationTool.forward
# ---------------------------------------------------------------------------

class TestAggregationToolForward:
    def test_returns_json_string(self, tmp_path):
        mock_internal = MagicMock(return_value={"aggregations": {}, "error": ""})
        with patch.object(ost, "_run_aggregation", mock_internal), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            output = ost.AggregationTool().forward("idx", SAMPLE_AGGS)
        assert isinstance(output, str)
        json.loads(output)

    def test_query_parameter_forwarded(self, tmp_path):
        mock_internal = MagicMock(return_value={"aggregations": {}, "error": ""})
        with patch.object(ost, "_run_aggregation", mock_internal), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            ost.AggregationTool().forward("idx", SAMPLE_AGGS, query={"jobstatus": "finished"})
        mock_internal.assert_called_once_with("idx", SAMPLE_AGGS, {"jobstatus": "finished"})

    def test_no_query_defaults_to_none(self, tmp_path):
        mock_internal = MagicMock(return_value={"aggregations": {}, "error": ""})
        with patch.object(ost, "_run_aggregation", mock_internal), \
             patch.object(ost, "STATS_FILE", tmp_path / "counts.json"):
            ost.AggregationTool().forward("idx", SAMPLE_AGGS)
        mock_internal.assert_called_once_with("idx", SAMPLE_AGGS, None)
