import pandas as pd
import pytest
from unittest.mock import patch

import mcp_opensearch.log_tools as lt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_df(rows, filter_col='ExitCode', filter_val=1):
    """Return a DataFrame with all columns needed by job_log_summaries."""
    df = pd.DataFrame(rows, columns=['bps_job_label', 'Err', 'Iwd'])
    df[filter_col] = filter_val
    return df


def write_log(tmp_path, filename, content):
    path = tmp_path / filename
    path.write_text(content)
    return filename


# Representative log snippets used across several tests.
LOG_SINGLE_INLINE = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.single_quantum_executor (task)(executor.py:316) - Execution of 'task' failed. Exception ValueError: connection timed out
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

LOG_TRACEBACK_BLOCK = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.mp_graph_executor (task)(mp_graph_executor.py:720) - quantum failed; processing will continue
Traceback (most recent call last):
  File "foo.py", line 1, in bar
    raise ValueError("connection timed out")
ValueError: connection timed out
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

LOG_RUNTIME_ERROR = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.mp_graph_executor (task)(mp_graph_executor.py:720) - quantum failed; processing will continue
Traceback (most recent call last):
  File "serialize.py", line 1, in write
    raise RuntimeError("Failed to serialize dataset")
RuntimeError: Failed to serialize dataset
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

LOG_MPGRAPH_WRAPPER = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.daf.butler.cli.utils ()(utils.py:218) - Caught an exception, details are in traceback:
Traceback (most recent call last):
  File "commands.py", line 1, in run
    executor.execute()
lsst.pipe.base.mp_graph_executor.MPGraphExecutorError: One or more tasks failed during execution.
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

# Mirrors the real two-block pattern: one inline ERROR + one traceback ERROR,
# both carrying the same exception, as emitted for each failed quantum.
LOG_TWO_BLOCKS_SAME_KEY = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.single_quantum_executor (task)(executor.py:316) - Execution of 'task' failed. Exception LinAlgError: matrix is not positive definite
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.mp_graph_executor (task)(mp_graph_executor.py:720) - quantum failed; processing will continue
Traceback (most recent call last):
  File "cholesky.py", line 1, in decompose
    raise LinAlgError("matrix is not positive definite")
LinAlgError: matrix is not positive definite
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

# Traceback with /lsst/ frames — tests call_chain extraction.
LOG_WITH_LSST_FRAMES = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.mp_graph_executor (task)(mp_graph_executor.py:720) - quantum failed; processing will continue
Traceback (most recent call last):
  File "/lsst/pipe/base/task.py", line 42, in runQuantum
    self.run(**inputs)
  File "/lsst/pipe/tasks/fit_turbulence.py", line 10, in run
    result = do_fit(data)
  File "/external/lib/numpy/linalg.py", line 1, in cholesky
    raise LinAlgError("not positive definite")
LinAlgError: not positive definite
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""

# Chained exception (two tracebacks) — tests exception_chain extraction.
LOG_CHAINED_EXCEPTION = """\
INFO 2026-06-24T09:00:00+01:00 lsst.pipe start
ERROR 2026-06-24T09:01:00+01:00 lsst.pipe.base.mp_graph_executor (task)(mp_graph_executor.py:720) - quantum failed; processing will continue
Traceback (most recent call last):
  File "foo.py", line 1, in connect
    socket.connect()
OSError: connection refused
During handling of the above exception, another exception occurred:
Traceback (most recent call last):
  File "foo.py", line 2, in retry
    raise RuntimeError("retry exhausted") from exc
RuntimeError: retry exhausted
INFO 2026-06-24T09:01:01+01:00 lsst.pipe end
"""


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_uuid_replaced(self):
        result = lt._normalize("dataset 019ee07a-bd29-7bde-9f28-d2c29780e5ec failed")
        assert '<uuid>' in result
        assert '019ee07a' not in result

    def test_lsst_class_replaced(self):
        result = lt._normalize("formatter lsst.obs.base.formatters.FitsFormatter failed")
        assert '<lsst_class>' in result
        assert 'lsst.obs' not in result

    def test_absolute_path_replaced(self):
        result = lt._normalize("reading /cvmfs/sw.lsst/some/path.fits failed")
        assert '<path>' in result
        assert '/cvmfs' not in result

    def test_bare_integers_replaced(self):
        result = lt._normalize("1457-th leading minor is not positive definite")
        assert '<N>' in result
        assert '1457' not in result

    def test_truncated_at_key_max_len(self):
        long_msg = "x" * (lt._KEY_MAX_LEN + 50)
        result = lt._normalize(long_msg)
        assert len(result) == lt._KEY_MAX_LEN

    def test_multiple_patterns_in_one_message(self):
        msg = (
            "Failure for dataset 019ee07a-bd29-7bde-9f28-d2c29780e5ec "
            "from lsst.obs.base.Formatter at /path/to/file.fits "
            "on port 1095: Read timed out"
        )
        result = lt._normalize(msg)
        assert '019ee07a' not in result
        assert 'lsst.obs' not in result
        assert '/path' not in result
        assert '1095' not in result

    def test_plain_message_unchanged_except_numbers(self):
        result = lt._normalize("Read timed out")
        assert result == "Read timed out"


# ---------------------------------------------------------------------------
# _extract_call_chain
# ---------------------------------------------------------------------------

class TestExtractCallChain:
    def test_empty_lines_returns_empty(self):
        assert lt._extract_call_chain([]) == []

    def test_non_lsst_frames_excluded(self):
        lines = [
            '  File "/external/lib/numpy.py", line 1, in cholesky\n',
            '  File "relative/path.py", line 2, in func\n',
        ]
        assert lt._extract_call_chain(lines) == []

    def test_lsst_frames_included(self):
        lines = [
            '  File "/lsst/pipe/base/task.py", line 42, in runQuantum\n',
        ]
        result = lt._extract_call_chain(lines)
        assert result == ["task.py:runQuantum"]

    def test_returns_filename_colon_funcname(self):
        lines = [
            '  File "/lsst/pipe/tasks/fit_turbulence.py", line 10, in run\n',
        ]
        result = lt._extract_call_chain(lines)
        assert result == ["fit_turbulence.py:run"]

    def test_mixed_frames_only_lsst_returned(self):
        lines = [
            '  File "/lsst/pipe/base/task.py", line 42, in runQuantum\n',
            '  File "/external/numpy.py", line 1, in cholesky\n',
            '  File "/lsst/pipe/tasks/fit.py", line 10, in run\n',
        ]
        result = lt._extract_call_chain(lines)
        assert result == ["task.py:runQuantum", "fit.py:run"]

    def test_preserves_frame_order(self):
        lines = [
            '  File "/lsst/a/first.py", line 1, in alpha\n',
            '  File "/lsst/b/second.py", line 2, in beta\n',
            '  File "/lsst/c/third.py", line 3, in gamma\n',
        ]
        result = lt._extract_call_chain(lines)
        assert result == ["first.py:alpha", "second.py:beta", "third.py:gamma"]


# ---------------------------------------------------------------------------
# _extract_exception_chain
# ---------------------------------------------------------------------------

class TestExtractExceptionChain:
    def test_empty_lines_returns_empty(self):
        assert lt._extract_exception_chain([]) == []

    def test_single_exception_line(self):
        lines = ["ValueError: something bad\n"]
        result = lt._extract_exception_chain(lines)
        assert len(result) == 1
        assert result[0].startswith("ValueError:")

    def test_skips_mpgraphexecutor(self):
        lines = [
            "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError: tasks failed\n"
        ]
        assert lt._extract_exception_chain(lines) == []

    def test_multiple_exceptions_ordered(self):
        lines = [
            "OSError: connection refused\n",
            "During handling of the above exception, another exception occurred:\n",
            "RuntimeError: retry exhausted\n",
        ]
        result = lt._extract_exception_chain(lines)
        assert len(result) == 2
        assert result[0].startswith("OSError:")
        assert result[1].startswith("RuntimeError:")

    def test_uses_simple_class_name(self):
        lines = ["numpy.linalg.LinAlgError: not positive definite\n"]
        result = lt._extract_exception_chain(lines)
        assert len(result) == 1
        assert result[0].startswith("LinAlgError:")
        assert "numpy.linalg" not in result[0]

    def test_indented_lines_not_matched(self):
        # Frame lines are indented — they should not be treated as exception lines.
        lines = [
            '  File "/lsst/pipe/base/task.py", line 42, in runQuantum\n',
            "ValueError: something bad\n",
        ]
        result = lt._extract_exception_chain(lines)
        assert len(result) == 1
        assert result[0].startswith("ValueError:")

    def test_message_normalized(self):
        lines = ["ValueError: read timeout on port 1095\n"]
        result = lt._extract_exception_chain(lines)
        assert "1095" not in result[0]
        assert "<N>" in result[0]


# ---------------------------------------------------------------------------
# _parse_block
# ---------------------------------------------------------------------------

class TestParseBlock:
    def test_empty_block_returns_none(self):
        assert lt._parse_block([]) is None

    def test_inline_exception_extracted(self):
        lines = [
            "ERROR 2026-06-24 executor.py:316 - Execution failed. "
            "Exception ValueError: something bad\n"
        ]
        result = lt._parse_block(lines)
        assert result is not None
        key, *_ = result
        assert key.startswith("ValueError:")

    def test_inline_exception_uses_simple_class_name(self):
        lines = [
            "ERROR 2026-06-24 executor.py:316 - Execution failed. "
            "Exception numpy.linalg.LinAlgError: matrix singular\n"
        ]
        key, *_ = lt._parse_block(lines)
        assert key.startswith("LinAlgError:")
        assert "numpy.linalg" not in key

    def test_traceback_block_extracts_last_exception_line(self):
        lines = [
            "ERROR 2026-06-24 mp_graph_executor.py:720 - quantum failed; processing will continue\n",
            "Traceback (most recent call last):\n",
            "  File 'foo.py', line 1, in bar\n",
            "    do_thing()\n",
            "ValueError: something bad\n",
        ]
        key, *_ = lt._parse_block(lines)
        assert key.startswith("ValueError:")

    def test_dotted_exception_name_in_traceback(self):
        lines = [
            "ERROR 2026-06-24 mp_graph_executor.py:720 - quantum failed; processing will continue\n",
            "Traceback (most recent call last):\n",
            "  File 'foo.py', line 1\n",
            "numpy.linalg.LinAlgError: matrix singular\n",
        ]
        key, *_ = lt._parse_block(lines)
        assert key.startswith("LinAlgError:")

    def test_mpgraphexecutor_block_discarded(self):
        lines = [
            "ERROR 2026-06-24 cli.utils - Caught an exception:\n",
            "Traceback (most recent call last):\n",
            "  File 'mp.py', line 1\n",
            "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError: One or more tasks failed.\n",
        ]
        assert lt._parse_block(lines) is None

    def test_block_with_no_exception_shape_returns_none(self):
        # Single ERROR line, no inline exception and no traceback
        lines = ["ERROR 2026-06-24 some_module - something happened\n"]
        assert lt._parse_block(lines) is None

    def test_block_text_contains_full_content(self):
        lines = [
            "ERROR 2026-06-24 executor.py:316 - Execution failed. Exception ValueError: bad\n",
            "Traceback (most recent call last):\n",
            "  File 'x.py', line 1\n",
            "ValueError: bad\n",
        ]
        _, text, *_ = lt._parse_block(lines)
        assert "Traceback" in text
        assert "ValueError: bad" in text

    def test_inline_exception_preferred_over_traceback_last_line(self):
        # First ERROR line says ValueError, traceback ends with a different type.
        lines = [
            "ERROR 2026-06-24 executor.py:316 - Execution failed. Exception ValueError: outer error\n",
            "Traceback (most recent call last):\n",
            "  File 'x.py', line 1\n",
            "OSError: inner cause\n",
        ]
        key, *_ = lt._parse_block(lines)
        # Inline exception (ValueError) takes priority
        assert key.startswith("ValueError:")

    def test_inline_block_call_chain_empty(self):
        # Inline ERROR lines have no traceback frames, so call_chain is always empty.
        lines = [
            "ERROR 2026-06-24 executor.py:316 - Execution failed. "
            "Exception ValueError: something bad\n"
        ]
        _, _, call_chain, _ = lt._parse_block(lines)
        assert call_chain == []

    def test_traceback_block_call_chain_populated_for_lsst_frames(self):
        lines = [
            "ERROR 2026-06-24 mp_graph_executor.py:720 - quantum failed\n",
            '  File "/lsst/pipe/base/task.py", line 42, in runQuantum\n',
            "ValueError: bad\n",
        ]
        _, _, call_chain, _ = lt._parse_block(lines)
        assert call_chain == ["task.py:runQuantum"]

    def test_exception_chain_populated(self):
        lines = [
            "ERROR 2026-06-24 mp_graph_executor.py:720 - quantum failed\n",
            "Traceback (most recent call last):\n",
            "  File 'x.py', line 1\n",
            "ValueError: something bad\n",
        ]
        _, _, _, exc_chain = lt._parse_block(lines)
        assert len(exc_chain) == 1
        assert exc_chain[0].startswith("ValueError:")


# ---------------------------------------------------------------------------
# _extract_errors  (real temp files, no mocks)
# ---------------------------------------------------------------------------

class TestExtractErrors:
    def test_nonexistent_file_returns_empty(self, tmp_path):
        assert lt._extract_errors(str(tmp_path / "nonexistent.out")) == []

    def test_file_with_no_error_lines_returns_empty(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text("INFO something\nVERBOSE more info\nWARNING mild issue\n")
        assert lt._extract_errors(str(log)) == []

    def test_inline_error_block_extracted(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text(LOG_SINGLE_INLINE)
        results = lt._extract_errors(str(log))
        assert len(results) == 1
        key, *_ = results[0]
        assert "ValueError" in key

    def test_traceback_block_extracted(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text(LOG_TRACEBACK_BLOCK)
        results = lt._extract_errors(str(log))
        assert len(results) == 1
        key, text, *_ = results[0]
        assert "ValueError" in key
        assert "Traceback" in text

    def test_mpgraphexecutor_wrapper_filtered_out(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text(LOG_MPGRAPH_WRAPPER)
        assert lt._extract_errors(str(log)) == []

    def test_two_blocks_same_key_both_returned(self, tmp_path):
        """Both inline and traceback blocks for the same quantum are returned;
        deduplication happens at the job_log_summaries level."""
        log = tmp_path / "test.out"
        log.write_text(LOG_TWO_BLOCKS_SAME_KEY)
        results = lt._extract_errors(str(log))
        keys = [r[0] for r in results]
        assert len(results) == 2
        assert keys[0] == keys[1]

    def test_traceback_block_is_longer_than_inline_block(self, tmp_path):
        """The traceback block should be longer so job_log_summaries keeps it as example."""
        log = tmp_path / "test.out"
        log.write_text(LOG_TWO_BLOCKS_SAME_KEY)
        results = lt._extract_errors(str(log))
        texts = [r[1] for r in results]
        assert max(len(t) for t in texts) > min(len(t) for t in texts)

    def test_multiple_distinct_error_types(self, tmp_path):
        log = tmp_path / "test.out"
        content = LOG_SINGLE_INLINE + LOG_RUNTIME_ERROR
        log.write_text(content)
        results = lt._extract_errors(str(log))
        keys = {r[0] for r in results}
        assert any("ValueError" in k for k in keys)
        assert any("RuntimeError" in k for k in keys)

    def test_block_lines_cap_respected(self, tmp_path):
        """Lines beyond _MAX_BLOCK_LINES are not accumulated."""
        # Build a block with more lines than the cap, exception beyond the cap
        filler = "  extra line\n" * (lt._MAX_BLOCK_LINES + 10)
        content = (
            "INFO 2026-06-24T09:00:00+01:00 lsst.pipe start\n"
            "ERROR 2026-06-24T09:01:00+01:00 mp_graph_executor.py:720 - quantum failed\n"
            + filler
            + "VeryLateError: this comes after the cap\n"
            "INFO 2026-06-24T09:01:01+01:00 lsst.pipe end\n"
        )
        log = tmp_path / "test.out"
        log.write_text(content)
        results = lt._extract_errors(str(log))
        # Exception line is beyond the cap, so block has no parseable exception
        assert results == []

    def test_lsst_frames_appear_in_call_chain(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text(LOG_WITH_LSST_FRAMES)
        results = lt._extract_errors(str(log))
        assert len(results) == 1
        _, _, call_chain, _ = results[0]
        assert "task.py:runQuantum" in call_chain
        assert "fit_turbulence.py:run" in call_chain

    def test_chained_exceptions_appear_in_exc_chain(self, tmp_path):
        log = tmp_path / "test.out"
        log.write_text(LOG_CHAINED_EXCEPTION)
        results = lt._extract_errors(str(log))
        assert len(results) == 1
        _, _, _, exc_chain = results[0]
        assert any(e.startswith("OSError:") for e in exc_chain)
        assert any(e.startswith("RuntimeError:") for e in exc_chain)


# ---------------------------------------------------------------------------
# job_log_summaries  (real temp files, mock only get_os_job_info)
# ---------------------------------------------------------------------------

class TestJobLogSummaries:
    def _run(self, df, tmp_path, **kwargs):
        with patch("mcp_opensearch.log_tools.get_os_job_info", return_value=df):
            return lt.job_log_summaries("batch1", "idx", "ExitCode != 0", **kwargs)

    def test_output_structure(self, tmp_path):
        write_log(tmp_path, "job.1.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        assert "taskA" in result
        task = result["taskA"]
        assert "total_jobs" in task
        assert "sampled_jobs" in task
        assert "errors" in task

    def test_total_and_sampled_equal_when_under_limit(self, tmp_path):
        for i in range(3):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(3)])
        result = self._run(df, tmp_path, max_jobs=500)
        assert result["taskA"]["total_jobs"] == 3
        assert result["taskA"]["sampled_jobs"] == 3

    def test_sampling_caps_sampled_jobs(self, tmp_path):
        for i in range(10):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(10)])
        result = self._run(df, tmp_path, max_jobs=4)
        assert result["taskA"]["total_jobs"] == 10
        assert result["taskA"]["sampled_jobs"] == 4

    def test_sampling_reads_fewer_files(self, tmp_path):
        """When sampled, count reflects sample size, not total."""
        for i in range(10):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(10)])
        result = self._run(df, tmp_path, max_jobs=4)
        errors = result["taskA"]["errors"]
        assert len(errors) > 0
        total_count = sum(e["count"] for e in errors.values())
        assert total_count <= 4

    def test_rate_equals_count_over_sampled_jobs(self, tmp_path):
        for i in range(4):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(4)])
        result = self._run(df, tmp_path)
        errors = result["taskA"]["errors"]
        assert len(errors) > 0
        for key, entry in errors.items():
            expected = round(entry["count"] / result["taskA"]["sampled_jobs"], 3)
            assert entry["rate"] == expected

    def test_all_same_error_type_rate_is_one(self, tmp_path):
        for i in range(4):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(4)])
        result = self._run(df, tmp_path)
        errors = result["taskA"]["errors"]
        assert len(errors) == 1
        only_entry = next(iter(errors.values()))
        assert only_entry["rate"] == 1.0
        assert only_entry["count"] == 4

    def test_max_examples_cap(self, tmp_path):
        for i in range(5):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(5)])
        result = self._run(df, tmp_path, max_examples=2)
        errors = result["taskA"]["errors"]
        for entry in errors.values():
            assert len(entry["examples"]) <= 2

    def test_errors_sorted_by_count_descending(self, tmp_path):
        # 3 ValueError jobs, 1 RuntimeError job
        for i in range(3):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        write_log(tmp_path, "job.3.0.out", LOG_RUNTIME_ERROR)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(4)])
        result = self._run(df, tmp_path)
        counts = [e["count"] for e in result["taskA"]["errors"].values()]
        assert counts == sorted(counts, reverse=True)

    def test_most_frequent_error_first_is_not_runtime(self, tmp_path):
        for i in range(3):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        write_log(tmp_path, "job.3.0.out", LOG_RUNTIME_ERROR)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(4)])
        result = self._run(df, tmp_path)
        first_key = next(iter(result["taskA"]["errors"]))
        assert "ValueError" in first_key

    def test_multiple_tasks_keyed_separately(self, tmp_path):
        write_log(tmp_path, "job_a.1.0.out", LOG_SINGLE_INLINE)
        write_log(tmp_path, "job_b.2.0.out", LOG_RUNTIME_ERROR)
        df = make_df([
            ("taskA", "job_a.1.0.out", str(tmp_path)),
            ("taskB", "job_b.2.0.out", str(tmp_path)),
        ])
        result = self._run(df, tmp_path)
        assert "taskA" in result
        assert "taskB" in result
        assert any("ValueError" in k for k in result["taskA"]["errors"])
        assert any("RuntimeError" in k for k in result["taskB"]["errors"])

    def test_missing_log_file_skipped_gracefully(self, tmp_path):
        df = make_df([("taskA", "nonexistent.999.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        assert result["taskA"]["errors"] == {}
        assert result["taskA"]["total_jobs"] == 1

    def test_max_jobs_none_processes_all(self, tmp_path):
        for i in range(5):
            write_log(tmp_path, f"job.{i}.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", f"job.{i}.0.out", str(tmp_path)) for i in range(5)])
        result = self._run(df, tmp_path, max_jobs=None)
        assert result["taskA"]["total_jobs"] == 5
        assert result["taskA"]["sampled_jobs"] == 5

    def test_deduplication_per_file_counts_once(self, tmp_path):
        """Two ERROR blocks with the same key in one file count as one job, not two."""
        write_log(tmp_path, "job.1.0.out", LOG_TWO_BLOCKS_SAME_KEY)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        errors = result["taskA"]["errors"]
        assert len(errors) == 1
        only_entry = next(iter(errors.values()))
        assert only_entry["count"] == 1

    def test_example_uses_longer_block(self, tmp_path):
        """The traceback block (longer) is stored as example, not the single-line inline block."""
        write_log(tmp_path, "job.1.0.out", LOG_TWO_BLOCKS_SAME_KEY)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        errors = result["taskA"]["errors"]
        only_entry = next(iter(errors.values()))
        example = only_entry["examples"][0]
        assert "Traceback" in example["text"]

    def test_example_has_required_keys(self, tmp_path):
        write_log(tmp_path, "job.1.0.out", LOG_SINGLE_INLINE)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        example = next(iter(result["taskA"]["errors"].values()))["examples"][0]
        assert "text" in example
        assert "call_chain" in example
        assert "exception_chain" in example

    def test_call_chain_populated_for_lsst_frames(self, tmp_path):
        write_log(tmp_path, "job.1.0.out", LOG_WITH_LSST_FRAMES)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        example = next(iter(result["taskA"]["errors"].values()))["examples"][0]
        assert "task.py:runQuantum" in example["call_chain"]
        assert "fit_turbulence.py:run" in example["call_chain"]

    def test_exception_chain_populated_for_chained_exceptions(self, tmp_path):
        write_log(tmp_path, "job.1.0.out", LOG_CHAINED_EXCEPTION)
        df = make_df([("taskA", "job.1.0.out", str(tmp_path))])
        result = self._run(df, tmp_path)
        example = next(iter(result["taskA"]["errors"].values()))["examples"][0]
        exc_chain = example["exception_chain"]
        assert any(e.startswith("OSError:") for e in exc_chain)
        assert any(e.startswith("RuntimeError:") for e in exc_chain)
