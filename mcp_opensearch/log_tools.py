import os
import re
import glob
from collections import defaultdict
from htc_job_history import get_os_job_info
from smolagents import tool
from .utils import track_calls

_MEM_PRESSURE_THRESHOLD = 0.9


def _to_float(val, default=0.0) -> float:
    try:
        f = float(val)
        return default if f != f else f  # f != f catches NaN
    except (TypeError, ValueError):
        return default


__all__ = ("retried_job_log_summaries",)

# --- log parsing ---

_LOG_TAG = re.compile(r'^(INFO|VERBOSE|WARNING|WARN|ERROR|FATAL|DEBUG)\s')
_EXC_INLINE = re.compile(r'[Ee]xception\s+([\w.]+):\s*(.*)')
_EXC_LINE = re.compile(r'^([\w]+(?:\.[\w]+)*):\s+(.*)')
_FRAME_RE = re.compile(r'^\s+File "([^"]+)", line \d+, in ([\w<>]+)')

# Normalization patterns applied to the exception message before building the key.
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)
_LSST_CLASS = re.compile(r'\blsst(?:\.\w+)+')  # lsst.x.y.ClassName → <lsst_class>
_PATH = re.compile(r'/\S+')                     # /absolute/path → <path>
_NUM = re.compile(r'\b\d+\b')                   # bare integers → <N>

# Skip these exception classes; they are uninformative wrappers.
_SKIP_EXC = frozenset({'MPGraphExecutorError'})

_TIMESTAMP = re.compile(r'\b(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)')
_BPS_EXIT = re.compile(r'^Command exited with code:\s*(\d+)')

_MAX_BLOCK_LINES = 200  # cap per ERROR block to bound memory; covers deepest tracebacks
_BPS_EXIT_CONTEXT = 5   # lines of context to include before the exit code line
_KEY_MAX_LEN = 200


def _normalize(msg: str) -> str:
    msg = _UUID.sub('<uuid>', msg)
    msg = _LSST_CLASS.sub('<lsst_class>', msg)
    msg = _PATH.sub('<path>', msg)
    msg = _NUM.sub('<N>', msg)
    return msg[:_KEY_MAX_LEN].strip()


def _extract_call_chain(lines: list[str]) -> list[str]:
    """Return 'filename:funcname' for each traceback frame whose path contains /lsst/."""
    result = []
    for line in lines:
        m = _FRAME_RE.match(line)
        if m:
            path, func = m.group(1), m.group(2)
            if '/lsst/' in path:
                result.append(f"{os.path.basename(path)}:{func}")
    return result


def _extract_exception_chain(lines: list[str]) -> list[str]:
    """Return ordered list of 'ExcType: normalized_msg' from column-0 exception lines."""
    result = []
    for line in lines:
        m = _EXC_LINE.match(line.rstrip())
        if m:
            exc_type, exc_msg = m.group(1), m.group(2)
            simple = exc_type.split('.')[-1]
            if simple not in _SKIP_EXC:
                result.append(f"{simple}: {_normalize(exc_msg)}")
    return result


def _parse_block(lines: list[str]) -> tuple[str, str, str | None, list[str], list[str]] | None:
    """Return (error_key, block_text, timestamp, call_chain, exception_chain) from an ERROR block,
    or None to discard."""
    if not lines:
        return None
    text = ''.join(lines).rstrip()
    first = lines[0]

    tm = _TIMESTAMP.search(first)
    timestamp = tm.group(1) if tm else None

    # Prefer the inline exception on the first ERROR line (single_quantum_executor style).
    m = _EXC_INLINE.search(first)
    if m:
        exc_type, exc_msg = m.group(1), m.group(2)
    else:
        # Fall back to the last exception-line in the block (mp_graph_executor traceback style).
        exc_type, exc_msg = '', ''
        for line in reversed(lines):
            if line.strip():
                m2 = _EXC_LINE.match(line)
                if m2:
                    exc_type, exc_msg = m2.group(1), m2.group(2)
                    break
        if not exc_type:
            return None

    if exc_type.split('.')[-1] in _SKIP_EXC:
        return None

    # Use simple class name in the key; full path available in block_text examples.
    key = f"{exc_type.split('.')[-1]}: {_normalize(exc_msg)}"
    call_chain = _extract_call_chain(lines)
    exc_chain = _extract_exception_chain(lines)
    return key, text, timestamp, call_chain, exc_chain


def _extract_errors(file_path: str) -> list[tuple[str, str, str | None, list[str], list[str]]]:
    """Stream through a log file and return list of (error_key, block_text, timestamp, call_chain, exc_chain)."""
    results = []
    block: list[str] = []
    in_error = False
    prev_lines: list[str] = []
    try:
        with open(file_path) as fobj:
            for line in fobj:
                m = _BPS_EXIT.match(line)
                if m:
                    code = int(m.group(1))
                    if code != 0:
                        context = ''.join(prev_lines[-_BPS_EXIT_CONTEXT:]) + line
                        results.append((
                            f"SIGKILL: exit code {code}",
                            context.rstrip(), None, [], [],
                        ))
                if _LOG_TAG.match(line):
                    if in_error and block:
                        parsed = _parse_block(block)
                        if parsed:
                            results.append(parsed)
                    in_error = line.startswith('ERROR')
                    block = [line] if in_error else []
                elif in_error and len(block) < _MAX_BLOCK_LINES:
                    block.append(line)
                prev_lines.append(line)
                if len(prev_lines) > _BPS_EXIT_CONTEXT:
                    prev_lines.pop(0)
            if in_error and block:
                parsed = _parse_block(block)
                if parsed:
                    results.append(parsed)
    except OSError:
        pass
    return results


# --- core logic ---

def job_log_summaries(
    job_batch_id: str,
    index: str,
    query: str,
    last_log_index: int = -1,
    max_examples: int = 3,
    max_jobs: int = 500,
) -> dict:
    """Scan job logs and tabulate error types with examples, per bps_job_label.

    If a task has more matching jobs than max_jobs, a random sample of
    max_jobs is read. Counts and rates then reflect the sample.

    Returns:
        {
            "bps_run":        str | None,  # BPS run identifier (includes DM ticket)
            "tasks": {
                bps_job_label: {
                    "total_jobs":        int,
                    "sampled_jobs":      int,
                    "resource_exhaustion": {
                        "signal_kills":    {"count": int, "rate": float,
                                            "example_jobs": list[str]},
                        "memory_pressure": {"count": int, "rate": float,
                                            "threshold": float,
                                            "examples": list[{"job": str,
                                                "memory_usage_mb": int,
                                                "request_memory_mb": int,
                                                "ratio": float}]},
                    },
                    "example_log_paths": list[str],  # up to max_examples representative paths
                    "errors": {
                        error_key: {
                            "count": int,
                            "rate": float,
                            "host_counts": dict[str, int],  # hostname -> count; empty if unavailable
                            "examples": list[{
                                "text": str,
                                "timestamp": str | None,  # ISO 8601 from the ERROR line
                                "call_chain": list[str],
                                "exception_chain": list[str],
                            }]
                        }
                    }
                }
            }
        }
        rate = count / sampled_jobs (an unbiased estimate of the true rate).
        host_counts are sorted descending by count; hostname is derived from
        LastRemoteHost with the slot prefix stripped (e.g. "slot1@host" -> "host").
    """
    df0 = get_os_job_info(job_batch_id, index=index).query(query)

    bps_run = df0['bps_run'].iloc[0] if len(df0) > 0 and 'bps_run' in df0.columns else None

    tasks = sorted(set(df0['bps_job_label']))
    task_results = {}

    for task in tasks:
        df = df0.query(f"bps_job_label=='{task}'")
        total = len(df)
        if max_jobs is not None and total > max_jobs:
            df = df.sample(max_jobs)
        n_sampled = len(df)

        counts: dict[str, int] = defaultdict(int)
        host_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        examples: dict[str, list[dict]] = defaultdict(list)
        example_log_paths: list[str] = []
        signal_kills: list[str] = []
        mem_pressure: list[dict] = []
        has_host = 'LastRemoteHost' in df.columns
        has_signal = 'ExitBySignal' in df.columns
        has_memory = 'MemoryUsage' in df.columns and 'RequestMemory' in df.columns
        has_exit_code = 'ExitCode' in df.columns
        has_provisioned = 'MemoryUsage' in df.columns and 'MemoryProvisioned' in df.columns

        for _, row in df.iterrows():
            if has_signal and row.get('ExitBySignal') == True:  # noqa: E712
                signal_kills.append(str(row.get('GlobalJobId', '')))
            if has_memory:
                mem_use = _to_float(row.get('MemoryUsage'))
                req_mem = _to_float(row.get('RequestMemory'))
                if req_mem > 0 and mem_use / req_mem > _MEM_PRESSURE_THRESHOLD:
                    mem_pressure.append({
                        'job': str(row.get('GlobalJobId', '')),
                        'memory_usage_mb': int(mem_use),
                        'request_memory_mb': int(req_mem),
                        'ratio': round(mem_use / req_mem, 3),
                    })

            raw_host = (row['LastRemoteHost'] or '') if has_host else ''
            host = raw_host.split('@', 1)[-1] if '@' in raw_host else raw_host

            # Synthesize an error entry for ExitCode 137 (SIGKILL) before
            # attempting log file lookup: SIGKILL'd jobs often have no log
            # file, and without this the failure would be silently dropped.
            if has_exit_code and int(row.get('ExitCode', 0) or 0) == 137:
                mem_use = _to_float(row.get('MemoryUsage')) if has_provisioned else 0.0
                prov_mem = _to_float(row.get('MemoryProvisioned')) if has_provisioned else 0.0
                mem_note = (f" mem={int(mem_use)}/{int(prov_mem)}MB"
                            f" ({mem_use/prov_mem:.0%})"
                            if prov_mem > 0 else "")
                is_oom = prov_mem > 0 and mem_use / prov_mem > _MEM_PRESSURE_THRESHOLD
                sigkill_key = ("OOMKill: ExitCode 137" if is_oom
                               else "SIGKILL: ExitCode 137")
                counts[sigkill_key] += 1
                if host:
                    host_counts[sigkill_key][host] += 1
                if len(examples[sigkill_key]) < max_examples:
                    examples[sigkill_key].append({
                        "text": f"{sigkill_key}{mem_note}",
                        "timestamp": None,
                        "call_chain": [],
                        "exception_chain": [],
                    })

            tokens = row['Err'].split('.')
            tokens[-2] = '*'
            pattern = os.path.join(row['Iwd'], '.'.join(tokens))
            try:
                log_path = sorted(glob.glob(pattern))[last_log_index]
            except IndexError:
                continue
            if not os.path.isfile(log_path):
                continue

            if len(example_log_paths) < max_examples:
                example_log_paths.append(log_path)

            # Count each distinct error key at most once per job file; keep
            # the longest block as the example (prefers full tracebacks over
            # the single-line single_quantum_executor ERROR entries).
            best: dict[str, tuple[str, str | None, list[str], list[str]]] = {}
            for key, text, timestamp, call_chain, exc_chain in _extract_errors(log_path):
                if key not in best or len(text) > len(best[key][0]):
                    best[key] = (text, timestamp, call_chain, exc_chain)

            for key, (text, timestamp, call_chain, exc_chain) in best.items():
                counts[key] += 1
                if host:
                    host_counts[key][host] += 1
                if len(examples[key]) < max_examples:
                    examples[key].append({
                        "text": text,
                        "timestamp": timestamp,
                        "call_chain": call_chain,
                        "exception_chain": exc_chain,
                    })

        task_results[task] = {
            "total_jobs": total,
            "sampled_jobs": n_sampled,
            "resource_exhaustion": {
                "signal_kills": {
                    "count": len(signal_kills),
                    "rate": round(len(signal_kills) / n_sampled, 3) if n_sampled else 0,
                    "example_jobs": signal_kills[:max_examples],
                },
                "memory_pressure": {
                    "count": len(mem_pressure),
                    "rate": round(len(mem_pressure) / n_sampled, 3) if n_sampled else 0,
                    "threshold": _MEM_PRESSURE_THRESHOLD,
                    "examples": sorted(mem_pressure, key=lambda x: -x['ratio'])[:max_examples],
                },
            },
            "example_log_paths": example_log_paths,
            "errors": {
                key: {
                    "count": counts[key],
                    "rate": round(counts[key] / n_sampled, 3),
                    "host_counts": dict(sorted(host_counts[key].items(),
                                               key=lambda x: -x[1])),
                    "examples": examples[key],
                }
                for key in sorted(counts, key=lambda k: -counts[k])
            },
        }

    return {
        "bps_run": bps_run,
        "tasks": task_results,
    }


# --- public tools ---

@tool
@track_calls("retried_job_log_summaries")
def retried_job_log_summaries(
        job_batch_id: str,
        index: str = 'htcondor-history-v1',
        max_examples: int = 3,
        max_jobs: int = 500,
) -> dict:
    """Return tabulated error summaries for retried jobs in a batch.

    Targets jobs with NumJobStarts > 1, reading the second-to-last log file
    to capture the failing retry attempt. This surfaces transient failures
    (e.g. infrastructure errors) that would be invisible in a failed-only
    query when the job ultimately succeeded on retry.

    Args:
        job_batch_id: The JobBatchId for the cluster.
        index: OpenSearch index. Default: 'htcondor-history-v1'
        max_examples: Max example error blocks per error type. Default: 3
        max_jobs: Max log files to read per task; random sample applied when
            exceeded. Default: 500

    Returns:
        {
            "bps_run": str | None,
            "tasks": {bps_job_label: {
                "total_jobs": int, "sampled_jobs": int,
                "example_log_paths": list[str],
                "errors": {error_key: {
                    "count": int, "rate": float,
                    "host_counts": dict[str, int],
                    "examples": list[{"text": str, "timestamp": str | None,
                                      "call_chain": list[str],
                                      "exception_chain": list[str]}]
                }}
            }}
        }
        rate = count / sampled_jobs (estimated true rate when sampled).
        host_counts maps hostname (slot prefix stripped) to occurrence count.
    """
    return job_log_summaries(job_batch_id, index, "NumJobStarts > 1",
                             last_log_index=-2, max_examples=max_examples,
                             max_jobs=max_jobs)
