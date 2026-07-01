import datetime
from pathlib import Path
from smolagents import tool
from .utils import track_calls


__all__ = ("format_log_summary_report", "write_report")

_SEP = "=" * 64
_TASK_MAX = 48   # truncation limit for task name in summary table
_ERR_MAX  = 44   # truncation limit for error key in summary table


def _time_range(examples: list[dict]) -> str:
    timestamps = sorted(e["timestamp"] for e in examples if e.get("timestamp"))
    if not timestamps:
        return "unknown"
    return timestamps[0] if timestamps[0] == timestamps[-1] else f"{timestamps[0]} – {timestamps[-1]}"


def _trunc(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n - 1] + "…"


@tool
@track_calls("format_log_summary_report")
def format_log_summary_report(summary: dict, job_batch_id: str, log_type: str = "retried") -> str:
    """Render the fixed-format structural sections of a log summary report.

    Call this after failed_job_log_summaries or retried_job_log_summaries.
    It produces four labelled sections — REPORT HEADER, TASKS WITH NO ERRORS,
    PER-TASK ERROR DATA, SUMMARY TABLE — which form the skeleton of the report.
    Write the overview, failure-type narratives, and key findings around them.

    Final report assembly order:
      [REPORT HEADER]
      Overview paragraph  (LLM)
      [TASKS WITH NO ERRORS]
      Failure sections — one per category (infrastructure / scientific / other)
        Each section: LLM narrative + relevant per-task blocks from PER-TASK ERROR DATA
      [SUMMARY TABLE]
      Key findings  (LLM)

    Args:
        summary: Dict returned by failed_job_log_summaries or retried_job_log_summaries.
        job_batch_id: The JobBatchId used in the query.
        log_type: "failed" or "retried". Default: "retried"

    Returns:
        Formatted text with the four structural sections.
    """
    analysis = ("Failed jobs (ExitCode != 0)" if log_type == "failed"
                else "Retried jobs (NumJobStarts > 1)")
    today = datetime.date.today().isoformat()
    tasks = summary.get("tasks", {})
    no_err  = {t: v for t, v in tasks.items() if not v.get("errors")}
    err_tasks = {t: v for t, v in tasks.items() if v.get("errors")}

    out = []

    # ── REPORT HEADER ────────────────────────────────────────────────────────
    out += [
        _SEP,
        "REPORT HEADER",
        _SEP,
        f"JobBatchId:   {job_batch_id}",
        f"Run:          {summary.get('bps_run') or 'N/A'}",
        f"JobBatchName: {summary.get('job_batch_name') or 'N/A'}",
        f"Analysis:     {analysis}",
        f"Generated:    {today}",
        "",
    ]

    # ── TASKS WITH NO ERRORS ─────────────────────────────────────────────────
    out += [_SEP, "TASKS WITH NO ERRORS", _SEP]
    if no_err:
        for task, info in sorted(no_err.items()):
            total, sampled = info["total_jobs"], info["sampled_jobs"]
            note = f"{sampled} sampled" if sampled < total else f"{total} jobs"
            out.append(f"  {task:<58} {note}")
    else:
        out.append("  (none)")
    out.append("")

    # ── PER-TASK ERROR DATA ───────────────────────────────────────────────────
    out += [_SEP, "PER-TASK ERROR DATA", _SEP]
    for task, info in sorted(err_tasks.items()):
        total, sampled = info["total_jobs"], info["sampled_jobs"]
        sample_note = f", {sampled} sampled" if sampled < total else ""
        out += ["", f"{task}  [{total} total{sample_note}]", "-" * 64]

        for rank, (key, err) in enumerate(info["errors"].items(), 1):
            trange = _time_range(err["examples"])
            out.append(f"  {rank}. {key}")
            out.append(f"     Count: {err['count']}   Rate: {err['rate']:.1%}   Time range: {trange}")

            for ex in err["examples"]:
                if ex.get("exception_chain"):
                    out.append("     Exception chain:")
                    out += [f"       {e}" for e in ex["exception_chain"]]
                    break

            for ex in err["examples"]:
                if ex.get("call_chain"):
                    chain = ex["call_chain"]
                    out.append("     Call chain:")
                    out.append(f"       {chain[0]}")
                    out += [f"       -> {f}" for f in chain[1:]]
                    break

            host_counts = err.get("host_counts", {})
            if host_counts:
                top = list(host_counts.items())[:5]
                hosts_str = ", ".join(f"{h} ({n})" for h, n in top)
                if len(host_counts) > 5:
                    hosts_str += f" (+{len(host_counts) - 5} more)"
                out.append(f"     Hosts: {hosts_str}")

        re_data = info.get("resource_exhaustion", {})
        sk = re_data.get("signal_kills", {})
        mp = re_data.get("memory_pressure", {})
        if sk.get("count") or mp.get("count"):
            out.append("  Resource exhaustion:")
            if sk.get("count"):
                out.append(f"    Signal kills:           {sk['count']} / {info['sampled_jobs']} ({sk['rate']:.1%})")
            if mp.get("count"):
                top_ex = mp.get("examples", [])
                ratio_str = f"  top ratio: {top_ex[0]['ratio']:.2f} ({top_ex[0]['memory_usage_mb']}MB / {top_ex[0]['request_memory_mb']}MB)" if top_ex else ""
                out.append(f"    Memory pressure (>{mp.get('threshold', 0.9):.0%}): {mp['count']} / {info['sampled_jobs']} ({mp['rate']:.1%}){ratio_str}")
        else:
            out.append("  Resource exhaustion:    none detected")

        paths = info.get("example_log_paths", [])
        if paths:
            out.append("  Example log paths:")
            out += [f"    {p}" for p in paths]
    out.append("")

    # ── SUMMARY TABLE ─────────────────────────────────────────────────────────
    out += [_SEP, "SUMMARY TABLE", _SEP]
    rows = []
    for task, info in sorted(tasks.items()):
        total, sampled = info["total_jobs"], info["sampled_jobs"]
        jobs_str = str(total) if sampled == total else f"{total}/{sampled}"
        if info.get("errors"):
            for key, err in info["errors"].items():
                rows.append((_trunc(task, _TASK_MAX), jobs_str,
                             _trunc(key, _ERR_MAX), str(err["count"]), f"{err['rate']:.1%}"))
        else:
            rows.append((_trunc(task, _TASK_MAX), jobs_str, "(none)", "", ""))

    headers = ["Task", "Jobs", "Top Error", "Count", "Rate"]
    col_w = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    out.append("  ".join(h.ljust(w) for h, w in zip(headers, col_w)))
    out.append("  ".join("-" * w for w in col_w))
    for r in rows:
        out.append("  ".join(v.ljust(w) for v, w in zip(r, col_w)))
    out.append("")

    return "\n".join(out)


@tool
@track_calls("write_report")
def write_report(report_text: str, job_batch_id: str) -> str:
    """Write a completed log summary report to a plain-text file.

    Always call this as the final step after assembling the full report.

    Args:
        report_text: The complete assembled report text.
        job_batch_id: The JobBatchId used in the analysis (e.g. "443398.0").

    Returns:
        The absolute path of the written file.
    """
    batch_id = str(job_batch_id)
    if batch_id.endswith('.0'):
        batch_id = batch_id[:-2]
    path = Path.cwd() / f"report_jobBatchId_{batch_id}.txt"
    path.write_text(report_text)
    return str(path)
