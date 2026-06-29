import os
import glob
from itertools import pairwise
import numpy as np
from htc_job_history import get_os_job_info
from smolagents import tool
from .utils import track_calls


__all__ = (
    "failed_job_log_summaries",
    "retried_job_log_summaries",
)


def load_log_summary(file_list: list[str]) -> str:
    """Read logs and extract only the relevant Error/Traceback lines
    to save context space.

    Args:
        file_list: List of file paths to read.
    """
    summary = []
    for file_path in file_list:
        # Find lines that start with a logging tag.
        indexes = []
        with open(file_path) as fobj:
            lines = fobj.readlines()
            for i, line in enumerate(lines):
                if line[:4] in ("WARN", "INFO", "ERRO", "VERB"):
                    indexes.append(i)
        indexes.append(len(lines) - 1)
        output_lines = []
        for i, j in pairwise(indexes):
            if lines[i].startswith("ERROR"):
                output_lines.extend([_.strip() for _ in lines[i:j]])
                # If no errors found, just take the last few lines for context
                if not output_lines:
                    output_lines = [_.strip() for _ in lines[-10:]]

                header = f"--- ANALYSIS OF {file_path} ---"
                output_lines.insert(0, header)
                summary.append('\n'.join(output_lines))
            if len(output_lines) > 10000:
                break

    return "\n".join(summary)


def job_log_summaries(job_batch_id, nsamp, index, query, last_log_index=-1):
    """Find all jobs in the job_batch_id cluster, satisfying the query
    condition, and provide summaries of the errors.

    Args:
        job_batch_id: The JobBatchId for the cluster being considered.
        nsamp: The maximum number of log files to consider, randomly sampled
            from the available log files, typically, 25-50 log files.
        index: The OpenSearch index to consider.
        query: The query conditions, e.g., "ExitCode != 0"
        last_log_index: The index of the sorted log files to consider.
            If last_log_index == -1, then the most recent log file will
            be returned.
    """
    df0 = get_os_job_info(job_batch_id, index=index).query(query)
    # Aggregate by task type.
    tasks = sorted(set(df0['bps_job_label']))
    log_summaries = {}
    for task in tasks:
        df = df0.query(f"bps_job_label=='{task}'")
        nrows = len(df)
        # Randomly sample up to nsamp log files.
        if nsamp is not None and nrows > nsamp:
            indexes = np.random.choice(range(nrows), size=nsamp, replace=False)
        else:
            indexes = list(range(nrows))
        log_files = []
        for index in indexes:
            row = df.iloc[index]
            # For multiple job starts, just grab the last one.
            tokens = row['Err'].split('.')
            tokens[-2] = '*'
            pattern = os.path.join(row['Iwd'], '.'.join(tokens))
            try:
                log_path = sorted(glob.glob(pattern))[last_log_index]
                if os.path.isfile(log_path):
                    log_files.append(log_path)
            except IndexError:
                pass
        log_summaries[task] = load_log_summary(log_files)
    return log_summaries


@tool
@track_calls("failed_job_log_summaries")
def failed_job_log_summaries(
        job_batch_id: str,
        nsamp: int = 50,
        index: str = 'htcondor-history-v1',
) -> dict[str: list]:
    """Return error message summaries of payload logs for failed jobs.

    Args:
        job_batch_id: The JobBatchId for the cluster being considered.
        nsamp: The maximum number of log files to consider, randomly sampled
            from the available log files.  Default: 50
        index: The OpenSearch index to consider. Default: 'htcondor-history-v1'
    """
    return job_log_summaries(job_batch_id, nsamp, index, "ExitCode != 0")


@tool
@track_calls("retried_job_log_summaries")
def retried_job_log_summaries(
        job_batch_id: str,
        nsamp: int = 25,
        index: str = 'htcondor-history-v1',
) -> dict[str: list]:
    """Return error message summaries of payload logs for jobs with multiple
    tries, i.e., with NumJobStarts > 1.

    Args:
        job_batch_id: The JobBatchId for the cluster being considered.
        nsamp: The maximum number of log files to consider, randomly sampled
            from the available log files.  Default: 25
        index: The OpenSearch index to consider. Default: 'htcondor-history-v1'
    """
    return job_log_summaries(job_batch_id, nsamp, index, "NumJobStarts > 1", -2)
