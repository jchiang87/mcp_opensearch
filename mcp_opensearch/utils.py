import os
import json
import functools
from pathlib import Path


__all__ = (
    "track_calls",
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
