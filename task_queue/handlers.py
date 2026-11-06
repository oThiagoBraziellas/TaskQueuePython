"""Demo handlers used by the command line interface."""

import random
import time
from typing import Any


def echo(payload: Any) -> Any:
    return payload


def sleep(payload: Any) -> Any:
    seconds = float((payload or {}).get("seconds", 1))
    time.sleep(seconds)
    return {"slept": seconds}


def flaky(payload: Any) -> Any:
    """Fails with probability ``p`` (default 0.6) to show retries and backoff."""
    if random.random() < float((payload or {}).get("p", 0.6)):
        raise RuntimeError("random failure")
    return "ok"


def always_fail(payload: Any) -> Any:
    raise RuntimeError("this task always fails")


DEMO = {"echo": echo, "sleep": sleep, "flaky": flaky, "fail": always_fail}
