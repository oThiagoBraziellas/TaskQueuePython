"""Retry delay policy."""

import random
from typing import Optional


def backoff_delay(
    attempt: int,
    base: float = 1.0,
    factor: float = 2.0,
    cap: float = 300.0,
    jitter: float = 0.0,
    rng: Optional[random.Random] = None,
) -> float:
    """Delay before retry number ``attempt`` (1 = first retry).

    ``min(cap, base * factor**(attempt-1))``, optionally spread by ``jitter``
    (a fraction: 0.2 means +/-20%) so that many failing tasks do not retry in
    lockstep.
    """
    if attempt < 1:
        raise ValueError("attempt starts at 1")
    if base < 0 or factor < 1 or cap < 0 or not 0 <= jitter < 1:
        raise ValueError("invalid backoff parameters")
    delay = min(cap, base * factor ** (attempt - 1))
    if jitter:
        r = (rng or random).uniform(-jitter, jitter)
        delay *= 1 + r
    return delay
