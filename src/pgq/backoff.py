import random


def backoff_seconds(attempts: int, base: float = 2.0, cap: float = 3600.0) -> float:
    """Exponential backoff with 50–100% jitter."""
    exponential = min(base ** attempts, cap)
    return exponential * (0.5 + random.random() * 0.5)