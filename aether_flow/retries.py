"""Advanced task retry policies with jitter, exception filters, and fallback hooks."""

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional, Set, Tuple, Type, Union


class BackoffStrategy(str, Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FULL_JITTER = "full_jitter"
    EQUAL_JITTER = "equal_jitter"


@dataclass
class AdvancedRetryPolicy:
    """Configurable retry policy with exponential backoff, jitter, and error filters."""

    max_retries: int = 3
    initial_delay: float = 0.5
    backoff_factor: float = 2.0
    max_delay: float = 30.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    retry_on: Optional[Tuple[Type[Exception], ...]] = None
    dont_retry_on: Optional[Tuple[Type[Exception], ...]] = None
    fallback: Optional[Callable[[Exception, dict], Any]] = None

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an exception should trigger a retry attempt."""
        if attempt > self.max_retries:
            return False

        # Exclude specific non-retryable errors
        if self.dont_retry_on and isinstance(exception, self.dont_retry_on):
            return False

        # If specific retry_on errors are given, must match
        if self.retry_on and not isinstance(exception, self.retry_on):
            return False

        return True

    def calculate_delay(self, attempt: int) -> float:
        """Compute delay before next retry based on selected strategy."""
        if attempt <= 0:
            return 0.0

        if self.strategy == BackoffStrategy.FIXED:
            delay = self.initial_delay

        elif self.strategy == BackoffStrategy.LINEAR:
            delay = self.initial_delay * attempt

        elif self.strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.initial_delay * (self.backoff_factor ** (attempt - 1))

        elif self.strategy == BackoffStrategy.FULL_JITTER:
            # Full jitter: Uniform(0, min(max_delay, initial_delay * 2^(attempt-1)))
            upper_bound = min(self.max_delay, self.initial_delay * (self.backoff_factor ** (attempt - 1)))
            delay = random.uniform(0.0, upper_bound)

        elif self.strategy == BackoffStrategy.EQUAL_JITTER:
            # Equal jitter: half deterministic, half random
            temp = min(self.max_delay, self.initial_delay * (self.backoff_factor ** (attempt - 1)))
            half = temp / 2.0
            delay = half + random.uniform(0.0, half)

        else:
            delay = self.initial_delay

        return round(min(self.max_delay, max(0.0, delay)), 4)
