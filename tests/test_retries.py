import pytest
from aether_flow.retries import AdvancedRetryPolicy, BackoffStrategy


def test_retry_policy_delays_and_strategies():
    # Fixed strategy
    p_fixed = AdvancedRetryPolicy(initial_delay=1.0, strategy=BackoffStrategy.FIXED)
    assert p_fixed.calculate_delay(1) == 1.0
    assert p_fixed.calculate_delay(3) == 1.0

    # Linear strategy
    p_lin = AdvancedRetryPolicy(initial_delay=0.5, strategy=BackoffStrategy.LINEAR)
    assert p_lin.calculate_delay(1) == 0.5
    assert p_lin.calculate_delay(2) == 1.0
    assert p_lin.calculate_delay(3) == 1.5

    # Exponential strategy
    p_exp = AdvancedRetryPolicy(initial_delay=0.5, backoff_factor=2.0, strategy=BackoffStrategy.EXPONENTIAL)
    assert p_exp.calculate_delay(1) == 0.5
    assert p_exp.calculate_delay(2) == 1.0
    assert p_exp.calculate_delay(3) == 2.0


def test_retry_policy_jitter_bounds():
    p_jitter = AdvancedRetryPolicy(
        initial_delay=1.0,
        backoff_factor=2.0,
        max_delay=10.0,
        strategy=BackoffStrategy.FULL_JITTER,
    )
    for attempt in range(1, 5):
        d = p_jitter.calculate_delay(attempt)
        assert 0.0 <= d <= 10.0


def test_retry_policy_exception_filtering():
    p = AdvancedRetryPolicy(
        max_retries=2,
        retry_on=(ConnectionError, TimeoutError),
        dont_retry_on=(ValueError,),
    )

    # Allowed errors
    assert p.should_retry(ConnectionError("network reset"), 1) is True
    assert p.should_retry(TimeoutError("gateway timeout"), 2) is True

    # Excluded errors
    assert p.should_retry(ValueError("bad format"), 1) is False

    # Max retries exceeded
    assert p.should_retry(ConnectionError("network reset"), 3) is False
