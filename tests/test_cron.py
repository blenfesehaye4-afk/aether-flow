from datetime import datetime, timezone
import pytest
from aether_flow.cron import CronTrigger


def test_cron_trigger_parsing_and_matching():
    # Every minute
    c_all = CronTrigger("* * * * *")
    dt = datetime(2026, 11, 10, 14, 30, 0, tzinfo=timezone.utc)
    assert c_all.matches(dt) is True

    # At minute 0 of every hour
    c_hourly = CronTrigger("0 * * * *")
    assert c_hourly.matches(datetime(2026, 11, 10, 14, 0, 0, tzinfo=timezone.utc)) is True
    assert c_hourly.matches(datetime(2026, 11, 10, 14, 15, 0, tzinfo=timezone.utc)) is False

    # Specific days and steps: every 15 minutes
    c_step = CronTrigger("*/15 * * * *")
    assert c_step.matches(datetime(2026, 11, 10, 14, 45, 0, tzinfo=timezone.utc)) is True
    assert c_step.matches(datetime(2026, 11, 10, 14, 46, 0, tzinfo=timezone.utc)) is False


def test_cron_trigger_next_fire_time():
    c = CronTrigger("0 12 * * *")  # Every day at 12:00
    base_dt = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    nxt = c.next_fire_time(base_dt)

    assert nxt.hour == 12
    assert nxt.minute == 0
    assert nxt.day == 10

    # Seconds until next
    sec = c.seconds_until_next(base_dt)
    assert sec == 2 * 3600  # 2 hours difference
