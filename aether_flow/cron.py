"""Standard 5-field cron syntax parser and trigger scheduler."""

import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Set


class CronTrigger:
    """Evaluates 5-part cron expressions (minute hour dom month dow)."""

    def __init__(self, expression: str):
        self.expression = expression.strip()
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression '{expression}'. Expected 5 fields: min hour dom month dow")

        self.minutes = self._parse_field(parts[0], 0, 59)
        self.hours = self._parse_field(parts[1], 0, 23)
        self.doms = self._parse_field(parts[2], 1, 31)
        self.months = self._parse_field(parts[3], 1, 12)
        # In cron: 0 = Sunday, 1 = Monday, ..., 6 = Saturday (also 7 = Sunday in some formats)
        self.dows = self._parse_dow_field(parts[4])

    @staticmethod
    def _parse_field(field_str: str, min_val: int, max_val: int) -> Set[int]:
        values: Set[int] = set()
        for sub in field_str.split(","):
            sub = sub.strip()
            if sub == "*":
                values.update(range(min_val, max_val + 1))
            elif sub.startswith("*/"):
                step = int(sub[2:])
                if step <= 0:
                    raise ValueError(f"Invalid step value '{step}' in cron field '{sub}'")
                values.update(i for i in range(min_val, max_val + 1) if (i - min_val) % step == 0)
            elif "-" in sub:
                start_s, end_s = sub.split("-", 1)
                start, end = int(start_s), int(end_s)
                values.update(range(start, end + 1))
            else:
                val = int(sub)
                if val < min_val or val > max_val:
                    raise ValueError(f"Value '{val}' out of range [{min_val}, {max_val}]")
                values.add(val)
        return values

    @staticmethod
    def _parse_dow_field(field_str: str) -> Set[int]:
        values = set()
        for sub in field_str.split(","):
            sub = sub.strip()
            if sub == "*":
                values.update(range(0, 7))
            elif sub.startswith("*/"):
                step = int(sub[2:])
                values.update(i for i in range(0, 7) if i % step == 0)
            elif "-" in sub:
                start_s, end_s = sub.split("-", 1)
                start, end = int(start_s), int(end_s)
                for val in range(start, end + 1):
                    values.add(0 if val == 7 else val)
            else:
                val = int(sub)
                values.add(0 if val == 7 else val)
        return values

    def matches(self, dt: datetime) -> bool:
        """Check if datetime matches this cron expression."""
        # Python weekday: Monday is 0, Sunday is 6.
        # Cron weekday: Sunday is 0, Monday is 1, ..., Saturday is 6.
        cron_dow = (dt.weekday() + 1) % 7

        return (
            dt.minute in self.minutes
            and dt.hour in self.hours
            and dt.day in self.doms
            and dt.month in self.months
            and cron_dow in self.dows
        )

    def next_fire_time(self, from_dt: Optional[datetime] = None) -> datetime:
        """Compute next upcoming minute timestamp matching the cron expression."""
        current = from_dt or datetime.now(timezone.utc)
        # Advance to start of next minute
        candidate = current.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Search up to 5 years into the future (limit iteration runaway)
        max_minutes = 5 * 365 * 24 * 60
        for _ in range(max_minutes):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)

        raise RuntimeError(f"Could not find matching next fire time for cron '{self.expression}'")

    def seconds_until_next(self, from_dt: Optional[datetime] = None) -> float:
        """Return float seconds from now (or from_dt) until next trigger."""
        current = from_dt or datetime.now(timezone.utc)
        nxt = self.next_fire_time(current)
        return max(0.0, (nxt - current).total_seconds())
