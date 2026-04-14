"""Data models for FastScheduler."""

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional


class JobStatus(Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSED = "missed"


@dataclass(order=True)
class Job:
    next_run: float = field(compare=True)
    func: Optional[Callable] = field(default=None, compare=False, repr=False)
    interval: Optional[float] = field(default=None, compare=False)
    job_id: str = field(default="", compare=False)
    func_name: str = field(default="", compare=False)
    func_module: str = field(default="", compare=False)
    args: tuple = field(default_factory=tuple, compare=False, repr=False)
    kwargs: dict = field(default_factory=dict, compare=False, repr=False)
    repeat: bool = field(default=False, compare=False)
    status: JobStatus = field(default=JobStatus.SCHEDULED, compare=False)
    created_at: float = field(default_factory=time.time, compare=False)
    last_run: Optional[float] = field(default=None, compare=False)
    run_count: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    retry_count: int = field(default=0, compare=False)
    catch_up: bool = field(default=True, compare=False)
    schedule_type: str = field(default="interval", compare=False)
    schedule_time: Optional[str] = field(default=None, compare=False)
    schedule_days: Optional[List[int]] = field(default=None, compare=False)
    timeout: Optional[float] = field(default=None, compare=False)
    paused: bool = field(default=False, compare=False)
    timezone: Optional[str] = field(default=None, compare=False)
    cron_expression: Optional[str] = field(default=None, compare=False)

    def to_dict(self) -> Dict:
        """Serialize job for persistence."""
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status.value,
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            "schedule_days": self.schedule_days,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
        }

    def get_schedule_description(self) -> str:
        """Get human-readable schedule description."""
        tz_suffix = f" ({self.timezone})" if self.timezone else ""

        if self.schedule_type == "cron" and self.cron_expression:
            return f"Cron: {self.cron_expression}{tz_suffix}"
        elif self.schedule_type == "daily" and self.schedule_time:
            return f"Daily at {self.schedule_time}{tz_suffix}"
        elif (
            self.schedule_type == "weekly" and self.schedule_time and self.schedule_days
        ):
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_names = [days[d] for d in self.schedule_days]
            return f"Every {', '.join(day_names)} at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "hourly" and self.schedule_time:
            return f"Hourly at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "interval" and self.interval:
            if self.interval < 60:
                return f"Every {int(self.interval)} seconds"
            elif self.interval < 3600:
                return f"Every {int(self.interval/60)} minutes"
            elif self.interval < 86400:
                return f"Every {int(self.interval/3600)} hours"
            else:
                return f"Every {int(self.interval/86400)} days"
        return "One-time job"


@dataclass
class JobHistory:
    job_id: str
    func_name: str
    status: str
    timestamp: float
    error: Optional[str] = None
    traceback: Optional[str] = None
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = None

    def to_dict(self):
        return {
            **asdict(self),
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
