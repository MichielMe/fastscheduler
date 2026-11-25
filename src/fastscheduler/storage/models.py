"""SQLModel table definitions for database storage backend."""

from typing import Optional

try:
    from sqlmodel import Field, SQLModel
except ImportError as e:
    raise ImportError(
        "SQLModel storage requires sqlmodel. "
        "Install with: pip install fastscheduler[database]"
    ) from e


class JobModel(SQLModel, table=True):
    """Database model for scheduled jobs."""
    
    __tablename__ = "scheduler_jobs"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(unique=True, index=True)
    func_name: str
    func_module: str
    next_run: float = Field(index=True)
    interval: Optional[float] = None
    repeat: bool = True
    status: str = "scheduled"
    created_at: float
    last_run: Optional[float] = None
    run_count: int = 0
    max_retries: int = 3
    retry_count: int = 0
    catch_up: bool = True
    schedule_type: str = "interval"
    schedule_time: Optional[str] = None
    schedule_days: Optional[str] = None  # Stored as JSON string "[0, 1, 2]"
    timeout: Optional[float] = None
    paused: bool = False
    timezone: Optional[str] = None
    cron_expression: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Job reconstruction."""
        import json
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status,
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            "schedule_days": json.loads(self.schedule_days) if self.schedule_days else None,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "JobModel":
        """Create from job dictionary."""
        import json
        schedule_days = data.get("schedule_days")
        if schedule_days is not None and not isinstance(schedule_days, str):
            schedule_days = json.dumps(schedule_days)
        
        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            func_module=data["func_module"],
            next_run=data["next_run"],
            interval=data.get("interval"),
            repeat=data.get("repeat", True),
            status=data.get("status", "scheduled"),
            created_at=data.get("created_at", 0),
            last_run=data.get("last_run"),
            run_count=data.get("run_count", 0),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            catch_up=data.get("catch_up", True),
            schedule_type=data.get("schedule_type", "interval"),
            schedule_time=data.get("schedule_time"),
            schedule_days=schedule_days,
            timeout=data.get("timeout"),
            paused=data.get("paused", False),
            timezone=data.get("timezone"),
            cron_expression=data.get("cron_expression"),
        )


class JobHistoryModel(SQLModel, table=True):
    """Database model for job execution history."""
    
    __tablename__ = "scheduler_history"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    func_name: str = Field(index=True)
    status: str
    timestamp: float = Field(index=True)
    error: Optional[str] = None
    traceback: Optional[str] = None
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from datetime import datetime
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "traceback": self.traceback,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "JobHistoryModel":
        """Create from history dictionary."""
        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            traceback=data.get("traceback"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class DeadLetterModel(SQLModel, table=True):
    """Database model for dead letter queue (failed jobs)."""
    
    __tablename__ = "scheduler_dead_letters"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    func_name: str = Field(index=True)
    status: str
    timestamp: float = Field(index=True)
    error: Optional[str] = None
    traceback: Optional[str] = None
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from datetime import datetime
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "traceback": self.traceback,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "DeadLetterModel":
        """Create from dead letter dictionary."""
        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            traceback=data.get("traceback"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class SchedulerMetadataModel(SQLModel, table=True):
    """Database model for scheduler metadata (job counter, stats)."""
    
    __tablename__ = "scheduler_metadata"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    key: str = Field(unique=True, index=True)
    value: str  # JSON-encoded value
    
    @classmethod
    def get_value(cls, key: str, default: str = "{}") -> str:
        """Get value placeholder - actual implementation in backend."""
        return default
