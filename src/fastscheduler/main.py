"""FastScheduler - Simple, powerful, persistent task scheduler."""

import asyncio
import heapq
import itertools
import logging
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from traceback import format_exception
from typing import TYPE_CHECKING, Callable, Dict, Iterator, List, Optional, Union
from zoneinfo import ZoneInfo

from .models import Job, JobHistory, JobStatus
from .schedulers import (
    CronScheduler,
    DailyScheduler,
    HourlyScheduler,
    IntervalScheduler,
    OnceScheduler,
    WeeklyScheduler,
)

if TYPE_CHECKING:
    from .storage import StorageBackend

try:
    from croniter import croniter

    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

logger = logging.getLogger("fastscheduler")
logger.addHandler(logging.NullHandler())


class FastScheduler:
    """
    FastScheduler - Simple, powerful, persistent task scheduler with async support.

    Args:
        state_file: Path to the JSON file for persisting scheduler state (used with JSON backend)
        storage: Storage backend - "json" (default), "sqlmodel", or a StorageBackend instance
        database_url: Database URL for SQLModel backend (e.g., "sqlite:///scheduler.db",
            "postgresql://user:pass@host/db", "mysql://user:pass@host/db")
        auto_start: If True, start the scheduler immediately
        quiet: If True, suppress most log messages
        max_history: Maximum number of history entries to keep (default: 10000)
        max_workers: Maximum number of worker threads for job execution (default: 10)
        history_retention_days: Maximum age of history entries in days (default: 7)
        max_dead_letters: Maximum number of failed job entries to keep in dead letter queue (default: 500)

    Examples:
        # JSON storage (default, backward compatible)
        scheduler = FastScheduler(state_file="scheduler.json")

        # SQLite database
        scheduler = FastScheduler(
            storage="sqlmodel",
            database_url="sqlite:///scheduler.db"
        )

        # PostgreSQL database
        scheduler = FastScheduler(
            storage="sqlmodel",
            database_url="postgresql://user:pass@localhost/mydb"
        )

        # Custom storage backend
        scheduler = FastScheduler(storage=MyCustomStorageBackend())
    """

    def __init__(
        self,
        state_file: str = "fastscheduler_state.json",
        storage: Optional[Union[str, "StorageBackend"]] = None,
        database_url: Optional[str] = None,
        auto_start: bool = False,
        quiet: bool = False,
        max_history: int = 10000,
        max_workers: int = 10,
        history_retention_days: int = 7,
        max_dead_letters: int = 500,
    ):
        self._storage = self._init_storage(storage, state_file, database_url, quiet)
        self.state_file = Path(state_file)

        self.jobs: List[Job] = []
        self.job_registry: Dict[str, Callable] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        self._job_counter: Iterator[int] = itertools.count()
        self._job_counter_value = 0
        self.history: List[JobHistory] = []
        self.max_history = max_history
        self.max_workers = max_workers
        self.history_retention_days = history_retention_days
        self.max_dead_letters = max_dead_letters
        self.quiet = quiet
        self._running_jobs: set = set()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="FastScheduler-Worker"
        )
        self._save_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="FastScheduler-Saver"
        )

        self.dead_letters: List[JobHistory] = []
        self._dead_letters_file = Path(
            str(self.state_file).replace(".json", "_dead_letters.json")
        )

        self.stats = {
            "total_runs": 0,
            "total_failures": 0,
            "total_retries": 0,
            "start_time": None,
        }

        self._load_state()
        self._load_dead_letters()

        if auto_start:
            self.start()

    def _init_storage(
        self,
        storage: Optional[Union[str, "StorageBackend"]],
        state_file: str,
        database_url: Optional[str],
        quiet: bool,
    ) -> "StorageBackend":
        """Initialize the storage backend."""
        from .storage import JSONStorageBackend, StorageBackend

        if isinstance(storage, StorageBackend):
            return storage

        if storage is None or storage == "json":
            return JSONStorageBackend(state_file=state_file, quiet=quiet)

        if storage == "sqlmodel":
            from .storage import get_sqlmodel_backend

            SQLModelStorageBackend = get_sqlmodel_backend()
            url = database_url or "sqlite:///fastscheduler.db"
            return SQLModelStorageBackend(database_url=url, quiet=quiet)

        raise ValueError(
            f"Unknown storage backend: {storage}. "
            "Use 'json', 'sqlmodel', or provide a StorageBackend instance."
        )

    # ==================== User-Friendly Scheduling API ====================

    def every(self, interval: Union[int, float]) -> IntervalScheduler:
        """Schedule a task to run every X seconds/minutes/hours/days."""
        return IntervalScheduler(self, interval)

    @property
    def daily(self) -> DailyScheduler:
        """Schedule a task to run daily at a specific time."""
        return DailyScheduler(self)

    @property
    def weekly(self) -> WeeklyScheduler:
        """Schedule a task to run weekly on specific days."""
        return WeeklyScheduler(self)

    @property
    def hourly(self) -> HourlyScheduler:
        """Schedule a task to run hourly at a specific minute."""
        return HourlyScheduler(self)

    def cron(self, expression: str) -> CronScheduler:
        """
        Schedule a task using a cron expression.

        Requires croniter: pip install fastscheduler[cron]

        Args:
            expression: Cron expression (e.g., "0 9 * * MON-FRI" for 9 AM on weekdays)

        Usage:
            @scheduler.cron("0 9 * * MON-FRI")
            def weekday_task():
                ...

            @scheduler.cron("*/5 * * * *")  # Every 5 minutes
            def frequent_task():
                ...
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install fastscheduler[cron]"
            )
        return CronScheduler(self, expression)

    def once(self, delay: Union[int, float]) -> OnceScheduler:
        """Schedule a one-time task."""
        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    def at(self, target_time: Union[datetime, str]) -> OnceScheduler:
        """Schedule a task at a specific datetime."""
        if isinstance(target_time, str):
            target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")

        delay = (target_time - datetime.now()).total_seconds()
        if delay < 0:
            raise ValueError("Target time is in the past")

        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    # ==================== Internal Methods ====================

    def _register_function(self, func: Callable):
        """Register a function for persistence."""
        self.job_registry[f"{func.__module__}.{func.__name__}"] = func

    def _next_job_id(self) -> str:
        """Generate next job ID (thread-safe)."""
        self._job_counter_value = next(self._job_counter)
        return f"job_{self._job_counter_value}"

    def _add_job(self, job: Job):
        """Add job to the priority queue."""
        with self.lock:
            if any(j.job_id == job.job_id for j in self.jobs):
                logger.warning(f"Job {job.job_id} already exists, skipping")
                return

            heapq.heappush(self.jobs, job)
            self._log_history(job.job_id, job.func_name, JobStatus.SCHEDULED)

            schedule_desc = job.get_schedule_description()
            if not self.quiet:
                logger.info(f"Scheduled: {job.func_name} - {schedule_desc}")

        self._save_state_async()

    def _log_history(
        self,
        job_id: str,
        func_name: str,
        status: JobStatus,
        error: Optional[str] = None,
        traceback: Optional[str] = None,
        run_count: int = 0,
        retry_count: int = 0,
        execution_time: Optional[float] = None,
    ):
        """Log job events to history."""
        history_entry = JobHistory(
            job_id=job_id,
            func_name=func_name,
            status=status.value,
            timestamp=time.time(),
            error=error,
            traceback=traceback,
            run_count=run_count,
            retry_count=retry_count,
            execution_time=execution_time,
        )

        with self.lock:
            self.history.append(history_entry)
            self._cleanup_history()

            if status == JobStatus.FAILED and error:
                self.dead_letters.append(history_entry)
                if len(self.dead_letters) > self.max_dead_letters:
                    self.dead_letters = self.dead_letters[-self.max_dead_letters :]
                self._save_dead_letters_async()

    def _cleanup_history(self):
        """Clean up old history entries based on count and age limits."""
        if self.history_retention_days > 0:
            cutoff_time = time.time() - (self.history_retention_days * 86400)
            self.history = [h for h in self.history if h.timestamp >= cutoff_time]

        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history :]

    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True
        self.stats["start_time"] = time.time()

        self._handle_missed_jobs()

        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="FastScheduler-Main",
        )
        self.thread.start()

        if not self.quiet:
            logger.info("FastScheduler started")
        self._save_state_async()

    def stop(self, wait: bool = True, timeout: int = 30):
        """Stop the scheduler gracefully."""
        if not self.running:
            return

        logger.info("Stopping scheduler...")
        self.running = False

        if wait and self.thread and self.thread.is_alive():
            self.thread.join(timeout=timeout)

        if wait:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._save_executor.shutdown(wait=True)
        else:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._save_executor.shutdown(wait=False)

        self._save_state()
        self._storage.close()

        if not self.quiet:
            logger.info("FastScheduler stopped")

    def _handle_missed_jobs(self):
        """Handle jobs that should have run while scheduler was stopped."""
        now = time.time()

        with self.lock:
            for job in self.jobs:
                if not job.catch_up:
                    continue

                if job.next_run < now and job.repeat:
                    if job.schedule_type in ["daily", "weekly", "hourly"]:
                        self._calculate_next_run(job)
                    elif job.interval:
                        missed_count = int((now - job.next_run) / job.interval)
                        if missed_count > 0:
                            if not self.quiet:
                                logger.warning(
                                    f"Job {job.func_name} missed {missed_count} runs, running now"
                                )
                            job.next_run = now

                elif job.next_run < now and not job.repeat:
                    if not self.quiet:
                        logger.warning(
                            f"One-time job {job.func_name} was missed, running now"
                        )
                    job.next_run = now

    def _calculate_next_run(self, job: Job):
        """Calculate next run time for time-based schedules."""
        if job.timezone:
            tz = ZoneInfo(job.timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()
            tz = None

        if job.schedule_type == "cron" and job.cron_expression and CRONITER_AVAILABLE:
            base_time = now if tz else datetime.now()
            cron = croniter(job.cron_expression, base_time)
            next_run = cron.get_next(datetime)
            job.next_run = next_run.timestamp()

        elif job.schedule_type == "daily" and job.schedule_time:
            hour, minute = map(int, job.schedule_time.split(":"))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(days=1)

            job.next_run = next_run.timestamp()

        elif job.schedule_type == "weekly" and job.schedule_time and job.schedule_days:
            hour, minute = map(int, job.schedule_time.split(":"))

            for i in range(8):
                check_date = now + timedelta(days=i)
                if check_date.weekday() in job.schedule_days:
                    next_run = check_date.replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    if next_run > now:
                        job.next_run = next_run.timestamp()
                        return

        elif job.schedule_type == "hourly" and job.schedule_time:
            minute = int(job.schedule_time.strip(":"))
            next_run = now.replace(minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(hours=1)

            job.next_run = next_run.timestamp()

    def _schedule_next_run(self, job: Job):
        """Calculate and set the next run time for a repeating job."""
        if job.schedule_type in ["daily", "weekly", "hourly", "cron"]:
            self._calculate_next_run(job)
        elif job.interval:
            job.next_run = time.time() + job.interval

    def _run(self):
        """Main scheduler loop - runs in background thread."""
        if not self.quiet:
            logger.info("Scheduler main loop started")

        while self.running:
            try:
                now = time.time()
                jobs_to_run = []

                with self.lock:
                    while self.jobs and self.jobs[0].next_run <= now:
                        job = heapq.heappop(self.jobs)

                        if job.paused:
                            job.next_run = time.time() + 1.0
                            heapq.heappush(self.jobs, job)
                            continue

                        jobs_to_run.append(job)
                        # Don't pre-schedule here - job will be re-scheduled
                        # in _execute_job after execution completes

                for job in jobs_to_run:
                    self._executor.submit(self._execute_job, job)

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}\n{traceback.format_exc()}")
                time.sleep(1)

        if not self.quiet:
            logger.info("Scheduler main loop stopped")

    def _execute_job(self, job: Job):
        """Execute a job with retries."""
        if job.func is None:
            logger.error(f"Job {job.func_name} has no function, skipping")
            return

        with self.lock:
            self._running_jobs.add(job.job_id)

        job.status = JobStatus.RUNNING
        job.last_run = time.time()
        job.run_count += 1

        self._log_history(
            job.job_id,
            job.func_name,
            JobStatus.RUNNING,
            run_count=job.run_count,
            retry_count=job.retry_count,
        )

        start_time = time.time()

        try:
            if asyncio.iscoroutinefunction(job.func):
                if job.timeout:
                    asyncio.run(
                        asyncio.wait_for(
                            job.func(*job.args, **job.kwargs), timeout=job.timeout
                        )
                    )
                else:
                    asyncio.run(job.func(*job.args, **job.kwargs))
            else:
                if job.timeout:
                    from concurrent.futures import TimeoutError as FuturesTimeoutError

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(job.func, *job.args, **job.kwargs)
                        try:
                            future.result(timeout=job.timeout)
                        except FuturesTimeoutError:
                            raise TimeoutError(f"Job timed out after {job.timeout}s")
                else:
                    job.func(*job.args, **job.kwargs)

            execution_time = time.time() - start_time

            with self.lock:
                self.stats["total_runs"] += 1

            self._log_history(
                job.job_id,
                job.func_name,
                JobStatus.COMPLETED,
                run_count=job.run_count,
                retry_count=job.retry_count,
                execution_time=execution_time,
            )

            if not self.quiet:
                logger.info(f"{job.func_name} completed ({execution_time:.2f}s)")

            # Schedule next run AFTER successful execution
            if job.repeat:
                job.status = JobStatus.SCHEDULED
                job.retry_count = 0
                self._schedule_next_run(job)
                with self.lock:
                    heapq.heappush(self.jobs, job)
            else:
                job.status = JobStatus.COMPLETED

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"

            with self.lock:
                self.stats["total_failures"] += 1

            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = JobStatus.SCHEDULED
                retry_delay = 2**job.retry_count
                job.next_run = time.time() + retry_delay

                with self.lock:
                    heapq.heappush(self.jobs, job)
                    self.stats["total_retries"] += 1

                if not self.quiet:
                    logger.warning(
                        f"{job.func_name} failed, retrying in {retry_delay}s "
                        f"({job.retry_count}/{job.max_retries})"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Retry {job.retry_count}/{job.max_retries}: {error_msg}",
                    traceback="".join(format_exception(e)),
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )
            else:
                job.status = JobStatus.FAILED
                if not self.quiet:
                    logger.error(
                        f"{job.func_name} failed after {job.max_retries} retries: {error_msg}"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Max retries exceeded: {error_msg}",
                    traceback="".join(format_exception(e)),
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )

                # For repeating jobs, schedule next regular run even after max retries
                if job.repeat:
                    job.retry_count = 0
                    self._schedule_next_run(job)
                    with self.lock:
                        heapq.heappush(self.jobs, job)

        finally:
            with self.lock:
                self._running_jobs.discard(job.job_id)
            self._save_state_async()

    def _save_state_async(self):
        """Save state asynchronously to avoid blocking."""
        try:
            self._save_executor.submit(self._save_state)
        except Exception as e:
            logger.error(f"Failed to queue state save: {e}")

    def _save_state(self):
        """Save state using storage backend."""
        try:
            with self.lock:
                jobs = [job.to_dict() for job in self.jobs]
                history = [h.to_dict() for h in self.history[-1000:]]

            self._storage.save_state(
                jobs=jobs,
                history=history,
                statistics=self.stats,
                job_counter=self._job_counter_value,
                scheduler_running=self.running,
            )

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _load_state(self):
        """Load state using storage backend."""
        try:
            state = self._storage.load_state()

            if state is None:
                return

            self._job_counter_value = state.get("_job_counter", 0)
            self._job_counter = itertools.count(self._job_counter_value)

            self.history = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in state.get("history", [])
            ]

            with self.lock:
                self._cleanup_history()

            self.stats.update(state.get("statistics", {}))

            job_data = state.get("jobs", [])
            restored_count = 0

            for jd in job_data:
                func_key = f"{jd['func_module']}.{jd['func_name']}"

                if func_key in self.job_registry:
                    job = Job(
                        job_id=jd["job_id"],
                        func=self.job_registry[func_key],
                        func_name=jd["func_name"],
                        func_module=jd["func_module"],
                        next_run=jd["next_run"],
                        interval=jd["interval"],
                        repeat=jd["repeat"],
                        status=JobStatus(jd["status"]),
                        created_at=jd["created_at"],
                        last_run=jd.get("last_run"),
                        run_count=jd.get("run_count", 0),
                        max_retries=jd.get("max_retries", 3),
                        retry_count=jd.get("retry_count", 0),
                        catch_up=jd.get("catch_up", True),
                        schedule_type=jd.get("schedule_type", "interval"),
                        schedule_time=jd.get("schedule_time"),
                        schedule_days=jd.get("schedule_days"),
                        timeout=jd.get("timeout"),
                        paused=jd.get("paused", False),
                        timezone=jd.get("timezone"),
                        cron_expression=jd.get("cron_expression"),
                    )
                    heapq.heappush(self.jobs, job)
                    restored_count += 1

            if restored_count > 0:
                if not self.quiet:
                    logger.info(f"Loaded state: {restored_count} jobs restored")

        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    def _load_dead_letters(self):
        """Load dead letter queue using storage backend."""
        try:
            dead_letter_dicts = self._storage.load_dead_letters()

            self.dead_letters = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in dead_letter_dicts
            ]

            if len(self.dead_letters) > self.max_dead_letters:
                self.dead_letters = self.dead_letters[-self.max_dead_letters :]

            if not self.quiet and self.dead_letters:
                logger.info(f"Loaded {len(self.dead_letters)} dead letter entries")

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")

    def _save_dead_letters_async(self):
        """Save dead letters asynchronously."""
        try:
            self._save_executor.submit(self._save_dead_letters)
        except Exception as e:
            logger.error(f"Failed to queue dead letters save: {e}")

    def _save_dead_letters(self):
        """Save dead letter queue using storage backend."""
        try:
            with self.lock:
                dead_letters = [dl.to_dict() for dl in self.dead_letters]

            self._storage.save_dead_letters(dead_letters, self.max_dead_letters)

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def get_dead_letters(self, limit: int = 100) -> List[Dict]:
        """Get dead letter queue entries (failed jobs)."""
        with self.lock:
            return [dl.to_dict() for dl in self.dead_letters[-limit:][::-1]]

    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries."""
        with self.lock:
            count = len(self.dead_letters)
            self.dead_letters = []
        self._save_dead_letters_async()
        return count

    # ==================== Monitoring & Management ====================

    def get_jobs(self) -> List[Dict]:
        """Get all scheduled jobs."""
        with self.lock:
            return [
                {
                    "job_id": job.job_id,
                    "func_name": job.func_name,
                    "status": (
                        JobStatus.RUNNING.value
                        if job.job_id in self._running_jobs
                        else ("paused" if job.paused else job.status.value)
                    ),
                    "schedule": job.get_schedule_description(),
                    "next_run": datetime.fromtimestamp(job.next_run).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "next_run_in": max(0, job.next_run - time.time()),
                    "run_count": job.run_count,
                    "retry_count": job.retry_count,
                    "paused": job.paused,
                    "last_run": (
                        datetime.fromtimestamp(job.last_run).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if job.last_run
                        else None
                    ),
                }
                for job in sorted(self.jobs, key=lambda j: j.next_run)
            ]

    def get_history(
        self, func_name: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """Get job history."""
        with self.lock:
            history = (
                self.history
                if not func_name
                else [h for h in self.history if h.func_name == func_name]
            )
            return [h.to_dict() for h in history[-limit:]]

    def get_statistics(self) -> Dict:
        """Get statistics."""
        with self.lock:
            stats = self.stats.copy()

            if stats["start_time"]:
                stats["uptime_seconds"] = time.time() - stats["start_time"]
                stats["uptime_readable"] = str(
                    timedelta(seconds=int(stats["uptime_seconds"]))
                )

            job_stats = defaultdict(
                lambda: {"completed": 0, "failed": 0, "total_runs": 0}
            )

            for event in self.history:
                if event.status in ["completed", "failed"]:
                    job_stats[event.func_name]["total_runs"] += 1
                    job_stats[event.func_name][event.status] += 1

            stats["per_job"] = dict(job_stats)
            stats["active_jobs"] = len(self.jobs)

            return stats

    def cancel_job(self, job_id: str) -> bool:
        """Cancel and remove a scheduled job by ID."""
        with self.lock:
            for i, job in enumerate(self.jobs):
                if job.job_id == job_id:
                    self.jobs.pop(i)
                    heapq.heapify(self.jobs)
                    self._log_history(job_id, job.func_name, JobStatus.COMPLETED)
                    if not self.quiet:
                        logger.info(f"Cancelled job: {job.func_name} ({job_id})")
                    self._save_state_async()
                    return True
        return False

    def cancel_job_by_name(self, func_name: str) -> int:
        """Cancel all jobs with the given function name."""
        with self.lock:
            cancelled = 0
            jobs_to_keep = []
            for job in self.jobs:
                if job.func_name == func_name:
                    self._log_history(job.job_id, job.func_name, JobStatus.COMPLETED)
                    cancelled += 1
                else:
                    jobs_to_keep.append(job)

            if cancelled > 0:
                self.jobs = jobs_to_keep
                heapq.heapify(self.jobs)
                if not self.quiet:
                    logger.info(f"Cancelled {cancelled} job(s) with name: {func_name}")
                self._save_state_async()

            return cancelled

    def pause_job(self, job_id: str) -> bool:
        """Pause a job (it will remain in the queue but won't execute)."""
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = True
                    if not self.quiet:
                        logger.info(f"Paused job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = False
                    if not self.quiet:
                        logger.info(f"Resumed job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True
        return False

    def run_job_now(self, job_id: str) -> bool:
        """Trigger immediate execution of a job (useful for debugging)."""
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    if job.job_id in self._running_jobs:
                        logger.warning(f"Job {job_id} is already running")
                        return False
                    if not self.quiet:
                        logger.info(f"Manually triggered: {job.func_name} ({job_id})")
                    self._executor.submit(self._execute_job, job)
                    return True
        return False

    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get a specific job by ID."""
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    return {
                        "job_id": job.job_id,
                        "func_name": job.func_name,
                        "status": (
                            JobStatus.RUNNING.value
                            if job.job_id in self._running_jobs
                            else ("paused" if job.paused else job.status.value)
                        ),
                        "schedule": job.get_schedule_description(),
                        "next_run": datetime.fromtimestamp(job.next_run).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "next_run_in": max(0, job.next_run - time.time()),
                        "run_count": job.run_count,
                        "retry_count": job.retry_count,
                        "paused": job.paused,
                        "timeout": job.timeout,
                        "last_run": (
                            datetime.fromtimestamp(job.last_run).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if job.last_run
                            else None
                        ),
                    }
        return None

    def print_status(self):
        """Print simple status."""
        status = "RUNNING" if self.running else "STOPPED"
        stats = self.get_statistics()
        jobs = self.get_jobs()

        print(f"\nFastScheduler [{status}]")
        if stats.get("uptime_readable"):
            print(f"Uptime: {stats['uptime_readable']}")
        print(
            f"Jobs: {len(jobs)} | Runs: {stats['total_runs']} | Failures: {stats['total_failures']}"
        )

        if jobs:
            print("\nActive jobs:")
            for job in jobs[:5]:
                next_in = job["next_run_in"]
                if next_in > 86400:
                    next_in_str = f"{int(next_in/86400)}d"
                elif next_in > 3600:
                    next_in_str = f"{int(next_in/3600)}h"
                elif next_in > 60:
                    next_in_str = f"{int(next_in/60)}m"
                elif next_in > 0:
                    next_in_str = f"{int(next_in)}s"
                else:
                    next_in_str = "now"

                status_char = {
                    "scheduled": " ",
                    "running": ">",
                    "completed": "+",
                    "failed": "x",
                }.get(job["status"], " ")

                print(
                    f"  [{status_char}] {job['func_name']:<20} {job['schedule']:<20} next: {next_in_str}"
                )

            if len(jobs) > 5:
                print(f"      ... and {len(jobs) - 5} more")
        print()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop(wait=True)
