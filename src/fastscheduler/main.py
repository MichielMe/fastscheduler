import asyncio
import heapq
import json
import logging
import re
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - FastScheduler - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("FastScheduler")


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

    def to_dict(self) -> Dict:
        """Serialize job for persistence"""
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
        }

    def get_schedule_description(self) -> str:
        """Get human-readable schedule description"""
        if self.schedule_type == "daily" and self.schedule_time:
            return f"Daily at {self.schedule_time}"
        elif (
            self.schedule_type == "weekly" and self.schedule_time and self.schedule_days
        ):
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_names = [days[d] for d in self.schedule_days]
            return f"Every {', '.join(day_names)} at {self.schedule_time}"
        elif self.schedule_type == "hourly" and self.schedule_time:
            return f"Hourly at {self.schedule_time}"
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


class FastScheduler:
    """
    FastScheduler - Simple, powerful, persistent task scheduler with async support
    """

    def __init__(
        self,
        state_file: str = "fastscheduler_state.json",
        auto_start: bool = False,
        quiet: bool = False,
    ):
        self.state_file = Path(state_file)
        self.jobs: List[Job] = []
        self.job_registry: Dict[str, Callable] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()  # Use RLock for re-entrant locking
        self._job_counter = 0
        self.history: List[JobHistory] = []
        self.max_history = 10000
        self.quiet = quiet  # Quiet mode for less verbose output
        self._running_jobs: set = set()  # Track currently executing jobs
        self._executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="FastScheduler-Worker"
        )
        self._save_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="FastScheduler-Saver"
        )

        # Statistics
        self.stats = {
            "total_runs": 0,
            "total_failures": 0,
            "total_retries": 0,
            "start_time": None,
        }

        # Load previous state
        self._load_state()

        if auto_start:
            self.start()

    # ==================== User-Friendly Scheduling API ====================

    def every(self, interval: Union[int, float]) -> "IntervalScheduler":
        """Schedule a task to run every X seconds/minutes/hours/days"""
        return IntervalScheduler(self, interval)

    @property
    def daily(self) -> "DailyScheduler":
        """Schedule a task to run daily at a specific time"""
        return DailyScheduler(self)

    @property
    def weekly(self) -> "WeeklyScheduler":
        """Schedule a task to run weekly on specific days"""
        return WeeklyScheduler(self)

    @property
    def hourly(self) -> "HourlyScheduler":
        """Schedule a task to run hourly at a specific minute"""
        return HourlyScheduler(self)

    def once(self, delay: Union[int, float]) -> "OnceScheduler":
        """Schedule a one-time task"""
        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    def at(self, target_time: Union[datetime, str]) -> "OnceScheduler":
        """Schedule a task at a specific datetime"""
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
        """Register a function for persistence"""
        self.job_registry[f"{func.__module__}.{func.__name__}"] = func

    def _add_job(self, job: Job):
        """Add job to the priority queue"""
        with self.lock:
            if any(j.job_id == job.job_id for j in self.jobs):
                logger.warning(f"Job {job.job_id} already exists, skipping")
                return

            heapq.heappush(self.jobs, job)
            self._log_history(job.job_id, job.func_name, JobStatus.SCHEDULED)

            schedule_desc = job.get_schedule_description()
            if not self.quiet:
                logger.info(f"Scheduled: {job.func_name} - {schedule_desc}")

        # Save state asynchronously
        self._save_state_async()

    def _log_history(
        self,
        job_id: str,
        func_name: str,
        status: JobStatus,
        error: Optional[str] = None,
        run_count: int = 0,
        retry_count: int = 0,
        execution_time: Optional[float] = None,
    ):
        """Log job events to history"""
        history_entry = JobHistory(
            job_id=job_id,
            func_name=func_name,
            status=status.value,
            timestamp=time.time(),
            error=error,
            run_count=run_count,
            retry_count=retry_count,
            execution_time=execution_time,
        )

        with self.lock:
            self.history.append(history_entry)
            if len(self.history) > self.max_history:
                self.history = self.history[-self.max_history :]

    def start(self):
        """Start the scheduler"""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True
        self.stats["start_time"] = time.time()

        self._handle_missed_jobs()

        # Start in daemon thread so it doesn't block
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,  # Daemon thread won't block program exit
            name="FastScheduler-Main",
        )
        self.thread.start()

        if not self.quiet:
            logger.info("FastScheduler started")
        self._save_state_async()

    def stop(self, wait: bool = True, timeout: int = 30):
        """Stop the scheduler gracefully"""
        if not self.running:
            return

        logger.info("Stopping scheduler...")
        self.running = False

        if wait and self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

        # Shutdown executors
        if wait:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._save_executor.shutdown(wait=True)
        else:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._save_executor.shutdown(wait=False)

        self._save_state()
        if not self.quiet:
            logger.info("FastScheduler stopped")

    def _handle_missed_jobs(self):
        """Handle jobs that should have run while scheduler was stopped"""
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
        """Calculate next run time for time-based schedules"""
        now = datetime.now()

        if job.schedule_type == "daily" and job.schedule_time:
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

    def _run(self):
        """Main scheduler loop - runs in background thread"""
        if not self.quiet:
            logger.info("Scheduler main loop started")

        while self.running:
            try:
                now = time.time()
                jobs_to_run = []

                # Collect jobs to run (minimize lock time)
                with self.lock:
                    while self.jobs and self.jobs[0].next_run <= now:
                        job = heapq.heappop(self.jobs)
                        jobs_to_run.append(job)

                        # Reschedule if recurring
                        if job.repeat:
                            if job.schedule_type in ["daily", "weekly", "hourly"]:
                                self._calculate_next_run(job)
                            elif job.interval:
                                job.next_run = time.time() + job.interval

                            job.status = JobStatus.SCHEDULED
                            job.retry_count = 0
                            heapq.heappush(self.jobs, job)

                # Execute jobs outside of lock
                for job in jobs_to_run:
                    self._executor.submit(self._execute_job, job)

                # Sleep to avoid busy-waiting
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}\n{traceback.format_exc()}")
                time.sleep(1)  # Sleep on error to prevent tight loop

        if not self.quiet:
            logger.info("Scheduler main loop stopped")

    def _execute_job(self, job: Job):
        """Execute a job with retries"""
        if job.func is None:
            logger.error(f"Job {job.func_name} has no function, skipping")
            return

        # Mark job as running
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
            # Check if function is async
            if asyncio.iscoroutinefunction(job.func):
                # Run async function
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                loop.run_until_complete(job.func(*job.args, **job.kwargs))
            else:
                # Run sync function
                job.func(*job.args, **job.kwargs)

            execution_time = time.time() - start_time

            # For recurring jobs, status should be SCHEDULED (already rescheduled)
            # For one-time jobs, status should be COMPLETED
            if job.repeat:
                job.status = JobStatus.SCHEDULED
            else:
                job.status = JobStatus.COMPLETED

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
                        f"{job.func_name} failed, retrying in {retry_delay}s ({job.retry_count}/{job.max_retries})"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Retry {job.retry_count}/{job.max_retries}: {error_msg}",
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
                    error=f"Max retries: {error_msg}",
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )

        finally:
            # Mark job as no longer running
            with self.lock:
                self._running_jobs.discard(job.job_id)
            self._save_state_async()

    def _save_state_async(self):
        """Save state asynchronously to avoid blocking"""
        try:
            self._save_executor.submit(self._save_state)
        except Exception as e:
            logger.error(f"Failed to queue state save: {e}")

    def _save_state(self):
        """Save state to disk"""
        try:
            state = {
                "version": "1.0",
                "metadata": {
                    "last_save": time.time(),
                    "last_save_readable": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "scheduler_running": self.running,
                },
                "jobs": [job.to_dict() for job in self.jobs],
                "history": [h.to_dict() for h in self.history[-1000:]],
                "statistics": self.stats,
                "_job_counter": self._job_counter,
            }

            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(state, f, indent=2)
            temp_file.replace(self.state_file)

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _load_state(self):
        """Load state from disk"""
        if not self.state_file.exists():
            if not self.quiet:
                logger.info("No previous state found, starting fresh")
            return

        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)

            self._job_counter = state.get("_job_counter", 0)

            self.history = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in state.get("history", [])
            ]

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
                    )
                    heapq.heappush(self.jobs, job)
                    restored_count += 1

            if restored_count > 0:
                if not self.quiet:
                    logger.info(f"Loaded state: {restored_count} jobs restored")

        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    # ==================== Monitoring & Management ====================

    def get_jobs(self) -> List[Dict]:
        """Get all scheduled jobs"""
        with self.lock:
            return [
                {
                    "job_id": job.job_id,
                    "func_name": job.func_name,
                    "status": (
                        JobStatus.RUNNING.value
                        if job.job_id in self._running_jobs
                        else job.status.value
                    ),
                    "schedule": job.get_schedule_description(),
                    "next_run": datetime.fromtimestamp(job.next_run).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "next_run_in": max(0, job.next_run - time.time()),
                    "run_count": job.run_count,
                    "retry_count": job.retry_count,
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
        """Get job history"""
        with self.lock:
            history = (
                self.history
                if not func_name
                else [h for h in self.history if h.func_name == func_name]
            )
            return [h.to_dict() for h in history[-limit:]]

    def get_statistics(self) -> Dict:
        """Get statistics"""
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

    def print_status(self):
        """Print simple status"""
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


# ==================== Schedulers ====================


class IntervalScheduler:
    def __init__(self, scheduler: FastScheduler, interval: float):
        self.scheduler = scheduler
        self.interval = interval
        self._max_retries = 3
        self._catch_up = True

    @property
    def seconds(self):
        return self

    @property
    def minutes(self):
        self.interval *= 60
        return self

    @property
    def hours(self):
        self.interval *= 3600
        return self

    @property
    def days(self):
        self.interval *= 86400
        return self

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def no_catch_up(self):
        self._catch_up = False
        return self

    def do(self, func: Callable, *args, **kwargs):
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.interval,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            interval=self.interval,
            job_id=f"job_{self.scheduler._job_counter}",
            args=args,
            kwargs=kwargs,
            repeat=True,
            max_retries=self._max_retries,
            catch_up=self._catch_up,
            schedule_type="interval",
        )
        self.scheduler._job_counter += 1
        self.scheduler._add_job(job)
        return func

    def __call__(self, func: Callable):
        return self.do(func)


class DailyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._max_retries = 3

    def at(self, time_str: str):
        return DailyAtScheduler(self.scheduler, time_str, self._max_retries)

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self


class DailyAtScheduler:
    def __init__(self, scheduler: FastScheduler, time_str: str, max_retries: int):
        self.scheduler = scheduler
        self.time_str = time_str
        self._max_retries = max_retries

        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format (24-hour)")

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        now = datetime.now()
        hour, minute = map(int, self.time_str.split(":"))
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=f"job_{self.scheduler._job_counter}",
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="daily",
            schedule_time=self.time_str,
        )
        self.scheduler._job_counter += 1
        self.scheduler._add_job(job)
        return func


class WeeklyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._days = []
        self._max_retries = 3

    @property
    def monday(self):
        self._days = [0]
        return self

    @property
    def tuesday(self):
        self._days = [1]
        return self

    @property
    def wednesday(self):
        self._days = [2]
        return self

    @property
    def thursday(self):
        self._days = [3]
        return self

    @property
    def friday(self):
        self._days = [4]
        return self

    @property
    def saturday(self):
        self._days = [5]
        return self

    @property
    def sunday(self):
        self._days = [6]
        return self

    @property
    def weekdays(self):
        self._days = [0, 1, 2, 3, 4]
        return self

    @property
    def weekends(self):
        self._days = [5, 6]
        return self

    def on(self, days: List[int]):
        self._days = days
        return self

    def at(self, time_str: str):
        if not self._days:
            raise ValueError("Must specify days before time")
        return WeeklyAtScheduler(
            self.scheduler, self._days, time_str, self._max_retries
        )

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self


class WeeklyAtScheduler:
    def __init__(
        self, scheduler: FastScheduler, days: List[int], time_str: str, max_retries: int
    ):
        self.scheduler = scheduler
        self.days = days
        self.time_str = time_str
        self._max_retries = max_retries

        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format")

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        now = datetime.now()
        hour, minute = map(int, self.time_str.split(":"))

        next_run = None
        for i in range(8):
            check_date = now + timedelta(days=i)
            if check_date.weekday() in self.days:
                candidate = check_date.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                )
                if candidate > now:
                    next_run = candidate
                    break

        if not next_run:
            next_run = now + timedelta(days=7)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=f"job_{self.scheduler._job_counter}",
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="weekly",
            schedule_time=self.time_str,
            schedule_days=self.days,
        )
        self.scheduler._job_counter += 1
        self.scheduler._add_job(job)
        return func


class HourlyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._max_retries = 3

    def at(self, minute_str: str):
        return HourlyAtScheduler(self.scheduler, minute_str, self._max_retries)

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self


class HourlyAtScheduler:
    def __init__(self, scheduler: FastScheduler, minute_str: str, max_retries: int):
        self.scheduler = scheduler
        self.minute_str = minute_str
        self._max_retries = max_retries

        if not re.match(r"^:\d{2}$", minute_str):
            raise ValueError("Minute must be in :MM format")

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        now = datetime.now()
        minute = int(self.minute_str.strip(":"))
        next_run = now.replace(minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(hours=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=f"job_{self.scheduler._job_counter}",
            interval=3600,
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="hourly",
            schedule_time=self.minute_str,
        )
        self.scheduler._job_counter += 1
        self.scheduler._add_job(job)
        return func


class OnceScheduler:
    def __init__(self, scheduler: FastScheduler, delay: float):
        self.scheduler = scheduler
        self.delay = delay
        self._decorator_mode = False
        self._max_retries = 3

    @property
    def seconds(self):
        return self

    @property
    def minutes(self):
        self.delay *= 60
        return self

    @property
    def hours(self):
        self.delay *= 3600
        return self

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def do(self, func: Callable, *args, **kwargs):
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.delay,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=f"job_{self.scheduler._job_counter}",
            args=args,
            kwargs=kwargs,
            repeat=False,
            max_retries=self._max_retries,
        )
        self.scheduler._job_counter += 1
        self.scheduler._add_job(job)

        if self._decorator_mode:
            return func
        return job

    def __call__(self, func: Callable):
        return self.do(func)
