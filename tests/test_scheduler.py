import asyncio
import time

import pytest

from fastscheduler import FastScheduler
from fastscheduler.main import Job


@pytest.fixture
def temp_state_file(tmp_path):
    """Provide temporary state file path"""
    return tmp_path / "test_scheduler.json"


@pytest.fixture
def scheduler(temp_state_file):
    """Create a scheduler instance for testing"""
    sched = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)
    yield sched
    if sched.running:
        sched.stop()


class TestBasicScheduling:
    """Test basic scheduling functionality"""

    def test_scheduler_initialization(self, scheduler):
        assert not scheduler.running
        assert scheduler.state_file is not None
        assert scheduler._executor is not None

    def test_every_seconds_decorator(self, scheduler):
        """Test scheduling with every().seconds"""
        executed = []

        @scheduler.every(1).seconds
        def test_job():
            executed.append(time.time())

        assert len(scheduler.jobs) == 1
        job = scheduler.jobs[0]
        assert job.interval == 1.0
        assert job.repeat is True

    def test_every_minutes_decorator(self, scheduler):
        """Test scheduling with every().minutes"""

        @scheduler.every(2).minutes
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.interval == 120.0

    def test_every_hours_decorator(self, scheduler):
        """Test scheduling with every().hours"""

        @scheduler.every(3).hours
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.interval == 10800.0

    def test_every_days_decorator(self, scheduler):
        """Test scheduling with every().days"""

        @scheduler.every(1).days
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.interval == 86400.0

    def test_daily_at_decorator(self, scheduler):
        """Test daily scheduling at specific time"""

        @scheduler.daily.at("14:30")
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "daily"
        assert job.schedule_time == "14:30"
        assert job.repeat is True

    def test_hourly_at_decorator(self, scheduler):
        """Test hourly scheduling at specific minute"""

        @scheduler.hourly.at(":30")
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "hourly"
        assert job.schedule_time == ":30"

    def test_weekly_decorator(self, scheduler):
        """Test weekly scheduling"""

        @scheduler.weekly.monday.at("09:00")
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "weekly"
        assert job.schedule_time == "09:00"
        assert 0 in job.schedule_days  # Monday

    def test_once_decorator(self, scheduler):
        """Test one-time scheduling"""

        @scheduler.once(10).seconds
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.repeat is False
        assert job.next_run > time.time()

    def test_job_with_retries(self, scheduler):
        """Test job with retry configuration"""

        @scheduler.every(10).seconds.retries(5)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.max_retries == 5

    def test_job_no_catch_up(self, scheduler):
        """Test job with catch_up disabled"""

        @scheduler.every(10).seconds.no_catch_up()
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.catch_up is False


class TestJobExecution:
    """Test job execution"""

    def test_sync_job_execution(self, scheduler):
        """Test synchronous job execution"""
        executed = []

        @scheduler.every(0.1).seconds
        def test_job():
            executed.append(1)

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        assert len(executed) >= 2

    def test_async_job_execution(self, scheduler):
        """Test asynchronous job execution"""
        executed = []

        @scheduler.every(0.1).seconds
        async def test_job():
            executed.append(1)
            await asyncio.sleep(0.01)

        scheduler.start()
        time.sleep(0.4)
        scheduler.stop()

        assert len(executed) >= 2

    def test_job_with_args(self, scheduler):
        """Test job execution with arguments"""
        results = []

        def test_job(x, y):
            results.append(x + y)

        scheduler.every(0.1).seconds.do(test_job, 2, 3)

        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        assert 5 in results

    def test_job_with_kwargs(self, scheduler):
        """Test job execution with keyword arguments"""
        results = []

        def test_job(x=0, y=0):
            results.append(x * y)

        scheduler.every(0.1).seconds.do(test_job, x=3, y=4)

        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        assert 12 in results

    def test_job_failure_retry(self, scheduler):
        """Test job retry on failure"""
        attempts = []

        @scheduler.every(0.1).seconds.retries(3)
        def failing_job():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("Intentional error")
            return "success"

        scheduler.start()
        time.sleep(0.5)
        scheduler.stop()

        # Should have attempted at least once
        assert len(attempts) >= 1

    def test_once_job_runs_once(self, scheduler):
        """Test that once() jobs only run once"""
        counter = []

        @scheduler.once(0.1).seconds
        def test_job():
            counter.append(1)

        scheduler.start()
        time.sleep(0.5)
        scheduler.stop()

        assert len(counter) == 1


class TestStatePersistence:
    """Test state persistence"""

    def test_state_save_and_load(self, temp_state_file):
        """Test saving and loading scheduler state"""
        # Create scheduler and add jobs
        scheduler1 = FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        )

        @scheduler1.every(0.1).seconds
        def test_job():
            pass

        # Start and stop to trigger state save and execution
        scheduler1.start()
        time.sleep(0.3)
        scheduler1.stop()

        # Verify state file was created
        assert temp_state_file.exists()

        # Load state in new scheduler - should load history
        scheduler2 = FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        )

        # Job functions can't be serialized, but history should persist
        assert len(scheduler2.history) > 0
        scheduler2.stop()

    def test_job_history_persistence(self, temp_state_file):
        """Test job history is persisted"""
        scheduler1 = FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        )

        @scheduler1.every(0.1).seconds
        def test_job():
            pass

        scheduler1.start()
        time.sleep(0.3)
        scheduler1.stop()

        # Verify history exists
        assert len(scheduler1.history) > 0

        # Load in new scheduler
        scheduler2 = FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        )
        assert len(scheduler2.history) > 0
        scheduler2.stop()

    def test_state_file_creation(self, temp_state_file):
        """Test state file is created"""
        assert not temp_state_file.exists()

        scheduler = FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        )

        @scheduler.every(10).seconds
        def test_job():
            pass

        scheduler._save_state()
        scheduler.stop()

        assert temp_state_file.exists()


class TestStatistics:
    """Test statistics and monitoring"""

    def test_get_jobs(self, scheduler):
        """Test get_jobs returns correct information"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["func_name"] == "test_job"
        assert "schedule" in jobs[0]

    def test_get_statistics(self, scheduler):
        """Test statistics collection"""

        @scheduler.every(0.1).seconds
        def test_job():
            pass

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        stats = scheduler.get_statistics()
        assert "active_jobs" in stats
        assert "total_runs" in stats
        assert "per_job" in stats
        assert stats["active_jobs"] == 1
        assert stats["total_runs"] > 0

    def test_get_history(self, scheduler):
        """Test history retrieval"""

        @scheduler.every(0.1).seconds
        def test_job():
            pass

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        history = scheduler.get_history(limit=5)
        assert len(history) > 0
        assert "job_id" in history[0]
        assert "func_name" in history[0]
        assert "status" in history[0]

    def test_history_by_func_name(self, scheduler):
        """Test filtering history by func_name"""

        @scheduler.every(0.1).seconds
        def test_job():
            pass

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        history = scheduler.get_history(func_name="test_job", limit=10)

        assert len(history) > 0
        for entry in history:
            assert entry["func_name"] == "test_job"

    def test_history_error_traceback(self, scheduler):
        """Test job traceback logging on failure"""
        attempts = []

        @scheduler.every(0.1).seconds.retries(3)
        def failing_job():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("Intentional error")
            return "success"

        scheduler.start()
        time.sleep(0.5)
        scheduler.stop()

        # Should have history with at least one error, with stack trace string
        history = scheduler.get_history()
        errors = [item for item in history if item["error"] is not None]
        assert len(errors) > 0
        assert "traceback" in errors[0]
        assert 'raise ValueError("Intentional error")' in errors[0]["traceback"]


class TestSchedulerLifecycle:
    """Test scheduler lifecycle management"""

    def test_start_stop(self, scheduler):
        """Test starting and stopping scheduler"""
        assert not scheduler.running

        scheduler.start()
        assert scheduler.running

        scheduler.stop()
        assert not scheduler.running

    def test_context_manager(self, temp_state_file):
        """Test using scheduler as context manager"""
        with FastScheduler(
            state_file=str(temp_state_file), quiet=True, auto_start=False
        ) as sched:
            assert sched is not None

            # Scheduler should be usable
            @sched.every(10).seconds
            def test_job():
                pass

        # Should have cleaned up
        assert not sched.running

    def test_double_start(self, scheduler):
        """Test starting scheduler twice doesn't cause issues"""
        scheduler.start()
        scheduler.start()  # Should handle gracefully
        assert scheduler.running
        scheduler.stop()

    def test_stop_timeout(self, scheduler):
        """Test stop with timeout"""

        @scheduler.every(0.1).seconds
        async def long_job():
            await asyncio.sleep(10)

        scheduler.start()
        time.sleep(0.2)

        start = time.time()
        scheduler.stop(timeout=2)
        elapsed = time.time() - start

        # Should stop within reasonable time
        assert elapsed < 15

    def test_stop_timeout_parameter_passed_to_thread_join(self, scheduler):
        """Test that stop() passes the timeout parameter to thread.join()"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        scheduler.start()
        assert scheduler.running
        time.sleep(0.1)

        original_join = scheduler.thread.join
        captured_timeout = []

        def patched_join(timeout=None):
            captured_timeout.append(timeout)
            return original_join(timeout=0.1)

        scheduler.thread.join = patched_join

        scheduler.stop(wait=True, timeout=3)

        assert len(captured_timeout) == 1, "thread.join() should have been called once"
        assert captured_timeout[0] == 3, (
            f"stop(timeout=3) should pass 3 to thread.join(), "
            f"but passed {captured_timeout[0]} instead"
        )


class TestJobScheduleDescriptions:
    """Test human-readable schedule descriptions"""

    def test_interval_description(self, scheduler):
        """Test interval schedule descriptions"""

        @scheduler.every(30).seconds
        def job1():
            pass

        @scheduler.every(5).minutes
        def job2():
            pass

        @scheduler.every(2).hours
        def job3():
            pass

        jobs = scheduler.get_jobs()
        assert "30 seconds" in jobs[0]["schedule"]
        assert "5 minutes" in jobs[1]["schedule"]
        assert "2 hours" in jobs[2]["schedule"]

    def test_daily_description(self, scheduler):
        """Test daily schedule description"""

        @scheduler.daily.at("14:30")
        def test_job():
            pass

        jobs = scheduler.get_jobs()
        assert "Daily at 14:30" in jobs[0]["schedule"]

    def test_weekly_description(self, scheduler):
        """Test weekly schedule description"""

        @scheduler.weekly.monday.at("09:00")
        def test_job():
            pass

        jobs = scheduler.get_jobs()
        assert "Mon" in jobs[0]["schedule"]
        assert "09:00" in jobs[0]["schedule"]


class TestJobManagement:
    """Test job cancellation, pause, and resume"""

    def test_cancel_job_by_id(self, scheduler):
        """Test cancelling a job by its ID"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        assert len(scheduler.jobs) == 1
        job_id = scheduler.jobs[0].job_id

        result = scheduler.cancel_job(job_id)
        assert result is True
        assert len(scheduler.jobs) == 0

    def test_cancel_job_not_found(self, scheduler):
        """Test cancelling a non-existent job"""
        result = scheduler.cancel_job("nonexistent_job")
        assert result is False

    def test_cancel_job_by_name(self, scheduler):
        """Test cancelling jobs by function name"""

        @scheduler.every(10).seconds
        def task_a():
            pass

        @scheduler.every(20).seconds
        def task_a():  # noqa: F811 - Intentional redefinition to test cancel_job_by_name
            pass

        @scheduler.every(30).seconds
        def task_b():
            pass

        assert len(scheduler.jobs) == 3

        cancelled = scheduler.cancel_job_by_name("task_a")
        assert cancelled == 2
        assert len(scheduler.jobs) == 1
        assert scheduler.jobs[0].func_name == "task_b"

    def test_cancel_job_by_name_not_found(self, scheduler):
        """Test cancelling jobs with non-existent name"""
        cancelled = scheduler.cancel_job_by_name("nonexistent")
        assert cancelled == 0

    def test_pause_job(self, scheduler):
        """Test pausing a job"""
        executed = []

        @scheduler.every(0.1).seconds
        def test_job():
            executed.append(1)

        job_id = scheduler.jobs[0].job_id

        scheduler.start()
        time.sleep(0.25)

        # Pause the job
        result = scheduler.pause_job(job_id)
        assert result is True

        executions_before_pause = len(executed)
        time.sleep(0.3)

        # Should not have executed more while paused
        assert len(executed) == executions_before_pause

        scheduler.stop()

    def test_resume_job(self, scheduler):
        """Test resuming a paused job - verify the API works"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        job_id = scheduler.jobs[0].job_id

        # Pause the job
        scheduler.pause_job(job_id)
        assert scheduler.jobs[0].paused is True

        # Resume the job
        result = scheduler.resume_job(job_id)
        assert result is True
        assert scheduler.jobs[0].paused is False

    def test_resume_job_execution(self, scheduler):
        """Test that resumed jobs actually execute"""
        executed = []

        @scheduler.every(0.2).seconds
        def test_job():
            executed.append(time.time())

        job_id = scheduler.jobs[0].job_id

        scheduler.start()
        time.sleep(0.5)  # Let it run a couple times

        initial_count = len(executed)
        assert initial_count >= 1

        # Pause and wait a bit for any in-flight job to complete
        scheduler.pause_job(job_id)
        time.sleep(0.3)

        paused_count = len(executed)
        time.sleep(0.5)

        # No more executions while paused (allow at most 1 more that was in-flight)
        assert len(executed) <= paused_count + 1
        final_paused_count = len(executed)

        # Resume and wait for execution
        scheduler.resume_job(job_id)
        time.sleep(1.5)  # Give plenty of time for the job to run again

        # Should have more executions after resume
        assert len(executed) > final_paused_count

        scheduler.stop()

    def test_pause_nonexistent_job(self, scheduler):
        """Test pausing a non-existent job"""
        result = scheduler.pause_job("nonexistent")
        assert result is False

    def test_resume_nonexistent_job(self, scheduler):
        """Test resuming a non-existent job"""
        result = scheduler.resume_job("nonexistent")
        assert result is False

    def test_get_job(self, scheduler):
        """Test getting a specific job by ID"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        job_id = scheduler.jobs[0].job_id

        job_info = scheduler.get_job(job_id)
        assert job_info is not None
        assert job_info["job_id"] == job_id
        assert job_info["func_name"] == "test_job"
        assert "schedule" in job_info
        assert "next_run" in job_info
        assert "paused" in job_info

    def test_get_job_not_found(self, scheduler):
        """Test getting a non-existent job"""
        job_info = scheduler.get_job("nonexistent")
        assert job_info is None

    def test_get_job_shows_paused_status(self, scheduler):
        """Test that get_job shows paused status"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        job_id = scheduler.jobs[0].job_id
        scheduler.pause_job(job_id)

        job_info = scheduler.get_job(job_id)
        assert job_info["paused"] is True
        assert job_info["status"] == "paused"

    def test_get_jobs_shows_paused_status(self, scheduler):
        """Test that get_jobs (plural) shows paused status"""

        @scheduler.every(10).seconds
        def test_job():
            pass

        job_id = scheduler.jobs[0].job_id
        scheduler.pause_job(job_id)

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["paused"] is True
        assert jobs[0]["status"] == "paused"


class TestJobTimeout:
    """Test job timeout functionality"""

    def test_timeout_configuration_interval(self, scheduler):
        """Test timeout configuration on interval jobs"""

        @scheduler.every(10).seconds.timeout(30)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.timeout == 30

    def test_timeout_configuration_daily(self, scheduler):
        """Test timeout configuration on daily jobs"""

        @scheduler.daily.at("14:30").timeout(60)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.timeout == 60

    def test_timeout_configuration_weekly(self, scheduler):
        """Test timeout configuration on weekly jobs"""

        @scheduler.weekly.monday.at("09:00").timeout(120)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.timeout == 120

    def test_timeout_configuration_hourly(self, scheduler):
        """Test timeout configuration on hourly jobs"""

        @scheduler.hourly.at(":30").timeout(45)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.timeout == 45

    def test_timeout_configuration_once(self, scheduler):
        """Test timeout configuration on one-time jobs"""

        @scheduler.once(60).seconds.timeout(30)
        def test_job():
            pass

        job = scheduler.jobs[0]
        assert job.timeout == 30

    def test_sync_job_timeout_triggers(self, scheduler):
        """Test that sync job timeout actually triggers"""
        executed = []

        @scheduler.every(0.1).seconds.timeout(0.2).retries(0)
        def slow_job():
            executed.append(1)
            time.sleep(1)  # Will exceed 0.2s timeout

        scheduler.start()
        time.sleep(0.5)
        scheduler.stop()

        # Job should have been attempted but failed due to timeout
        assert len(executed) >= 1
        stats = scheduler.get_statistics()
        assert stats["total_failures"] >= 1

    def test_async_job_timeout_triggers(self, scheduler):
        """Test that async job timeout actually triggers"""
        executed = []

        @scheduler.every(0.1).seconds.timeout(0.2).retries(0)
        async def slow_async_job():
            executed.append(1)
            await asyncio.sleep(1)  # Will exceed 0.2s timeout

        scheduler.start()
        time.sleep(0.5)
        scheduler.stop()

        # Job should have been attempted but failed due to timeout
        assert len(executed) >= 1
        stats = scheduler.get_statistics()
        assert stats["total_failures"] >= 1

    def test_job_completes_within_timeout(self, scheduler):
        """Test that jobs completing within timeout succeed"""
        executed = []

        @scheduler.every(0.1).seconds.timeout(5)
        def fast_job():
            executed.append(1)
            time.sleep(0.01)  # Well within 5s timeout

        scheduler.start()
        time.sleep(0.4)
        scheduler.stop()

        assert len(executed) >= 2
        stats = scheduler.get_statistics()
        assert stats["total_failures"] == 0


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_invalid_time_format(self, scheduler):
        """Test handling of invalid time format"""
        with pytest.raises(Exception):

            @scheduler.daily.at("25:00")  # Invalid hour
            def test_job():
                pass

    def test_job_without_function(self, scheduler):
        """Test scheduling without function"""
        job = Job(
            next_run=time.time() + 10,
            interval=10,
            job_id="test",
            func_name="test",
            func_module="test",
        )
        scheduler._add_job(job)

        # Should handle missing function gracefully
        scheduler.start()
        time.sleep(0.1)
        scheduler.stop()

    def test_empty_scheduler(self, scheduler):
        """Test scheduler with no jobs"""
        scheduler.start()
        time.sleep(0.1)
        scheduler.stop()

        stats = scheduler.get_statistics()
        assert stats["active_jobs"] == 0
        assert stats["total_runs"] == 0

    def test_rapid_start_stop(self, scheduler):
        """Test rapid start/stop cycles"""

        @scheduler.every(1).seconds
        def test_job():
            pass

        for _ in range(3):
            scheduler.start()
            time.sleep(0.05)
            scheduler.stop()
            time.sleep(0.05)

        # Should handle without errors
        assert not scheduler.running


class TestTimezoneSupport:
    """Test timezone support for time-based schedules"""

    def test_daily_with_timezone(self, scheduler):
        """Test daily schedule with timezone"""

        @scheduler.daily.at("09:00", tz="America/New_York")
        def morning_task():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "daily"
        assert job.timezone == "America/New_York"
        assert "America/New_York" in job.get_schedule_description()

    def test_daily_with_tz_method(self, scheduler):
        """Test daily schedule with tz() method"""

        @scheduler.daily.tz("Europe/London").at("14:30")
        def afternoon_task():
            pass

        job = scheduler.jobs[0]
        assert job.timezone == "Europe/London"

    def test_weekly_with_timezone(self, scheduler):
        """Test weekly schedule with timezone"""

        @scheduler.weekly.monday.at("09:00", tz="Asia/Tokyo")
        def weekly_task():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "weekly"
        assert job.timezone == "Asia/Tokyo"

    def test_hourly_with_timezone(self, scheduler):
        """Test hourly schedule with timezone"""

        @scheduler.hourly.at(":30", tz="UTC")
        def hourly_task():
            pass

        job = scheduler.jobs[0]
        assert job.schedule_type == "hourly"
        assert job.timezone == "UTC"

    def test_schedule_description_includes_timezone(self, scheduler):
        """Test that schedule description includes timezone"""

        @scheduler.daily.at("09:00", tz="America/Los_Angeles")
        def west_coast_task():
            pass

        job = scheduler.jobs[0]
        desc = job.get_schedule_description()
        assert "America/Los_Angeles" in desc
        assert "09:00" in desc


class TestCronScheduler:
    """Test cron expression support"""

    def test_cron_basic_expression(self, scheduler):
        """Test basic cron expression"""
        try:

            @scheduler.cron("*/5 * * * *")
            def frequent_task():
                pass

            job = scheduler.jobs[0]
            assert job.schedule_type == "cron"
            assert job.cron_expression == "*/5 * * * *"
            assert job.repeat is True
        except ImportError:
            pytest.skip("croniter not installed")

    def test_cron_weekday_expression(self, scheduler):
        """Test cron expression for weekdays"""
        try:

            @scheduler.cron("0 9 * * MON-FRI")
            def weekday_task():
                pass

            job = scheduler.jobs[0]
            assert job.cron_expression == "0 9 * * MON-FRI"
            assert "Cron:" in job.get_schedule_description()
        except ImportError:
            pytest.skip("croniter not installed")

    def test_cron_with_timezone(self, scheduler):
        """Test cron expression with timezone"""
        try:

            @scheduler.cron("0 9 * * MON-FRI").tz("America/New_York")
            def nyc_task():
                pass

            job = scheduler.jobs[0]
            assert job.timezone == "America/New_York"
            assert "America/New_York" in job.get_schedule_description()
        except ImportError:
            pytest.skip("croniter not installed")

    def test_cron_with_timeout(self, scheduler):
        """Test cron expression with timeout"""
        try:

            @scheduler.cron("*/10 * * * *").timeout(60)
            def timed_task():
                pass

            job = scheduler.jobs[0]
            assert job.timeout == 60
        except ImportError:
            pytest.skip("croniter not installed")

    def test_cron_with_retries(self, scheduler):
        """Test cron expression with retry configuration"""
        try:

            @scheduler.cron("0 * * * *").retries(5)
            def hourly_task():
                pass

            job = scheduler.jobs[0]
            assert job.max_retries == 5
        except ImportError:
            pytest.skip("croniter not installed")

    def test_invalid_cron_expression(self, scheduler):
        """Test invalid cron expression raises error"""
        try:
            with pytest.raises(ValueError):

                @scheduler.cron("invalid cron")
                def bad_task():
                    pass

        except ImportError:
            pytest.skip("croniter not installed")


class TestConfigurableConstants:
    """Test configurable constants (max_history, max_workers)"""

    def test_default_max_history(self, temp_state_file):
        """Test default max_history value"""
        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)
        assert sched.max_history == 10000
        sched.stop()

    def test_custom_max_history(self, temp_state_file):
        """Test custom max_history value"""
        sched = FastScheduler(
            state_file=str(temp_state_file), quiet=True, max_history=5000
        )
        assert sched.max_history == 5000
        sched.stop()

    def test_default_max_workers(self, temp_state_file):
        """Test default max_workers value"""
        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)
        assert sched.max_workers == 10
        sched.stop()

    def test_custom_max_workers(self, temp_state_file):
        """Test custom max_workers value"""
        sched = FastScheduler(
            state_file=str(temp_state_file), quiet=True, max_workers=20
        )
        assert sched.max_workers == 20
        # Verify executor was created with correct value
        assert sched._executor._max_workers == 20
        sched.stop()

    def test_history_respects_max_history(self, temp_state_file):
        """Test that history respects max_history limit"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(
            state_file=str(temp_state_file), quiet=True, max_history=5
        )

        # Add more than max_history entries
        for i in range(10):
            sched._log_history(f"job_{i}", f"test_func_{i}", JobStatus.COMPLETED)

        # History should be trimmed
        assert len(sched.history) <= 5
        sched.stop()

    def test_default_history_retention_days(self, temp_state_file):
        """Test default history_retention_days value"""
        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)
        assert sched.history_retention_days == 7
        sched.stop()

    def test_custom_history_retention_days(self, temp_state_file):
        """Test custom history_retention_days value"""
        sched = FastScheduler(
            state_file=str(temp_state_file), quiet=True, history_retention_days=14
        )
        assert sched.history_retention_days == 14
        sched.stop()

    def test_history_retention_removes_old_entries(self, temp_state_file):
        """Test that old history entries are removed based on retention days"""
        from fastscheduler.main import JobHistory, JobStatus

        sched = FastScheduler(
            state_file=str(temp_state_file), quiet=True, history_retention_days=7
        )

        now = time.time()

        # Manually add history entries with different ages
        sched.history = [
            # Old entry (10 days ago) - should be removed
            JobHistory(
                job_id="job_old",
                func_name="old_func",
                status="completed",
                timestamp=now - (10 * 86400),
            ),
            # Recent entry (1 day ago) - should be kept
            JobHistory(
                job_id="job_recent",
                func_name="recent_func",
                status="completed",
                timestamp=now - (1 * 86400),
            ),
            # Very recent entry (1 hour ago) - should be kept
            JobHistory(
                job_id="job_new",
                func_name="new_func",
                status="completed",
                timestamp=now - 3600,
            ),
        ]

        # Trigger cleanup by logging new history
        sched._log_history("job_x", "test_func", JobStatus.COMPLETED)

        # Old entry should be removed, recent ones should remain plus new one
        assert len(sched.history) == 3
        func_names = [h.func_name for h in sched.history]
        assert "old_func" not in func_names
        assert "recent_func" in func_names
        assert "new_func" in func_names
        assert "test_func" in func_names

        sched.stop()

    def test_history_retention_zero_disables_time_cleanup(self, temp_state_file):
        """Test that setting retention_days=0 disables time-based cleanup"""
        from fastscheduler.main import JobHistory, JobStatus

        sched = FastScheduler(
            state_file=str(temp_state_file),
            quiet=True,
            history_retention_days=0,
            max_history=1000,
        )

        now = time.time()

        # Add an old entry
        sched.history = [
            JobHistory(
                job_id="job_old",
                func_name="old_func",
                status="completed",
                timestamp=now - (30 * 86400),  # 30 days old
            ),
        ]

        # Trigger cleanup
        sched._log_history("job_x", "test_func", JobStatus.COMPLETED)

        # Old entry should still be there (time cleanup disabled)
        assert len(sched.history) == 2
        func_names = [h.func_name for h in sched.history]
        assert "old_func" in func_names

        sched.stop()


class TestDeadLetterQueue:
    """Tests for dead letter queue functionality"""

    def test_default_max_dead_letters(self, temp_state_file):
        """Test default max_dead_letters value"""
        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)
        assert sched.max_dead_letters == 500
        sched.stop()

    def test_custom_max_dead_letters(self, temp_state_file):
        """Test custom max_dead_letters value"""
        sched = FastScheduler(
            state_file=str(temp_state_file),
            quiet=True,
            max_dead_letters=100,
        )
        assert sched.max_dead_letters == 100
        sched.stop()

    def test_dead_letter_file_path(self, temp_state_file):
        """Test dead letter file path is derived from state file"""
        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)
        expected = str(temp_state_file).replace(".json", "_dead_letters.json")
        assert str(sched._dead_letters_file) == expected
        sched.stop()

    def test_dead_letter_queue_adds_failed_jobs(self, temp_state_file):
        """Test that failures are added to dead letter queue"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)

        # Log a final failure (with "Max retries:" prefix)
        sched._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error="Max retries: Some error",
        )

        assert len(sched.dead_letters) == 1
        assert sched.dead_letters[0].func_name == "failing_job"
        assert "Max retries" in sched.dead_letters[0].error
        sched.stop()

    def test_dead_letter_queue_adds_retry_failures(self, temp_state_file):
        """Test that retry failures are also added to dead letter queue"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)

        # Log a retry failure
        sched._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error="Retry 1/3: Some error",
        )

        # All failures with errors go to dead letter queue
        assert len(sched.dead_letters) == 1
        assert sched.dead_letters[0].func_name == "failing_job"
        sched.stop()

    def test_dead_letter_queue_ignores_non_error_failures(self, temp_state_file):
        """Test that failures without error messages are not added to dead letter queue"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)

        # Log a failure without error message
        sched._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error=None,
        )

        assert len(sched.dead_letters) == 0
        sched.stop()

    def test_dead_letter_queue_respects_max_limit(self, temp_state_file):
        """Test that dead letter queue respects max limit"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(
            state_file=str(temp_state_file),
            quiet=True,
            max_dead_letters=5,
        )

        # Add more than max entries
        for i in range(10):
            sched._log_history(
                job_id=f"job_{i}",
                func_name=f"failing_job_{i}",
                status=JobStatus.FAILED,
                error=f"Max retries: Error {i}",
            )

        assert len(sched.dead_letters) == 5
        # Should keep the last 5
        func_names = [dl.func_name for dl in sched.dead_letters]
        assert "failing_job_5" in func_names
        assert "failing_job_9" in func_names
        assert "failing_job_0" not in func_names
        sched.stop()

    def test_get_dead_letters(self, temp_state_file):
        """Test get_dead_letters returns dict format"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)

        sched._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error="Max retries: Error",
            execution_time=1.5,
        )

        dead_letters = sched.get_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0]["func_name"] == "failing_job"
        assert dead_letters[0]["error"] == "Max retries: Error"
        assert "timestamp_readable" in dead_letters[0]
        sched.stop()

    def test_clear_dead_letters(self, temp_state_file):
        """Test clearing dead letter queue"""
        from fastscheduler.main import JobStatus

        sched = FastScheduler(state_file=str(temp_state_file), quiet=True)

        # Add some entries
        for i in range(3):
            sched._log_history(
                job_id=f"job_{i}",
                func_name=f"failing_job_{i}",
                status=JobStatus.FAILED,
                error=f"Max retries: Error {i}",
            )

        assert len(sched.dead_letters) == 3

        # Clear them
        count = sched.clear_dead_letters()
        assert count == 3
        assert len(sched.dead_letters) == 0
        sched.stop()

    def test_dead_letters_persist_and_load(self, temp_state_file):
        """Test that dead letters are persisted and loaded"""
        import time as time_module

        from fastscheduler.main import JobStatus

        # Create scheduler and add dead letters
        sched1 = FastScheduler(state_file=str(temp_state_file), quiet=True)
        sched1._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error="Max retries: Error",
        )
        sched1._save_dead_letters()  # Force sync save
        sched1.stop()

        # Give file system time to write
        time_module.sleep(0.1)

        # Create new scheduler and verify dead letters loaded
        sched2 = FastScheduler(state_file=str(temp_state_file), quiet=True)
        assert len(sched2.dead_letters) == 1
        assert sched2.dead_letters[0].func_name == "failing_job"
        sched2.stop()
