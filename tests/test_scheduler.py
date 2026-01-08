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
        time.sleep(0.3)
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
        scheduler1 = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)

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
        scheduler2 = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)

        # Job functions can't be serialized, but history should persist
        assert len(scheduler2.history) > 0
        scheduler2.stop()

    def test_job_history_persistence(self, temp_state_file):
        """Test job history is persisted"""
        scheduler1 = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)

        @scheduler1.every(0.1).seconds
        def test_job():
            pass

        scheduler1.start()
        time.sleep(0.3)
        scheduler1.stop()

        # Verify history exists
        assert len(scheduler1.history) > 0

        # Load in new scheduler
        scheduler2 = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)
        assert len(scheduler2.history) > 0
        scheduler2.stop()

    def test_state_file_creation(self, temp_state_file):
        """Test state file is created"""
        assert not temp_state_file.exists()

        scheduler = FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False)

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
        with FastScheduler(state_file=str(temp_state_file), quiet=True, auto_start=False) as sched:
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

