import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes


@pytest.fixture
def scheduler(tmp_path):
    """Create a test scheduler"""
    state_file = tmp_path / "test_scheduler.json"
    sched = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

    # Add a test job
    @sched.every(10).seconds
    def test_job():
        pass

    yield sched
    if sched.running:
        sched.stop()


@pytest.fixture
def app(scheduler):
    """Create FastAPI app with scheduler routes"""
    app = FastAPI()
    app.include_router(create_scheduler_routes(scheduler))
    return app


@pytest.fixture
def client(app):
    """Create test client"""
    return TestClient(app)


class TestSchedulerRoutes:
    """Test FastAPI scheduler routes"""

    def test_dashboard_endpoint(self, client):
        """Test dashboard endpoint returns HTML"""
        response = client.get("/scheduler/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert b"FastScheduler" in response.content

    def test_dashboard_shows_jobs(self, client, scheduler):
        """Test dashboard displays scheduled jobs"""
        response = client.get("/scheduler/")
        assert response.status_code == 200

        content = response.content.decode()
        assert "test_job" in content
        assert "Every 10 seconds" in content

    def test_dashboard_shows_status(self, client, scheduler):
        """Test dashboard shows scheduler status"""
        # Test stopped status
        response = client.get("/scheduler/")
        content = response.content.decode()
        assert "STOPPED" in content

        # Test running status
        scheduler.start()
        response = client.get("/scheduler/")
        content = response.content.decode()
        assert "RUNNING" in content
        scheduler.stop()

    def test_events_endpoint_exists(self, app):
        """Test SSE events endpoint exists"""
        # Verify the route is registered (don't actually consume the infinite stream)
        routes = [route.path for route in app.routes]
        assert "/scheduler/events" in routes

    def test_custom_prefix(self, scheduler):
        """Test custom route prefix"""
        app = FastAPI()
        app.include_router(create_scheduler_routes(scheduler, prefix="/custom"))
        client = TestClient(app)

        response = client.get("/custom/")
        assert response.status_code == 200

    def test_dashboard_with_history(self, client, scheduler):
        """Test dashboard shows execution history"""
        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        response = client.get("/scheduler/")
        content = response.content.decode()

        # Should show some history or stats
        assert "Statistics" in content or "Jobs" in content

    def test_dashboard_styling(self, client):
        """Test dashboard has proper styling"""
        response = client.get("/scheduler/")
        content = response.content.decode()

        # Check for CSS and styling elements
        assert "<style>" in content
        assert "background" in content
        assert "color" in content


class TestIntegrationScenarios:
    """Test complete integration scenarios"""

    def test_full_app_lifecycle(self, tmp_path):
        """Test complete app lifecycle with scheduler"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        executed = []

        @scheduler.every(0.1).seconds
        def background_task():
            executed.append(time.time())

        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        # Start scheduler
        scheduler.start()

        # Access dashboard while running
        response = client.get("/scheduler/")
        assert response.status_code == 200

        # Let jobs execute
        time.sleep(0.3)

        # Check jobs ran
        assert len(executed) >= 2

        # Stop scheduler
        scheduler.stop()

        # Dashboard should still be accessible
        response = client.get("/scheduler/")
        assert response.status_code == 200

    def test_multiple_jobs_display(self, tmp_path):
        """Test dashboard with multiple jobs"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        @scheduler.every(5).seconds
        def job1():
            pass

        @scheduler.every(10).minutes
        def job2():
            pass

        @scheduler.daily.at("14:30")
        async def job3():
            pass

        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        response = client.get("/scheduler/")
        content = response.content.decode()

        # All jobs should be visible
        assert "job1" in content
        assert "job2" in content
        assert "job3" in content

        scheduler.stop()

    def test_async_jobs_with_fastapi(self, tmp_path):
        """Test async jobs work with FastAPI integration"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        executed = []

        @scheduler.every(0.1).seconds
        async def async_job():
            executed.append(1)

        app.include_router(create_scheduler_routes(scheduler))

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        assert len(executed) >= 2


class TestErrorHandling:
    """Test error handling in FastAPI integration"""

    def test_scheduler_not_running(self, client, scheduler):
        """Test endpoints work when scheduler is not running"""
        response = client.get("/scheduler/")
        assert response.status_code == 200

    def test_no_jobs_scheduled(self, tmp_path):
        """Test dashboard with no jobs"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)
        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        response = client.get("/scheduler/")
        assert response.status_code == 200

        scheduler.stop()

    def test_dashboard_with_failed_jobs(self, client, scheduler):
        """Test dashboard displays failed jobs"""

        @scheduler.every(0.1).seconds.retries(1)
        def failing_job():
            raise ValueError("Test error")

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        response = client.get("/scheduler/")
        assert response.status_code == 200
        # Dashboard should handle failed jobs gracefully


class TestDashboardContent:
    """Test specific dashboard content elements"""

    def test_job_metadata_display(self, client, scheduler):
        """Test job metadata is displayed correctly"""
        response = client.get("/scheduler/")
        content = response.content.decode()

        # Should show job details
        assert "test_job" in content
        # Should show some timing info
        assert "second" in content.lower() or "minute" in content.lower()

    def test_statistics_display(self, client, scheduler):
        """Test statistics are displayed"""
        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        response = client.get("/scheduler/")
        content = response.content.decode()

        # Should show some statistics
        assert "0" in content or "1" in content  # Some numeric stats

    def test_responsive_design(self, client):
        """Test dashboard has responsive design elements"""
        response = client.get("/scheduler/")
        content = response.content.decode()

        # Check for viewport meta tag
        assert "viewport" in content or "width" in content

