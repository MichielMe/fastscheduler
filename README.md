# FastScheduler

Simple, lightweight task scheduler for Python with async support.

## Features

- Simple decorator-based API
- Async/await support
- Persistent state (survives restarts)
- FastAPI integration with web dashboard
- Automatic retries on failure
- Quiet mode for minimal logging

## Installation

```bash
pip install fastscheduler
```

## Quick Start

```python
from fastscheduler import FastScheduler

# Create scheduler
scheduler = FastScheduler(quiet=True)

# Schedule tasks
@scheduler.every(10).seconds
def task():
    print("Task executed")

@scheduler.daily.at("14:30")
async def daily_task():
    print("Daily task at 2:30 PM")

# Start scheduler
scheduler.start()
```

## Examples

### Basic Usage

```python
import time
from fastscheduler import FastScheduler

scheduler = FastScheduler(quiet=True)

@scheduler.every(5).seconds
def quick_task():
    print(f"[{time.strftime('%H:%M:%S')}] Task running")

scheduler.start()

# Keep program running
try:
    while True:
        time.sleep(60)
        scheduler.print_status()  # Simple status output
except KeyboardInterrupt:
    scheduler.stop()
```

### FastAPI Integration

```python
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes

app = FastAPI()
scheduler = FastScheduler(quiet=True)

# Add web dashboard at /scheduler
app.include_router(create_scheduler_routes(scheduler))

@scheduler.every(30).seconds
def background_task():
    print("Background work")

scheduler.start()
```

## Scheduling Options

### Interval-based

```python
@scheduler.every(10).seconds
@scheduler.every(5).minutes
@scheduler.every(2).hours
@scheduler.every(1).days
```

### Time-based

```python
@scheduler.daily.at("09:00")      # Daily at 9 AM
@scheduler.hourly.at(":30")       # Every hour at :30
@scheduler.weekly.monday.at("10:00")  # Every Monday at 10 AM
```

### One-time

```python
@scheduler.once(60)  # Run once after 60 seconds
@scheduler.at("2024-12-25 00:00:00")  # Run at specific datetime
```

## Configuration

```python
# Create scheduler with options
scheduler = FastScheduler(
    state_file="scheduler.json",  # Persistence file
    quiet=True,                   # Minimal logging
    auto_start=False              # Don't auto-start
)

# Configure individual jobs
@scheduler.every(10).seconds.retries(5)  # Retry up to 5 times
@scheduler.every(1).hours.no_catch_up()  # Don't run missed jobs
```

## Web Dashboard

The FastAPI integration includes a simple dark-mode dashboard:

- View all scheduled jobs
- Monitor execution history
- Check scheduler statistics
- Auto-refreshes every 10 seconds

Access at: `http://localhost:8000/scheduler/`

## API

### Core Methods

- `scheduler.start()` - Start the scheduler
- `scheduler.stop()` - Stop gracefully
- `scheduler.print_status()` - Print simple status
- `scheduler.get_jobs()` - Get all scheduled jobs
- `scheduler.get_statistics()` - Get runtime statistics

## License

MIT
