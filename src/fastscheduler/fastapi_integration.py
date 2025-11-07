import asyncio
import json
from typing import AsyncGenerator, Optional

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, StreamingResponse


def create_scheduler_routes(scheduler, prefix: str = "/scheduler"):
    """
    Create FastAPI routes for scheduler management

    Usage:
        from fastapi import FastAPI
        from fastscheduler import FastScheduler
        from fastapi_integration import create_scheduler_routes

        app = FastAPI()
        scheduler = FastScheduler()

        app.include_router(create_scheduler_routes(scheduler))

        scheduler.start()
    """
    router = APIRouter(prefix=prefix, tags=["scheduler"])

    async def event_generator() -> AsyncGenerator[str, None]:
        """Generate SSE events for real-time updates"""
        while True:
            try:
                # Get current state
                stats = scheduler.get_statistics()
                jobs = scheduler.get_jobs()
                history = scheduler.get_history(limit=10)

                # Prepare data
                data = {
                    "running": scheduler.running,
                    "stats": stats,
                    "jobs": jobs,
                    "history": history,
                }

                # Send as SSE event
                yield f"data: {json.dumps(data)}\n\n"

                # Update every second
                await asyncio.sleep(1)
            except Exception:
                # On error, wait and retry
                await asyncio.sleep(1)

    @router.get("/events")
    async def events():
        """SSE endpoint for real-time updates"""
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/", response_class=HTMLResponse)
    async def dashboard():
        """Simple web dashboard"""
        stats = scheduler.get_statistics()
        jobs = scheduler.get_jobs()
        history = scheduler.get_history(limit=20)

        status_color = "#10b981" if scheduler.running else "#ef4444"
        status_text = "RUNNING" if scheduler.running else "STOPPED"

        jobs_html = ""
        for job in jobs:
            next_in = job["next_run_in"]
            if next_in > 86400:
                next_str = f"{int(next_in/86400)}d"
            elif next_in > 3600:
                next_str = f"{int(next_in/3600)}h"
            elif next_in > 60:
                next_str = f"{int(next_in/60)}m"
            else:
                next_str = f"{int(next_in)}s"

            jobs_html += f"""
            <div class="job-card">
                <div class="job-header">
                    <span class="job-name">{job['func_name']}</span>
                    <span class="job-status status-{job['status']}">{job['status']}</span>
                </div>
                <div class="job-details">
                    <div>Schedule: {job['schedule']}</div>
                    <div>Next run: {next_str}</div>
                    <div>Total runs: {job['run_count']}</div>
                </div>
            </div>
            """

        history_html = ""
        for event in history[-10:]:
            status_symbol = {
                "scheduled": "○",
                "running": "●",
                "completed": "✓",
                "failed": "✗",
                "missed": "!",
            }
            symbol = status_symbol.get(event["status"], "-")

            history_html += f"""
            <div class="history-item status-{event['status']}">
                <span class="history-icon">{symbol}</span>
                <span class="history-name">{event['func_name']}</span>
                <span class="history-status">{event['status']}</span>
                <span class="history-time">{event['timestamp_readable']}</span>
            </div>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>FastScheduler</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: -apple-system, system-ui, 'Segoe UI', Roboto, monospace;
                    background: #0a0a0a;
                    color: #e5e5e5;
                    min-height: 100vh;
                    padding: 20px;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                .header {{
                    background: #111111;
                    border: 1px solid #262626;
                    border-radius: 8px;
                    padding: 24px;
                    margin-bottom: 20px;
                }}
                .title {{
                    font-size: 1.8em;
                    font-weight: 600;
                    margin-bottom: 8px;
                    color: #fafafa;
                    letter-spacing: -0.5px;
                }}
                .status {{
                    font-size: 1em;
                    font-weight: 500;
                    color: {status_color};
                    margin-bottom: 20px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }}
                .stats {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                    gap: 12px;
                }}
                .stat-card {{
                    background: #1a1a1a;
                    border: 1px solid #262626;
                    color: #e5e5e5;
                    padding: 16px;
                    border-radius: 6px;
                }}
                .stat-value {{
                    font-size: 1.8em;
                    font-weight: 600;
                    margin-bottom: 4px;
                    color: #fafafa;
                }}
                .stat-label {{
                    font-size: 0.8em;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    color: #737373;
                }}
                .section {{
                    background: #111111;
                    border: 1px solid #262626;
                    border-radius: 8px;
                    padding: 24px;
                    margin-bottom: 20px;
                }}
                .section-title {{
                    font-size: 1.2em;
                    font-weight: 600;
                    margin-bottom: 16px;
                    color: #fafafa;
                }}
                .jobs-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                    gap: 12px;
                }}
                .job-card {{
                    background: #1a1a1a;
                    border: 1px solid #262626;
                    border-radius: 6px;
                    padding: 16px;
                    transition: border-color 0.2s;
                }}
                .job-card:hover {{
                    border-color: #404040;
                }}
                .job-header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 12px;
                }}
                .job-name {{
                    font-size: 1em;
                    font-weight: 600;
                    color: #fafafa;
                }}
                .job-status {{
                    padding: 3px 8px;
                    border-radius: 4px;
                    font-size: 0.75em;
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 0.3px;
                }}
                .status-scheduled {{
                    background: #1a2332;
                    color: #60a5fa;
                    border: 1px solid #1e3a5f;
                }}
                .status-running {{
                    background: #2d2617;
                    color: #fbbf24;
                    border: 1px solid #44361f;
                }}
                .status-completed {{
                    background: #14281d;
                    color: #34d399;
                    border: 1px solid #1c3829;
                }}
                .status-failed {{
                    background: #2d1815;
                    color: #f87171;
                    border: 1px solid #441f1c;
                }}
                .job-details {{
                    color: #a3a3a3;
                    font-size: 0.85em;
                    line-height: 1.6;
                    font-family: monospace;
                }}
                .job-details div {{
                    margin-bottom: 4px;
                }}
                .history-item {{
                    display: grid;
                    grid-template-columns: 30px 1fr 100px 140px;
                    gap: 12px;
                    align-items: center;
                    padding: 12px;
                    border-bottom: 1px solid #262626;
                    font-size: 0.9em;
                }}
                .history-item:last-child {{
                    border-bottom: none;
                }}
                .history-icon {{
                    font-family: monospace;
                    text-align: center;
                    color: #737373;
                }}
                .history-item.status-completed .history-icon {{
                    color: #34d399;
                }}
                .history-item.status-failed .history-icon {{
                    color: #f87171;
                }}
                .history-item.status-running .history-icon {{
                    color: #fbbf24;
                }}
                .history-name {{
                    font-weight: 500;
                    color: #e5e5e5;
                    font-family: monospace;
                }}
                .history-status {{
                    text-transform: uppercase;
                    font-size: 0.75em;
                    font-weight: 500;
                    letter-spacing: 0.3px;
                    color: #737373;
                }}
                .history-time {{
                    color: #525252;
                    font-size: 0.85em;
                    font-family: monospace;
                }}
                p {{
                    color: #737373;
                }}
                .connection-status {{
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    padding: 8px 16px;
                    background: #1a1a1a;
                    border: 1px solid #262626;
                    border-radius: 6px;
                    font-size: 0.8em;
                    font-family: monospace;
                }}
                .connection-status.connected {{
                    border-color: #10b981;
                    color: #10b981;
                }}
                .connection-status.disconnected {{
                    border-color: #ef4444;
                    color: #ef4444;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div class="title">FastScheduler</div>
                    <div id="status" class="status">{status_text}</div>
                    <div id="stats" class="stats">
                        <div class="stat-card">
                            <div class="stat-value">{len(jobs)}</div>
                            <div class="stat-label">Active Jobs</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{stats['total_runs']}</div>
                            <div class="stat-label">Total Runs</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{stats['total_failures']}</div>
                            <div class="stat-label">Failures</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{stats.get('uptime_readable', 'N/A')}</div>
                            <div class="stat-label">Uptime</div>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <div class="section-title">Scheduled Jobs</div>
                    <div id="jobs" class="jobs-grid">
                        {jobs_html or '<p>No jobs scheduled</p>'}
                    </div>
                </div>
                
                <div class="section">
                    <div class="section-title">Recent History</div>
                    <div id="history" class="history-list">
                        {history_html or '<p>No history yet</p>'}
                    </div>
                </div>
            </div>
            <div id="connection" class="connection-status disconnected">Connecting...</div>
            
            <script>
                let eventSource = null;
                
                function formatNextRun(seconds) {{
                    if (seconds > 86400) return Math.floor(seconds / 86400) + 'd';
                    if (seconds > 3600) return Math.floor(seconds / 3600) + 'h';
                    if (seconds > 60) return Math.floor(seconds / 60) + 'm';
                    if (seconds > 0) return Math.floor(seconds) + 's';
                    return 'now';
                }}
                
                function updateDashboard(data) {{
                    // Update status
                    const statusEl = document.getElementById('status');
                    statusEl.textContent = data.running ? 'RUNNING' : 'STOPPED';
                    statusEl.style.color = data.running ? '#10b981' : '#ef4444';
                    
                    // Update stats
                    const statsEl = document.getElementById('stats');
                    statsEl.innerHTML = `
                        <div class="stat-card">
                            <div class="stat-value">${{data.jobs.length}}</div>
                            <div class="stat-label">Active Jobs</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${{data.stats.total_runs || 0}}</div>
                            <div class="stat-label">Total Runs</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${{data.stats.total_failures || 0}}</div>
                            <div class="stat-label">Failures</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${{data.stats.uptime_readable || 'N/A'}}</div>
                            <div class="stat-label">Uptime</div>
                        </div>
                    `;
                    
                    // Update jobs
                    const jobsEl = document.getElementById('jobs');
                    if (data.jobs.length === 0) {{
                        jobsEl.innerHTML = '<p>No jobs scheduled</p>';
                    }} else {{
                        jobsEl.innerHTML = data.jobs.map(job => `
                            <div class="job-card">
                                <div class="job-header">
                                    <span class="job-name">${{job.func_name}}</span>
                                    <span class="job-status status-${{job.status}}">${{job.status}}</span>
                                </div>
                                <div class="job-details">
                                    <div>Schedule: ${{job.schedule}}</div>
                                    <div>Next run: ${{formatNextRun(job.next_run_in)}}</div>
                                    <div>Total runs: ${{job.run_count}}</div>
                                </div>
                            </div>
                        `).join('');
                    }}
                    
                    // Update history
                    const historyEl = document.getElementById('history');
                    if (data.history.length === 0) {{
                        historyEl.innerHTML = '<p>No history yet</p>';
                    }} else {{
                        const statusSymbols = {{
                            scheduled: '○',
                            running: '●',
                            completed: '✓',
                            failed: '✗',
                            missed: '!'
                        }};
                        
                        historyEl.innerHTML = data.history.slice(-10).reverse().map(event => `
                            <div class="history-item status-${{event.status}}">
                                <span class="history-icon">${{statusSymbols[event.status] || '-'}}</span>
                                <span class="history-name">${{event.func_name}}</span>
                                <span class="history-status">${{event.status}}</span>
                                <span class="history-time">${{event.timestamp_readable}}</span>
                            </div>
                        `).join('');
                    }}
                }}
                
                function connect() {{
                    if (eventSource) {{
                        eventSource.close();
                    }}
                    
                    eventSource = new EventSource('{prefix}/events');
                    
                    eventSource.onopen = function() {{
                        const connEl = document.getElementById('connection');
                        connEl.textContent = 'Live';
                        connEl.className = 'connection-status connected';
                    }};
                    
                    eventSource.onmessage = function(event) {{
                        try {{
                            const data = JSON.parse(event.data);
                            updateDashboard(data);
                        }} catch (e) {{
                            console.error('Failed to parse SSE data:', e);
                        }}
                    }};
                    
                    eventSource.onerror = function() {{
                        eventSource.close();
                        const connEl = document.getElementById('connection');
                        connEl.textContent = 'Reconnecting...';
                        connEl.className = 'connection-status disconnected';
                        setTimeout(connect, 2000);
                    }};
                }}
                
                connect();
                
                window.addEventListener('beforeunload', function() {{
                    if (eventSource) {{
                        eventSource.close();
                    }}
                }});
            </script>
        </body>
        </html>
        """
        return html

    @router.get("/api/status")
    async def get_status():
        """Get scheduler status"""
        return {"running": scheduler.running, "statistics": scheduler.get_statistics()}

    @router.get("/api/jobs")
    async def get_jobs():
        """Get all scheduled jobs"""
        return {"jobs": scheduler.get_jobs()}

    @router.get("/api/history")
    async def get_history(func_name: Optional[str] = None, limit: int = 50):
        """Get job history"""
        return {"history": scheduler.get_history(func_name, limit)}

    return router
