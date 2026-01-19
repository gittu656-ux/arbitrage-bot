"""
Dashboard startup script for Railway deployment.
"""
import uvicorn
from src.dashboard.app import DashboardApp

if __name__ == "__main__":
    # Initialize dashboard
    dashboard = DashboardApp(
        db_path="data/arbitrage_events.db",
        port=8000
    )
    
    # Run with uvicorn
    uvicorn.run(
        dashboard.get_app(),
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
