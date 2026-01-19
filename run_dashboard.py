"""
Dashboard startup script for Railway deployment.
"""
import os
import uvicorn
from pathlib import Path

# Ensure data directory exists
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

# Import after ensuring directory exists
from src.dashboard.app import DashboardApp

if __name__ == "__main__":
    # Get port from environment or default to 8000
    port = int(os.getenv("PORT", 8000))
    
    # Initialize dashboard
    dashboard = DashboardApp(
        db_path="data/arbitrage_events.db",
        port=port
    )
    
    # Get the FastAPI app instance
    app = dashboard.get_app()
    
    # Run with uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
