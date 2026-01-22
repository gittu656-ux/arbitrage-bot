"""
FastAPI dashboard for monitoring arbitrage bot.
"""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import json

try:
    from src.database import ArbitrageDatabase
    from src.logger import setup_logger
except ImportError:
    from database import ArbitrageDatabase
    from logger import setup_logger


class DashboardApp:
    """FastAPI application for arbitrage bot dashboard."""
    
    def __init__(self, db_path: str, port: int = 8000):
        self.app = FastAPI(title="Arbitrage Bot Dashboard")
        self.db = ArbitrageDatabase(db_path)
        self.port = port
        self.logger = setup_logger("dashboard")
        
        # Setup templates
        template_dir = Path(__file__).parent / "templates"
        template_dir.mkdir(exist_ok=True)
        self.templates = Jinja2Templates(directory=str(template_dir))
        
        # Setup routes
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup FastAPI routes."""
        
        @self.app.get("/", response_class=HTMLResponse)
        async def root(request: Request):
            """Redirect to dashboard."""
            return await dashboard(request)
        
        @self.app.get("/dashboard", response_class=HTMLResponse)
        async def dashboard(request: Request):
            """Main dashboard page."""
            stats = self.db.get_statistics()
            
            # Get recent opportunities for preview
            recent = self.db.get_recent_opportunities(limit=5)
            
            return self.templates.TemplateResponse(
                "dashboard.html",
                {
                    "request": request,
                    "stats": stats,
                    "recent_opportunities": recent,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        @self.app.get("/opportunities", response_class=HTMLResponse)
        async def opportunities(request: Request, limit: int = 100):
            """List all arbitrage opportunities."""
            opps = self.db.get_all_opportunities(limit=limit)
            
            return self.templates.TemplateResponse(
                "opportunities.html",
                {
                    "request": request,
                    "opportunities": opps,
                    "limit": limit,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        @self.app.get("/logs", response_class=HTMLResponse)
        async def logs(request: Request):
            """View recent logs and errors."""
            # Read recent log entries (last 100 lines)
            # Use absolute path relative to bot root
            bot_root = Path(__file__).parent.parent.parent
            log_file = bot_root / "logs" / "arbitrage_bot.log"
            log_entries = []
            
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                        # Get last 100 lines
                        log_entries = lines[-100:]
                except Exception as e:
                    self.logger.error(f"Error reading log file: {e}")
            
            return self.templates.TemplateResponse(
                "logs.html",
                {
                    "request": request,
                    "log_entries": log_entries,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        @self.app.get("/api/stats")
        async def api_stats():
            """API endpoint for statistics (JSON)."""
            return self.db.get_statistics()
        
        @self.app.get("/api/opportunities")
        async def api_opportunities(limit: int = 100):
            """API endpoint for opportunities (JSON)."""
            return self.db.get_all_opportunities(limit=limit)
    
    def get_app(self):
        """Get FastAPI app instance."""
        return self.app

