import asyncio
import os
import sys
import logging
from pathlib import Path
import uvicorn
from contextlib import asynccontextmanager

# Fix Windows console encoding
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

# Import Bot and Dashboard
from src.main import ArbitrageBot
from src.dashboard.app import DashboardApp

# Global bot instance
bot_instance = None

@asynccontextmanager
async def lifespan(app):
    # STARTUP: Launch the bot in the background
    print("🚀 Web Server starting...")
    global bot_instance
    try:
        # Initialize Bot
        bot_instance = ArbitrageBot()
        
        # Wrapper to delay bot startup
        async def delayed_bot_start():
            print("⏳ Waiting 10s for Web Server to bind port...")
            await asyncio.sleep(10)
            print("🚀 Starting Arbitrage Bot cycle...")
            try:
                await bot_instance.run()
            except Exception as e:
                print(f"❌ Bot crashed: {e}")
                import traceback
                traceback.print_exc()

        # Run Bot in background task
        asyncio.create_task(delayed_bot_start())
        print("✅ Bot background task scheduled (starts in 10s)!")
    except Exception as e:
        print(f"❌ Failed to initialize bot: {e}")
        import traceback
        traceback.print_exc()
    
    yield
    
    # SHUTDOWN: Cleanup
    print("🛑 Shutting down...")

# Initialize Dashboard with Lifespan
port = int(os.getenv("PORT", 8000))
dashboard = DashboardApp(db_path="data/arbitrage_events.db", port=port)
app = dashboard.get_app()

# Attach lifespan manager to the app
app.router.lifespan_context = lifespan

# Add Health Check for Railway/Docker
@app.get("/health")
def health_check():
    return {"status": "ok", "bot_running": bot_instance is not None}

if __name__ == "__main__":
    # Run server
    uvicorn.run(app, host="0.0.0.0", port=port)
