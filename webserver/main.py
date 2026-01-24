"""
Main FastAPI server for Polymarket dashboard.
"""

import logging
import sys
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.polymarket import PolymarketClient
from src.utils import load_environment_file
from webapi.routes import router, set_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Polymarket Dashboard",
    description="Web dashboard for Polymarket data collection and analysis",
    version="0.1.0",
)

# Initialize Polymarket client
logger.info("Initializing Polymarket client...")
load_environment_file()
client = PolymarketClient()
set_client(client)
logger.info("Polymarket client initialized")

# Include API routes
app.include_router(router, prefix="/api", tags=["api"])

# Note: Next.js runs as a separate service on port 3000
# The FastAPI server only serves the API endpoints
# Next.js proxies API requests to this server via next.config.js rewrites
@app.get("/")
async def read_root():
    return {
        "message": "Polymarket Dashboard API",
        "docs": "/docs",
        "note": "Frontend is served by Next.js on port 3000. API endpoints are available at /api/*",
    }


def main():
    """Run the server"""
    logger.info("Starting Polymarket Dashboard server...")
    uvicorn.run(
        "webserver.main:app",
        host="0.0.0.0",
        port=7567,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
