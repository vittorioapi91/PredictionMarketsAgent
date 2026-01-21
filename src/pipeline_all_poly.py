import os
import logging
from datetime import datetime
import subprocess
from pathlib import Path
try:
    from src.utils import get_storage_path
except ImportError:
    # If running as a script directly, use relative import
    from utils import get_storage_path

# Set up logging
logging.basicConfig(
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def create_directories():
    """Create necessary directories if they don't exist."""
    # Get environment-specific storage paths
    raw_data_dir = get_storage_path('raw_data')
    open_markets_dir = get_storage_path('open_markets')
    env_storage_dir = get_storage_path()
    
    dirs = [
        env_storage_dir,
        raw_data_dir,
        open_markets_dir
    ]
    
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Checked directory: {directory}")

def run_subprocess(command):
    """Run a subprocess with controlled output."""
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        for line in result.stdout.split('\n'):
            line = line.lower().strip()
            if any(key in line for key in ['fetched', 'total markets', 'saved to']):
                logger.info(line)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(command)}")
        logger.error(e.stderr)
        return False

def run_pipeline():
    """Run the full Polymarket data collection pipeline."""
    date_today = datetime.now().strftime('%Y%m%d')
    
    # Step 1: Create directories
    create_directories()
    
    # Get project root directory (one level up from src/)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_dir = os.path.join(project_root, "src")
    
    # Step 2: Collect all Polymarket data
    logger.info("Step 1/2: Collecting Polymarket data...")
    if not run_subprocess(["python", os.path.join(src_dir, "main.py")]):
        return
    
    # Step 3: Filter for open markets
    logger.info("Step 2/2: Filtering open markets...")
    if not run_subprocess(["python", os.path.join(src_dir, "get_open_markets.py")]):
        return
    
    logger.info(f"Pipeline completed successfully - {date_today}")

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("Starting Polymarket data collection pipeline...")
    
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Pipeline execution completed in {duration:.2f} seconds")