"""Utility functions for PredictionMarketsAgent"""
import os
import subprocess
from dotenv import load_dotenv


def get_environment():
    """
    Get the environment suffix based on git branch.
    Returns: 'prod', 'test', or 'dev'
    """
    try:
        # Get project root directory (one level up from src/)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=project_root
        )
        branch = result.stdout.strip()
        
        # Determine environment suffix based on branch
        if branch == "main":
            return "prod"
        elif branch == "staging":
            return "test"
        elif branch.startswith("dev/"):
            return "dev"
        else:
            # Default to dev for other branches
            return "dev"
    except (subprocess.CalledProcessError, FileNotFoundError):
        # If git is not available or not a git repo, default to dev
        return "dev"


def load_environment_file():
    """
    Load the appropriate .env file based on the current environment.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = get_environment()
    env_file = os.path.join(project_root, f".env-{env}")
    
    # Load environment-specific .env file
    if os.path.exists(env_file):
        load_dotenv(env_file, override=True)
    else:
        # Fallback to default .env if environment-specific file doesn't exist
        default_env = os.path.join(project_root, ".env")
        if os.path.exists(default_env):
            load_dotenv(default_env)


def get_storage_path(subpath=""):
    """
    Get the storage path based on environment.
    
    Args:
        subpath: Additional path components (e.g., 'raw_data', 'open_markets')
    
    Returns:
        Full path to environment-specific storage directory
    """
    # Get project root directory (one level up from src/)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = get_environment()
    
    # Build path: storage/{env}/{subpath}
    if subpath:
        storage_path = os.path.join(project_root, "storage", env, subpath)
    else:
        storage_path = os.path.join(project_root, "storage", env)
    
    return storage_path
