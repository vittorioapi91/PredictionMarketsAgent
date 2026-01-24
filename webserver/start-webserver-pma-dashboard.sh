#!/bin/bash

# Start script for Polymarket Dashboard FastAPI server

set -e

# Get the project root directory (one level up from webserver/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
elif [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
else
    echo "Warning: No virtual environment found. Using system Python."
fi

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Determine which Python to use (venv if activated, otherwise system)
if [ -n "$VIRTUAL_ENV" ]; then
    PYTHON_CMD="python"
    PIP_CMD="pip"
    echo "Using virtual environment: $VIRTUAL_ENV"
else
    PYTHON_CMD="python3"
    PIP_CMD="pip3"
    echo "Using system Python"
fi

# Check if required packages are installed
echo "Checking dependencies..."
$PYTHON_CMD -c "import fastapi, uvicorn" 2>/dev/null || {
    echo "Error: fastapi or uvicorn not installed."
    echo "Installing dependencies..."
    $PIP_CMD install -q 'fastapi>=0.104.0' 'uvicorn[standard]>=0.24.0' || {
        echo "Failed to install dependencies. Please install manually:"
        echo "  $PIP_CMD install -r requirements-prod.txt"
    exit 1
    }
    echo "Dependencies installed successfully."
}

# Start the server
echo "Starting Polymarket Dashboard server on port 7567..."
echo "Server will be available at: http://127.0.0.1:7567"
echo "API documentation at: http://127.0.0.1:7567/docs"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

$PYTHON_CMD -m webserver.main
