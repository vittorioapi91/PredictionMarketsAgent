#!/bin/bash

# Start script for Next.js Polymarket Dashboard

set -e

# Get the project root directory (one level up from webui/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to webui directory
cd "$SCRIPT_DIR"

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "Node modules not found. Installing dependencies..."
    if command -v npm &> /dev/null; then
        npm install
    elif command -v yarn &> /dev/null; then
        yarn install
    elif command -v pnpm &> /dev/null; then
        pnpm install
    else
        echo "Error: No package manager found (npm, yarn, or pnpm)"
        exit 1
    fi
fi

# Check if Next.js is installed
if ! npm list next &> /dev/null && ! yarn list next &> /dev/null && ! pnpm list next &> /dev/null; then
    echo "Next.js not found. Installing dependencies..."
    if command -v npm &> /dev/null; then
        npm install
    elif command -v yarn &> /dev/null; then
        yarn install
    elif command -v pnpm &> /dev/null; then
        pnpm install
    fi
fi

# Determine which package manager to use
if command -v pnpm &> /dev/null; then
    PKG_MANAGER="pnpm"
elif command -v yarn &> /dev/null; then
    PKG_MANAGER="yarn"
else
    PKG_MANAGER="npm"
fi

echo "Using package manager: $PKG_MANAGER"
echo "Starting Next.js development server on http://localhost:3782"
echo "Make sure the FastAPI backend is running on http://localhost:7567"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start Next.js dev server
exec $PKG_MANAGER run dev
