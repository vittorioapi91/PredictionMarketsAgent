# Web Server

Main FastAPI server application that provides the API backend for the Polymarket Dashboard.

## Structure

- `main.py` - Main server application
- `__init__.py` - Package initialization
- `start-webserver-pma-dashboard.sh` - Script to start the FastAPI server

## Architecture

The application consists of two separate services:

1. **FastAPI Backend** (this server) - Runs on port **7567**
   - Provides REST API endpoints at `/api/*`
   - API documentation at `/docs`
   - Health check at `/api/health`

2. **Next.js Frontend** - Runs on port **3000** (see `webui/` directory)
   - Next.js + TypeScript dashboard
   - Proxies API requests to FastAPI backend
   - Started separately with `./webui/start.sh`

## Running the Server

### Start FastAPI Backend

```bash
# From project root
./webserver/start-webserver-pma-dashboard.sh

# Or using uvicorn directly
uvicorn webserver.main:app --reload --host 0.0.0.0 --port 7567
```

The API server runs on port **7567** by default.

### Start Next.js Frontend

```bash
# From project root
./webui/start.sh

# Or manually
cd webui
npm install  # First time only
npm run dev
```

The frontend runs on port **3782** by default.

## Features

- REST API endpoints from `webapi/`
- API documentation at `/docs` (Swagger UI)
- Health check endpoint at `/api/health`
- Polymarket client integration
- Redis order book publishing support

## Nginx Configuration

Nginx configuration is managed separately in the infrastructure. The server should be proxied from `predictionmarketsagent.local.info` to the appropriate services:
- API: `http://127.0.0.1:7567`
- Frontend: `http://127.0.0.1:3782` (or serve Next.js build statically)
