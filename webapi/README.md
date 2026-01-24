# Web API

This package contains the API routes and endpoints for the Polymarket Dashboard.

## Structure

- `routes.py` - API route definitions
- `__init__.py` - Package initialization

## API Endpoints

- `GET /api/health` - Health check
- `GET /api/markets` - List markets (with filters)
- `GET /api/markets/{condition_id}` - Get market details
- `GET /api/markets/{condition_id}/orderbook` - Get order book
- `GET /api/markets/{condition_id}/volume` - Get volume data

## Usage

The routes are included in the main FastAPI app in `webserver/main.py`.
