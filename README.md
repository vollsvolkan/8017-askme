# 8017 Askme Service

High Performance Product Search and Import API

## Features
- Massive barcode search (up to 10 million codes)
- 32-worker parallel processing
- Redis caching for fast searches
- MongoDB integration
- Pulsar event streaming
- Admin token authentication
- Source field tracking (products/icecat_index)

## Endpoints
- GET /health - Health check
- GET /products/search - Product search
- POST /askme/code - Massive code search
- POST /products/import - Import products
- GET /categories - Get categories
- GET /brands - Get brands

## Authentication
- Public endpoints: No token required
- Admin endpoints: Bearer token required
- Admin token: Set in ADMIN_MASTER_TOKEN env var

## Port
8017

## Dependencies
- FastAPI
- Uvicorn
- MongoDB
- Redis
- Pulsar Client
