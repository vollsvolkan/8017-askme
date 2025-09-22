"""
Askme Service - Product Search and Import API
Port: 8017
"""
from fastapi import FastAPI, HTTPException, Depends, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
import uvicorn
import sys
import os
import asyncio
import logging
import traceback
from typing import List, Optional, Dict, Any
from bson import ObjectId
import json
from pydantic import BaseModel

# Add shared library to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../shared'))

from database.connection import (
    get_collection, 
    get_redis_client, 
    close_connections,
    get_collection_async,
    get_redis_client_async
)
from config.config_manager import ConfigManager
from worker_pool import worker_pool, start_worker_pool, stop_worker_pool

# Load configuration
config = ConfigManager()

# Security
security = HTTPBearer(auto_error=False)

def verify_admin_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Verify admin token or allow access without token for public endpoints"""
    if credentials is None:
        # No token provided - allow access for public endpoints
        return None
    
    token = credentials.credentials
    admin_token = os.getenv('ADMIN_MASTER_TOKEN', 'admin-super-secret-master-key-2024')
    
    if token == admin_token:
        return {"role": "admin", "token": token}
    else:
        raise HTTPException(
            status_code=401, 
            detail="Invalid admin token"
        )

def convert_objectid(obj):
    """Convert MongoDB ObjectId to string for JSON serialization"""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_objectid(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(item) for item in obj]
    return obj

# Pydantic Models for API Documentation
class CodeSearchRequest(BaseModel):
    codes: List[str]
    types: Optional[List[str]] = None
    parallel: bool = True

class CodeSearchResult(BaseModel):
    worker_id: int
    search_code: str
    search_type: str
    result: Optional[Dict[str, Any]] = None
    timestamp: str
    status: str

class CodeSearchResponse(BaseModel):
    service: str
    request: Dict[str, Any]
    results: Dict[str, Any]
    performance: Dict[str, Any]
    worker_pool: Dict[str, Any]
    timestamp: str

# Create FastAPI app
app = FastAPI(
    title="Askme Service",
    description="Product Search and Import Service - Search products in database and import new ones",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add security scheme to OpenAPI
app.openapi_schema = None

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    from fastapi.openapi.utils import get_openapi
    openapi_schema = get_openapi(
        title="Askme Service",
        version="2.0.0",
        description="Product Search and Import Service - Search products in database and import new ones",
        routes=app.routes,
    )
    
    # Add security scheme
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Admin token for protected endpoints"
        }
    }
    
    # Add security to protected endpoints
    for path in openapi_schema["paths"]:
        for method in openapi_schema["paths"][path]:
            if method in ["post", "put", "delete"]:
                openapi_schema["paths"][path][method]["security"] = [{"BearerAuth": []}]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    print(f"🚀 Askme Service starting up...")
    print(f"📅 Started at: {datetime.utcnow()}")
    print(f"🔧 Port: 8017")
    print(f"🌐 Host: {config.get_host()}")
    print(f"🗄️ MongoDB: {config.get_mongo_uri()}")
    print(f"🔴 Redis: {config.get_redis_host()}:{config.get_redis_port()}")
    
    # Test database connections
    try:
        # Test MongoDB
        products_collection = get_collection("products")
        print("✅ MongoDB connection successful")
        
        # Test Redis
        redis_client = get_redis_client()
        redis_client.ping()
        print("✅ Redis connection successful")
        
        # Start worker pool for high-performance searches
        print("⚙️ Starting high-performance worker pool...")
        await start_worker_pool()
        print("✅ Worker pool started successfully")
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event"""
    print(f"🛑 Askme Service shutting down...")
    
    # Stop worker pool
    print("⚙️ Stopping worker pool...")
    await stop_worker_pool()
    print("✅ Worker pool stopped")
    
    close_connections()

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "askme-service",
        "version": "2.0.0",
        "status": "running",
        "port": 8017,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test MongoDB
        products_collection = get_collection("products")
        mongo_status = "healthy"
    except Exception as e:
        mongo_status = f"unhealthy: {str(e)}"
    
    try:
        # Test Redis
        redis_client = get_redis_client()
        redis_client.ping()
        redis_status = "healthy"
    except Exception as e:
        redis_status = f"unhealthy: {str(e)}"
    
    return {
        "service": "askme-service",
        "status": "healthy" if mongo_status == "healthy" and redis_status == "healthy" else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0",
        "port": 8017,
        "databases": {
            "mongodb": mongo_status,
            "redis": redis_status
        }
    }

@app.get("/products/search")
async def search_products(
    query: str = Query(..., description="Search query"),
    limit: int = Query(10, description="Maximum number of results"),
    skip: int = Query(0, description="Number of results to skip")
):
    """Search products in database"""
    try:
        products_collection = get_collection("products")
        
        # Create comprehensive search filter - search in multiple fields including barcode, SKU, EAN, UPC, GTIN, MPN
        search_filter = {
            "$or": [
                # Basic text fields
                {"name": {"$regex": query, "$options": "i"}},
                {"description": {"$regex": query, "$options": "i"}},
                {"brand": {"$regex": query, "$options": "i"}},
                {"category": {"$regex": query, "$options": "i"}},
                {"sku": {"$regex": query, "$options": "i"}},
                
                # Barcode fields (multiple variations)
                {"barcode": {"$regex": query, "$options": "i"}},
                {"barcode1": {"$regex": query, "$options": "i"}},
                {"barcode2": {"$regex": query, "$options": "i"}},
                {"barcode3": {"$regex": query, "$options": "i"}},
                {"barcode4": {"$regex": query, "$options": "i"}},
                {"barcode5": {"$regex": query, "$options": "i"}},
                {"barcode6": {"$regex": query, "$options": "i"}},
                {"barcode7": {"$regex": query, "$options": "i"}},
                {"barcode8": {"$regex": query, "$options": "i"}},
                {"barcode9": {"$regex": query, "$options": "i"}},
                {"barcode10": {"$regex": query, "$options": "i"}},
                {"barcode11": {"$regex": query, "$options": "i"}},
                {"barcode12": {"$regex": query, "$options": "i"}},
                {"barcode13": {"$regex": query, "$options": "i"}},
                {"barcode14": {"$regex": query, "$options": "i"}},
                {"barcode15": {"$regex": query, "$options": "i"}},
                
                # EAN/UPC/GTIN fields (array and single values)
                {"EAN": {"$regex": query, "$options": "i"}},
                {"EAN_UPCS.EAN_UPC.@Value": {"$regex": query, "$options": "i"}},
                {"EAN_UPCS.EAN_UPC": {"$elemMatch": {"@Value": {"$regex": query, "$options": "i"}}}},
                {"UPC": {"$regex": query, "$options": "i"}},
                {"GTIN": {"$regex": query, "$options": "i"}},
                
                # MPN and product identifiers
                {"MPN": {"$regex": query, "$options": "i"}},
                {"M_Prod_ID": {"$regex": query, "$options": "i"}},
                {"M_Prod_ID.#text": {"$regex": query, "$options": "i"}},
                {"product_id": {"$regex": query, "$options": "i"}},
                {"@Product_ID": {"$regex": query, "$options": "i"}},
                {"@Prod_ID": {"$regex": query, "$options": "i"}},
                {"UUID_product": {"$regex": query, "$options": "i"}},
                
                # Additional searchable fields
                {"@Model_Name": {"$regex": query, "$options": "i"}},
                {"@Supplier_name": {"$regex": query, "$options": "i"}},
                {"@Supplier_id": {"$regex": query, "$options": "i"}}
            ]
        }
        
        # Execute search
        cursor = products_collection.find(search_filter).skip(skip).limit(limit)
        products = list(cursor)
        
        # Count total results
        total = products_collection.count_documents(search_filter)
        
        return {
            "query": query,
            "total": total,
            "results": convert_objectid(products),
            "limit": limit,
            "skip": skip,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.get("/products/{product_id}")
async def get_product(product_id: str):
    """Get product by ID"""
    try:
        products_collection = get_collection("products")
        product = products_collection.find_one({"_id": product_id})
        
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        return {
            "product": convert_objectid(product),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get product: {str(e)}")

@app.get("/products")
async def list_products(
    page: int = Query(1, description="Page number"),
    size: int = Query(20, description="Page size"),
    category: Optional[str] = Query(None, description="Filter by category"),
    brand: Optional[str] = Query(None, description="Filter by brand")
):
    """List products with pagination and filters"""
    try:
        products_collection = get_collection("products")
        
        # Build filter
        filter_query = {}
        if category:
            filter_query["category"] = {"$regex": category, "$options": "i"}
        if brand:
            filter_query["brand"] = {"$regex": brand, "$options": "i"}
        
        # Calculate skip
        skip = (page - 1) * size
        
        # Execute query
        cursor = products_collection.find(filter_query).skip(skip).limit(size)
        products = list(cursor)
        
        # Count total
        total = products_collection.count_documents(filter_query)
        
        return {
            "products": convert_objectid(products),
            "pagination": {
                "page": page,
                "size": size,
                "total": total,
                "pages": (total + size - 1) // size
            },
            "filters": {
                "category": category,
                "brand": brand
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list products: {str(e)}")

@app.post("/products/import")
async def import_product(product_data: Dict[str, Any], auth: Optional[Dict] = Depends(verify_admin_token)):
    """Import a new product"""
    try:
        products_collection = get_collection("products")
        
        # Add timestamp
        product_data["created_at"] = datetime.utcnow()
        product_data["updated_at"] = datetime.utcnow()
        
        # Insert product
        result = products_collection.insert_one(product_data)
        
        return {
            "message": "Product imported successfully",
            "product_id": str(result.inserted_id),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@app.put("/products/{product_id}")
async def update_product(product_id: str, product_data: Dict[str, Any], auth: Optional[Dict] = Depends(verify_admin_token)):
    """Update an existing product"""
    try:
        products_collection = get_collection("products")
        
        # Add update timestamp
        product_data["updated_at"] = datetime.utcnow()
        
        # Update product
        result = products_collection.update_one(
            {"_id": product_id},
            {"$set": product_data}
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Product not found")
        
        return {
            "message": "Product updated successfully",
            "product_id": product_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@app.delete("/products/{product_id}")
async def delete_product(product_id: str, auth: Optional[Dict] = Depends(verify_admin_token)):
    """Delete a product"""
    try:
        products_collection = get_collection("products")
        
        result = products_collection.delete_one({"_id": product_id})
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Product not found")
        
        return {
            "message": "Product deleted successfully",
            "product_id": product_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.get("/categories")
async def get_categories():
    """Get all product categories"""
    try:
        products_collection = get_collection("products")
        
        # Aggregate to get unique categories
        pipeline = [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        categories = list(products_collection.aggregate(pipeline))
        
        return {
            "categories": convert_objectid(categories),
            "total_categories": len(categories),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get categories: {str(e)}")

@app.get("/brands")
async def get_brands():
    """Get all product brands"""
    try:
        products_collection = get_collection("products")
        
        # Aggregate to get unique brands
        pipeline = [
            {"$group": {"_id": "$brand", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        brands = list(products_collection.aggregate(pipeline))
        
        return {
            "brands": convert_objectid(brands),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get brands: {str(e)}")

@app.get("/stats")
async def get_stats():
    """Get service statistics"""
    try:
        products_collection = get_collection("products")
        
        # Get basic stats
        total_products = products_collection.count_documents({})
        
        # Get category stats
        category_pipeline = [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_categories = list(products_collection.aggregate(category_pipeline))
        
        # Get brand stats
        brand_pipeline = [
            {"$group": {"_id": "$brand", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_brands = list(products_collection.aggregate(brand_pipeline))
        
        return {
            "total_products": total_products,
            "top_categories": convert_objectid(top_categories),
            "top_brands": convert_objectid(top_brands),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.get("/icecat_index")
async def get_icecat_index(
    skip: int = Query(0, ge=0, description="Number of documents to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of documents to return")
):
    """Get icecat_index collection documents"""
    try:
        icecat_collection = get_collection("icecat_index")
        
        # Get documents with pagination
        documents = list(icecat_collection.find({}).skip(skip).limit(limit))
        
        return {
            "collection": "icecat_index",
            "total": len(documents),
            "skip": skip,
            "limit": limit,
            "results": convert_objectid(documents),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get icecat_index: {str(e)}")

@app.get("/icecat_index/search")
async def search_icecat_index(
    query: str = Query(..., description="Search query"),
    skip: int = Query(0, ge=0, description="Number of documents to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of documents to return")
):
    """Search icecat_index collection"""
    try:
        icecat_collection = get_collection("icecat_index")
        
        # Create comprehensive search filter
        search_filter = {
            "$or": [
                # Basic text fields
                {"@Model_Name": {"$regex": query, "$options": "i"}},
                {"@Product_ID": {"$regex": query, "$options": "i"}},
                {"@Prod_ID": {"$regex": query, "$options": "i"}},
                {"M_Prod_ID.#text": {"$regex": query, "$options": "i"}},
                
                # EAN/UPC/GTIN fields
                {"EAN_UPCS.EAN_UPC.@Value": {"$regex": query, "$options": "i"}},
                {"EAN_UPCS.EAN_UPC": {"$elemMatch": {"@Value": {"$regex": query, "$options": "i"}}}},
                
                # Additional searchable fields
                {"@Supplier_name": {"$regex": query, "$options": "i"}},
                {"@Supplier_id": {"$regex": query, "$options": "i"}}
            ]
        }
        
        # Execute search
        documents = list(icecat_collection.find(search_filter).skip(skip).limit(limit))
        
        return {
            "query": query,
            "total": len(documents),
            "results": convert_objectid(documents),
            "skip": skip,
            "limit": limit,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search icecat_index: {str(e)}")

@app.post("/code")
async def search_by_code_massive(
    request: Dict[str, Any],
    auth: Optional[Dict] = Depends(verify_admin_token)
):
    """MASSIVE CODE SEARCH - Mission Critical Service for millions of barcodes"""
    try:
        search_codes = request.get("codes", [])
        search_types = request.get("types", None)  # Optional: specify search types
        use_parallel = request.get("parallel", True)  # Use parallel processing
        
        if not search_codes:
            raise HTTPException(status_code=400, detail="No search codes provided")
        
        total_codes = len(search_codes)
        print(f"🚀 MASSIVE CODE SEARCH REQUEST: {total_codes:,} codes")
        
        # Validate input size
        if total_codes > 10000000:  # 10 million limit
            raise HTTPException(status_code=400, detail="Too many codes. Maximum 10 million allowed.")
        
        start_time = datetime.utcnow()
        
        if use_parallel and total_codes > 1:
            # Use high-performance parallel search
            results = await worker_pool.search_by_code_parallel(search_codes, search_types)
        else:
            # Single code search
            results = []
            for i, code in enumerate(search_codes):
                search_type = search_types[i] if search_types and i < len(search_types) else "auto"
                result = await worker_pool.search_by_code_single(code, search_type)
                results.append(result)
        
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        # Calculate statistics
        successful_searches = len([r for r in results if r.get('status') == 'success'])
        failed_searches = len([r for r in results if r.get('status') == 'error'])
        found_products = len([r for r in results if r.get('result')])
        
        return {
            "service": "askme-code-search",
            "request": {
                "total_codes": total_codes,
                "use_parallel": use_parallel,
                "search_types": search_types
            },
            "results": {
                "total_processed": len(results),
                "successful": successful_searches,
                "failed": failed_searches,
                "products_found": found_products,
                "success_rate": f"{(successful_searches/total_codes*100):.2f}%" if total_codes > 0 else "0%",
                "details": results  # Add detailed results
            },
            "performance": {
                "processing_time_seconds": processing_time,
                "codes_per_second": f"{total_codes/processing_time:.2f}" if processing_time > 0 else "0",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            },
            "worker_pool": {
                "total_workers": worker_pool.worker_count,
                "active_workers": len([w for w in worker_pool.workers if w.status == "searching"]),
                "idle_workers": len([w for w in worker_pool.workers if w.status == "idle"])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Massive code search failed: {str(e)}")

@app.get("/code/status")
async def get_code_search_status():
    """Get worker pool status and performance metrics"""
    try:
        pool_status = worker_pool.get_pool_status()
        worker_details = worker_pool.get_worker_details()
        
        # Calculate performance metrics
        total_tasks = sum(w.get('tasks_processed', 0) for w in worker_details)
        active_workers = len([w for w in worker_details if w.get('status') == 'searching'])
        idle_workers = len([w for w in worker_details if w.get('status') == 'idle'])
        
        return {
            "service": "askme-code-search",
            "worker_pool": {
                **pool_status,
                "performance_metrics": {
                    "total_tasks_processed": total_tasks,
                    "active_workers": active_workers,
                    "idle_workers": idle_workers,
                    "utilization_rate": f"{(active_workers/worker_pool.worker_count*100):.2f}%" if worker_pool.worker_count > 0 else "0%"
                }
            },
            "workers": worker_details,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")

@app.post("/code/bulk-index")
async def bulk_index_products(
    request: Dict[str, Any],
    auth: Optional[Dict] = Depends(verify_admin_token)
):
    """Bulk index products to Redis for faster searches"""
    try:
        limit = request.get("limit", 10000)
        clear_existing = request.get("clear_existing", False)
        
        if clear_existing:
            from redis_search_manager import redis_search_manager
            redis_search_manager.clear_cache()
            print("🗑️ Existing cache cleared")
        
        # Start bulk indexing
        from redis_search_manager import redis_search_manager
        indexed_count = redis_search_manager.bulk_index_products(limit)
        
        return {
            "message": "Bulk indexing completed",
            "indexed_products": indexed_count,
            "limit": limit,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk indexing failed: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8017,
        reload=True,
        log_level="info"
    )
