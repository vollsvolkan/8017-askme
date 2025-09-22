"""
Worker Pool for Parallel Product Search
Mission Critical Service with 8 concurrent workers
"""
import asyncio
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import sys
import os
import json

# Add shared library to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../shared'))

from database.connection import get_collection
from redis_search_manager import redis_search_manager

# Pulsar imports
try:
    import pulsar
    PULSAR_AVAILABLE = True
except ImportError:
    PULSAR_AVAILABLE = False
    print("⚠️ Pulsar client not available. Install with: pip install pulsar-client")


class SearchWorker:
    """Individual search worker for parallel processing"""
    
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.status = "idle"
        self.tasks_processed = 0
        self.last_activity = None
        self.pulsar_client = None
        self.pulsar_producer = None
        
    async def _init_pulsar(self):
        """Initialize Pulsar client and producer"""
        if not PULSAR_AVAILABLE:
            return
            
        try:
            # Get Pulsar URL from environment
            pulsar_url = os.getenv('PULSAR_URL', 'pulsar://event.volls.me:6650')
            
            # Create Pulsar client
            self.pulsar_client = pulsar.Client(pulsar_url)
            
            # Create producer for icecat_products-find event
            self.pulsar_producer = self.pulsar_client.create_producer(
                'icecat_products-find',
                producer_name=f'askme-worker-{self.worker_id}'
            )
            
            print(f"✅ Worker {self.worker_id}: Pulsar producer initialized")
            
        except Exception as e:
            print(f"❌ Worker {self.worker_id}: Pulsar initialization failed: {e}")
            self.pulsar_client = None
            self.pulsar_producer = None
    
    async def _send_icecat_event(self, code: str, product_data: Dict[str, Any]):
        """Send event to Pulsar when product found in icecat_index"""
        if not self.pulsar_producer:
            return
            
        try:
            event_data = {
                "event_type": "icecat_products_find",
                "timestamp": datetime.utcnow().isoformat(),
                "worker_id": self.worker_id,
                "search_code": code,
                "source": "icecat_index",
                "product_data": product_data
            }
            
            # Send event to Pulsar
            self.pulsar_producer.send(
                json.dumps(event_data).encode('utf-8'),
                properties={
                    'event_type': 'icecat_products_find',
                    'source': 'askme-service',
                    'worker_id': str(self.worker_id)
                }
            )
            
            print(f"📡 Worker {self.worker_id}: Icecat event sent for code {code}")
            
        except Exception as e:
            print(f"❌ Worker {self.worker_id}: Failed to send Pulsar event: {e}")
        
    async def search_by_code(self, search_code: str, search_type: str = "auto") -> Dict[str, Any]:
        """Search product by code using this worker"""
        try:
            self.status = "searching"
            self.last_activity = datetime.utcnow()
            
            print(f"🔍 Worker {self.worker_id}: Searching for {search_type} = {search_code}")
            
            # Determine search type if auto
            if search_type == "auto":
                search_type = self._detect_search_type(search_code)
            
            # Perform search based on type
            result = await self._perform_search(search_code, search_type)
            
            self.tasks_processed += 1
            self.status = "idle"
            
            return {
                "worker_id": self.worker_id,
                "search_code": search_code,
                "search_type": search_type,
                "result": result,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            self.status = "error"
            print(f"❌ Worker {self.worker_id} error: {e}")
            
            return {
                "worker_id": self.worker_id,
                "search_code": search_code,
                "search_type": search_type,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "error"
            }
    
    def _detect_search_type(self, code: str) -> str:
        """Auto-detect search type based on code format"""
        if not code:
            return "unknown"
        
        code = str(code).strip()
        
        # EAN-13: 13 digits
        if len(code) == 13 and code.isdigit():
            return "ean"
        
        # EAN-12: 12 digits
        if len(code) == 12 and code.isdigit():
            return "ean"
        
        # UPC: 12 digits
        if len(code) == 12 and code.isdigit():
            return "upc"
        
        # GTIN: 8-14 digits
        if 8 <= len(code) <= 14 and code.isdigit():
            return "gtin"
        
        # SKU: alphanumeric, usually shorter
        if len(code) <= 20 and any(c.isalpha() for c in code):
            return "sku"
        
        # MPN: alphanumeric, can be longer
        if len(code) <= 50:
            return "mpn"
        
        # Barcode: default fallback
        return "barcode"
    
    async def _perform_search(self, code: str, search_type: str) -> Optional[Dict[str, Any]]:
        """Perform search based on type"""
        try:
            if search_type == "ean":
                return redis_search_manager.search_by_ean(code)
            elif search_type == "upc":
                return redis_search_manager.search_by_ean(code)  # UPC is also EAN
            elif search_type == "gtin":
                return redis_search_manager.search_by_ean(code)  # GTIN includes EAN
            elif search_type == "mpn":
                return redis_search_manager.search_by_mpn(code)
            elif search_type == "sku":
                return redis_search_manager.search_by_sku(code)
            elif search_type == "barcode":
                return redis_search_manager.search_by_barcode(code)
            else:
                # Fallback to comprehensive search
                return await self._comprehensive_search(code)
                
        except Exception as e:
            print(f"❌ Worker {self.worker_id} search error: {e}")
            return None
    
    async def _comprehensive_search(self, code: str) -> Optional[Dict[str, Any]]:
        """Comprehensive search across all fields in both collections"""
        try:
            # First search in products collection
            products_collection = get_collection("products")
            
            # Create comprehensive search filter for products
            products_search_filter = {
                "$or": [
                    # Barcode fields
                    {"barcode": code},
                    {"barcode1": code},
                    {"barcode2": code},
                    {"barcode3": code},
                    {"barcode4": code},
                    {"barcode5": code},
                    {"barcode6": code},
                    {"barcode7": code},
                    {"barcode8": code},
                    {"barcode9": code},
                    {"barcode10": code},
                    {"barcode11": code},
                    {"barcode12": code},
                    {"barcode13": code},
                    {"barcode14": code},
                    {"barcode15": code},
                    
                    # EAN/UPC/GTIN fields
                    {"EAN": code},
                    {"UPC": code},
                    {"GTIN": code},
                    {"EAN_UPCS.EAN_UPC": {"$elemMatch": {"@Value": code}}},
                    
                    # MPN fields
                    {"MPN": code},
                    {"M_Prod_ID": code},
                    {"M_Prod_ID.#text": code},
                    
                    # SKU and identifiers
                    {"sku": code},
                    {"product_id": code},
                    {"@Product_ID": code},
                    {"@Prod_ID": code},
                    {"UUID_product": code}
                ]
            }
            
            product = products_collection.find_one(products_search_filter)
            if product:
                product['source'] = 'products'
                return product
            
            # If not found in products, search in icecat_index collection
            icecat_collection = get_collection("icecat_index")
            
            # Create comprehensive search filter for icecat_index
            icecat_search_filter = {
                "$or": [
                    # EAN/UPC/GTIN fields (icecat_index format)
                    {"EAN_UPCS.EAN_UPC": {"$elemMatch": {"@Value": code}}},
                    
                    # Product identifiers
                    {"@Product_ID": code},
                    {"@Prod_ID": code},
                    {"M_Prod_ID.#text": code},
                    
                    # Model name
                    {"@Model_Name": {"$regex": code, "$options": "i"}},
                    
                    # Supplier info
                    {"@Supplier_id": code},
                    {"@Supplier_name": {"$regex": code, "$options": "i"}}
                ]
            }
            
            product = icecat_collection.find_one(icecat_search_filter)
            if product:
                product['source'] = 'icecat_index'
                
                # Send Pulsar event for icecat_index products
                await self._send_icecat_event(code, product)
                
                return product
            
            return None
            
        except Exception as e:
            print(f"❌ Worker {self.worker_id} comprehensive search error: {e}")
            return None


class WorkerPool:
    """High-performance worker pool for millions of concurrent barcode searches"""
    
    def __init__(self, worker_count: int = 32):  # Increased to 32 workers
        self.worker_count = worker_count
        self.workers: List[SearchWorker] = []
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
        self.is_running = False
        self.batch_size = 1000  # Process 1000 codes per batch
        self.max_concurrent_batches = 10  # 10 concurrent batches
        
    async def start(self):
        """Start the worker pool"""
        try:
            print(f"🚀 Starting Worker Pool with {self.worker_count} workers...")
            
            # Create workers
            for i in range(self.worker_count):
                worker = SearchWorker(i + 1)
                self.workers.append(worker)
                
                # Initialize Pulsar for each worker
                await worker._init_pulsar()
            
            self.is_running = True
            print(f"✅ Worker Pool started with {len(self.workers)} workers")
            
        except Exception as e:
            print(f"❌ Error starting worker pool: {e}")
            raise
    
    async def stop(self):
        """Stop the worker pool"""
        try:
            print("🛑 Stopping Worker Pool...")
            self.is_running = False
            
            # Close Pulsar connections for all workers
            for worker in self.workers:
                if worker.pulsar_producer:
                    worker.pulsar_producer.close()
                if worker.pulsar_client:
                    worker.pulsar_client.close()
            
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            print("✅ Worker Pool stopped")
            
        except Exception as e:
            print(f"❌ Error stopping worker pool: {e}")
    
    async def search_by_code_parallel(self, search_codes: List[str], search_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search multiple codes in parallel using all workers - optimized for millions of codes"""
        try:
            if not self.is_running:
                raise RuntimeError("Worker pool is not running")
            
            if not search_codes:
                return []
            
            total_codes = len(search_codes)
            print(f"🚀 Starting MASSIVE parallel search for {total_codes:,} codes...")
            
            # For millions of codes, use batch processing
            if total_codes > 10000:
                return await self._batch_search_millions(search_codes, search_types)
            
            # For smaller amounts, use regular parallel processing
            return await self._parallel_search_small(search_codes, search_types)
            
        except Exception as e:
            print(f"❌ Error in parallel search: {e}")
            return []
    
    async def _parallel_search_small(self, search_codes: List[str], search_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Parallel search for smaller amounts of codes"""
        try:
            print(f"🔍 Using small batch parallel search for {len(search_codes)} codes...")
            
            # Prepare search tasks
            tasks = []
            for i, code in enumerate(search_codes):
                search_type = search_types[i] if search_types and i < len(search_types) else "auto"
                worker = self.workers[i % len(self.workers)]
                task = worker.search_by_code(code, search_type)
                tasks.append(task)
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processed_results.append({
                        "search_code": search_codes[i],
                        "error": str(result),
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "error"
                    })
                else:
                    processed_results.append(result)
            
            print(f"✅ Small batch search completed. Found {len([r for r in processed_results if r.get('status') == 'success'])} results")
            return processed_results
            
        except Exception as e:
            print(f"❌ Error in small batch search: {e}")
            return []
    
    async def _batch_search_millions(self, search_codes: List[str], search_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Batch search for millions of codes - optimized for high throughput"""
        try:
            total_codes = len(search_codes)
            print(f"🔥 MASSIVE BATCH SEARCH: Processing {total_codes:,} codes in batches of {self.batch_size}")
            
            all_results = []
            processed_count = 0
            
            # Process in batches
            for batch_start in range(0, total_codes, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_codes)
                batch_codes = search_codes[batch_start:batch_end]
                batch_types = None
                if search_types:
                    batch_types = search_types[batch_start:batch_end]
                
                print(f"📦 Processing batch {batch_start//self.batch_size + 1}: codes {batch_start:,} to {batch_end:,}")
                
                # Process this batch
                batch_results = await self._process_batch(batch_codes, batch_types)
                all_results.extend(batch_results)
                
                processed_count += len(batch_codes)
                print(f"📊 Progress: {processed_count:,}/{total_codes:,} codes processed ({processed_count/total_codes*100:.1f}%)")
                
                # Small delay to prevent overwhelming the system
                if batch_end < total_codes:
                    await asyncio.sleep(0.01)
            
            print(f"🎉 MASSIVE BATCH SEARCH COMPLETED! Processed {len(all_results):,} results")
            return all_results
            
        except Exception as e:
            print(f"❌ Error in massive batch search: {e}")
            return []
    
    async def _process_batch(self, batch_codes: List[str], batch_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Process a single batch of codes"""
        try:
            # Create tasks for this batch
            tasks = []
            for i, code in enumerate(batch_codes):
                search_type = batch_types[i] if batch_types and i < len(batch_types) else "auto"
                worker = self.workers[i % len(self.workers)]
                task = worker.search_by_code(code, search_type)
                tasks.append(task)
            
            # Execute batch tasks concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process batch results
            processed_results = []
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    processed_results.append({
                        "search_code": batch_codes[i],
                        "error": str(result),
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "error"
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
            
        except Exception as e:
            print(f"❌ Error processing batch: {e}")
            return []
    
    async def search_by_code_single(self, search_code: str, search_type: str = "auto") -> Dict[str, Any]:
        """Search single code using available worker"""
        try:
            if not self.is_running:
                raise RuntimeError("Worker pool is not running")
            
            # Find available worker
            available_worker = None
            for worker in self.workers:
                if worker.status == "idle":
                    available_worker = worker
                    break
            
            if not available_worker:
                # All workers busy, use first one
                available_worker = self.workers[0]
            
            # Perform search
            result = await available_worker.search_by_code(search_code, search_type)
            return result
            
        except Exception as e:
            print(f"❌ Error in single search: {e}")
            return {
                "search_code": search_code,
                "search_type": search_type,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "error"
            }
    
    def get_pool_status(self) -> Dict[str, Any]:
        """Get worker pool status"""
        try:
            worker_statuses = []
            for worker in self.workers:
                worker_statuses.append({
                    "worker_id": worker.worker_id,
                    "status": worker.status,
                    "tasks_processed": worker.tasks_processed,
                    "last_activity": worker.last_activity.isoformat() if worker.last_activity else None
                })
            
            return {
                "total_workers": len(self.workers),
                "running": self.is_running,
                "workers": worker_statuses,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_worker_details(self) -> List[Dict[str, Any]]:
        """Get detailed worker information"""
        try:
            details = []
            for worker in self.workers:
                details.append({
                    "worker_id": worker.worker_id,
                    "status": worker.status,
                    "tasks_processed": worker.tasks_processed,
                    "last_activity": worker.last_activity.isoformat() if worker.last_activity else None,
                    "uptime": (datetime.utcnow() - worker.last_activity).total_seconds() if worker.last_activity else None
                })
            return details
            
        except Exception as e:
            return [{"error": str(e)}]


# Global worker pool instance - optimized for millions of searches
worker_pool = WorkerPool(32)  # 32 workers for high throughput


async def start_worker_pool():
    """Start the global worker pool"""
    await worker_pool.start()


async def stop_worker_pool():
    """Stop the global worker pool"""
    await worker_pool.stop()
