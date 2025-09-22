"""
Redis Search Manager for Fast Product Lookup
Combines MongoDB indexes with Redis caching for machine-level performance
"""
import json
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import sys
import os

# Add shared library to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../shared'))

from database.connection import get_redis_client, get_collection


class RedisSearchManager:
    """Redis Search Manager for fast product lookup"""
    
    def __init__(self):
        self.redis_client = get_redis_client()
        self.cache_ttl = 3600  # 1 hour cache TTL
        self.search_index_prefix = "product_search:"
        self.barcode_index_prefix = "barcode:"
        self.ean_index_prefix = "ean:"
        self.upc_index_prefix = "upc:"
        self.gtin_index_prefix = "gtin:"
        self.mpn_index_prefix = "mpn:"
        self.sku_index_prefix = "sku:"
        
    def _generate_cache_key(self, search_type: str, value: str) -> str:
        """Generate cache key for search"""
        return f"{search_type}:{hashlib.md5(value.encode()).hexdigest()}"
    
    def _get_product_cache_key(self, product_id: str) -> str:
        """Get product cache key"""
        return f"product:{product_id}"
    
    def cache_product(self, product: Dict[str, Any]) -> None:
        """Cache a product in Redis"""
        try:
            product_id = str(product.get('_id', ''))
            if not product_id:
                return
                
            # Cache the full product
            cache_key = self._get_product_cache_key(product_id)
            self.redis_client.setex(
                cache_key, 
                self.cache_ttl, 
                json.dumps(product, default=str)
            )
            
            # Index by barcode fields
            self._index_barcode_fields(product)
            
            # Index by EAN/UPC/GTIN fields
            self._index_ean_upc_gtin_fields(product)
            
            # Index by MPN
            self._index_mpn_fields(product)
            
            # Index by SKU
            self._index_sku_fields(product)
            
            print(f"✅ Product {product_id} cached and indexed in Redis")
            
        except Exception as e:
            print(f"❌ Error caching product: {e}")
    
    def _index_barcode_fields(self, product: Dict[str, Any]) -> None:
        """Index product by barcode fields"""
        try:
            product_id = str(product.get('_id', ''))
            
            # Index all barcode variations
            for i in range(1, 16):
                barcode_key = f"barcode{i}" if i > 1 else "barcode"
                barcode_value = product.get(barcode_key, "")
                
                if barcode_value:
                    index_key = f"{self.barcode_index_prefix}{barcode_value}"
                    self.redis_client.setex(
                        index_key,
                        self.cache_ttl,
                        product_id
                    )
                    
        except Exception as e:
            print(f"❌ Error indexing barcode fields: {e}")
    
    def _index_ean_upc_gtin_fields(self, product: Dict[str, Any]) -> None:
        """Index product by EAN/UPC/GTIN fields"""
        try:
            product_id = str(product.get('_id', ''))
            collection_name = product.get('_collection', 'products')  # Track collection
            
            # Index EAN field
            ean_value = product.get('EAN', "")
            if ean_value:
                index_key = f"{self.ean_index_prefix}{ean_value}"
                self.redis_client.setex(
                    index_key,
                    self.cache_ttl,
                    f"{product_id}:{collection_name}"
                )
            
            # Index EAN_UPCS array (icecat_index format)
            ean_upcs = product.get('EAN_UPCS', {})
            if ean_upcs and isinstance(ean_upcs, dict):
                ean_upc_list = ean_upcs.get('EAN_UPC', [])
                if isinstance(ean_upc_list, list):
                    for ean_upc in ean_upc_list:
                        if isinstance(ean_upc, dict):
                            ean_value = ean_upc.get('@Value', '')
                            if ean_value:
                                # Index as EAN
                                index_key = f"{self.ean_index_prefix}{ean_value}"
                                self.redis_client.setex(
                                    index_key,
                                    self.cache_ttl,
                                    f"{product_id}:{collection_name}"
                                )
                                
                                # Index as UPC if format is GTIN-12
                                if ean_upc.get('@Format') == 'GTIN-12':
                                    index_key = f"{self.upc_index_prefix}{ean_value}"
                                    self.redis_client.setex(
                                        index_key,
                                        self.cache_ttl,
                                        f"{product_id}:{collection_name}"
                                    )
                                
                                # Index as GTIN
                                index_key = f"{self.gtin_index_prefix}{ean_value}"
                                self.redis_client.setex(
                                    index_key,
                                    self.cache_ttl,
                                    f"{product_id}:{collection_name}"
                                )
            
            # Index UPC field
            upc_value = product.get('UPC', "")
            if upc_value:
                index_key = f"{self.upc_index_prefix}{upc_value}"
                self.redis_client.setex(
                    index_key,
                    self.cache_ttl,
                    f"{product_id}:{collection_name}"
                )
            
            # Index GTIN field
            gtin_value = product.get('GTIN', "")
            if gtin_value:
                index_key = f"{self.gtin_index_prefix}{gtin_value}"
                self.redis_client.setex(
                    index_key,
                    self.cache_ttl,
                    f"{product_id}:{collection_name}"
                )
                    
        except Exception as e:
            print(f"❌ Error indexing EAN/UPC/GTIN fields: {e}")
    
    def _index_mpn_fields(self, product: Dict[str, Any]) -> None:
        """Index product by MPN fields"""
        try:
            product_id = str(product.get('_id', ''))
            
            # Index MPN field
            mpn_value = product.get('MPN', "")
            if mpn_value:
                index_key = f"{self.mpn_index_prefix}{mpn_value}"
                self.redis_client.setex(
                    index_key,
                    self.cache_ttl,
                    product_id
                )
            
            # Index M_Prod_ID array
            m_prod_ids = product.get('M_Prod_ID', [])
            if isinstance(m_prod_ids, list):
                for m_prod_id in m_prod_ids:
                    if isinstance(m_prod_id, str):
                        index_key = f"{self.mpn_index_prefix}{m_prod_id}"
                        self.redis_client.setex(
                            index_key,
                            self.cache_ttl,
                            product_id
                        )
                    elif isinstance(m_prod_id, dict) and '#text' in m_prod_id:
                        index_key = f"{self.mpn_index_prefix}{m_prod_id['#text']}"
                        self.redis_client.setex(
                            index_key,
                            self.cache_ttl,
                            product_id
                        )
                    
        except Exception as e:
            print(f"❌ Error indexing MPN fields: {e}")
    
    def _index_sku_fields(self, product: Dict[str, Any]) -> None:
        """Index product by SKU field"""
        try:
            product_id = str(product.get('_id', ''))
            sku_value = product.get('sku', "")
            
            if sku_value:
                index_key = f"{self.sku_index_prefix}{sku_value}"
                self.redis_client.setex(
                    index_key,
                    self.cache_ttl,
                    product_id
                )
                    
        except Exception as e:
            print(f"❌ Error indexing SKU field: {e}")
    
    def search_by_barcode(self, barcode: str) -> Optional[Dict[str, Any]]:
        """Search product by barcode (Redis cache first, then MongoDB)"""
        try:
            # Try Redis cache first
            index_key = f"{self.barcode_index_prefix}{barcode}"
            cached_product_id = self.redis_client.get(index_key)
            
            if cached_product_id:
                # Get product from cache
                product_cache_key = self._get_product_cache_key(cached_product_id.decode())
                cached_product = self.redis_client.get(product_cache_key)
                
                if cached_product:
                    print(f"✅ Product found in Redis cache by barcode: {barcode}")
                    return json.loads(cached_product)
            
            # Fallback to MongoDB search
            print(f"🔍 Searching MongoDB for barcode: {barcode}")
            products_collection = get_collection("products")
            
            # Search in all barcode fields
            barcode_filter = {
                "$or": [
                    {"barcode": barcode},
                    {"barcode1": barcode},
                    {"barcode2": barcode},
                    {"barcode3": barcode},
                    {"barcode4": barcode},
                    {"barcode5": barcode},
                    {"barcode6": barcode},
                    {"barcode7": barcode},
                    {"barcode8": barcode},
                    {"barcode9": barcode},
                    {"barcode10": barcode},
                    {"barcode11": barcode},
                    {"barcode12": barcode},
                    {"barcode13": barcode},
                    {"barcode14": barcode},
                    {"barcode15": barcode}
                ]
            }
            
            product = products_collection.find_one(barcode_filter)
            
            if product:
                # Cache the found product
                self.cache_product(product)
                return product
            
            return None
            
        except Exception as e:
            print(f"❌ Error searching by barcode: {e}")
            return None
    
    def search_by_ean(self, ean: str) -> Optional[Dict[str, Any]]:
        """Search product by EAN in both collections"""
        try:
            # First, search in Redis cache
            index_key = f"{self.ean_index_prefix}{ean}"
            cached_product_ids = self.redis_client.keys(index_key)
            
            if cached_product_ids:
                for product_id_key in cached_product_ids:
                    cached_product_id = self.redis_client.get(product_id_key)
                    if cached_product_id:
                        # Parse collection info
                        if ':' in cached_product_id:
                            product_id, collection_name = cached_product_id.decode().split(':', 1)
                        else:
                            product_id = cached_product_id.decode()
                            collection_name = 'products'
                        
                        # Get the cached product
                        cached_product = self.redis_client.get(f"product:{product_id}")
                        if cached_product:
                            print(f"✅ Product found in Redis cache by EAN: {ean} from {collection_name}")
                            return json.loads(cached_product)
            
            # Fallback to MongoDB search in both collections
            print(f"🔍 Searching MongoDB for EAN: {ean} in both collections...")
            
            # Search in products collection
            products_collection = get_collection("products")
            product = products_collection.find_one({"EAN": ean})
            if product:
                product['_collection'] = 'products'
                self.cache_product(product)
                return product
            
            # Search in products EAN_UPCS array
            product = products_collection.find_one({
                "EAN_UPCS.EAN_UPC.@Value": ean
            })
            if product:
                product['_collection'] = 'products'
                self.cache_product(product)
                return product
            
            # Search in icecat_index collection
            icecat_collection = get_collection("icecat_index")
            product = icecat_collection.find_one({
                "EAN_UPCS.EAN_UPC.@Value": ean
            })
            if product:
                product['_collection'] = 'icecat_index'
                self.cache_product(product)
                return product
            
            return None
            
        except Exception as e:
            print(f"❌ Error searching by EAN: {e}")
            return None
    
    def search_by_mpn(self, mpn: str) -> Optional[Dict[str, Any]]:
        """Search product by MPN (Redis cache first, then MongoDB)"""
        try:
            # Try Redis cache first
            index_key = f"{self.mpn_index_prefix}{mpn}"
            cached_product_id = self.redis_client.get(index_key)
            
            if cached_product_id:
                # Get product from cache
                product_cache_key = self._get_product_cache_key(cached_product_id.decode())
                cached_product = self.redis_client.get(product_cache_key)
                
                if cached_product:
                    print(f"✅ Product found in Redis cache by MPN: {mpn}")
                    return json.loads(cached_product)
            
            # Fallback to MongoDB search
            print(f"🔍 Searching MongoDB for MPN: {mpn}")
            products_collection = get_collection("products")
            
            # Search in MPN fields
            mpn_filter = {
                "$or": [
                    {"MPN": mpn},
                    {"M_Prod_ID": mpn},
                    {"M_Prod_ID.#text": mpn}
                ]
            }
            
            product = products_collection.find_one(mpn_filter)
            
            if product:
                # Cache the found product
                self.cache_product(product)
                return product
            
            return None
            
        except Exception as e:
            print(f"❌ Error searching by MPN: {e}")
            return None
    
    def search_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """Search product by SKU (Redis cache first, then MongoDB)"""
        try:
            # Try Redis cache first
            index_key = f"{self.sku_index_prefix}{sku}"
            cached_product_id = self.redis_client.get(index_key)
            
            if cached_product_id:
                # Get product from cache
                product_cache_key = self._get_product_cache_key(cached_product_id.decode())
                cached_product = self.redis_client.get(product_cache_key)
                
                if cached_product:
                    print(f"✅ Product found in Redis cache by SKU: {sku}")
                    return json.loads(cached_product)
            
            # Fallback to MongoDB search
            print(f"🔍 Searching MongoDB for SKU: {sku}")
            products_collection = get_collection("products")
            
            product = products_collection.find_one({"sku": sku})
            
            if product:
                # Cache the found product
                self.cache_product(product)
                return product
            
            return None
            
        except Exception as e:
            print(f"❌ Error searching by SKU: {e}")
            return None
    
    def bulk_index_products(self, limit: int = 1000) -> int:
        """Bulk index products from MongoDB to Redis from both collections"""
        try:
            print(f"🚀 Starting bulk index of {limit} products from both collections...")
            total_indexed = 0
            
            # Index products collection
            try:
                products_collection = get_collection("products")
                products = list(products_collection.find({}).limit(limit // 2))
                
                for product in products:
                    try:
                        # Add collection info
                        product['_collection'] = 'products'
                        self.cache_product(product)
                        total_indexed += 1
                        
                        if total_indexed % 100 == 0:
                            print(f"📊 Indexed {total_indexed} products...")
                            
                    except Exception as e:
                        print(f"❌ Error indexing product {product.get('_id', 'unknown')}: {e}")
                        continue
                
                print(f"✅ Indexed {len(products)} products to Redis")
            except Exception as e:
                print(f"❌ Error indexing products collection: {e}")
            
            # Index icecat_index collection
            try:
                icecat_collection = get_collection("icecat_index")
                icecat_products = list(icecat_collection.find({}).limit(limit // 2))
                
                for product in icecat_products:
                    try:
                        # Add collection info
                        product['_collection'] = 'icecat_index'
                        self.cache_product(product)
                        total_indexed += 1
                        
                        if total_indexed % 100 == 0:
                            print(f"📊 Indexed {total_indexed} products...")
                            
                    except Exception as e:
                        print(f"❌ Error indexing icecat product {product.get('_id', 'unknown')}: {e}")
                        continue
                
                print(f"✅ Indexed {len(icecat_products)} icecat_index products to Redis")
            except Exception as e:
                print(f"❌ Error indexing icecat_index collection: {e}")
            
            print(f"🎉 Total bulk indexed {total_indexed} products to Redis")
            return total_indexed
            
        except Exception as e:
            print(f"❌ Error in bulk indexing: {e}")
            return 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        try:
            stats = {
                "total_keys": self.redis_client.dbsize(),
                "barcode_indexes": len([k for k in self.redis_client.keys(f"{self.barcode_index_prefix}*")]),
                "ean_indexes": len([k for k in self.redis_client.keys(f"{self.ean_index_prefix}*")]),
                "upc_indexes": len([k for k in self.redis_client.keys(f"{self.upc_index_prefix}*")]),
                "gtin_indexes": len([k for k in self.redis_client.keys(f"{self.gtin_index_prefix}*")]),
                "mpn_indexes": len([k for k in self.redis_client.keys(f"{self.mpn_index_prefix}*")]),
                "sku_indexes": len([k for k in self.redis_client.keys(f"{self.sku_index_prefix}*")]),
                "cached_products": len([k for k in self.redis_client.keys("product:*")]),
                "timestamp": datetime.utcnow().isoformat()
            }
            return stats
            
        except Exception as e:
            print(f"❌ Error getting cache stats: {e}")
            return {"error": str(e)}
    
    def clear_cache(self) -> bool:
        """Clear all cached data"""
        try:
            # Clear all search indexes
            patterns = [
                f"{self.barcode_index_prefix}*",
                f"{self.ean_index_prefix}*",
                f"{self.upc_index_prefix}*",
                f"{self.gtin_index_prefix}*",
                f"{self.mpn_index_prefix}*",
                f"{self.sku_index_prefix}*",
                "product:*"
            ]
            
            for pattern in patterns:
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
            
            print("✅ Cache cleared successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error clearing cache: {e}")
            return False


# Global instance
redis_search_manager = RedisSearchManager()
