#!/usr/bin/env python3
"""Geocoding module"""

# Standard library imports
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Third-party library imports
import pandas as pd
import requests

# Enter your Google Maps API Key here (for local/assignment environment only).
# Note: Do not commit real keys to public repositories or share them.
HARDCODED_GOOGLE_MAPS_API_KEY = "AIzaSyCC2cyWU43T_MF4As54r2sn6E-rHvhb6Pk"

# ============================================================================
# Google Maps API Quota Manager
# ============================================================================

class GoogleMapsQuotaManager:
    """Google Maps API quota and rate limit manager"""
    
    def __init__(self, daily_limit: int = 10000, requests_per_second: float = 1.0):
        self.daily_limit = daily_limit
        self.requests_per_second = requests_per_second
        self.request_count = 0
        self.last_request_time = 0
        self.current_date = datetime.now().date()
        self.lock = threading.RLock()
        
    def can_make_request(self) -> bool:
        """Check if a request can be made"""
        with self.lock:
            # Check if it's a new day, reset counter if so
            today = datetime.now().date()
            if today != self.current_date:
                self.current_date = today
                self.request_count = 0
            
            # Check daily quota
            if self.request_count >= self.daily_limit:
                print(f"Warning: Reached Google Maps API daily quota limit: {self.request_count}/{self.daily_limit}")
                return False
            
            return True
    
    def wait_for_rate_limit(self):
        """Wait for rate limit"""
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 1.0 / self.requests_per_second
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                print(f"  [Rate limit] Waiting {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.request_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get quota statistics"""
        with self.lock:
            return {
                'date': str(self.current_date),
                'requests_made': self.request_count,
                'daily_limit': self.daily_limit,
                'remaining': max(0, self.daily_limit - self.request_count),
                'usage_percent': (self.request_count / self.daily_limit) * 100
            }

# ============================================================================
# Geocoding Cache Manager
# ============================================================================

class GeocodingCache:
    """Persistent geocoding cache manager"""
    
    def __init__(self, cache_file: str = "geocoding_cache.json"):
        self.cache_file = Path(cache_file)
        self.cache = {}
        self.lock = threading.RLock()  # Reentrant lock, supports multithreading
        self.load_cache()
    
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key"""
        return hashlib.md5(query.lower().strip().encode('utf-8')).hexdigest()
    
    def load_cache(self):
        """Load cache from file"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
                print(f"Geocoding cache loaded: {len(self.cache)} records")
            else:
                self.cache = {}
                print("Geocoding cache file does not exist, creating new cache")
        except Exception as e:
            print(f"Failed to load geocoding cache: {e}")
            self.cache = {}
    
    def save_cache(self):
        """Save cache to file"""
        try:
            with self.lock:
                # Save new cache directly (no backup file creation)
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
                
                print(f"Geocoding cache saved: {len(self.cache)} records")
        except Exception as e:
            print(f"Failed to save geocoding cache: {e}")
    
    def get(self, query: str) -> Optional[Dict]:
        """Get cached result"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            cached_entry = self.cache.get(cache_key)
            if cached_entry:
                return cached_entry.get('result')
            return None
    
    def _set_cache_entry(self, query: str, result: Optional[Dict]) -> None:
        """Common method to set cache entry"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            self.cache[cache_key] = {
                'query': query,
                'result': result,
                'cached_at': time.time(),
                'cache_key': cache_key
            }
    
    def set(self, query: str, result: Dict) -> None:
        """Set cache result"""
        self._set_cache_entry(query, result)
    
    def set_none(self, query: str) -> None:
        """Cache failed result (avoid duplicate queries)"""
        self._set_cache_entry(query, None)
    
    
    

# Global cache instance
_global_cache = None
_cache_lock = threading.Lock()

def get_global_cache(cache_file: str = None) -> GeocodingCache:
    """Get global cache instance (singleton pattern)"""
    global _global_cache
    
    if _global_cache is None:
        with _cache_lock:
            if _global_cache is None:
                if cache_file is None:
                    cache_file = "data/geocoding_cache.json"
                _global_cache = GeocodingCache(cache_file)
    
    return _global_cache

def save_global_cache():
    """Save global cache"""
    global _global_cache
    if _global_cache:
        _global_cache.save_cache()


def initialize_geocoding_cache(cache_file: str = None):
    """Initialize geocoding cache (called at program startup)"""
    global _global_cache
    if cache_file is None:
        cache_file = "data/geocoding_cache.json"
    
    print("Initializing geocoding cache...")
    _global_cache = GeocodingCache(cache_file)
    print(f"Geocoding cache initialization complete: {len(_global_cache.cache)} records loaded")
    return _global_cache

def geocode_single_station(args):
    """Single power station geocoding (thread function)"""
    thread_id = threading.get_ident()
    idx, row, table_type = args
    
    try:
        # Support normalized column names
        name = row.get('power_station_name', row.get('project_name', 
               row.get('Power station name', row.get('Project Name', 'Unknown'))))
        print(f"  [Thread{thread_id}] Processing station {idx+1}: {name}")
        
        # Create thread-specific geocoder (prefer hardcoded key, fallback to environment variable if empty)
        geocoder = Geocoder(api_key=HARDCODED_GOOGLE_MAPS_API_KEY or None)
        geocode_result = geocoder.geocode_power_station(row, table_type)
        
        return {
            'idx': idx,
            'success': geocode_result['lat'] is not None,
            'result': geocode_result
        }
        
    except Exception as e:
        print(f"  [Thread{thread_id}] Station {idx+1} processing failed: {e}")
        return {
            'idx': idx,
            'success': False,
            'result': None,
            'error': str(e)
        }

# Geocoding column definitions
GEOCODE_COLUMNS = [
    'lat', 'lon', 'formatted_address', 'place_id', 'postcode',
    'bbox_south', 'bbox_north', 'bbox_west', 'bbox_east'
]

def initialize_geocode_columns(df: pd.DataFrame) -> None:
    """Initialize geocoding columns"""
    for col in GEOCODE_COLUMNS:
        df[col] = None

def update_dataframe_with_results(df: pd.DataFrame, results: list) -> int:
    """Update DataFrame with geocoding results"""
    success_count = 0
    
    for result in results:
        if result['success'] and result['result']:
            idx = result['idx']
            geocode_result = result['result']
            
            # Update geocoding results
            for col in GEOCODE_COLUMNS:
                df.at[idx, col] = geocode_result.get(col)
            
            success_count += 1
    
    return success_count

def add_geocoding_to_cer_data(df: pd.DataFrame, table_type: str, max_workers: int = 3) -> pd.DataFrame:
    """Add geocoding to CER data (multithreaded version)"""
    print(f"Starting geocoding processing for {table_type} ({max_workers} threads)...")
    
    # Initialize geocoding columns
    initialize_geocode_columns(df)
    
    total_rows = len(df)
    print(f"Preparing to process {total_rows} power stations...")
    
    # Prepare multithreaded tasks
    tasks = [(idx, row, table_type) for idx, row in df.iterrows()]
    
    # Multithreaded processing
    results = []
    success_count = 0
    
    try:
        # Multithreaded processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_idx = {
                executor.submit(geocode_single_station, task): task[0] 
                for task in tasks
            }
            
            # Collect results
            for future in as_completed(future_to_idx):
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        print(f"  [Thread{threading.get_ident()}] Station {result['idx']+1} geocoding successful")
                    else:
                        print(f"  [Thread{threading.get_ident()}] Station {result['idx']+1} geocoding failed")
                        
                except Exception as e:
                    idx = future_to_idx[future]
                    print(f"  [Thread{threading.get_ident()}] Station {idx+1} thread exception: {e}")
                    results.append({
                        'idx': idx,
                        'success': False,
                        'result': None,
                        'error': str(e)
                    })
        
        # Thread-safe DataFrame update
        print(f"\nUpdating DataFrame...")
        success_count = update_dataframe_with_results(df, results)
        
        # Save persistent cache
        print("Saving geocoding cache...")
        save_global_cache()
        
        print(f"Geocoding processing complete: {success_count}/{total_rows} power stations successfully located")
        return df
        
    except Exception as e:
        print(f"Geocoding processing failed: {e}")
        # Try to save cache even if failed
        save_global_cache()
        return df

# ============================================================================
# NGER Geocoding Enhancement
# ============================================================================


def geocode_single_nger(args):
    """Single NGER facility geocoding (thread function)."""
    thread_id = threading.get_ident()
    idx, row = args
    try:
        name = row.get('facilityname', 'Unknown')
        print(f"  [Thread{thread_id}] Processing facility {idx+1}: {name}")

        geocoder = Geocoder(api_key=HARDCODED_GOOGLE_MAPS_API_KEY or None)
        geocode_result = geocoder.geocode_nger(row)

        return {
            'idx': idx,
            'success': geocode_result['lat'] is not None,
            'result': geocode_result
        }
    except Exception as e:
        print(f"  [Thread{thread_id}] Facility {idx+1} processing failed: {e}")
        return {
            'idx': idx,
            'success': False,
            'result': None,
            'error': str(e)
        }


def add_geocoding_to_nger_data(df: pd.DataFrame, max_workers: int = 5) -> pd.DataFrame:
    """Add geocoding to NGER data (multithreaded). Input df should contain facilityname/state columns."""
    if df is None or df.empty:
        return df

    print(f"Starting geocoding processing for NGER facilities ({max_workers} threads)...")
    initialize_geocode_columns(df)

    tasks = [(idx, row) for idx, row in df.iterrows()]
    results = []
    total_rows = len(tasks)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {executor.submit(geocode_single_nger, task): task[0] for task in tasks}
            for future in as_completed(future_to_idx):
                try:
                    results.append(future.result())
                except Exception as e:
                    idx = future_to_idx[future]
                    print(f"  [Thread{threading.get_ident()}] Facility {idx+1} thread exception: {e}")
                    results.append({'idx': idx, 'success': False, 'result': None, 'error': str(e)})

        success_count = update_dataframe_with_results(df, results)
        print(f"NGER geocoding processing complete: {success_count}/{total_rows} facilities successfully located")
        save_global_cache()
        return df
    except Exception as e:
        print(f"NGER geocoding processing failed: {e}")
        save_global_cache()
        return df

# ============================================================================
# Geocoder
# ============================================================================

class Geocoder:
    """Geocoder - Google Maps API version"""
    
    def __init__(self, use_persistent_cache: bool = True, api_key: str = None):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'COMP5339-Assignment1/1.0'})
        
        # Google Maps Geocoding API configuration
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.api_key = api_key or os.getenv('GOOGLE_MAPS_API_KEY')
        
        if not self.api_key:
            raise ValueError("Google Maps API key is required. Set GOOGLE_MAPS_API_KEY environment variable or pass api_key parameter.")
        
        # Cache configuration
        self.cache = {}  # Memory cache (for fast access)
        self.use_persistent_cache = use_persistent_cache
        self.persistent_cache = get_global_cache() if use_persistent_cache else None
        
        # Quota manager (Essentials level: 10,000 free requests per month)
        self.quota_manager = GoogleMapsQuotaManager(daily_limit=10000, requests_per_second=1.0)
        
        print(f"Google Maps Geocoding API initialized, daily quota: {self.quota_manager.daily_limit}")
    
        
    def geocode_query(self, query: str) -> Optional[Dict]:
        """Execute geocoding query (Google Maps API version)"""
        # 1. First check memory cache
        if query in self.cache:
            print(f"  [Cache hit-memory] Query: {query}")
            return self.cache[query]

        # 2. Check persistent cache
        if self.use_persistent_cache and self.persistent_cache:
            cached_result = self.persistent_cache.get(query)
            if cached_result is not None:
                self.cache[query] = cached_result
                print(f"  [Cache hit-persistent] Query: {query}")
                return cached_result

        # 3. Check quota limits
        if not self.quota_manager.can_make_request():
            print(f"  [Quota limit] Skipping query: {query}")
            return None

        # 4. Rate limiting
        self.quota_manager.wait_for_rate_limit()

        print(f"  [Google API call] Query: {query}")
        params = {
            'address': query,
            'key': self.api_key,
            'region': 'au',  # Bias towards Australian results
            'language': 'en'
        }

        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            
            # Check API status
            if data.get('status') != 'OK':
                if data.get('status') == 'ZERO_RESULTS':
                    # No results, cache None
                    self.cache[query] = None
                    if self.use_persistent_cache and self.persistent_cache:
                        self.persistent_cache.set_none(query)
                    print(f"  [Google API no results] Query: {query}")
                    return None
                else:
                    # Other error status
                    error_msg = data.get('error_message', data.get('status', 'Unknown error'))
                    print(f"  [Google API error] Query: {query} -> {error_msg}")
                    return None

            results = data.get('results', [])
            if results:
                result = results[0]  # Take first result
                location = result.get('geometry', {}).get('location', {})
                
                geocode_result = {
                    'lat': float(location.get('lat', 0)),
                    'lon': float(location.get('lng', 0)),
                    'formatted_address': result.get('formatted_address', ''),
                    'place_id': result.get('place_id', ''),
                    'postcode': ''  # Extract from address_components
                }

                # Extract postcode
                for component in result.get('address_components', []):
                    if 'postal_code' in component.get('types', []):
                        geocode_result['postcode'] = component.get('long_name', '')
                        break

                # Parse bounding box (if available)
                try:
                    viewport = result.get('geometry', {}).get('viewport', {})
                    if viewport:
                        northeast = viewport.get('northeast', {})
                        southwest = viewport.get('southwest', {})
                        if northeast and southwest:
                            geocode_result.update({
                                'bbox_south': float(southwest.get('lat', 0)),
                                'bbox_north': float(northeast.get('lat', 0)),
                                'bbox_west': float(southwest.get('lng', 0)),
                                'bbox_east': float(northeast.get('lng', 0))
                            })
                except Exception:
                    pass

                # Save cache (success)
                self.cache[query] = geocode_result
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set(query, geocode_result)

                print(f"  [Google API success] Query: {query} -> {result.get('formatted_address', 'N/A')}")
                return geocode_result
            else:
                # Empty results
                self.cache[query] = None
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set_none(query)
                print(f"  [Google API no results] Query: {query}")
                return None

        except Exception as e:
            print(f"  [Google API failed] Query: {query} -> Error: {e}")
            return None
    
    def build_geocode_queries(self, row: pd.Series, table_type: str) -> list:
        """Build geocoding query list"""
        queries = []

        def norm(s: Any) -> str:
            v = str(s).strip() if s is not None else ''
            if v == '' or v.lower() in {'n/a', 'na', 'nan', 'none', '-'}:
                return ''
            return v
        
        if table_type == "approved_power_stations":
            # Support normalized column names
            name = norm(row.get('power_station_name', row.get('Power station name', '')))
            state = norm(row.get('state', row.get('State', '')))
            postcode = norm(row.get('postcode', row.get('Postcode', '')))
            
            if name and state:
                if postcode:
                    queries.extend([
                        f"{postcode}, {state}, Australia",
                        f"{name}, {postcode}, {state}, Australia"
                    ])
                
                if name and ',' in name:
                    main_name = name.split(',')[0].strip()
                    queries.append(f"{main_name}, {state}, Australia")
                
                queries.extend([
                    f"{name}, {state}, Australia",
                    f"{name} power station, {state}, Australia",
                    f"{state}, Australia"
                ])
                
        elif table_type in ["committed_power_stations", "probable_power_stations"]:
            # Support normalized column names
            name = norm(row.get('project_name', row.get('Project Name', '')))
            state = norm(row.get('state', row.get('State ', row.get('State', ''))))
            
            if name:
                if name and ',' in name:
                    main_name = name.split(',')[0].strip()
                    if state:
                        queries.append(f"{main_name}, {state}, Australia")
                    queries.append(f"{main_name}, Australia")
                
                if state:
                    queries.extend([
                        f"{name}, {state}, Australia",
                        f"{state}, Australia"
                    ])
                
                queries.extend([
                    f"{name} power station, Australia",
                    f"{name} renewable energy, Australia" if table_type == "committed_power_stations" else f"{name} {norm(row.get('fuel_source', row.get('Fuel Source', '')))}, {state}, Australia" if table_type == "probable_power_stations" and state and norm(row.get('fuel_source', row.get('Fuel Source', ''))) else None
                ])
                
                # Filter out None values
                queries = [q for q in queries if q is not None]
        
        # Filter queries containing invalid tokens
        invalid_tokens = {'n/a', 'na', 'nan', 'none'}
        filtered = []
        seen = set()
        for q in queries:
            ql = q.lower()
            if any(tok in ql for tok in invalid_tokens):
                continue
            if q not in seen:
                filtered.append(q)
                seen.add(q)
        return filtered
    
    def geocode_power_station(self, row: pd.Series, table_type: str) -> Dict:
        """Geocode power station"""
        geocode_result = {'lat': None, 'lon': None, 'formatted_address': None, 'place_id': None, 'postcode': None,
                         'bbox_south': None, 'bbox_north': None, 'bbox_west': None, 'bbox_east': None}
        
        queries = self.build_geocode_queries(row, table_type)
        print(f"  Preparing to try {len(queries)} geocoding queries...")
        
        for i, query in enumerate(queries, 1):
            if query:
                print(f"  Query {i}/{len(queries)}: {query}")
                result = self.geocode_query(query)
                if result:
                    geocode_result.update(result)
                    print(f"  Geocoding successful: {result.get('formatted_address', 'N/A')}")
                    break
        
        if not geocode_result['lat']: 
            print(f"  Geocoding failed: tried {len(queries)} queries with no results")
        return geocode_result
    
    def build_nger_queries(self, row: pd.Series) -> list:
        """Build geocoding query candidates based on NGER row."""
        queries = []

        def norm(s: Any) -> str:
            v = str(s).strip() if s is not None else ''
            if v == '' or v.lower() in {'n/a', 'na', 'nan', 'none', '-'}:
                return ''
            return v

        facility = norm(row.get('facilityname', ''))
        state = norm(row.get('state', ''))
        reporting = norm(row.get('reportingentity', ''))
        corp = norm(row.get('controllingcorporation', ''))

        # Prioritize facility name + state
        if facility and state:
            queries.append(f"{facility}, {state}, Australia")
        if facility:
            queries.extend([
                f"{facility} facility, Australia",
                f"{facility}, Australia"
            ])
        # Fallback to enterprise/controlling corporation + state
        if reporting and state:
            queries.append(f"{reporting}, {state}, Australia")
        if corp and state:
            queries.append(f"{corp}, {state}, Australia")
        # Finally just state
        if state:
            queries.append(f"{state}, Australia")

        # Deduplicate and filter invalid tokens
        invalid_tokens = {'n/a', 'na', 'nan', 'none'}
        deduped = []
        seen = set()
        for q in queries:
            if not q:
                continue
            ql = q.lower()
            if any(tok in ql for tok in invalid_tokens):
                continue
            if q not in seen:
                deduped.append(q)
                seen.add(q)
        return deduped
    
    def geocode_nger(self, row: pd.Series) -> Dict:
        """Geocode NGER facility"""
        geocode_result = {'lat': None, 'lon': None, 'formatted_address': None, 'place_id': None, 'postcode': None,
                         'bbox_south': None, 'bbox_north': None, 'bbox_west': None, 'bbox_east': None}
        
        queries = self.build_nger_queries(row)
        print(f"  Preparing to try {len(queries)} geocoding queries...")
        
        for i, query in enumerate(queries, 1):
            if query:
                print(f"  Query {i}/{len(queries)}: {query}")
                result = self.geocode_query(query)
                if result:
                    geocode_result.update(result)
                    print(f"  Geocoding successful: {result.get('formatted_address', 'N/A')}")
                    break
        
        if not geocode_result['lat']: 
            print(f"  Geocoding failed: tried {len(queries)} queries with no results")
        return geocode_result
