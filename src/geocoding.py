#!/usr/bin/env python3
"""地理编码模块"""

import requests
import time
import pandas as pd
from typing import Dict, Optional, Any
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib
from pathlib import Path
import os
from datetime import datetime, timedelta

# 直接在此处填写你的 Google Maps API Key（仅用于本地/作业环境）。
# 注意：不要把真实密钥提交到公共仓库或共享。
HARDCODED_GOOGLE_MAPS_API_KEY = "AIzaSyCC2cyWU43T_MF4As54r2sn6E-rHvhb6Pk"

# ============================================================================
# Google Maps API 配额管理器
# ============================================================================

class GoogleMapsQuotaManager:
    """Google Maps API配额和速率限制管理器"""
    
    def __init__(self, daily_limit: int = 10000, requests_per_second: float = 1.0):
        self.daily_limit = daily_limit
        self.requests_per_second = requests_per_second
        self.request_count = 0
        self.last_request_time = 0
        self.current_date = datetime.now().date()
        self.lock = threading.RLock()
        
    def can_make_request(self) -> bool:
        """检查是否可以发起请求"""
        with self.lock:
            # 检查是否是新的一天，如果是则重置计数器
            today = datetime.now().date()
            if today != self.current_date:
                self.current_date = today
                self.request_count = 0
            
            # 检查日配额
            if self.request_count >= self.daily_limit:
                print(f"⚠️ 已达到Google Maps API日配额限制: {self.request_count}/{self.daily_limit}")
                return False
            
            return True
    
    def wait_for_rate_limit(self):
        """等待速率限制"""
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 1.0 / self.requests_per_second
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                print(f"  [速率限制] 等待 {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.request_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """获取配额统计"""
        with self.lock:
            return {
                'date': str(self.current_date),
                'requests_made': self.request_count,
                'daily_limit': self.daily_limit,
                'remaining': max(0, self.daily_limit - self.request_count),
                'usage_percent': (self.request_count / self.daily_limit) * 100
            }

# ============================================================================
# 地理编码缓存管理器
# ============================================================================

class GeocodingCache:
    """地理编码持久化缓存管理器"""
    
    def __init__(self, cache_file: str = "geocoding_cache.json"):
        self.cache_file = Path(cache_file)
        self.cache = {}
        self.lock = threading.RLock()  # 可重入锁，支持多线程
        self.load_cache()
    
    def _get_cache_key(self, query: str) -> str:
        """生成缓存键"""
        return hashlib.md5(query.lower().strip().encode('utf-8')).hexdigest()
    
    def load_cache(self):
        """从文件加载缓存"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
                print(f"✓地理编码缓存已加载: {len(self.cache)} 条记录")
            else:
                self.cache = {}
                print("✓地理编码缓存文件不存在，创建新缓存")
        except Exception as e:
            print(f"✗加载地理编码缓存失败: {e}")
            self.cache = {}
    
    def save_cache(self):
        """保存缓存到文件"""
        try:
            with self.lock:
                # 直接保存新缓存（不创建备份文件）
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
                
                print(f"✓地理编码缓存已保存: {len(self.cache)} 条记录")
        except Exception as e:
            print(f"✗保存地理编码缓存失败: {e}")
    
    def get(self, query: str) -> Optional[Dict]:
        """获取缓存结果"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            cached_entry = self.cache.get(cache_key)
            if cached_entry:
                return cached_entry.get('result')
            return None
    
    def _set_cache_entry(self, query: str, result: Optional[Dict]) -> None:
        """设置缓存条目的通用方法"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            self.cache[cache_key] = {
                'query': query,
                'result': result,
                'cached_at': time.time(),
                'cache_key': cache_key
            }
    
    def set(self, query: str, result: Dict) -> None:
        """设置缓存结果"""
        self._set_cache_entry(query, result)
    
    def set_none(self, query: str) -> None:
        """缓存失败结果（避免重复查询）"""
        self._set_cache_entry(query, None)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self.lock:
            total = len(self.cache)
            successful = sum(1 for v in self.cache.values() if v.get('result') is not None)
            failed = total - successful
            
            return {
                'total_queries': total,
                'successful_queries': successful,
                'failed_queries': failed,
                'success_rate': (successful / total * 100) if total > 0 else 0,
                'cache_file': str(self.cache_file),
                'cache_size_mb': self.cache_file.stat().st_size / (1024 * 1024) if self.cache_file.exists() else 0
            }
    
    def clear_cache(self):
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            if self.cache_file.exists():
                self.cache_file.unlink()
            print("✓地理编码缓存已清空")
    
    def cleanup_old_entries(self, max_age_days: int = 30):
        """清理过期缓存条目"""
        with self.lock:
            current_time = time.time()
            max_age_seconds = max_age_days * 24 * 60 * 60
            
            old_keys = []
            for key, value in self.cache.items():
                cached_at = value.get('cached_at', 0)
                if current_time - cached_at > max_age_seconds:
                    old_keys.append(key)
            
            for key in old_keys:
                del self.cache[key]
            
            if old_keys:
                print(f"✓清理了 {len(old_keys)} 个过期缓存条目")
                self.save_cache()

# 全局缓存实例
_global_cache = None
_cache_lock = threading.Lock()

def get_global_cache(cache_file: str = None) -> GeocodingCache:
    """获取全局缓存实例（单例模式）"""
    global _global_cache
    
    if _global_cache is None:
        with _cache_lock:
            if _global_cache is None:
                if cache_file is None:
                    cache_file = "data/geocoding_cache.json"
                _global_cache = GeocodingCache(cache_file)
    
    return _global_cache

def save_global_cache():
    """保存全局缓存"""
    global _global_cache
    if _global_cache:
        _global_cache.save_cache()

def clear_global_cache():
    """清空全局缓存"""
    global _global_cache
    if _global_cache:
        _global_cache.clear_cache()

def initialize_geocoding_cache(cache_file: str = None):
    """初始化地理编码缓存（在程序启动时调用）"""
    global _global_cache
    if cache_file is None:
        cache_file = "data/geocoding_cache.json"
    
    print("正在初始化地理编码缓存...")
    _global_cache = GeocodingCache(cache_file)
    print(f"✓地理编码缓存初始化完成: {len(_global_cache.cache)} 条记录已加载")
    return _global_cache

def geocode_single_station(args):
    """单个电站地理编码（线程函数）"""
    thread_id = threading.get_ident()
    idx, row, table_type = args
    
    try:
        name = row.get('Power station name', row.get('Project Name', 'Unknown'))
        print(f"  [线程{thread_id}] 处理第{idx+1}个电站: {name}")
        
        # 创建线程专用的地理编码器（优先使用硬编码Key，留空则回退到环境变量）
        geocoder = Geocoder(api_key=HARDCODED_GOOGLE_MAPS_API_KEY or None)
        geocode_result = geocoder.geocode_power_station(row, table_type)
        
        return {
            'idx': idx,
            'success': geocode_result['lat'] is not None,
            'result': geocode_result
        }
        
    except Exception as e:
        print(f"  ✗[线程{thread_id}] 第{idx+1}个电站处理失败: {e}")
        return {
            'idx': idx,
            'success': False,
            'result': None,
            'error': str(e)
        }

# 地理编码列定义
GEOCODE_COLUMNS = [
    'lat', 'lon', 'formatted_address', 'place_id', 'postcode',
    'bbox_south', 'bbox_north', 'bbox_west', 'bbox_east'
]

def initialize_geocode_columns(df: pd.DataFrame) -> None:
    """初始化地理编码列"""
    for col in GEOCODE_COLUMNS:
        df[col] = None

def update_dataframe_with_results(df: pd.DataFrame, results: list) -> int:
    """更新DataFrame with地理编码结果"""
    success_count = 0
    for result in results:
        if result['success'] and result['result']:
            idx = result['idx']
            geocode_result = result['result']
            
            for col in GEOCODE_COLUMNS:
                df.at[idx, col] = geocode_result.get(col)
            
            success_count += 1
    return success_count

def add_geocoding_to_cer_data(df: pd.DataFrame, table_type: str, max_workers: int = 3) -> pd.DataFrame:
    """为CER数据添加地理编码（多线程版本）"""
    print(f"开始对{table_type}进行地理编码处理（{max_workers}个线程）...")
    
    # 初始化地理编码列
    initialize_geocode_columns(df)
    
    total_rows = len(df)
    print(f"准备处理{total_rows}个电站...")
    
    # 准备多线程任务
    tasks = [(idx, row, table_type) for idx, row in df.iterrows()]
    
    # 多线程处理
    results = []
    success_count = 0
    
    try:
        # 多线程处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_idx = {
                executor.submit(geocode_single_station, task): task[0] 
                for task in tasks
            }
            
            # 收集结果
            for future in as_completed(future_to_idx):
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        print(f"  ✓[线程{threading.get_ident()}] 第{result['idx']+1}个电站地理编码成功")
                    else:
                        print(f"  ✗[线程{threading.get_ident()}] 第{result['idx']+1}个电站地理编码失败")
                        
                except Exception as e:
                    idx = future_to_idx[future]
                    print(f"  ✗[线程{threading.get_ident()}] 第{idx+1}个电站线程异常: {e}")
                    results.append({
                        'idx': idx,
                        'success': False,
                        'result': None,
                        'error': str(e)
                    })
        
        # 线程安全地更新DataFrame
        print(f"\\n正在更新DataFrame...")
        success_count = update_dataframe_with_results(df, results)
        
        # 保存持久化缓存
        print("正在保存地理编码缓存...")
        save_global_cache()
        
        print(f"✓地理编码处理完成: {success_count}/{total_rows} 个电站成功获取位置")
        return df
        
    except Exception as e:
        print(f"✗地理编码处理失败: {e}")
        # 即使失败也尝试保存缓存
        save_global_cache()
        return df

# ============================================================================
# NGER 地理编码增强
# ============================================================================


def geocode_single_nger(args):
    """单个NGER设施地理编码（线程函数）。"""
    thread_id = threading.get_ident()
    idx, row = args
    try:
        name = row.get('facilityname', 'Unknown')
        print(f"  [线程{thread_id}] 处理第{idx+1}个设施: {name}")

        geocoder = Geocoder(api_key=HARDCODED_GOOGLE_MAPS_API_KEY or None)
        geocode_result = geocoder.geocode_nger(row)

        return {
            'idx': idx,
            'success': geocode_result['lat'] is not None,
            'result': geocode_result
        }
    except Exception as e:
        print(f"  ✗[线程{thread_id}] 第{idx+1}个设施处理失败: {e}")
        return {
            'idx': idx,
            'success': False,
            'result': None,
            'error': str(e)
        }


def add_geocoding_to_nger_data(df: pd.DataFrame, max_workers: int = 5) -> pd.DataFrame:
    """为NGER数据添加地理编码（多线程）。传入的df应包含 facilityname/state 等列。"""
    if df is None or df.empty:
        return df

    print(f"开始对NGER设施进行地理编码处理（{max_workers}个线程）...")
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
                    print(f"  ✗[线程{threading.get_ident()}] 第{idx+1}个设施线程异常: {e}")
                    results.append({'idx': idx, 'success': False, 'result': None, 'error': str(e)})

        success_count = update_dataframe_with_results(df, results)
        print(f"✓NGER地理编码处理完成: {success_count}/{total_rows} 个设施成功获取位置")
        save_global_cache()
        return df
    except Exception as e:
        print(f"✗NGER地理编码处理失败: {e}")
        save_global_cache()
        return df

# ============================================================================
# 地理编码器
# ============================================================================

class Geocoder:
    """地理编码器 - Google Maps API版本"""
    
    def __init__(self, use_persistent_cache: bool = True, api_key: str = None):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'COMP5339-Assignment1/1.0'})
        
        # Google Maps Geocoding API配置
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.api_key = api_key or os.getenv('GOOGLE_MAPS_API_KEY')
        
        if not self.api_key:
            raise ValueError("Google Maps API key is required. Set GOOGLE_MAPS_API_KEY environment variable or pass api_key parameter.")
        
        # 缓存配置
        self.cache = {}  # 内存缓存（用于快速访问）
        self.use_persistent_cache = use_persistent_cache
        self.persistent_cache = get_global_cache() if use_persistent_cache else None
        
        # 配额管理器（Essentials级别：每月10,000次免费）
        self.quota_manager = GoogleMapsQuotaManager(daily_limit=10000, requests_per_second=1.0)
        
        print(f"✓ Google Maps Geocoding API已初始化，日配额: {self.quota_manager.daily_limit}")
    
    def get_quota_stats(self) -> Dict[str, Any]:
        """获取配额统计信息"""
        return self.quota_manager.get_stats()
        
    def geocode_query(self, query: str) -> Optional[Dict]:
        """执行地理编码查询（Google Maps API版本）"""
        # 1. 首先检查内存缓存
        if query in self.cache:
            print(f"  [缓存命中-内存] 查询: {query}")
            return self.cache[query]

        # 2. 检查持久化缓存
        if self.use_persistent_cache and self.persistent_cache:
            cached_result = self.persistent_cache.get(query)
            if cached_result is not None:
                self.cache[query] = cached_result
                print(f"  [缓存命中-持久化] 查询: {query}")
                return cached_result

        # 3. 检查配额限制
        if not self.quota_manager.can_make_request():
            print(f"  [配额限制] 跳过查询: {query}")
            return None

        # 4. 速率限制
        self.quota_manager.wait_for_rate_limit()

        print(f"  [Google API调用] 查询: {query}")
        params = {
            'address': query,
            'key': self.api_key,
            'region': 'au',  # 偏向澳大利亚结果
            'language': 'en'
        }

        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            
            # 检查API状态
            if data.get('status') != 'OK':
                if data.get('status') == 'ZERO_RESULTS':
                    # 无结果，缓存None
                    self.cache[query] = None
                    if self.use_persistent_cache and self.persistent_cache:
                        self.persistent_cache.set_none(query)
                    print(f"  [Google API无结果] 查询: {query}")
                    return None
                else:
                    # 其他错误状态
                    error_msg = data.get('error_message', data.get('status', 'Unknown error'))
                    print(f"  [Google API错误] 查询: {query} -> {error_msg}")
                    return None

            results = data.get('results', [])
            if results:
                result = results[0]  # 取第一个结果
                location = result.get('geometry', {}).get('location', {})
                
                geocode_result = {
                    'lat': float(location.get('lat', 0)),
                    'lon': float(location.get('lng', 0)),
                    'formatted_address': result.get('formatted_address', ''),
                    'place_id': result.get('place_id', ''),
                    'postcode': ''  # 从address_components中提取
                }

                # 提取邮编
                for component in result.get('address_components', []):
                    if 'postal_code' in component.get('types', []):
                        geocode_result['postcode'] = component.get('long_name', '')
                        break

                # 解析边界框（如果有）
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

                # 保存缓存（成功）
                self.cache[query] = geocode_result
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set(query, geocode_result)

                print(f"  [Google API成功] 查询: {query} -> {result.get('formatted_address', 'N/A')}")
                return geocode_result
            else:
                # 空结果
                self.cache[query] = None
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set_none(query)
                print(f"  [Google API无结果] 查询: {query}")
                return None

        except Exception as e:
            print(f"  [Google API失败] 查询: {query} -> 错误: {e}")
            return None
    
    def build_geocode_queries(self, row: pd.Series, table_type: str) -> list:
        """构建地理编码查询列表"""
        queries = []

        def norm(s: Any) -> str:
            v = str(s).strip() if s is not None else ''
            if v == '' or v.lower() in {'n/a', 'na', 'nan', 'none', '-'}:
                return ''
            return v
        
        if table_type == "approved_power_stations":
            name = norm(row.get('Power station name', ''))
            state = norm(row.get('State', ''))
            postcode = norm(row.get('Postcode', ''))
            
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
            name = norm(row.get('Project Name', ''))
            state = norm(row.get('State', '')) if 'State' in row.index else ''
            
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
                    f"{name} renewable energy, Australia" if table_type == "committed_power_stations" else f"{name} {norm(row.get('Fuel Source', ''))}, {state}, Australia" if table_type == "probable_power_stations" and state and norm(row.get('Fuel Source', '')) else None
                ])
                
                # 过滤掉None值
                queries = [q for q in queries if q is not None]
        
        # 过滤包含无效标记的查询
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
        """对电站进行地理编码"""
        geocode_result = {'lat': None, 'lon': None, 'formatted_address': None, 'place_id': None, 'postcode': None,
                         'bbox_south': None, 'bbox_north': None, 'bbox_west': None, 'bbox_east': None}
        
        queries = self.build_geocode_queries(row, table_type)
        print(f"  准备尝试 {len(queries)} 个地理编码查询...")
        
        for i, query in enumerate(queries, 1):
            if query:
                print(f"  查询 {i}/{len(queries)}: {query}")
                result = self.geocode_query(query)
                if result:
                    geocode_result.update(result)
                    print(f"  ✓地理编码成功: {result.get('formatted_address', 'N/A')}")
                    break
        
        if not geocode_result['lat']: 
            print(f"  ✗地理编码失败: 尝试了 {len(queries)} 个查询均无结果")
        return geocode_result
    
    def build_nger_queries(self, row: pd.Series) -> list:
        """根据NGER行构建地理编码查询候选。"""
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

        # 优先使用 设施名 + 州
        if facility and state:
            queries.append(f"{facility}, {state}, Australia")
        if facility:
            queries.extend([
                f"{facility} facility, Australia",
                f"{facility}, Australia"
            ])
        # 回退使用企业/控股公司 + 州
        if reporting and state:
            queries.append(f"{reporting}, {state}, Australia")
        if corp and state:
            queries.append(f"{corp}, {state}, Australia")
        # 最后仅州
        if state:
            queries.append(f"{state}, Australia")

        # 去重、过滤包含无效标记
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
        """对电站进行地理编码"""
        geocode_result = {'lat': None, 'lon': None, 'formatted_address': None, 'place_id': None, 'postcode': None,
                         'bbox_south': None, 'bbox_north': None, 'bbox_west': None, 'bbox_east': None}
        
        queries = self.build_nger_queries(row)
        print(f"  准备尝试 {len(queries)} 个地理编码查询...")
        
        for i, query in enumerate(queries, 1):
            if query:
                print(f"  查询 {i}/{len(queries)}: {query}")
                result = self.geocode_query(query)
                if result:
                    geocode_result.update(result)
                    print(f"  ✓地理编码成功: {result.get('formatted_address', 'N/A')}")
                    break
        
        if not geocode_result['lat']: 
            print(f"  ✗地理编码失败: 尝试了 {len(queries)} 个查询均无结果")
        return geocode_result
