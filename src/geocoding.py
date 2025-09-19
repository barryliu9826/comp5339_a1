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
    
    def set(self, query: str, result: Dict) -> None:
        """设置缓存结果"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            self.cache[cache_key] = {
                'query': query,
                'result': result,
                'cached_at': time.time(),
                'cache_key': cache_key
            }
    
    def set_none(self, query: str) -> None:
        """缓存失败结果（避免重复查询）"""
        with self.lock:
            cache_key = self._get_cache_key(query)
            self.cache[cache_key] = {
                'query': query,
                'result': None,
                'cached_at': time.time(),
                'cache_key': cache_key
            }
    
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

# ============================================================================
# 地理编码器
# ============================================================================

class Geocoder:
    """地理编码器"""
    
    def __init__(self, use_persistent_cache: bool = True):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'COMP5339-Assignment1/1.0'})
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.cache = {}  # 内存缓存（用于快速访问）
        self.use_persistent_cache = use_persistent_cache
        self.persistent_cache = get_global_cache() if use_persistent_cache else None
        
    def geocode_query(self, query: str) -> Optional[Dict]:
        """执行地理编码查询"""
        # 1. 首先检查内存缓存
        if query in self.cache: 
            return self.cache[query]
        
        # 2. 检查持久化缓存
        if self.use_persistent_cache and self.persistent_cache:
            cached_result = self.persistent_cache.get(query)
            if cached_result is not None:
                # 将结果加载到内存缓存
                self.cache[query] = cached_result
                return cached_result
        
        try:
            params = {
                'q': query,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'au',  # 限制澳大利亚
                'addressdetails': 1
            }
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            results = response.json()
            if results:
                result = results[0]
                geocode_result = {
                    'lat': float(result.get('lat', 0)),
                    'lon': float(result.get('lon', 0)),
                    'formatted_address': result.get('display_name', ''),
                    'place_id': result.get('place_id', ''),
                    'osm_type': result.get('osm_type', ''),
                    'osm_id': result.get('osm_id', ''),
                    'confidence': float(result.get('importance', 0)),
                    'match_type': result.get('type', ''),
                    'locality': result.get('address', {}).get('suburb', ''),
                    'postcode': result.get('address', {}).get('postcode', ''),
                    'state_full': result.get('address', {}).get('state', ''),
                    'country': result.get('address', {}).get('country', ''),
                    'geocode_query': query,
                    'geocode_provider': 'nominatim'
                }
                
                # 3. 保存到内存缓存
                self.cache[query] = geocode_result
                
                # 4. 保存到持久化缓存
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set(query, geocode_result)
                
                return geocode_result
            else:
                # 缓存失败结果
                self.cache[query] = None
                if self.use_persistent_cache and self.persistent_cache:
                    self.persistent_cache.set_none(query)
                return None
        except Exception as e:
            print(f"  地理编码API调用失败: {e}")
            # 缓存失败结果
            if self.use_persistent_cache and self.persistent_cache:
                self.persistent_cache.set_none(query)
            return None
        finally:
            time.sleep(1.1)
    
    def geocode_power_station(self, row: pd.Series, table_type: str) -> Dict:
        """对电站进行地理编码"""
        geocode_result = {'lat': None, 'lon': None, 'formatted_address': None, 'place_id': None, 'osm_type': None, 'osm_id': None,
                         'confidence': None, 'match_type': None, 'locality': None, 'postcode': None, 'state_full': None, 'country': None,
                         'geocode_query': None, 'geocode_provider': None}
        
        queries = []
        if table_type == "approved_power_stations":
            # 正确的列名是 'Power station name' (小写s)
            name = row.get('Power station name', '').strip()
            state = row.get('State', '').strip()
            postcode = row.get('Postcode', '').strip()
            
            if name and state:
                # 尝试多种查询策略
                if postcode:
                    queries.append(f"{postcode}, {state}, Australia")  # 先尝试邮编+州
                    queries.append(f"{name}, {postcode}, {state}, Australia")
                
                # 提取主要地名进行查询
                if ',' in name:
                    main_name = name.split(',')[0].strip()
                    queries.append(f"{main_name}, {state}, Australia")
                
                queries.append(f"{name}, {state}, Australia")
                queries.append(f"{name} power station, {state}, Australia")
                
                # 最后尝试只查询州
                queries.append(f"{state}, Australia")
                
        elif table_type == "committed_power_stations":
            name = row.get('Project Name', '').strip()
            state = row.get('State', '').strip() if 'State' in row.index else ''
            if name:
                # 提取主要地名
                if ',' in name:
                    main_name = name.split(',')[0].strip()
                    if state:
                        queries.append(f"{main_name}, {state}, Australia")
                    queries.append(f"{main_name}, Australia")
                
                if state:
                    queries.append(f"{name}, {state}, Australia")
                    queries.append(f"{state}, Australia")  # 最后尝试只查询州
                queries.append(f"{name} power station, Australia")
                queries.append(f"{name} renewable energy, Australia")
                
        elif table_type == "probable_power_stations":
            name = row.get('Project Name', '').strip()
            fuel = row.get('Fuel Source', '').strip() if 'Fuel Source' in row.index else ''
            state = row.get('State', '').strip() if 'State' in row.index else ''
            if name:
                # 提取主要地名
                if ',' in name:
                    main_name = name.split(',')[0].strip()
                    if state:
                        queries.append(f"{main_name}, {state}, Australia")
                    queries.append(f"{main_name}, Australia")
                
                if state and fuel:
                    queries.append(f"{name} {fuel}, {state}, Australia")
                if state:
                    queries.append(f"{name}, {state}, Australia")
                    queries.append(f"{state}, Australia")  # 最后尝试只查询州
                queries.append(f"{name} power station, Australia")
        
        for query in queries:
            if query:
                print(f"  尝试地理编码查询: {query}")
                result = self.geocode_query(query)
                if result:
                    geocode_result.update(result)
                    print(f"  ✓地理编码成功: {result.get('formatted_address', 'N/A')}")
                    break
        
        if not geocode_result['lat']: print(f"  ✗地理编码失败: 无法找到位置")
        return geocode_result

def geocode_single_station(args):
    """单个电站地理编码（线程函数）"""
    thread_id = threading.get_ident()
    idx, row, table_type = args
    
    try:
        name = row.get('Power station name', row.get('Project Name', 'Unknown'))
        print(f"  [线程{thread_id}] 处理第{idx+1}个电站: {name}")
        
        # 创建线程专用的地理编码器
        geocoder = Geocoder()
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

def add_geocoding_to_cer_data(df: pd.DataFrame, table_type: str, max_workers: int = 3) -> pd.DataFrame:
    """为CER数据添加地理编码（多线程版本）"""
    print(f"开始对{table_type}进行多线程地理编码处理（{max_workers}个线程）...")
    
    geocode_columns = ['lat', 'lon', 'formatted_address', 'place_id', 'osm_type', 'osm_id',
                      'confidence', 'match_type', 'locality', 'postcode', 'state_full', 
                      'country', 'geocode_query', 'geocode_provider']
    
    # 初始化地理编码列
    for col in geocode_columns: 
        df[col] = None
    
    total_rows = len(df)
    print(f"准备处理{total_rows}个电站...")
    
    # 准备多线程任务
    tasks = [(idx, row, table_type) for idx, row in df.iterrows()]
    
    # 多线程处理
    results = []
    success_count = 0
    
    try:
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
                        success_count += 1
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
        
        # 更新DataFrame
        print(f"\\n正在更新DataFrame...")
        for result in results:
            if result['success'] and result['result']:
                idx = result['idx']
                geocode_result = result['result']
                
                for col in geocode_columns:
                    df.at[idx, col] = geocode_result.get(col)
        
        # 保存持久化缓存
        print("正在保存地理编码缓存...")
        save_global_cache()
        
        print(f"✓多线程地理编码处理完成: {success_count}/{total_rows} 个电站成功获取位置")
        return df
        
    except Exception as e:
        print(f"✗多线程地理编码处理失败: {e}")
        # 即使失败也尝试保存缓存
        try:
            save_global_cache()
        except:
            pass
        return df

def add_geocoding_to_cer_data_single(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """为CER数据添加地理编码（单线程版本，保持向后兼容）"""
    print(f"开始对{table_type}进行地理编码处理...")
    
    geocoder = Geocoder()
    geocode_columns = ['lat', 'lon', 'formatted_address', 'place_id', 'osm_type', 'osm_id',
                      'confidence', 'match_type', 'locality', 'postcode', 'state_full', 
                      'country', 'geocode_query', 'geocode_provider']
    
    for col in geocode_columns: df[col] = None
    
    total_rows, success_count = len(df), 0
    
    try:
        for idx, row in df.iterrows():
            name = row.get('Power station name', row.get('Project Name', 'Unknown'))
            print(f"  处理第{idx+1}/{total_rows}个电站: {name}")
            
            geocode_result = geocoder.geocode_power_station(row, table_type)
            
            # 更新DataFrame
            for col in geocode_columns:
                df.at[idx, col] = geocode_result.get(col)
            
            if geocode_result['lat']: success_count += 1
        
        # 保存持久化缓存
        print("正在保存地理编码缓存...")
        save_global_cache()
        
        print(f"✓地理编码处理完成: {success_count}/{total_rows} 个电站成功获取位置")
        return df
        
    except Exception as e:
        print(f"✗地理编码处理失败: {e}")
        # 即使失败也尝试保存缓存
        try:
            save_global_cache()
        except:
            pass
        return df

