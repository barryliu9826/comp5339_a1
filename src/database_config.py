#!/usr/bin/env python3
"""数据库配置和操作"""

# 标准库导入
import threading
import time
from typing import List

# 第三方库导入
import psycopg2
import psycopg2.pool
import pandas as pd
import numpy as np

# 本地模块导入
from excel_utils import get_merged_cells, read_merged_headers

# 表级别的锁字典，用于更细粒度的锁控制
_table_locks = {}
_table_locks_lock = threading.Lock()

DB_CONFIG = {
    'host': 'localhost', 'port': 5432, 'user': 'postgres', 
    'password': 'postgre', 'database': 'postgres'
}

# 全局连接池
_connection_pool = None
_pool_lock = threading.Lock()

# 连接跟踪（用于调试）
_active_connections = set()
_connections_lock = threading.Lock()

def get_connection_pool(minconn=1, maxconn=10):
    """获取数据库连接池（单例模式）"""
    global _connection_pool
    
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                try:
                    _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=minconn,
                        maxconn=maxconn,
                        **DB_CONFIG
                    )
                    print(f"✓PostgreSQL连接池创建成功: {minconn}-{maxconn}个连接")
                    # 尝试启用PostGIS扩展（若已启用将被忽略）
                    try:
                        _conn = _connection_pool.getconn()
                        if _conn:
                            with _conn.cursor() as _cur:
                                _cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                                _conn.commit()
                                print("✓PostGIS扩展已启用或已存在")
                        if _conn:
                            _connection_pool.putconn(_conn)
                    except Exception as ee:
                        print(f"⚠启用PostGIS扩展失败: {ee}")
                except Exception as e:
                    print(f"✗PostgreSQL连接池创建失败: {e}")
                    return None
    
    return _connection_pool

def test_connection(conn):
    """测试连接是否有效"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except:
        return False

def track_connection(conn):
    """跟踪连接"""
    with _connections_lock:
        _active_connections.add(id(conn))

def get_db_connection():
    """获取数据库连接（从连接池）"""
    pool = get_connection_pool()
    if not pool:
        return None
    
    try:
        conn = pool.getconn()
        if not conn:
            return None
            
        # 测试连接是否有效
        if test_connection(conn):
            track_connection(conn)
            return conn
        
        # 连接无效，尝试重新获取
        print("✗连接测试失败，尝试重新获取")
        try:
            pool.putconn(conn, close=True)
        except:
            pass
            
        conn = pool.getconn()
        if conn and test_connection(conn):
            track_connection(conn)
            return conn
            
        return None
    except Exception as e:
        print(f"✗从连接池获取连接失败: {e}")
        return None

def return_db_connection(conn):
    """归还数据库连接到连接池"""
    if not conn:
        return
    
    # 检查连接是否被跟踪（防止归还未从池中获取的连接）
    conn_id = id(conn)
    with _connections_lock:
        if conn_id not in _active_connections:
            print("✗尝试归还未跟踪的连接，直接关闭")
            safe_close_connection(conn)
            return
        _active_connections.discard(conn_id)
    
    if not _connection_pool:
        print("✗连接池不存在，无法归还连接")
        safe_close_connection(conn)
        return
    
    try:
        # 检查连接是否仍然有效
        if not test_connection(conn):
            print("✗连接已失效，直接关闭")
            safe_close_connection(conn)
            return
        
        # 归还连接到池
        _connection_pool.putconn(conn)
    except Exception as e:
        print(f"✗归还连接到连接池失败: {e}")
        safe_close_connection(conn)

def safe_close_connection(conn):
    """安全关闭连接"""
    try:
        conn.close()
    except:
        pass

def handle_db_operation(operation_name: str, conn, operation_func, *args, **kwargs):
    """统一处理数据库操作的错误处理"""
    try:
        result = operation_func(*args, **kwargs)
        if result is not False:
            conn.commit()
        return result
    except Exception as e:
        print(f"✗{operation_name}失败: {e}")
        conn.rollback()
        return False

def close_connection_pool():
    """关闭连接池"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        
        # 清理连接跟踪
        with _connections_lock:
            _active_connections.clear()
        
        print("✓数据库连接池已关闭")

def get_table_lock(table_name: str):
    """获取表级别的锁"""
    with _table_locks_lock:
        if table_name not in _table_locks:
            _table_locks[table_name] = threading.Lock()
        return _table_locks[table_name]

def table_exists(cursor, table_name: str) -> bool:
    """检查表是否存在"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]

def create_table_safe(cursor, table_name: str, create_sql: str) -> bool:
    """安全创建表"""
    try:
        if not table_exists(cursor, table_name):
            cursor.execute(create_sql)
            print(f"✓表创建成功: {table_name}")
            return True
        else:
            print(f"✓表已存在: {table_name}")
            return True
    except Exception as e:
        print(f"✗表创建失败: {table_name} - {e}")
        return False

def create_nger_table_impl(cursor):
    """创建NGER表的实现"""
    create_sql = """
    CREATE TABLE nger_unified (
        id SERIAL PRIMARY KEY,
        year_label TEXT,
        facilityname TEXT,
        state TEXT,
        facility_type TEXT,
        primaryfuel TEXT,
        reportingentity TEXT,
        controllingcorporation TEXT,
        electricity_production_gj NUMERIC,
        electricity_production_mwh NUMERIC,
        emission_intensity_tco2e_mwh NUMERIC,
        scope1_emissions_tco2e NUMERIC,
        scope2_emissions_tco2e NUMERIC,
        total_emissions_tco2e NUMERIC,
        grid_info TEXT,
        grid_connected BOOLEAN,
        important_notes TEXT,
        lat NUMERIC,
        lon NUMERIC,
        formatted_address TEXT,
        place_id TEXT,
        osm_type TEXT,
        osm_id TEXT,
        confidence NUMERIC,
        match_type TEXT,
        locality TEXT,
        postcode TEXT,
        state_full TEXT,
        country TEXT,
        geocode_query TEXT,
        geocode_provider TEXT
    );
    """
    ok = create_table_safe(cursor, 'nger_unified', create_sql)
    if not ok:
        return False
    # 确保geometry列
    try:
        ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
    except Exception as e:
        print(f"⚠NGER几何列处理失败: {e}")
    return True

def create_nger_table(conn) -> bool:
    """创建NGER表（在单线程中调用）"""
    cursor = conn.cursor()
    return handle_db_operation("NGER表创建", conn, create_nger_table_impl, cursor)

def create_cer_tables_impl(cursor):
    """创建CER表的实现"""
    cer_table_types = ['approved_power_stations', 'committed_power_stations', 'probable_power_stations']
    
    for table_type in cer_table_types:
        clean_table = clean_name(f"cer_{table_type}")
        create_sql = f"CREATE TABLE {clean_table} (id SERIAL PRIMARY KEY);"
        
        if not create_table_safe(cursor, clean_table, create_sql):
            return False
    
    return True

def create_cer_tables(conn) -> bool:
    """创建CER表（在单线程中调用）"""
    cursor = conn.cursor()
    return handle_db_operation("CER表创建", conn, create_cer_tables_impl, cursor)

def create_abs_table_safe(conn, merged_cell_value: str, columns: List[str]) -> str:
    """创建ABS表（在单线程中调用）"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(merged_cell_value)
        
        # 构建创建表的SQL
        create_sql = f"CREATE TABLE {clean_table} (id SERIAL PRIMARY KEY, code TEXT, label TEXT, year INTEGER, geographic_level INTEGER"
        
        used = {'id', 'code', 'label', 'year', 'geographic_level'}
        for col in columns[3:]:
            clean_col = clean_name(col)
            original = clean_col
            counter = 1
            while clean_col in used:
                clean_col = f"{original}_{counter}"
                counter += 1
            used.add(clean_col)
            create_sql += f", {clean_col} TEXT"
        
        create_sql += ");"
        
        if create_table_safe(cursor, clean_table, create_sql):
            conn.commit()
            return clean_table
        else:
            conn.rollback()
            return None
            
    except Exception as e:
        print(f"✗ABS表创建失败: {merged_cell_value} - {e}")
        conn.rollback()
        return None

def create_all_abs_tables(conn, file_path: str) -> bool:
    """预创建所有ABS表（在单线程中调用）"""
    try:
        cursor = conn.cursor()
        
        # 地理级别定义
        levels = {
            "Table 1": {"desc": "州级", "level": 0},
            "Table 2": {"desc": "地方政府级", "level": 1}
        }
        
        for sheet_name in ["Table 1", "Table 2"]:
            level_info = levels[sheet_name]
            print(f"正在预创建ABS表: {sheet_name}({level_info['desc']})...")
            
            try:
                merged_cells = get_merged_cells(file_path, sheet_name)
                df = read_merged_headers(file_path, sheet_name)
                print(f"发现{len(merged_cells)}个合并单元格需要创建表")
                
                for cell in merged_cells:
                    start_col, end_col = cell['start_col'] - 1, cell['end_col']
                    selected_cols = ['Code', 'Label', 'Year'] + list(df.columns[start_col:end_col])
                    
                    # 创建表
                    table_name = create_abs_table_safe(conn, cell['value'], selected_cols)
                    if not table_name:
                        print(f"✗ABS表创建失败: {cell['value']}")
                        return False
                
                print(f"✓ABS表预创建完成: {sheet_name} - {len(merged_cells)}个表")
                
            except Exception as e:
                print(f"✗ABS表预创建失败: {sheet_name} - {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"✗ABS表预创建失败: {e}")
        conn.rollback()
        return False


def clean_name(name: str, idx: int = 0) -> str:
    """统一名称清理"""
    if not name or str(name).strip() == '':
        return f"col_{idx + 1}"
    
    clean = str(name).strip().lower()
    # 替换特殊字符
    replacements = {' ': '_', '-': '_', '(': '', ')': '', '%': 'percent', 
                   ':': '', ',': '', '\n': '_', '\r': '_', '__': '_'}
    for old, new in replacements.items():
        clean = clean.replace(old, new)
    
    # 只保留字母数字和下划线
    clean = ''.join(c for c in clean if c.isalnum() or c == '_')
    # 如果以数字开头，添加前缀
    if clean and clean[0].isdigit():
        clean = f"col_{clean}"
    return clean[:50]

def make_unique(names: List[str]) -> List[str]:
    """确保唯一性"""
    seen, unique = set(), []
    for name in names:
        original, counter = name, 1
        while name in seen:
            name = f"{original}_{counter}"
            counter += 1
        seen.add(name)
        unique.append(name)
    return unique

def safe_data_prep(df: pd.DataFrame) -> List[tuple]:
    """安全数据准备"""
    data = []
    for _, row in df.reset_index(drop=True).iterrows():
        row_data = []
        for i in range(len(df.columns)):
            value = row.iat[i]
            if pd.isna(value) or value is None:
                row_data.append(None)
            elif isinstance(value, (pd.Series, np.ndarray, list, dict, tuple)):
                row_data.append(str(value))
            else:
                row_data.append(str(value))
        data.append(tuple(row_data))
    return data

def batch_insert(cursor, insert_sql: str, data: List[tuple], batch_size: int = 1000) -> None:
    """批量插入数据"""
    for i in range(0, len(data), batch_size):
        cursor.executemany(insert_sql, data[i:i + batch_size])

def prepare_insert_sql(table_name: str, columns: List[str]) -> str:
    """准备插入SQL语句"""
    placeholders = ', '.join(['%s'] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

def create_insert_table(conn, table_name: str, df: pd.DataFrame, extra_cols: List[tuple] = None) -> bool:
    """建表并插入数据"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(table_name)
        
        # 建表SQL
        cols = [clean_name(col, i) for i, col in enumerate(df.columns)]
        cols = make_unique(cols)
        
        # 使用表级别锁来避免多线程竞争条件
        table_lock = get_table_lock(clean_table)
        with table_lock:
            # 构建创建表的SQL
            create_sql = f"CREATE TABLE {clean_table} (\nid SERIAL PRIMARY KEY"
            if extra_cols:
                for col_name, col_type in extra_cols:
                    create_sql += f",\n{col_name} {col_type}"
            for col in cols:
                create_sql += f",\n{col} TEXT"
            create_sql += "\n);"
            
            if not create_table_safe(cursor, clean_table, create_sql):
                conn.rollback()
                return False
        
        data = safe_data_prep(df)
        if data:
            all_cols = ([col[0] for col in extra_cols] if extra_cols else []) + cols
            insert_sql = prepare_insert_sql(clean_table, all_cols)
            batch_insert(cursor, insert_sql, data)
        
        conn.commit()
        print(f"✓数据表创建和插入完成: {clean_table} ({len(data)}行)")
        return True
        
    except Exception as e:
        print(f"✗数据表处理失败: {table_name} - {e}")
        conn.rollback()
        return False

# 专用函数
def save_nger_data(conn, year_label: str, df: pd.DataFrame) -> bool:
    """保存NGER数据"""
    try:
        cursor = conn.cursor()
        
        mappings = {
            'facility_type': ['type'], 'electricity_production_gj': ['electricityproductiongj'],
            'electricity_production_mwh': ['electricityproductionmwh'],
            'emission_intensity_tco2e_mwh': ['emissionintensitytco2emwh', 'emissionintensitytmwh'],
            'scope1_emissions_tco2e': ['scope1tco2e', 'totalscope1emissionstco2e'],
            'scope2_emissions_tco2e': ['scope2tco2e', 'totalscope2emissionstco2e', 'totalscope2emissionstco2e2'],
            'total_emissions_tco2e': ['totalemissionstco2e'], 'grid_info': ['grid'],
            'grid_connected': ['gridconnected', 'gridconnected2'], 'important_notes': ['importantnotes']
        }
        
        # 确保地理编码列存在（即使表已存在）
        geocode_fields = {'lat': 'NUMERIC', 'lon': 'NUMERIC', 'formatted_address': 'TEXT', 'place_id': 'TEXT',
                          'osm_type': 'TEXT', 'osm_id': 'TEXT', 'confidence': 'NUMERIC', 'match_type': 'TEXT',
                          'locality': 'TEXT', 'postcode': 'TEXT', 'state_full': 'TEXT', 'country': 'TEXT',
                          'geocode_query': 'TEXT', 'geocode_provider': 'TEXT',
                          'bbox_south': 'NUMERIC', 'bbox_north': 'NUMERIC', 'bbox_west': 'NUMERIC', 'bbox_east': 'NUMERIC',
                          'polygon_geojson': 'TEXT'}

        for col_name, col_type in geocode_fields.items():
            try:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = %s 
                        AND column_name = %s
                    );
                """, ('nger_unified', col_name))
                if not cursor.fetchone()[0]:
                    cursor.execute(f"ALTER TABLE nger_unified ADD COLUMN {col_name} {col_type}")
                    print(f"  ✓添加NGER列: {col_name} ({col_type})")
            except Exception as e:
                print(f"  ⚠添加NGER列失败: {col_name} - {e}")

        # 统一布尔解析函数（针对 grid_connected 字段）
        def _parse_bool(value):
            try:
                if isinstance(value, bool):
                    return value
                s = str(value).strip().lower()
                truthy = {
                    'true', 'yes', '1', 'y', 't', 'connected', 'on-grid', 'on grid', 'ongrid', 'ON'
                }
                falsy = {
                    'false', 'no', '0', 'n', 'f', 'not connected', 'disconnected', 'off-grid', 'off grid', 'offgrid', 'OFF'
                }
                if s in truthy:
                    return True
                if s in falsy:
                    return False
                return None
            except Exception:
                return None

        data = []
        for _, row in df.iterrows():
            row_data = [year_label]
            
            # 基础列
            for col in ['facilityname', 'state', 'primaryfuel', 'reportingentity', 'controllingcorporation']:
                value = row.get(col) if col in df.columns else None
                row_data.append(str(value) if value and not pd.isna(value) else None)
            
            # 映射列
            for target_col, source_cols in mappings.items():
                value = None
                for source_col in source_cols:
                    if source_col in df.columns:
                        val = row.get(source_col)
                        if val and not pd.isna(val) and str(val).strip():
                            if target_col == 'grid_connected':
                                value = _parse_bool(val)
                            elif target_col.endswith(('_gj', '_mwh', '_tco2e')):
                                try:
                                    value = float(str(val).replace(',', ''))
                                except:
                                    value = None
                            else:
                                value = str(val)
                            break
                row_data.append(value)

            # 追加地理编码列值
            for field in geocode_fields.keys():
                value = row.get(field)
                if field in ['lat', 'lon', 'confidence'] and value is not None and not pd.isna(value):
                    try:
                        row_data.append(float(value))
                    except:
                        row_data.append(None)
                else:
                    row_data.append(str(value) if value is not None and not pd.isna(value) and str(value).strip() else None)
            data.append(tuple(row_data))
        
        cols = ['year_label', 'facilityname', 'state', 'primaryfuel', 'reportingentity', 'controllingcorporation',
                'facility_type', 'electricity_production_gj', 'electricity_production_mwh',
                'emission_intensity_tco2e_mwh', 'scope1_emissions_tco2e', 'scope2_emissions_tco2e',
                'total_emissions_tco2e', 'grid_info', 'grid_connected', 'important_notes'] + list(geocode_fields.keys())
        
        # 批量插入
        insert_sql = prepare_insert_sql('nger_unified', cols)
        batch_insert(cursor, insert_sql, data)
        
        # 插入完成后生成/更新geom列
        try:
            ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, 'nger_unified', 'polygon_geojson',
                                            'bbox_west', 'bbox_south', 'bbox_east', 'bbox_north')
        except Exception as e:
            print(f"  ⚠更新NGER几何列失败: {e}")

        conn.commit()
        print(f"  ✓NGER数据入库成功: {len(data)}行 -> nger_unified表")
        return True
        
    except Exception as e:
        print(f"  ✗NGER数据入库失败: {e}")
        conn.rollback()
        return False

def save_cer_data(conn, table_type: str, df: pd.DataFrame) -> bool:
    """保存CER数据（表已存在，动态添加列）"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(f"cer_{table_type}")
        
        # 原始列和地理编码列
        geocode_fields = {'lat': 'NUMERIC', 'lon': 'NUMERIC', 'formatted_address': 'TEXT', 'place_id': 'TEXT',
                         'osm_type': 'TEXT', 'osm_id': 'TEXT', 'confidence': 'NUMERIC', 'match_type': 'TEXT',
                         'locality': 'TEXT', 'postcode': 'TEXT', 'state_full': 'TEXT', 'country': 'TEXT',
                         'geocode_query': 'TEXT', 'geocode_provider': 'TEXT',
                         'bbox_south': 'NUMERIC', 'bbox_north': 'NUMERIC', 'bbox_west': 'NUMERIC', 'bbox_east': 'NUMERIC',
                         'polygon_geojson': 'TEXT'}
        geocode_column_names = set(geocode_fields.keys())
        original_cols = [col for col in df.columns if col not in geocode_column_names]
        
        # 准备列信息用于数据插入
        used_names = {'id'}
        clean_original_cols = []
        for col in original_cols:
            clean_col = clean_name(col)
            # 避免与地理编码字段冲突
            if clean_col in ['postcode', 'state_full', 'country', 'locality']:
                clean_col = f"original_{clean_col}"
            # 确保唯一性
            original_name = clean_col
            counter = 1
            while clean_col in used_names:
                clean_col = f"{original_name}_{counter}"
                counter += 1
            used_names.add(clean_col)
            clean_original_cols.append(clean_col)
        
        # 动态添加列到表
        all_columns = clean_original_cols + list(geocode_fields.keys())
        for col_name in all_columns:
            try:
                # 检查列是否存在
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = %s 
                        AND column_name = %s
                    );
                """, (clean_table, col_name))
                column_exists = cursor.fetchone()[0]
                
                if not column_exists:
                    # 确定列类型
                    if col_name in geocode_fields:
                        col_type = geocode_fields[col_name]
                    else:
                        col_type = 'TEXT'  # 默认为TEXT类型
                    
                    # 添加列
                    alter_sql = f"ALTER TABLE {clean_table} ADD COLUMN {col_name} {col_type}"
                    cursor.execute(alter_sql)
                    print(f"  ✓添加列: {col_name} ({col_type})")
            except Exception as e:
                print(f"  ⚠添加列失败: {col_name} - {e}")
                # 继续处理其他列
        
        # 准备数据
        data = []
        
        for _, row in df.iterrows():
            row_data = [str(row.get(col)) if row.get(col) and not pd.isna(row.get(col)) else None for col in original_cols]
            
            for field in geocode_fields.keys():
                value = row.get(field)
                if field in ['lat', 'lon', 'confidence'] and value is not None:
                    try:
                        row_data.append(float(value))
                    except:
                        row_data.append(None)
                else:
                    row_data.append(str(value) if value and not pd.isna(value) else None)
            
            data.append(tuple(row_data))
        
        # 插入
        insert_sql = prepare_insert_sql(clean_table, all_columns)
        batch_insert(cursor, insert_sql, data)
        
        # 为CER表创建/更新geom列
        try:
            ensure_geometry_column_and_index(cursor, clean_table, 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, clean_table, 'polygon_geojson',
                                            'bbox_west', 'bbox_south', 'bbox_east', 'bbox_north')
        except Exception as e:
            print(f"  ⚠更新CER几何列失败: {e}")

        conn.commit()
        print(f"  ✓CER数据入库成功: {clean_table} ({len(data)}行，含地理编码)")
        return True
        
    except Exception as e:
        print(f"  ✗CER数据入库失败: {e}")
        conn.rollback()
        return False

def create_abs_table(conn, merged_cell_value: str, columns: List[str]) -> str:
    """创建ABS表（表已存在，只返回表名）"""
    clean_table = clean_name(merged_cell_value)
    print(f"✓ABS表已存在: {clean_table}")
    return clean_table

def insert_abs_data(conn, table_name: str, df: pd.DataFrame, geo_level: int = None) -> bool:
    """插入ABS数据"""
    try:
        cursor = conn.cursor()
        
        cols = ['code', 'label', 'year', 'geographic_level']
        used = set(cols)
        for col in df.columns[3:]:
            clean_col = clean_name(col)
            original = clean_col
            counter = 1
            while clean_col in used:
                clean_col = f"{original}_{counter}"
                counter += 1
            used.add(clean_col)
            cols.append(clean_col)
        
        data = []
        for _, row in df.iterrows():
            row_data = []
            # 前3列
            for i in range(3):
                value = row[df.columns[i]]
                if pd.isna(value):
                    row_data.append(None)
                else:
                    str_val = str(value)
                    if str_val == '-':
                        row_data.append(None)
                    elif i == 0 and len(str_val) > 50:
                        row_data.append(str_val[:50])
                    elif i == 2:
                        try:
                            row_data.append(int(float(str_val)))
                        except:
                            row_data.append(None)
                    else:
                        row_data.append(str_val)
            
            # 添加geographic_level
            row_data.append(geo_level if geo_level is not None else -1)
            
            # 其他列
            for col in df.columns[3:]:
                value = row[col]
                if pd.isna(value) or str(value) == '-':
                    row_data.append(None)
                else:
                    row_data.append(str(value))
            
            data.append(tuple(row_data))
        
        insert_sql = prepare_insert_sql(table_name, cols)
        batch_insert(cursor, insert_sql, data, 10000)
        
        conn.commit()
        print(f"✓ABS数据插入成功: {len(data)}行")
        return True
        
    except Exception as e:
        print(f"✗ABS数据插入失败: {e}")
        conn.rollback()
        return False

# =============================================================================
# PostGIS/Geometry 辅助函数
# =============================================================================

def geometry_column_exists(cursor, table_name: str, geom_col: str = 'geom') -> bool:
    """检查geometry列是否存在。"""
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        );
        """,
        (table_name, geom_col)
    )
    return bool(cursor.fetchone()[0])


def ensure_geometry_column_and_index(cursor, table_name: str, lat_col: str = 'lat', lon_col: str = 'lon', geom_col: str = 'geom') -> None:
    """确保存在geometry(Point,4326)列，并由lat/lon填充，创建GiST索引。"""
    # 1) 添加geometry列
    if not geometry_column_exists(cursor, table_name, geom_col):
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {geom_col} geometry(Point, 4326);")
            print(f"  ✓添加geometry列: {table_name}.{geom_col}")
        except Exception as e:
            # 若因扩展未启用失败则上层应已尝试启用
            raise e
    # 2) 用lat/lon更新geom（仅空值）
    update_sql = f"""
        UPDATE {table_name}
        SET {geom_col} = ST_SetSRID(ST_MakePoint(NULLIF({lon_col}::text,'')::double precision,
                                                 NULLIF({lat_col}::text,'')::double precision), 4326)
        WHERE {geom_col} IS NULL AND {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL;
    """
    cursor.execute(update_sql)
    # 3) 创建GiST索引（若不存在）
    index_name = f"{table_name}_{geom_col}_gist"
    cursor.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'i'
                  AND c.relname = %s
            ) THEN
                EXECUTE format('CREATE INDEX %I ON %I USING GIST (%I);', %s, %s, %s);
            END IF;
        END$$;
        """,
        (index_name, index_name, table_name, geom_col)
    )
    print(f"  ✓geometry索引已确保: {index_name}")

def ensure_area_and_bbox_geometries(cursor, table_name: str,
                                    polygon_geojson_col: str = 'polygon_geojson',
                                    bbox_w_col: str = 'bbox_west', bbox_s_col: str = 'bbox_south',
                                    bbox_e_col: str = 'bbox_east', bbox_n_col: str = 'bbox_north',
                                    area_geom_col: str = 'geom_area', bbox_geom_col: str = 'geom_bbox') -> None:
    """确保区域多边形与bbox多边形几何列存在并填充，同时创建GiST索引。"""
    # 区域多边形列
    if not geometry_column_exists(cursor, table_name, area_geom_col):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {area_geom_col} geometry(MultiPolygon, 4326);")
        print(f"  ✓添加geometry列: {table_name}.{area_geom_col}")
    # bbox多边形列
    if not geometry_column_exists(cursor, table_name, bbox_geom_col):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {bbox_geom_col} geometry(Polygon, 4326);")
        print(f"  ✓添加geometry列: {table_name}.{bbox_geom_col}")

    # 用GeoJSON填充区域多边形（仅空值）
    cursor.execute(f"""
        UPDATE {table_name}
        SET {area_geom_col} =
            CASE
                WHEN {polygon_geojson_col} IS NOT NULL AND {polygon_geojson_col} <> '' THEN
                    CASE
                        WHEN jsonb_typeof({polygon_geojson_col}::jsonb) = 'object' THEN ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON({polygon_geojson_col}), 4326))
                        ELSE NULL
                    END
                ELSE {area_geom_col}
            END
        WHERE {area_geom_col} IS NULL;
    """)

    # 用bbox填充bbox多边形（仅空值）
    cursor.execute(f"""
        UPDATE {table_name}
        SET {bbox_geom_col} =
            CASE
                WHEN {bbox_w_col} IS NOT NULL AND {bbox_s_col} IS NOT NULL AND {bbox_e_col} IS NOT NULL AND {bbox_n_col} IS NOT NULL THEN
                    ST_MakeEnvelope({bbox_w_col}::double precision, {bbox_s_col}::double precision,
                                    {bbox_e_col}::double precision, {bbox_n_col}::double precision, 4326)
                ELSE {bbox_geom_col}
            END
        WHERE {bbox_geom_col} IS NULL;
    """)

    # 索引
    for col in (area_geom_col, bbox_geom_col):
        index_name = f"{table_name}_{col}_gist"
        cursor.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'i'
                      AND c.relname = %s
                ) THEN
                    EXECUTE format('CREATE INDEX %I ON %I USING GIST (%I);', %s, %s, %s);
                END IF;
            END$$;
            """,
            (index_name, index_name, table_name, col)
        )
        print(f"  ✓geometry索引已确保: {index_name}")