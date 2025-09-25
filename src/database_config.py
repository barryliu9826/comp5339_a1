#!/usr/bin/env python3
"""数据库配置和操作"""

# 标准库导入
import threading
from typing import List

# 第三方库导入
import psycopg2
import psycopg2.pool
import pandas as pd

# 本地模块导入
from excel_utils import get_merged_cells, read_merged_headers
from state_standardizer import standardize_dataframe_states, standardize_state_name
from data_cleaner import *


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
    """创建NGER表的实现（使用规范化列名）"""
    
    # 定义NGER表的列结构
    column_definitions = {
        'year_label': 'TEXT',
        'start_year': 'INTEGER', 
        'stop_year': 'INTEGER',
        'facility_name': 'TEXT',
        'state': 'TEXT',
        'facility_type': 'TEXT',
        'primary_fuel': 'TEXT',
        'reporting_entity': 'TEXT',
        'controlling_corporation': 'TEXT',
        'electricity_production_gj': 'NUMERIC',
        'electricity_production_mwh': 'NUMERIC',
        'emission_intensity_tco2e_mwh': 'NUMERIC',
        'scope1_emissions_tco2e': 'NUMERIC',
        'scope2_emissions_tco2e': 'NUMERIC',
        'total_emissions_tco2e': 'NUMERIC',
        'grid_info': 'TEXT',
        'grid_connected': 'BOOLEAN',
        'important_notes': 'TEXT',
        'lat': 'NUMERIC',
        'lon': 'NUMERIC',
        'formatted_address': 'TEXT',
        'place_id': 'TEXT',
        'postcode': 'TEXT',
        'bbox_south': 'NUMERIC',
        'bbox_north': 'NUMERIC',
        'bbox_west': 'NUMERIC',
        'bbox_east': 'NUMERIC'
    }
    
    create_sql = create_table_sql_with_normalized_columns('nger_unified', column_definitions)
    ok = create_table_safe(cursor, 'nger_unified', create_sql)
    if not ok:
        return False
    
    print(f"✓NGER表创建完成（规范化列名）")
    
    # 确保geometry列
    try:
        ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
    except Exception as e:
        print(f"⚠NGER几何列处理失败: {e}")
    return True

def create_nger_table(conn) -> bool:
    """创建NGER表（在单线程中调用）"""
    try:
        cursor = conn.cursor()
        result = create_nger_table_impl(cursor)
        if result is not False:
            conn.commit()
        return result
    except Exception as e:
        print(f"✗NGER表创建失败: {e}")
        conn.rollback()
        return False

def create_cer_tables_impl(cursor):
    """创建CER表的实现（使用规范化列名）"""
    
    cer_table_types = ['approved_power_stations', 'committed_power_stations', 'probable_power_stations']
    
    for table_type in cer_table_types:
        # 基础列结构
        column_definitions = {
            'accreditation_code': 'TEXT',
            'power_station_name': 'TEXT',
            'project_name': 'TEXT', 
            'state': 'TEXT',
            'postcode': 'TEXT',
            'installed_capacity_mw': 'NUMERIC',
            'mw_capacity': 'NUMERIC',
            'fuel_source': 'TEXT',
            'accreditation_start_date': 'TEXT',
            'approval_date': 'TEXT',
            'committed_date': 'TEXT',
            'committed_date_year': 'INTEGER',
            'committed_date_month': 'INTEGER',
            'accreditation_start_date_year': 'INTEGER',
            'accreditation_start_date_month': 'INTEGER',
            'approval_date_year': 'INTEGER',
            'approval_date_month': 'INTEGER',
            # 地理编码字段
            'lat': 'NUMERIC',
            'lon': 'NUMERIC',
            'formatted_address': 'TEXT',
            'place_id': 'TEXT',
            'bbox_south': 'NUMERIC',
            'bbox_north': 'NUMERIC',
            'bbox_west': 'NUMERIC',
            'bbox_east': 'NUMERIC'
        }
        
        normalized_table_name = normalize_db_column_name(f"cer_{table_type}")
        create_sql = create_table_sql_with_normalized_columns(normalized_table_name, column_definitions)
        
        if not create_table_safe(cursor, normalized_table_name, create_sql):
            return False
        
        print(f"✓CER表创建完成（规范化列名）: {normalized_table_name}")
    
    return True

def create_cer_tables(conn) -> bool:
    """创建CER表（在单线程中调用）"""
    try:
        cursor = conn.cursor()
        result = create_cer_tables_impl(cursor)
        if result is not False:
            conn.commit()
        return result
    except Exception as e:
        print(f"✗CER表创建失败: {e}")
        conn.rollback()
        return False


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
                    # 安全列范围（防止越界）
                    total_cols = len(df.columns)
                    if total_cols < 3:
                        print(f"✗跳过: 列不足3列，无法构建基础列 Code/Label/Year -> {cell['value']}")
                        continue
                    start_col_safe = max(0, min(start_col, total_cols))
                    end_col_safe = max(start_col_safe, min(end_col, total_cols))
                    if start_col_safe >= end_col_safe:
                        print(f"✗跳过: 无效列范围 [{start_col},{end_col}) -> [{start_col_safe},{end_col_safe}) : {cell['value']}")
                        continue
                    selected_cols = ['Code', 'Label', 'Year'] + list(df.columns[start_col_safe:end_col_safe])
                    
                    # 创建表
                    try:
                        clean_table = normalize_db_column_name(cell['value'])
                        
                        # 使用规范化列名和类型检测创建表
                        
                        # 检测列类型（基于这个子集的数据）
                        # 基于安全范围提取子集
                        idx_slice = [0, 1, 2] + list(range(start_col_safe, end_col_safe))
                        subset_df = df.iloc[:, idx_slice]
                        subset_df.columns = selected_cols
                        column_types = detect_numeric_columns(subset_df)
                        
                        # 创建列名规范化列表（与 selected_cols 等长、顺序对齐）
                        normalized_cols = normalize_column_mapping(selected_cols)
                        
                        # 构建列定义字典
                        column_definitions = {
                            'code': 'TEXT',
                            'label': 'TEXT', 
                            'year': 'INTEGER',
                            'geographic_level': 'INTEGER',
                            'standardized_state': 'TEXT',
                            'lga_code_clean': 'TEXT',
                            'lga_name_clean': 'TEXT'
                        }
                        
                        # 添加数据列
                        for col, normalized_col in zip(selected_cols[3:], normalized_cols[3:]):  # 跳过Code, Label, Year
                            
                            # 根据检测到的类型设置SQL类型
                            col_type = column_types.get(col, 'text')
                            if col_type in ['integer']:
                                sql_type = 'INTEGER'
                            elif col_type in ['float', 'percentage', 'currency']:
                                sql_type = 'NUMERIC'
                            else:
                                sql_type = 'TEXT'
                            
                            column_definitions[normalized_col] = sql_type
                        
                        # 创建表（为ABS表添加前缀）
                        normalized_table_name = normalize_db_column_name(f"abs_{cell['value']}")  # 使用统一函数
                        create_sql = create_table_sql_with_normalized_columns(
                            normalized_table_name, 
                            column_definitions
                        )
                        
                        if not create_table_safe(cursor, normalized_table_name, create_sql):
                            print(f"✗ABS表创建失败: {cell['value']}")
                            return False
                        else:
                            # 报告列名规范化和类型检测结果
                            print_column_mapping_report(selected_cols, normalized_cols)
                            numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
                            if numeric_cols:
                                print(f"  📊{cell['value']}: 检测到{len(numeric_cols)}个数值列")
                    except Exception as e:
                        print(f"✗ABS表创建失败: {cell['value']} - {e}")
                        return False
                
                print(f"✓ABS表预创建完成: {sheet_name} - {len(merged_cells)}个表")
                
            except Exception as e:
                print(f"✗ABS表预创建失败: {sheet_name} - {e}")
                return False
        
        # 成功创建后提交事务，确保表实际存在
        conn.commit()
        return True
        
    except Exception as e:
        print(f"✗ABS表预创建失败: {e}")
        conn.rollback()
        return False





def batch_insert(cursor, insert_sql: str, data: List[tuple], batch_size: int = 1000) -> None:
    """批量插入数据"""
    for i in range(0, len(data), batch_size):
        cursor.executemany(insert_sql, data[i:i + batch_size])

def prepare_insert_sql(table_name: str, columns: List[str]) -> str:
    """准备插入SQL语句"""
    placeholders = ', '.join(['%s'] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"


# 专用函数
def save_nger_data(conn, year_label: str, df: pd.DataFrame) -> bool:
    """保存NGER数据"""
    try:
        cursor = conn.cursor()
        
        # 标准化州名
        print(f"  📍标准化NGER州名...")
        standardize_dataframe_states(df, 'state')
        
        # 使用规范化的列名映射
        mappings = {
            'facility_type': ['type'],
            'electricity_production_gj': ['electricityproductiongj'],
            'electricity_production_mwh': ['electricityproductionmwh'],
            'emission_intensity_tco2e_mwh': ['emissionintensitytco2emwh', 'emissionintensitytmwh'],
            'scope1_emissions_tco2e': ['scope1tco2e', 'totalscope1emissionstco2e'],
            'scope2_emissions_tco2e': ['scope2tco2e', 'totalscope2emissionstco2e', 'totalscope2emissionstco2e2'],
            'total_emissions_tco2e': ['totalemissionstco2e'],
            'grid_info': ['grid'],
            'grid_connected': ['gridconnected', 'gridconnected2'],
            'important_notes': ['importantnotes']
        }
        
        # 规范化原始列名到数据库列名的映射
        column_name_mapping = {
            'facilityname': 'facility_name',
            'primaryfuel': 'primary_fuel',
            'reportingentity': 'reporting_entity',
            'controllingcorporation': 'controlling_corporation'
        }
        
        # 确保地理编码列存在（即使表已存在）
        geocode_fields = {
            'lat': 'NUMERIC',
            'lon': 'NUMERIC',
            'formatted_address': 'TEXT',
            'place_id': 'TEXT',
            'postcode': 'TEXT',
            'bbox_south': 'NUMERIC',
            'bbox_north': 'NUMERIC',
            'bbox_west': 'NUMERIC',
            'bbox_east': 'NUMERIC'
        }

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
                    'true', 'yes', '1', 'y', 't', 'connected', 'on-grid', 'on grid', 'ongrid', 'on'
                }
                falsy = {
                    'false', 'no', '0', 'n', 'f', 'not connected', 'disconnected', 'off-grid', 'off grid', 'offgrid', 'off'
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
            
            # 添加时间列
            start_year = row.get('start_year') if 'start_year' in df.columns else None
            stop_year = row.get('stop_year') if 'stop_year' in df.columns else None
            row_data.append(start_year)
            row_data.append(stop_year)
            
            # 基础列（使用规范化的列名）
            basic_columns = ['facilityname', 'state', 'primaryfuel', 'reportingentity', 'controllingcorporation']
            for col in basic_columns:
                value = row.get(col) if col in df.columns else None
                has_value = (value is not None) and (not pd.isna(value)) and (str(value).strip() != '') and (str(value).strip().lower() not in {'nan', 'none', '-'})
                row_data.append(str(value).strip() if has_value else None)
            
            # 映射列
            for target_col, source_cols in mappings.items():
                value = None
                for source_col in source_cols:
                    if source_col in df.columns:
                        val = row.get(source_col)
                        # 不要用 truthiness 过滤，否则会把 False/0 当作空值
                        has_value = (val is not None) and (not pd.isna(val)) and (str(val).strip() != '')
                        if has_value:
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
        
        # 使用规范化的列名
        cols = ['year_label', 'start_year', 'stop_year', 'facility_name', 'state', 'primary_fuel', 'reporting_entity', 'controlling_corporation',
                'facility_type', 'electricity_production_gj', 'electricity_production_mwh',
                'emission_intensity_tco2e_mwh', 'scope1_emissions_tco2e', 'scope2_emissions_tco2e',
                'total_emissions_tco2e', 'grid_info', 'grid_connected', 'important_notes'] + list(geocode_fields.keys())
        
        # 批量插入
        insert_sql = prepare_insert_sql('nger_unified', cols)
        batch_insert(cursor, insert_sql, data)
        
        # 插入完成后生成/更新geom列
        try:
            ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, 'nger_unified',
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
    """保存CER数据（使用规范化列名，表已存在）"""
    try:
        cursor = conn.cursor()
        normalized_table_name = normalize_db_column_name(f"cer_{table_type}")
        
        # 标准化州名
        print(f"  📍标准化CER州名...")
        standardize_dataframe_states(df, 'state')
        
        # 原始列和地理编码列
        geocode_fields = {
            'lat': 'NUMERIC',
            'lon': 'NUMERIC',
            'formatted_address': 'TEXT',
            'place_id': 'TEXT',
            'postcode': 'TEXT',
            'bbox_south': 'NUMERIC',
            'bbox_north': 'NUMERIC',
            'bbox_west': 'NUMERIC',
            'bbox_east': 'NUMERIC'
        }
        geocode_column_names = set(geocode_fields.keys())
        original_cols = [col for col in df.columns if col not in geocode_column_names]
        
        # 准备列信息用于数据插入
        used_names = {'id'}
        clean_original_cols = []
        for col in original_cols:
            clean_col = normalize_db_column_name(col)
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
                """, (normalized_table_name, col_name))
                column_exists = cursor.fetchone()[0]
                
                if not column_exists:
                    # 确定列类型
                    if col_name in geocode_fields:
                        col_type = geocode_fields[col_name]
                    else:
                        col_type = 'TEXT'  # 默认为TEXT类型
                    
                    # 添加列
                    alter_sql = f"ALTER TABLE {normalized_table_name} ADD COLUMN {col_name} {col_type}"
                    cursor.execute(alter_sql)
                    print(f"  ✓添加列: {col_name} ({col_type})")
            except Exception as e:
                print(f"  ⚠添加列失败: {col_name} - {e}")
                # 继续处理其他列
        
        # 准备数据
        data = []
        
        for _, row in df.iterrows():
            row_data = []
            for col in original_cols:
                val = row.get(col)
                has_value = (val is not None) and (not pd.isna(val)) and (str(val).strip() != '') and (str(val).strip().lower() not in {'nan', 'none', '-'})
                row_data.append(str(val).strip() if has_value else None)
            
            for field in geocode_fields.keys():
                value = row.get(field)
                if field in ['lat', 'lon', 'confidence'] and value is not None and not pd.isna(value):
                    try:
                        row_data.append(float(value))
                    except:
                        row_data.append(None)
                else:
                    has_value = (value is not None) and (not pd.isna(value)) and (str(value).strip() != '') and (str(value).strip().lower() not in {'nan', 'none', '-'})
                    row_data.append(str(value).strip() if has_value else None)
            
            data.append(tuple(row_data))
        
        # 插入
        insert_sql = prepare_insert_sql(normalized_table_name, all_columns)
        batch_insert(cursor, insert_sql, data)
        
        # 为CER表创建/更新geom列
        try:
            ensure_geometry_column_and_index(cursor, normalized_table_name, 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, normalized_table_name,
                                            'bbox_west', 'bbox_south', 'bbox_east', 'bbox_north')
        except Exception as e:
            print(f"  ⚠更新CER几何列失败: {e}")

        conn.commit()
        print(f"  ✓CER数据入库成功: {normalized_table_name} ({len(data)}行，含地理编码)")
        return True
        
    except Exception as e:
        print(f"  ✗CER数据入库失败: {e}")
        conn.rollback()
        return False

def create_abs_table(conn, merged_cell_value: str, columns: List[str]) -> str:
    """创建ABS表（表已存在，只返回表名）"""
    clean_table = normalize_db_column_name(f"abs_{merged_cell_value}")
    print(f"✓ABS表已存在: {clean_table}")
    return clean_table

def create_abs_table_with_types(conn, merged_cell_value: str, columns: List[str], column_types: dict) -> str:
    """创建ABS表（基于预检测的列类型，表已在预创建阶段存在）"""
    clean_table = normalize_db_column_name(f"abs_{merged_cell_value}")
    print(f"✓ABS表已存在（带类型）: {clean_table}")
    return clean_table

def insert_abs_data(conn, table_name: str, df: pd.DataFrame, geo_level: int = None) -> bool:
    """插入ABS数据（包含数值转换和LGA标准化）"""
    try:
        cursor = conn.cursor()
        
        # 使用新的ABS数据清理工具
        
        print(f"  🧹开始ABS数据清理...")
        
        # 标准化州名
        print(f"  📍标准化ABS州名...")
        if 'Label' in df.columns:
            df['standardized_state'] = df['Label'].apply(standardize_state_name)
        
        # 执行数值转换和LGA标准化
        df_cleaned, column_types = process_abs_data_with_cleaning(df)
        
        # 准备列名映射
        cols = ['code', 'label', 'year', 'geographic_level']
        used = set(cols)
        
        # 添加标准化列
        if 'standardized_state' in df_cleaned.columns:
            cols.append('standardized_state')
            used.add('standardized_state')
        
        # 处理数据列
        for col in df_cleaned.columns[3:]:
            if col == 'standardized_state':
                continue
            clean_col = normalize_db_column_name(col)
            original = clean_col
            counter = 1
            while clean_col in used:
                clean_col = f"{original}_{counter}"
                counter += 1
            used.add(clean_col)
            cols.append(clean_col)
        
        # 准备插入数据
        data = []
        for _, row in df_cleaned.iterrows():
            row_data = []
            
            # 前3列：Code, Label, Year
            for i in range(3):
                value = row[df_cleaned.columns[i]]
                if pd.isna(value):
                    row_data.append(None)
                else:
                    str_val = str(value).strip()
                    lower_val = str_val.lower()
                    if str_val == '-' or str_val == '' or lower_val in {'nan', 'none', 'null'}:
                        row_data.append(None)
                    elif i == 0 and len(str_val) > 50:  # Code列截断
                        row_data.append(str_val[:50])
                    elif i == 2:  # Year列转整数
                        try:
                            row_data.append(int(float(str_val)))
                        except:
                            row_data.append(None)
                    else:
                        row_data.append(str_val)
            
            # 添加geographic_level
            row_data.append(geo_level if geo_level is not None else -1)
            
            # 添加标准化列
            if 'standardized_state' in df_cleaned.columns:
                value = row.get('standardized_state')
                row_data.append(value if value is not None and not pd.isna(value) else None)
            
            # 处理数据列（已经过数值转换）
            for col in df_cleaned.columns[3:]:
                if col == 'standardized_state':
                    continue
                    
                value = row[col]
                
                # 对于已转换的数值列，直接使用数值
                col_type = column_types.get(col, 'text')
                if col_type != 'text' and not pd.isna(value):
                    row_data.append(value)  # 数值已经转换过
                elif pd.isna(value):
                    row_data.append(None)
                else:
                    # 文本列的处理
                    str_val = str(value).strip()
                    lower_val = str_val.lower()
                    if str_val == '-' or str_val == '' or lower_val in {'nan', 'none', 'null'}:
                        row_data.append(None)
                    else:
                        row_data.append(str_val)
            
            data.append(tuple(row_data))
        
        insert_sql = prepare_insert_sql(table_name, cols)
        batch_insert(cursor, insert_sql, data, 10000)
        
        conn.commit()
        
        # 统计报告
        numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
        print(f"✓ABS数据插入成功: {len(data)}行")
        if numeric_cols:
            print(f"  📊包含{len(numeric_cols)}个数值列")
        
        return True
        
    except Exception as e:
        print(f"✗ABS数据插入失败: {e}")
        conn.rollback()
        return False

def insert_abs_data_cleaned(conn, table_name: str, df: pd.DataFrame, geo_level: int = None, column_types: dict = None) -> bool:
    """插入已清理的ABS数据（数据已在入库前完成清理）"""
    try:
        cursor = conn.cursor()
        
        print(f"  💾插入已清理的ABS数据到: {table_name}")
        
        # 准备列名映射（数据已经包含标准化列）
        cols = ['code', 'label', 'year', 'geographic_level']
        used = set(cols)
        
        # 添加标准化列
        for std_col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
            if std_col in df.columns:
                cols.append(std_col)
                used.add(std_col)
        
        # 处理数据列，并建立 原列 -> 规范化列 的映射（保持顺序与唯一性）
        original_to_clean = {}
        for col in df.columns[3:]:
            if col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
                continue
            clean_col = normalize_db_column_name(col)
            original = clean_col
            counter = 1
            while clean_col in used:
                clean_col = f"{original}_{counter}"
                counter += 1
            used.add(clean_col)
            cols.append(clean_col)
            original_to_clean[col] = clean_col

        # 在插入前，确保所有列在目标表中已经存在（防止列名映射不一致导致的缺列错误）
        try:
            for clean_col in cols:
                # 检查列是否已存在
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                          AND table_name = %s 
                          AND column_name = %s
                    );
                    """,
                    (table_name, clean_col)
                )
                if not bool(cursor.fetchone()[0]):
                    # 推断列类型：优先使用传入的 column_types（基于原列名），否则根据列名猜测
                    sql_type = 'TEXT'
                    if clean_col in ['code', 'label', 'standardized_state', 'lga_code_clean', 'lga_name_clean']:
                        sql_type = 'TEXT'
                    elif clean_col in ['year', 'geographic_level']:
                        sql_type = 'INTEGER'
                    else:
                        # 反查原列名以获得类型提示
                        source_col = None
                        for orig, mapped in original_to_clean.items():
                            if mapped == clean_col:
                                source_col = orig
                                break
                        if column_types and source_col and source_col in column_types:
                            ct = column_types[source_col]
                            if ct in ['integer']:
                                sql_type = 'INTEGER'
                            elif ct in ['float', 'percentage', 'currency']:
                                sql_type = 'NUMERIC'
                            else:
                                sql_type = 'TEXT'
                        else:
                            # 基于列名启发推断
                            lc = clean_col.lower()
                            if any(k in lc for k in ['percent', 'rate', 'ratio']):
                                sql_type = 'NUMERIC'
                            elif any(k in lc for k in ['count', 'number', 'total']):
                                sql_type = 'INTEGER'
                            else:
                                sql_type = 'TEXT'
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {clean_col} {sql_type}")
                        print(f"  ✓添加ABS列: {table_name}.{clean_col} ({sql_type})")
                    except Exception as ee:
                        print(f"  ⚠添加ABS列失败: {table_name}.{clean_col} - {ee}")
        except Exception as ee:
            print(f"  ⚠ABS列校验/补充失败: {ee}")
        
        # 准备插入数据（数据已经清理过）
        data = []
        for _, row in df.iterrows():
            row_data = []
            
            # 前3列：Code, Label, Year
            for i in range(3):
                value = row[df.columns[i]]
                if pd.isna(value):
                    row_data.append(None)
                else:
                    str_val = str(value).strip()
                    if str_val in ['-', '', 'nan', 'none', 'null'] or str_val.lower() in ['nan', 'none', 'null']:
                        row_data.append(None)
                    elif i == 0 and len(str_val) > 50:  # Code列截断
                        row_data.append(str_val[:50])
                    elif i == 2:  # Year列转整数
                        try:
                            row_data.append(int(float(str_val)))
                        except:
                            row_data.append(None)
                    else:
                        row_data.append(str_val)
            
            # 添加geographic_level
            row_data.append(geo_level if geo_level is not None else -1)
            
            # 添加标准化列
            for std_col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
                if std_col in df.columns:
                    value = row.get(std_col)
                    row_data.append(value if value is not None and not pd.isna(value) else None)
            
            # 处理数据列（已经清理过，直接使用）
            for col in df.columns[3:]:
                if col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
                    continue
                    
                value = row[col]
                if pd.isna(value):
                    row_data.append(None)
                else:
                    # 数据已经清理过，直接使用
                    row_data.append(value)
            
            data.append(tuple(row_data))
        
        insert_sql = prepare_insert_sql(table_name, cols)
        batch_insert(cursor, insert_sql, data, 10000)
        
        conn.commit()
        
        # 简化的统计报告
        print(f"  ✓ABS数据入库成功: {len(data)}行（已预清理）")
        
        return True
        
    except Exception as e:
        print(f"  ✗ABS数据入库失败: {e}")
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
    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST ({geom_col});")
    except Exception as e:
        # 兼容老版本 Postgres 无 IF NOT EXISTS 的情况：忽略已存在错误
        try:
            cursor.execute(f"SELECT 1 FROM pg_class WHERE relname = %s;", (index_name,))
            exists = bool(cursor.fetchone())
            if not exists:
                cursor.execute(f"CREATE INDEX {index_name} ON {table_name} USING GIST ({geom_col});")
        except Exception:
            pass
    print(f"  ✓geometry索引已确保: {index_name}")

def ensure_area_and_bbox_geometries(cursor, table_name: str,
                                    bbox_w_col: str = 'bbox_west', bbox_s_col: str = 'bbox_south',
                                    bbox_e_col: str = 'bbox_east', bbox_n_col: str = 'bbox_north',
                                    bbox_geom_col: str = 'geom_bbox') -> None:
    """确保bbox多边形几何列存在并填充，同时创建GiST索引。"""
    # bbox多边形列
    if not geometry_column_exists(cursor, table_name, bbox_geom_col):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {bbox_geom_col} geometry(Polygon, 4326);")
        print(f"  ✓添加geometry列: {table_name}.{bbox_geom_col}")

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
    index_name = f"{table_name}_{bbox_geom_col}_gist"
    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST ({bbox_geom_col});")
    except Exception as e:
        try:
            cursor.execute(f"SELECT 1 FROM pg_class WHERE relname = %s;", (index_name,))
            exists = bool(cursor.fetchone())
            if not exists:
                cursor.execute(f"CREATE INDEX {index_name} ON {table_name} USING GIST ({bbox_geom_col});")
        except Exception:
            pass
    print(f"  ✓geometry索引已确保: {index_name}")