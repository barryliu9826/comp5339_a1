#!/usr/bin/env python3
"""数据库配置和操作"""

import psycopg2
import pandas as pd
import numpy as np
from typing import List

DB_CONFIG = {
    'host': 'localhost', 'port': 5432, 'user': 'postgres', 
    'password': 'postgre', 'database': 'postgres'
}

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓PostgreSQL数据库连接成功")
        return conn
    except Exception as e:
        print(f"✗PostgreSQL数据库连接失败: {e}")
        return None

def clean_name(name: str, idx: int = 0) -> str:
    """统一名称清理"""
    if not name or str(name).strip() == '':
        return f"col_{idx + 1}"
    
    clean = str(name).strip().lower()
    for old, new in {' ': '_', '-': '_', '(': '', ')': '', '%': 'percent', 
                    ':': '', ',': '', '\n': '_', '\r': '_', '__': '_'}.items():
        clean = clean.replace(old, new)
    
    clean = ''.join(c for c in clean if c.isalnum() or c == '_')
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
            try:
                if pd.isna(value) or value is None:
                    row_data.append(None)
                elif isinstance(value, (pd.Series, np.ndarray, list, dict, tuple)):
                    row_data.append(str(value))
                else:
                    row_data.append(str(value))
            except:
                row_data.append(str(value) if value is not None else None)
        data.append(tuple(row_data))
    return data

def create_insert_table(conn, table_name: str, df: pd.DataFrame, extra_cols: List[tuple] = None) -> bool:
    """建表并插入数据"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(table_name)
        
        # 建表SQL
        cols = [clean_name(col, i) for i, col in enumerate(df.columns)]
        cols = make_unique(cols)
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {clean_table} (\nid SERIAL PRIMARY KEY"
        if extra_cols:
            for col_name, col_type in extra_cols:
                create_sql += f",\n{col_name} {col_type}"
        for col in cols:
            create_sql += f",\n{col} TEXT"
        create_sql += "\n);"
        
        cursor.execute(create_sql)
        
        data = safe_data_prep(df)
        if data:
            all_cols = ([col[0] for col in extra_cols] if extra_cols else []) + cols
            placeholders = ', '.join(['%s'] * len(all_cols))
            insert_sql = f"INSERT INTO {clean_table} ({', '.join(all_cols)}) VALUES ({placeholders})"
            
            for i in range(0, len(data), 1000):
                cursor.executemany(insert_sql, data[i:i + 1000])
        
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
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS nger_unified (
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
            important_notes TEXT
        );
        """
        cursor.execute(create_sql)
        
        mappings = {
            'facility_type': ['type'], 'electricity_production_gj': ['electricityproductiongj'],
            'electricity_production_mwh': ['electricityproductionmwh'],
            'emission_intensity_tco2e_mwh': ['emissionintensitytco2emwh', 'emissionintensitytmwh'],
            'scope1_emissions_tco2e': ['scope1tco2e', 'totalscope1emissionstco2e'],
            'scope2_emissions_tco2e': ['scope2tco2e', 'totalscope2emissionstco2e', 'totalscope2emissionstco2e2'],
            'total_emissions_tco2e': ['totalemissionstco2e'], 'grid_info': ['grid'],
            'grid_connected': ['gridconnected', 'gridconnected2'], 'important_notes': ['importantnotes']
        }
        
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
                                value = str(val).lower() in ['true', 'yes', '1', 'connected']
                            elif target_col.endswith(('_gj', '_mwh', '_tco2e')):
                                try:
                                    value = float(str(val).replace(',', ''))
                                except:
                                    value = None
                            else:
                                value = str(val)
                            break
                row_data.append(value)
            data.append(tuple(row_data))
        
        cols = ['year_label', 'facilityname', 'state', 'primaryfuel', 'reportingentity', 'controllingcorporation',
                'facility_type', 'electricity_production_gj', 'electricity_production_mwh',
                'emission_intensity_tco2e_mwh', 'scope1_emissions_tco2e', 'scope2_emissions_tco2e',
                'total_emissions_tco2e', 'grid_info', 'grid_connected', 'important_notes']
        
        # 批量插入
        placeholders = ', '.join(['%s'] * len(cols))
        insert_sql = f"INSERT INTO nger_unified ({', '.join(cols)}) VALUES ({placeholders})"
        
        for i in range(0, len(data), 1000):
            cursor.executemany(insert_sql, data[i:i + 1000])
        
        conn.commit()
        print(f"  ✓NGER数据入库成功: {len(data)}行 -> nger_unified表")
        return True
        
    except Exception as e:
        print(f"  ✗NGER数据入库失败: {e}")
        conn.rollback()
        return False

def save_cer_data(conn, table_type: str, df: pd.DataFrame) -> bool:
    """保存CER数据"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(f"cer_{table_type}")
        
        # 原始列和地理编码列
        original_cols = [col for col in df.columns if not col.startswith(('lat', 'lon', 'formatted_address', 'place_id', 'osm_', 'confidence', 'match_type', 'locality', 'postcode', 'state_full', 'country', 'geocode_'))]
        geocode_fields = {'lat': 'NUMERIC', 'lon': 'NUMERIC', 'formatted_address': 'TEXT', 'place_id': 'TEXT',
                         'osm_type': 'TEXT', 'osm_id': 'TEXT', 'confidence': 'NUMERIC', 'match_type': 'TEXT',
                         'locality': 'TEXT', 'postcode': 'TEXT', 'state_full': 'TEXT', 'country': 'TEXT',
                         'geocode_query': 'TEXT', 'geocode_provider': 'TEXT'}
        
        # 建表
        create_sql = f"CREATE TABLE IF NOT EXISTS {clean_table} (id SERIAL PRIMARY KEY"
        
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
            create_sql += f", {clean_col} TEXT"
        
        for field, field_type in geocode_fields.items(): create_sql += f", {field} {field_type}"
        create_sql += ");"
        cursor.execute(create_sql)
        
        # 准备数据
        all_columns = clean_original_cols + list(geocode_fields.keys())
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
        insert_sql = f"INSERT INTO {clean_table} ({', '.join(all_columns)}) VALUES ({', '.join(['%s'] * len(all_columns))})"
        for i in range(0, len(data), 1000): cursor.executemany(insert_sql, data[i:i + 1000])
        
        conn.commit()
        print(f"  ✓CER数据入库成功: {clean_table} ({len(data)}行，含地理编码)")
        return True
        
    except Exception as e:
        print(f"  ✗CER数据入库失败: {e}")
        conn.rollback()
        return False

def create_abs_table(conn, merged_cell_value: str, columns: List[str]) -> str:
    """创建ABS表"""
    try:
        cursor = conn.cursor()
        clean_table = clean_name(merged_cell_value)
        
        create_sql = f"""CREATE TABLE IF NOT EXISTS {clean_table} (
                            id SERIAL PRIMARY KEY,
                            code TEXT,
                            label TEXT,
                            year INTEGER,
                            geographic_level INTEGER"""
        
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
        cursor.execute(create_sql)
        conn.commit()
        print(f"✓ABS表创建成功: {clean_table}")
        return clean_table
    except Exception as e:
        print(f"✗ABS表创建失败: {merged_cell_value} - {e}")
        conn.rollback()
        return None

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
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        for i in range(0, len(data), 10000): cursor.executemany(insert_sql, data[i:i + 10000])
        
        conn.commit()
        print(f"✓ABS数据插入成功: {len(data)}行")
        return True
        
    except Exception as e:
        print(f"✗ABS数据插入失败: {e}")
        conn.rollback()
        return False