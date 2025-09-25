#!/usr/bin/env python3
"""
统一数据清理模块
整合所有数据清理、规范化和质量修复功能
"""

import pandas as pd
import re
from typing import Dict, List, Set, Optional, Any, Tuple


# =============================================================================
# 常量定义
# =============================================================================

# 月份缩写到数字的映射（通用）
MONTH_ABBR_TO_NUM = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

# 缺失值标识（通用）
MISSING_VALUE_INDICATORS = ['-', '', 'nan', 'NaN', 'none', 'None', 'NULL', 'null', 'N/A', 'n/a']


# =============================================================================
# 通用辅助函数
# =============================================================================

def is_missing_value(value: Any) -> bool:
    """
    检查值是否为缺失值
    
    Args:
        value: 要检查的值
        
    Returns:
        True如果是缺失值，False否则
    """
    if pd.isna(value) or value is None:
        return True
    
    str_val = str(value).strip()
    return str_val in MISSING_VALUE_INDICATORS or str_val.lower() in [x.lower() for x in MISSING_VALUE_INDICATORS]


# =============================================================================
# 数据库列名规范化功能 (原 db_column_normalizer.py)
# =============================================================================

def normalize_db_column_name(name: str, reserved_words: Set[str] = None) -> str:
    """
    规范化数据库列名
    
    Args:
        name: 原始列名
        reserved_words: 数据库保留字集合
        
    Returns:
        规范化后的列名
    """
    if not name or str(name).strip() == '':
        return 'unnamed_column'
    
    # 默认的PostgreSQL保留字
    if reserved_words is None:
        reserved_words = {
            'user', 'order', 'group', 'select', 'from', 'where', 'insert', 'update', 
            'delete', 'create', 'drop', 'alter', 'table', 'index', 'view', 'database',
            'schema', 'primary', 'foreign', 'key', 'constraint', 'references', 'check',
            'unique', 'not', 'null', 'default', 'auto_increment', 'serial', 'boolean',
            'integer', 'varchar', 'text', 'date', 'time', 'timestamp', 'numeric',
            'real', 'double', 'precision', 'decimal', 'char', 'binary', 'blob'
        }
    
    # 第1步：基础清理
    clean_name = str(name).strip()
    
    # 第2步：转换为小写
    clean_name = clean_name.lower()
    
    # 第3步：处理特殊字符和缩写
    # 常见的单位和缩写规范化
    unit_replacements = {
        r'\(mw\)': '_mw',
        r'\(gj\)': '_gj', 
        r'\(mwh\)': '_mwh',
        r'\(tco2e\)': '_tco2e',
        r'\(s\)': 's',
        r'\(%\)': '_percent',
        r'\$': 'dollar_',
    }
    
    for pattern, replacement in unit_replacements.items():
        clean_name = re.sub(pattern, replacement, clean_name)
    
    # 第4步：移除其他特殊字符，保留字母数字和空格
    clean_name = re.sub(r'[^\w\s]', '', clean_name)
    
    # 第5步：空格转下划线
    clean_name = re.sub(r'\s+', '_', clean_name)
    
    # 第6步：多个下划线合并为一个
    clean_name = re.sub(r'_+', '_', clean_name)
    
    # 第7步：去除首尾下划线
    clean_name = clean_name.strip('_')
    
    # 第8步：处理空结果
    if not clean_name:
        clean_name = 'unnamed_column'
    
    # 第9步：如果以数字开头，添加前缀
    if clean_name[0].isdigit():
        clean_name = f'col_{clean_name}'
    
    # 第10步：检查是否为保留字
    if clean_name.lower() in reserved_words:
        clean_name = f'{clean_name}_col'
    
    # 第11步：长度限制（PostgreSQL标识符限制为63字符）
    if len(clean_name) > 60:  # 留3个字符给可能的后缀
        clean_name = clean_name[:60]
    
    return clean_name


def normalize_column_mapping(columns: List[str]) -> List[str]:
    """
    按输入顺序返回等长的规范化列名列表（确保唯一性）
    
    Args:
        columns: 原始列名列表
        
    Returns:
        与输入等长的规范化列名列表（位置对齐），会对重复名添加 _1/_2 后缀
    """
    normalized_list = []
    used_names = set()
    
    for original_col in columns:
        normalized = normalize_db_column_name(original_col)
        
        # 处理重复的规范化名称（基于出现顺序）
        if normalized in used_names:
            counter = 1
            base_name = normalized
            while f"{base_name}_{counter}" in used_names:
                counter += 1
            normalized = f"{base_name}_{counter}"
        
        used_names.add(normalized)
        normalized_list.append(normalized)
    
    return normalized_list


def create_table_sql_with_normalized_columns(table_name: str, 
                                           column_definitions: Dict[str, str],
                                           primary_key: str = 'id',
                                           additional_constraints: List[str] = None) -> str:
    """
    创建带有规范化列名的建表SQL
    
    Args:
        table_name: 表名
        column_definitions: {规范化列名: SQL类型} 字典
        primary_key: 主键列名
        additional_constraints: 额外的约束条件
        
    Returns:
        建表SQL语句
    """
    # 规范化表名
    normalized_table_name = normalize_db_column_name(table_name)
    
    # 构建列定义（确保列名唯一）
    column_parts = []
    used_norm_cols = set()
    
    # 主键
    pk_norm = normalize_db_column_name(primary_key)
    if primary_key not in column_definitions:
        column_parts.append(f"{pk_norm} SERIAL PRIMARY KEY")
        used_norm_cols.add(pk_norm)
    
    # 其他列
    for col_name, col_type in column_definitions.items():
        norm = normalize_db_column_name(col_name)
        base = norm
        if norm in used_norm_cols:
            counter = 1
            while f"{base}_{counter}" in used_norm_cols:
                counter += 1
            norm = f"{base}_{counter}"
        used_norm_cols.add(norm)
        column_parts.append(f"{norm} {col_type}")
    
    # 额外约束
    if additional_constraints:
        column_parts.extend(additional_constraints)
    
    sql = f"CREATE TABLE IF NOT EXISTS {normalized_table_name} (\n"
    sql += ",\n".join(f"    {part}" for part in column_parts)
    sql += "\n);"
    
    return sql


def get_standard_column_types() -> Dict[str, str]:
    """
    获取标准的列类型映射
    
    Returns:
        {列名模式: SQL类型} 字典
    """
    return {
        # 基础字段
        'id': 'SERIAL PRIMARY KEY',
        'name': 'TEXT',
        'code': 'TEXT',
        'label': 'TEXT',
        'state': 'TEXT',
        'postcode': 'TEXT',
        
        # 时间字段
        'year': 'INTEGER',
        'month': 'INTEGER',
        'start_year': 'INTEGER',
        'stop_year': 'INTEGER',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        
        # 数值字段
        'capacity_mw': 'NUMERIC',
        'production_gj': 'NUMERIC',
        'production_mwh': 'NUMERIC',
        'emissions_tco2e': 'NUMERIC',
        'intensity_tco2e_mwh': 'NUMERIC',
        'percent': 'NUMERIC',
        'dollar': 'NUMERIC',
        'count': 'INTEGER',
        'population': 'INTEGER',
        
        # 布尔字段
        'connected': 'BOOLEAN',
        'active': 'BOOLEAN',
        'enabled': 'BOOLEAN',
        
        # 地理字段
        'lat': 'NUMERIC',
        'lon': 'NUMERIC',
        'latitude': 'NUMERIC',
        'longitude': 'NUMERIC',
        'bbox_north': 'NUMERIC',
        'bbox_south': 'NUMERIC',
        'bbox_east': 'NUMERIC',
        'bbox_west': 'NUMERIC',
        'formatted_address': 'TEXT',
        'place_id': 'TEXT',
        
        # 默认文本类型
        'default': 'TEXT'
    }


def infer_column_type(column_name: str, sample_values: List = None) -> str:
    """
    推断列的SQL类型
    
    Args:
        column_name: 规范化后的列名
        sample_values: 样本值列表（可选）
        
    Returns:
        推断的SQL类型
    """
    standard_types = get_standard_column_types()
    
    # 精确匹配
    if column_name in standard_types:
        return standard_types[column_name]
    
    # 模式匹配
    col_lower = column_name.lower()
    
    # 数值类型
    if any(pattern in col_lower for pattern in ['capacity', 'mw', 'gj', 'mwh', 'tco2e', 'emissions', 'production']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['percent', 'rate', 'ratio', 'intensity']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['dollar', 'gdp', 'income', 'revenue', 'cost']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['count', 'number', 'population', 'total']):
        return 'INTEGER'
    
    # 时间类型
    if any(pattern in col_lower for pattern in ['year', 'month', 'day']):
        return 'INTEGER'
    
    if any(pattern in col_lower for pattern in ['date', 'time']):
        return 'DATE'
    
    # 布尔类型
    if any(pattern in col_lower for pattern in ['connected', 'active', 'enabled', 'grid']):
        return 'BOOLEAN'
    
    # 地理类型
    if any(pattern in col_lower for pattern in ['lat', 'lon', 'latitude', 'longitude', 'bbox']):
        return 'NUMERIC'
    
    # 默认文本类型
    return 'TEXT'


def print_column_mapping_report(original_columns: List[str], normalized_columns: List[str]):
    """
    打印列名规范化报告（基于等长的原始/规范化列名列表）
    
    Args:
        original_columns: 原始列名列表
        normalized_columns: 位置对齐的规范化列名列表
    """
    print(f"📋 列名规范化报告: {len(original_columns)} 个列")
    
    changes = []
    unchanged = []
    
    for orig_col, norm_col in zip(original_columns, normalized_columns):
        if orig_col != norm_col:
            changes.append((orig_col, norm_col))
        else:
            unchanged.append(orig_col)
    
    if changes:
        print(f"  ✓ {len(changes)} 个列名已规范化:")
        for orig, norm in changes[:10]:  # 只显示前10个
            print(f"    - {orig} → {norm}")
        if len(changes) > 10:
            print(f"    ... 还有 {len(changes) - 10} 个列名变化")
    
    if unchanged:
        print(f"  ✓ {len(unchanged)} 个列名无需更改")
    
    print()


# =============================================================================
# ABS数据清理功能 (原 abs_data_cleaner.py)
# =============================================================================

def detect_numeric_columns(df: pd.DataFrame, start_col: int = 3) -> Dict[str, str]:
    """
    检测数值列类型并返回列名到类型的映射
    
    Args:
        df: DataFrame
        start_col: 从第几列开始检测（前面通常是Code/Label/Year）
        
    Returns:
        dict: {列名: 数据类型} 其中类型为 'integer', 'float', 'percentage', 'currency', 'text'
    """
    column_types = {}
    
    for col in df.columns[start_col:]:
        if col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
            continue
            
        # 采样前100个非空值进行类型判断
        sample_values = df[col].dropna().head(100)
        if sample_values.empty:
            column_types[col] = 'text'
            continue
            
        # 转为字符串并清理
        str_values = [str(v).strip() for v in sample_values if not is_missing_value(v)]
        if not str_values:
            column_types[col] = 'text'
            continue
            
        # 分析模式
        numeric_count = 0
        percentage_count = 0
        currency_count = 0
        
        for val in str_values[:50]:  # 只看前50个有效值
            # 百分比检测
            if '%' in val or 'percent' in val.lower():
                percentage_count += 1
                continue
                
            # 货币检测
            if any(symbol in val for symbol in ['$', '€', '£', '¥', 'AUD', 'USD']):
                currency_count += 1
                continue
                
            # 数值检测（包含千分位逗号）
            clean_val = re.sub(r'[,\s]', '', val)  # 移除逗号和空格
            try:
                if '.' in clean_val:
                    float(clean_val)
                    numeric_count += 1
                else:
                    int(clean_val)
                    numeric_count += 1
            except ValueError:
                pass
        
        total_checked = len(str_values[:50])
        numeric_ratio = numeric_count / total_checked if total_checked > 0 else 0
        
        # 类型判断
        if percentage_count > total_checked * 0.3:
            column_types[col] = 'percentage'
        elif currency_count > total_checked * 0.3:
            column_types[col] = 'currency'
        elif numeric_ratio > 0.7:
            # 进一步判断整数还是浮点数
            has_decimal = any('.' in re.sub(r'[,\s]', '', str(v)) for v in str_values[:20] if str(v).strip())
            column_types[col] = 'float' if has_decimal else 'integer'
        else:
            column_types[col] = 'text'
    
    return column_types


def clean_numeric_value(value: Any, target_type: str = 'float') -> Optional[float]:
    """
    通用数值清理和转换函数
    
    Args:
        value: 原始值
        target_type: 目标类型 ('integer', 'float', 'percentage', 'currency', 'capacity')
        
    Returns:
        转换后的数值或None
    """
    if pd.isna(value):
        return None
        
    if is_missing_value(value):
        return None
    
    str_val = str(value).strip()
    
    try:
        # 处理百分比
        if target_type == 'percentage':
            # 移除%符号，转换为小数
            clean_val = re.sub(r'[%\s]', '', str_val)
            clean_val = re.sub(r'[,]', '', clean_val)  # 移除千分位
            return float(clean_val) / 100.0
        
        # 处理货币
        elif target_type == 'currency':
            # 移除货币符号和千分位
            clean_val = re.sub(r'[$€£¥,\s]|AUD|USD|EUR|GBP', '', str_val, flags=re.IGNORECASE)
            return float(clean_val)
        
        # 处理容量（移除单位标识）
        elif target_type == 'capacity':
            # 移除千分位逗号、空格和单位标识（如MW, mw等）
            clean_val = re.sub(r'[,\s]', '', str_val)
            clean_val = re.sub(r'[a-zA-Z]+', '', clean_val)
            return float(clean_val)
        
        # 处理普通数值
        else:
            # 移除千分位逗号和多余空格
            clean_val = re.sub(r'[,\s]', '', str_val)
            if target_type == 'integer':
                return float(int(float(clean_val)))  # 先转float再转int避免小数点问题
            else:  # float
                return float(clean_val)
                
    except (ValueError, TypeError):
        return None




def process_abs_data_with_cleaning(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    处理ABS数据，包括数值转换
    
    Args:
        df: 原始DataFrame
        
    Returns:
        (处理后的DataFrame, 列类型映射)
    """
    print("  🔍检测ABS数值列类型...")
    
    # 1. 检测数值列类型
    column_types = detect_numeric_columns(df)
    numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
    
    if numeric_cols:
        print(f"  ✓检测到{len(numeric_cols)}个数值列: {list(numeric_cols.keys())}")
        for col, col_type in list(numeric_cols.items())[:5]:  # 只显示前5个
            print(f"    - {col}: {col_type}")
        if len(numeric_cols) > 5:
            print(f"    ... 还有{len(numeric_cols)-5}个数值列")
    
    # 2. 转换数值列
    print("  🔢转换数值列...")
    df_processed = df.copy()
    converted_count = 0
    for col, col_type in column_types.items():
        if col_type != 'text' and col in df_processed.columns:
            # 创建新的数值列
            numeric_col = f"{col}_numeric"
            df_processed[numeric_col] = df_processed[col].apply(
                lambda x: clean_numeric_value(x, col_type)
            )
            
            # 统计成功转换的数量
            success_count = df_processed[numeric_col].notna().sum()
            if success_count > 0:
                converted_count += success_count
                # 将原列替换为数值列
                df_processed[col] = df_processed[numeric_col]
                df_processed.drop(columns=[numeric_col], inplace=True)
    
    if converted_count > 0:
        print(f"  ✓数值转换完成: {converted_count}个值成功转换")
    else:
        print("  ⚠️未发现需要转换的数值")
    
    return df_processed, column_types


# =============================================================================
# 通用辅助函数
# =============================================================================

def standardize_fuel_type(value: Any) -> Optional[str]:
    """
    通用燃料类型标准化函数
    
    Args:
        value: 原始燃料类型值
        
    Returns:
        标准化后的燃料类型名称
    """
    if is_missing_value(value):
        return None
    
    val_str = str(value).strip().lower()
    
    # 统一的燃料类型映射
    fuel_mapping = {
        # 可再生能源
        'solar': 'Solar',
        'wind': 'Wind',
        'hydro': 'Hydro',
        'biomass': 'Biomass',
        'biofuel': 'Biofuel',
        'bagasse': 'Bagasse',
        'wood': 'Biomass',
        
        # 化石燃料
        'coal': 'Coal',
        'black coal': 'Black Coal',
        'brown coal': 'Brown Coal',
        'gas': 'Natural Gas',
        'natural gas': 'Natural Gas',
        'diesel': 'Diesel',
        
        # 特殊燃料
        'coal seam methane': 'Coal Seam Gas',
        'coal seam gas': 'Coal Seam Gas',
        'waste coal mine gas': 'Coal Mine Gas',
        'coal mine gas': 'Coal Mine Gas',
        'landfill gas': 'Landfill Gas',
        
        # 储能
        'battery': 'Battery Storage',
        'battery storage': 'Battery Storage',
    }
    
    # 直接匹配
    if val_str in fuel_mapping:
        return fuel_mapping[val_str]
    
    # 部分匹配
    for key, value in fuel_mapping.items():
        if key in val_str:
            return value
    
    # 默认返回首字母大写的格式
    return str(value).strip().title()


def clean_facility_name(value: Any, name_type: str = 'facility') -> Optional[str]:
    """
    通用设施/电站名称清理函数
    
    Args:
        value: 原始名称值
        name_type: 名称类型 ('facility', 'station', 'project')
        
    Returns:
        清理后的名称
    """
    if is_missing_value(value):
        return None
    
    name = str(value).strip()
    
    # 跳过特殊的汇总行
    if name.lower() in ['corporate total', 'facility', 'total', 'summary']:
        return name
    
    if name_type == 'station':
        # CER电站名称特殊处理：移除冗余的描述性后缀
        patterns_to_remove = [
            r'\s*-\s*(Solar|Wind|Gas|Hydro|Battery)\s*(w\s*SGU)?\s*-\s*[A-Z]{2,3}$',
            r'\s*-\s*(Solar|Wind|Gas|Hydro|Battery)$',
            r'\s*w\s*SGU\s*$',
            r'\s*wSGU\s*$'
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # 通用清理
    # 标准化分隔符
    name = re.sub(r'\s*-\s*', ' - ', name)
    name = re.sub(r'\s*,\s*', ', ', name)
    
    # 标准化括号格式
    name = re.sub(r'\s*\(\s*([^)]+)\s*\)\s*', r' (\1)', name)
    
    # 清理多余空格
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# =============================================================================
# CER数据清理功能 (原 cer_data_cleaner.py)
# =============================================================================

def normalize_cer_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    规范化CER数据的列名
    
    Args:
        df: 原始DataFrame
        
    Returns:
        列名规范化后的DataFrame
    """
    df_normalized = df.copy()
    
    # 列名映射规则
    column_mappings = {
        # 基础信息列
        'accreditation code': 'accreditation_code',
        'power station name': 'power_station_name',
        'project name': 'project_name',
        'state ': 'state',  # 处理尾部空格
        'state': 'state',
        'postcode': 'postcode',
        
        # 容量相关
        'installed capacity (mw)': 'installed_capacity_mw',
        'mw capacity': 'mw_capacity',
        
        # 燃料类型
        'fuel source (s)': 'fuel_source',
        'fuel source': 'fuel_source',
        
        # 日期相关
        'accreditation start date': 'accreditation_start_date',
        'approval date': 'approval_date',
        'committed date (month/year)': 'committed_date',
    }
    
    # 创建新的列名映射
    new_columns = {}
    for col in df.columns:
        # 清理列名：去除首尾空格，转小写
        clean_col = str(col).strip().lower()
        
        # 查找映射
        if clean_col in column_mappings:
            new_columns[col] = column_mappings[clean_col]
        else:
            # 默认规范化：空格转下划线，去除特殊字符
            normalized = re.sub(r'[^\w\s]', '', clean_col)  # 去除特殊字符
            normalized = re.sub(r'\s+', '_', normalized)    # 空格转下划线
            normalized = re.sub(r'_+', '_', normalized)     # 多个下划线合并
            normalized = normalized.strip('_')              # 去除首尾下划线
            new_columns[col] = normalized
    
    # 重命名列
    df_normalized = df_normalized.rename(columns=new_columns)
    
    print(f"  ✓CER列名规范化完成: {len(new_columns)}个列")
    
    # 显示主要的列名变化
    important_changes = []
    for old_col, new_col in new_columns.items():
        if old_col != new_col and any(key in old_col.lower() for key in ['state', 'power', 'capacity', 'fuel']):
            important_changes.append(f"{old_col} → {new_col}")
    
    if important_changes:
        print("  主要列名变化:")
        for change in important_changes[:5]:  # 只显示前5个
            print(f"    - {change}")
    
    return df_normalized


def process_cer_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理CER数据的时间列，将MMM-YYYY格式拆分为year和month列
    
    Args:
        df: DataFrame
        
    Returns:
        添加了year和month列的DataFrame
    """
    df_processed = df.copy()
    
    # 使用通用月份映射
    month_abbr_to_num = MONTH_ABBR_TO_NUM
    
    # 查找包含日期的列
    date_columns = []
    for col in df_processed.columns:
        if any(keyword in col.lower() for keyword in ['date', 'committed']):
            date_columns.append(col)
    
    processed_count = 0
    
    for date_col in date_columns:
        # 创建year和month列
        year_col = f"{date_col}_year"
        month_col = f"{date_col}_month"
        
        def parse_mmm_yyyy(date_str):
            """解析MMM-YYYY格式的日期"""
            if pd.isna(date_str) or not date_str:
                return None, None
            
            try:
                date_str = str(date_str).strip().lower()
                if '-' in date_str:
                    parts = date_str.split('-')
                    if len(parts) == 2:
                        month_abbr = parts[0].strip()
                        year_str = parts[1].strip()
                        
                        # 转换月份
                        month_num = month_abbr_to_num.get(month_abbr)
                        if month_num is None:
                            return None, None
                        
                        # 转换年份
                        try:
                            year_num = int(year_str)
                            return year_num, month_num
                        except ValueError:
                            return None, None
                return None, None
            except Exception:
                return None, None
        
        # 应用解析函数
        parsed_dates = df_processed[date_col].apply(parse_mmm_yyyy)
        
        # 拆分年份和月份
        years = [item[0] if item[0] is not None else None for item in parsed_dates]
        months = [item[1] if item[1] is not None else None for item in parsed_dates]
        
        df_processed[year_col] = years
        df_processed[month_col] = months
        
        # 统计成功转换的数量
        success_count = sum(1 for y, m in zip(years, months) if y is not None and m is not None)
        if success_count > 0:
            processed_count += success_count
            print(f"  ✓时间列处理: {date_col} → {year_col}, {month_col} ({success_count}条记录)")
    
    if processed_count == 0:
        print("  ⚠️未找到需要处理的时间列")
    else:
        print(f"  ✓CER时间处理完成: 共处理{processed_count}条时间记录")
    
    return df_processed


def convert_cer_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    转换CER数据中的数值列
    
    Args:
        df: DataFrame
        
    Returns:
        数值列转换后的DataFrame
    """
    df_processed = df.copy()
    
    # 识别需要转换的数值列（容量相关）
    capacity_columns = []
    for col in df_processed.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['capacity', 'mw', 'installed']):
            capacity_columns.append(col)
    
    if not capacity_columns:
        print("  ⚠️未找到需要转换的容量列")
        return df_processed
    
    
    converted_count = 0
    
    for col in capacity_columns:
        # 转换数值（使用通用函数）
        original_values = df_processed[col].copy()
        df_processed[col] = df_processed[col].apply(lambda x: clean_numeric_value(x, 'capacity'))
        
        # 统计成功转换的数量
        success_count = df_processed[col].notna().sum()
        original_count = original_values.notna().sum()
        
        if success_count > 0:
            converted_count += success_count
            print(f"  ✓数值转换: {col} ({success_count}/{original_count}条记录)")
    
    if converted_count > 0:
        print(f"  ✓CER数值转换完成: 共转换{converted_count}个数值")
    else:
        print("  ⚠️未成功转换任何数值")
    
    return df_processed


def process_cer_data_with_cleaning(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    处理CER数据，包括列名规范化、时间处理和数值转换
    
    Args:
        df: 原始DataFrame
        table_type: 表类型（如 "approved_power_stations"）
        
    Returns:
        处理后的DataFrame
    """
    print(f"  🧹开始CER数据清理: {table_type}")
    
    # 1. 列名规范化
    print("  📝规范化列名...")
    df_processed = normalize_cer_column_names(df)
    
    # 2. 时间处理
    print("  🕐处理时间列...")
    df_processed = process_cer_time_columns(df_processed)
    
    # 3. 数值转换
    print("  🔢转换数值列...")
    df_processed = convert_cer_numeric_columns(df_processed)
    
    print(f"  ✓CER数据清理完成: {table_type} ({df_processed.shape})")
    
    return df_processed


# =============================================================================
# 数据质量修复功能 (原 data_quality_fixer.py)
# =============================================================================

def fix_missing_values(df: pd.DataFrame, missing_indicators: List[str] = None) -> pd.DataFrame:
    """
    统一处理缺失值标识
    
    Args:
        df: 原始DataFrame
        missing_indicators: 缺失值标识列表
        
    Returns:
        修复后的DataFrame
    """
    if missing_indicators is None:
        missing_indicators = MISSING_VALUE_INDICATORS
    
    df_fixed = df.copy()
    
    # 统计修复情况
    fix_count = 0
    
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':  # 只处理文本列
            for indicator in missing_indicators:
                # 精确匹配缺失值标识
                mask = df_fixed[col].astype(str).str.strip() == indicator
                count = mask.sum()
                if count > 0:
                    df_fixed.loc[mask, col] = None
                    fix_count += count
    
    print(f"  ✓缺失值修复: 共修复 {fix_count} 个缺失值标识")
    return df_fixed


def parse_date_flexible(date_str: str) -> Optional[Tuple[int, int, int]]:
    """
    灵活解析多种日期格式
    
    Args:
        date_str: 日期字符串
        
    Returns:
        (year, month, day) 元组，如果解析失败返回None
    """
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    
    # 使用通用月份映射
    month_abbr = MONTH_ABBR_TO_NUM
    
    try:
        # 格式1: DD/MM/YYYY
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
        
        # 格式2: MMM-YYYY
        elif '-' in date_str and len(date_str.split('-')) == 2:
            parts = date_str.split('-')
            month_str, year_str = parts[0].strip().lower(), parts[1].strip()
            if month_str in month_abbr:
                year, month = int(year_str), month_abbr[month_str]
                return (year, month, 1)  # 默认为月初
        
        # 格式3: YYYY-MM-DD
        elif '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # 年份在前
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
        
        # 格式4: DD-MM-YYYY
        elif '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            if len(parts[2]) == 4:  # 年份在后
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
    
    except (ValueError, IndexError):
        pass
    
    return None


def fix_date_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    修复多种日期格式
    
    Args:
        df: 数据DataFrame
        
    Returns:
        修复后的DataFrame
    """
    df_fixed = df.copy()
    
    print("  📅修复日期格式...")
    
    # 查找所有日期列
    date_columns = [col for col in df_fixed.columns if 'date' in col.lower()]
    
    total_fixed = 0
    
    for date_col in date_columns:
        if date_col not in df_fixed.columns:
            continue
            
        print(f"    处理日期列: {date_col}")
        
        # 创建标准化的日期列
        year_col = f"{date_col}_year_fixed"
        month_col = f"{date_col}_month_fixed"
        day_col = f"{date_col}_day_fixed"
        iso_col = f"{date_col}_iso"
        
        years, months, days, iso_dates = [], [], [], []
        success_count = 0
        
        for idx, date_val in enumerate(df_fixed[date_col]):
            parsed = parse_date_flexible(date_val)
            
            if parsed:
                year, month, day = parsed
                years.append(year)
                months.append(month)
                days.append(day)
                
                # 生成ISO格式日期
                try:
                    iso_date = f"{year:04d}-{month:02d}-{day:02d}"
                    iso_dates.append(iso_date)
                    success_count += 1
                except:
                    iso_dates.append(None)
            else:
                years.append(None)
                months.append(None)
                days.append(None)
                iso_dates.append(None)
        
        # 添加新列
        df_fixed[year_col] = years
        df_fixed[month_col] = months
        df_fixed[day_col] = days
        df_fixed[iso_col] = iso_dates
        
        total_fixed += success_count
        print(f"      ✓成功解析 {success_count}/{len(df_fixed)} 个日期")
    
    if total_fixed > 0:
        print(f"  ✓日期格式修复完成: 共修复 {total_fixed} 个日期值")
    else:
        print(f"  ⚠️未找到需要修复的日期列")
    
    return df_fixed


def fix_nger_specific_issues(df: pd.DataFrame) -> pd.DataFrame:
    """
    修复NGER数据特有的问题
    
    Args:
        df: NGER数据DataFrame
        
    Returns:
        修复后的DataFrame
    """
    df_fixed = df.copy()
    
    print("  🔧修复NGER特有问题...")
    
    # 1. 统一缺失值处理
    df_fixed = fix_missing_values(df_fixed)
    
    # 2. 标准化布尔字段
    if 'gridconnected' in df_fixed.columns:
        def standardize_grid_connected(value):
            if pd.isna(value) or value is None:
                return None
            
            val_str = str(value).strip().lower()
            if val_str in ['on', 'connected', 'yes', 'true', '1']:
                return 'Connected'
            elif val_str in ['off', 'disconnected', 'no', 'false', '0']:
                return 'Disconnected'
            else:
                return None
        
        original_count = df_fixed['gridconnected'].notna().sum()
        df_fixed['gridconnected'] = df_fixed['gridconnected'].apply(standardize_grid_connected)
        fixed_count = df_fixed['gridconnected'].notna().sum()
        print(f"    - gridconnected字段标准化: {original_count} → {fixed_count}")
    
    # 3. 标准化燃料类型
    if 'primaryfuel' in df_fixed.columns:
        df_fixed['primaryfuel'] = df_fixed['primaryfuel'].apply(standardize_fuel_type)
        print(f"    - primaryfuel字段标准化完成")
    
    # 4. 清理设施名称
    if 'facilityname' in df_fixed.columns:
        df_fixed['facilityname'] = df_fixed['facilityname'].apply(lambda x: clean_facility_name(x, 'facility'))
        print(f"    - facilityname字段清理完成")
    
    return df_fixed


def fix_cer_specific_issues(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    修复CER数据特有的问题
    
    Args:
        df: CER数据DataFrame
        table_type: 表类型
        
    Returns:
        修复后的DataFrame
    """
    df_fixed = df.copy()
    
    print(f"  🔧修复CER特有问题: {table_type}...")
    
    # 1. 统一缺失值处理
    df_fixed = fix_missing_values(df_fixed)
    
    # 2. 修复日期格式
    df_fixed = fix_date_formats(df_fixed)
    
    # 3. 清理电站/项目名称
    name_columns = []
    if 'power_station_name' in df_fixed.columns:
        name_columns.append('power_station_name')
    elif 'Power station name' in df_fixed.columns:
        name_columns.append('Power station name')
    if 'project_name' in df_fixed.columns:
        name_columns.append('project_name')
    elif 'Project Name' in df_fixed.columns:
        name_columns.append('Project Name')
    
    for name_col in name_columns:
        df_fixed[name_col] = df_fixed[name_col].apply(lambda x: clean_facility_name(x, 'station'))
        print(f"    - {name_col}字段清理完成")
    
    # 4. 标准化燃料类型
    fuel_columns = []
    if 'fuel_source' in df_fixed.columns:
        fuel_columns.append('fuel_source')
    elif 'Fuel Source' in df_fixed.columns:
        fuel_columns.append('Fuel Source')
    elif 'Fuel Source (s)' in df_fixed.columns:
        fuel_columns.append('Fuel Source (s)')
    
    for fuel_col in fuel_columns:
        df_fixed[fuel_col] = df_fixed[fuel_col].apply(standardize_fuel_type)
        print(f"    - {fuel_col}字段标准化完成")
    
    return df_fixed


def process_data_quality_fixes(df: pd.DataFrame, data_type: str, **kwargs) -> pd.DataFrame:
    """
    统一的数据质量修复入口函数
    
    Args:
        df: 原始DataFrame
        data_type: 数据类型 ('nger', 'cer', 'abs')
        **kwargs: 额外参数
        
    Returns:
        修复后的DataFrame
    """
    print(f"  🔧开始数据质量修复: {data_type.upper()}")
    
    if data_type.lower() == 'nger':
        return fix_nger_specific_issues(df)
    elif data_type.lower() == 'cer':
        table_type = kwargs.get('table_type', 'unknown')
        return fix_cer_specific_issues(df, table_type)
    elif data_type.lower() == 'abs':
        # ABS数据修复可以在这里添加
        return fix_missing_values(df)
    else:
        # 默认只处理缺失值
        return fix_missing_values(df)

