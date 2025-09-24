#!/usr/bin/env python3
"""
时间格式处理工具模块
用于在数据入库前统一处理时间格式
"""

import pandas as pd
from typing import Tuple, Optional

# 月份缩写到数字的映射
MONTH_ABBR_TO_NUM = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

def split_nger_year(year_label: str) -> Tuple[Optional[int], Optional[int]]:
    """
    将NGER的年份标签拆分成start_year和stop_year
    
    Args:
        year_label: 格式如 "2023-24"
        
    Returns:
        tuple: (start_year, stop_year)
    """
    if not year_label or pd.isna(year_label):
        return None, None
    
    try:
        year_str = str(year_label).strip()
        if '-' in year_str:
            parts = year_str.split('-')
            if len(parts) == 2:
                start_year = int(parts[0])
                # 处理两位数年份，如 "23-24"
                if len(parts[1]) == 2:
                    stop_year = int(f"{start_year // 100}{parts[1]:0>2}")
                else:
                    stop_year = int(parts[1])
                return start_year, stop_year
    except (ValueError, IndexError):
        pass
    
    return None, None

def convert_cer_date(date_str: str) -> Optional[str]:
    """
    将CER的日期格式从 "MMM-YYYY" 转换为 "YYYY-MM"
    
    Args:
        date_str: 格式如 "Dec-2019"
        
    Returns:
        str: 格式如 "2019-12"
    """
    if not date_str or pd.isna(date_str):
        return None
    
    try:
        date_str = str(date_str).strip()
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 2:
                month_abbr = parts[0].strip()
                year = parts[1].strip()
                
                if month_abbr in MONTH_ABBR_TO_NUM:
                    return f"{year}-{MONTH_ABBR_TO_NUM[month_abbr]}"
    except Exception:
        pass
    
    return None

def process_nger_time_format(df: pd.DataFrame, year_label: str) -> pd.DataFrame:
    """
    处理NGER数据的时间格式，添加start_year和stop_year列
    
    Args:
        df: NGER数据DataFrame
        year_label: 年份标签，如 "2023-24"
        
    Returns:
        DataFrame: 添加了时间列的数据
    """
    df_processed = df.copy()
    
    # 添加原始年份标签
    df_processed['year_label'] = year_label
    
    # 拆分年份
    start_year, stop_year = split_nger_year(year_label)
    df_processed['start_year'] = start_year
    df_processed['stop_year'] = stop_year
    
    print(f"  ✓NGER时间格式处理: {year_label} -> {start_year}, {stop_year}")
    return df_processed

def process_cer_time_format(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    处理CER数据的时间格式，转换日期格式
    
    Args:
        df: CER数据DataFrame
        table_type: 表类型，如 "committed_power_stations"
        
    Returns:
        DataFrame: 转换了时间格式的数据
    """
    df_processed = df.copy()
    
    # 查找包含日期的列
    date_columns = []
    for col in df_processed.columns:
        if any(keyword in col.lower() for keyword in ['date', 'committed']):
            date_columns.append(col)
    
    converted_count = 0
    for date_col in date_columns:
        # 创建格式化的日期列
        formatted_col = f"{date_col}_formatted"
        df_processed[formatted_col] = df_processed[date_col].apply(convert_cer_date)
        
        # 统计转换成功的数量
        success_count = df_processed[formatted_col].notna().sum()
        if success_count > 0:
            converted_count += success_count
            print(f"  ✓CER时间格式处理: 列'{date_col}' -> '{formatted_col}' ({success_count}条记录)")
    
    if converted_count == 0:
        print(f"  ⚠CER表'{table_type}'中未找到需要转换的日期列")
    
    return df_processed

def process_abs_time_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理ABS数据的时间格式（保持不变，仅验证）
    
    Args:
        df: ABS数据DataFrame
        
    Returns:
        DataFrame: 原始数据（时间格式保持不变）
    """
    # ABS数据的时间格式保持不变，只需要验证
    if 'Year' in df.columns:
        year_col = df['Year']
        if not year_col.empty:
            min_year = year_col.min()
            max_year = year_col.max()
            print(f"  ✓ABS时间格式验证: 年份范围 {min_year}-{max_year} (整数格式，保持不变)")
    
    return df
