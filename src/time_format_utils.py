#!/usr/bin/env python3
"""
Time format processing utility module
Used for unified time format processing before data insertion
"""

# Standard library imports
from typing import Optional, Tuple

# Third-party library imports
import pandas as pd

# Local module imports
from data_cleaner import process_cer_time_columns

def split_nger_year(year_label: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Split NGER year label into start_year and stop_year
    Args:
        year_label: Format like "2023-24"
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
                # Handle two-digit years like "23-24"
                if len(parts[1]) == 2:
                    stop_year = int(f"{start_year // 100}{parts[1]:0>2}")
                else:
                    stop_year = int(parts[1])
                return start_year, stop_year
    except (ValueError, IndexError):
        pass
    
    return None, None

def process_nger_time_format(df: pd.DataFrame, year_label: str) -> pd.DataFrame:
    """
    Process NGER data time format, add start_year and stop_year columns
    Args:
        df: NGER data DataFrame
        year_label: Year label like "2023-24"
    Returns:
        DataFrame: Data with added time columns
    """
    df_processed = df.copy()
    
    # Add original year label
    df_processed['year_label'] = year_label
    
    # Split year
    start_year, stop_year = split_nger_year(year_label)
    df_processed['start_year'] = start_year
    df_processed['stop_year'] = stop_year
    
    print(f"  NGER time format processing: {year_label} -> {start_year}, {stop_year}")
    return df_processed

def process_cer_time_format(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    Process CER data time format, split MMM-YYYY into year and month columns
    Args:
        df: CER data DataFrame
        table_type: Table type like "committed_power_stations"
    Returns:
        DataFrame: Data with added year and month columns
    """
    print(f"  Processing CER time format: {table_type}")
    return process_cer_time_columns(df)

def process_abs_time_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process ABS data time format (unchanged, validation only)
    Args:
        df: ABS data DataFrame
    Returns:
        DataFrame: Original data (time format unchanged)
    """
    if 'Year' in df.columns and not df['Year'].empty:
        min_year, max_year = df['Year'].min(), df['Year'].max()
        print(f"  ABS time format validation: year range {min_year}-{max_year} (integer format, unchanged)")
    
    return df
