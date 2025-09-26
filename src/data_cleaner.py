#!/usr/bin/env python3
"""
Unified Data Cleaning Module
Integrates all data cleaning, normalization, and quality repair functionality
"""

# Standard library imports
import re
from typing import Any, Dict, List, Optional, Set, Tuple

# Third-party library imports
import pandas as pd

# =============================================================================
# Constants Definition
# =============================================================================

# Month abbreviation to number mapping (general)
MONTH_ABBR_TO_NUM = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

# Missing value indicators (general)
MISSING_VALUE_INDICATORS = ['-', '', 'nan', 'NaN', 'none', 'None', 'NULL', 'null', 'N/A', 'n/a']

# =============================================================================
# General Helper Functions
# =============================================================================

def is_missing_value(value: Any) -> bool:
    """
    Check if a value is a missing value
    Args:
        value: Value to check
    Returns:
        True if it's a missing value, False otherwise
    """
    if pd.isna(value) or value is None:
        return True
    
    str_val = str(value).strip()
    return str_val in MISSING_VALUE_INDICATORS or str_val.lower() in [x.lower() for x in MISSING_VALUE_INDICATORS]

# =============================================================================
# Database Column Name Normalization Functions (originally db_column_normalizer.py)
# =============================================================================

def normalize_db_column_name(name: str, reserved_words: Set[str] = None) -> str:
    """
    Normalize database column names
    Args:
        name: Original column name
        reserved_words: Set of database reserved words
    Returns:
        Normalized column name
    """
    if not name or str(name).strip() == '':
        return 'unnamed_column'
    
    # Default PostgreSQL reserved words
    if reserved_words is None:
        reserved_words = {
            'user', 'order', 'group', 'select', 'from', 'where', 'insert', 'update', 
            'delete', 'create', 'drop', 'alter', 'table', 'index', 'view', 'database',
            'schema', 'primary', 'foreign', 'key', 'constraint', 'references', 'check',
            'unique', 'not', 'null', 'default', 'auto_increment', 'serial', 'boolean',
            'integer', 'varchar', 'text', 'date', 'time', 'timestamp', 'numeric',
            'real', 'double', 'precision', 'decimal', 'char', 'binary', 'blob'
        }
    
    # Step 1: Basic cleaning
    clean_name = str(name).strip()
    
    # Step 2: Convert to lowercase
    clean_name = clean_name.lower()
    
    # Step 3: Handle special characters and abbreviations
    # Common unit and abbreviation normalization
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
    
    # Step 4: Remove other special characters, keep alphanumeric and spaces
    clean_name = re.sub(r'[^\w\s]', '', clean_name)
    
    # Step 5: Convert spaces to underscores
    clean_name = re.sub(r'\s+', '_', clean_name)
    
    # Step 6: Merge multiple underscores into one
    clean_name = re.sub(r'_+', '_', clean_name)
    
    # Step 7: Remove leading and trailing underscores
    clean_name = clean_name.strip('_')
    
    # Step 8: Handle empty results
    if not clean_name:
        clean_name = 'unnamed_column'
    
    # Step 9: Add prefix if starts with digit
    if clean_name[0].isdigit():
        clean_name = f'col_{clean_name}'
    
    # Step 10: Check if it's a reserved word
    if clean_name.lower() in reserved_words:
        clean_name = f'{clean_name}_col'
    
    # Step 11: Length limit (PostgreSQL identifier limit is 63 characters)
    if len(clean_name) > 60:  # Leave 3 characters for possible suffix
        clean_name = clean_name[:60]
    
    return clean_name

def normalize_column_mapping(columns: List[str]) -> List[str]:
    """
    Return normalized column name list of equal length in input order (ensuring uniqueness)
    Args:
        columns: Original column name list
    Returns:
        Normalized column name list of equal length (position aligned), adds _1/_2 suffix for duplicates
    """
    normalized_list = []
    used_names = set()
    
    for original_col in columns:
        normalized = normalize_db_column_name(original_col)
        
        # Handle duplicate normalized names (based on occurrence order)
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
    Create table SQL with normalized column names
    Args:
        table_name: Table name
        column_definitions: Dictionary of {normalized column name: SQL type}
        primary_key: Primary key column name
        additional_constraints: Additional constraint conditions
    Returns:
        CREATE TABLE SQL statement
    """
    # Normalize table name
    normalized_table_name = normalize_db_column_name(table_name)
    
    # Build column definitions (ensure column name uniqueness)
    column_parts = []
    used_norm_cols = set()
    
    # Primary key
    pk_norm = normalize_db_column_name(primary_key)
    if primary_key not in column_definitions:
        column_parts.append(f"{pk_norm} SERIAL PRIMARY KEY")
        used_norm_cols.add(pk_norm)
    
    # Other columns
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
    
    # Additional constraints
    if additional_constraints:
        column_parts.extend(additional_constraints)
    
    sql = f"CREATE TABLE IF NOT EXISTS {normalized_table_name} (\n"
    sql += ",\n".join(f"    {part}" for part in column_parts)
    sql += "\n);"
    
    return sql

def get_standard_column_types() -> Dict[str, str]:
    """
    Get standard column type mapping
    Returns:
        Dictionary of {column name pattern: SQL type}
    """
    return {
        # Basic fields
        'id': 'SERIAL PRIMARY KEY',
        'name': 'TEXT',
        'code': 'TEXT',
        'label': 'TEXT',
        'state': 'TEXT',
        'postcode': 'TEXT',
        
        # Time fields
        'year': 'INTEGER',
        'month': 'INTEGER',
        'start_year': 'INTEGER',
        'stop_year': 'INTEGER',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        
        # Numeric fields
        'capacity_mw': 'NUMERIC',
        'production_gj': 'NUMERIC',
        'production_mwh': 'NUMERIC',
        'emissions_tco2e': 'NUMERIC',
        'intensity_tco2e_mwh': 'NUMERIC',
        'percent': 'NUMERIC',
        'dollar': 'NUMERIC',
        'count': 'INTEGER',
        'population': 'INTEGER',
        
        # Boolean fields
        'connected': 'BOOLEAN',
        'active': 'BOOLEAN',
        'enabled': 'BOOLEAN',
        
        # Geographic fields
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
        
        # Default text type
        'default': 'TEXT'
    }

def infer_column_type(column_name: str, sample_values: List = None) -> str:
    """
    Infer SQL type for a column
    Args:
        column_name: Normalized column name
        sample_values: Sample value list (optional)
    Returns:
        Inferred SQL type
    """
    standard_types = get_standard_column_types()
    
    # Exact match
    if column_name in standard_types:
        return standard_types[column_name]
    
    # Pattern matching
    col_lower = column_name.lower()
    
    # Numeric types
    if any(pattern in col_lower for pattern in ['capacity', 'mw', 'gj', 'mwh', 'tco2e', 'emissions', 'production']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['percent', 'rate', 'ratio', 'intensity']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['dollar', 'gdp', 'income', 'revenue', 'cost']):
        return 'NUMERIC'
    
    if any(pattern in col_lower for pattern in ['count', 'number', 'population', 'total']):
        return 'INTEGER'
    
    # Time types
    if any(pattern in col_lower for pattern in ['year', 'month', 'day']):
        return 'INTEGER'
    
    if any(pattern in col_lower for pattern in ['date', 'time']):
        return 'DATE'
    
    # Boolean types
    if any(pattern in col_lower for pattern in ['connected', 'active', 'enabled', 'grid']):
        return 'BOOLEAN'
    
    # Geographic types
    if any(pattern in col_lower for pattern in ['lat', 'lon', 'latitude', 'longitude', 'bbox']):
        return 'NUMERIC'
    
    # Default text type
    return 'TEXT'

def print_column_mapping_report(original_columns: List[str], normalized_columns: List[str]):
    """
    Print column name normalization report (based on equal-length original/normalized column name lists)
    Args:
        original_columns: Original column name list
        normalized_columns: Position-aligned normalized column name list
    """
    print(f"📋 Column name normalization report: {len(original_columns)} columns")
    
    changes = []
    unchanged = []
    
    for orig_col, norm_col in zip(original_columns, normalized_columns):
        if orig_col != norm_col:
            changes.append((orig_col, norm_col))
        else:
            unchanged.append(orig_col)
    
    if changes:
        print(f"  ✓ {len(changes)} column names normalized:")
        for orig, norm in changes[:10]:  # Only show first 10
            print(f"    - {orig} → {norm}")
        if len(changes) > 10:
            print(f"    ... {len(changes) - 10} more column name changes")
    
    if unchanged:
        print(f"  ✓ {len(unchanged)} column names unchanged")
    
    print()

# =============================================================================
# ABS Data Cleaning Functions (originally abs_data_cleaner.py)
# =============================================================================

def detect_numeric_columns(df: pd.DataFrame, start_col: int = 3) -> Dict[str, str]:
    """
    Detect numeric column types and return column name to type mapping
    Args:
        df: DataFrame
        start_col: Starting column index for detection (usually Code/Label/Year columns are before this)
    Returns:
        dict: {column name: data type} where types are 'integer', 'float', 'percentage', 'currency', 'text'
    """
    column_types = {}
    
    for col in df.columns[start_col:]:
        if col in ['standardized_state', 'lga_code_clean', 'lga_name_clean']:
            continue
            
        # Sample first 100 non-null values for type determination
        sample_values = df[col].dropna().head(100)
        if sample_values.empty:
            column_types[col] = 'text'
            continue
            
        # Convert to string and clean
        str_values = [str(v).strip() for v in sample_values if not is_missing_value(v)]
        if not str_values:
            column_types[col] = 'text'
            continue
            
        # Analyze patterns
        numeric_count = 0
        percentage_count = 0
        currency_count = 0
        
        for val in str_values[:50]:  # Only check first 50 valid values
            # Percentage detection
            if '%' in val or 'percent' in val.lower():
                percentage_count += 1
                continue
                
            # Currency detection
            if any(symbol in val for symbol in ['$', '€', '£', '¥', 'AUD', 'USD']):
                currency_count += 1
                continue
                
            # Numeric detection (including thousand separators)
            clean_val = re.sub(r'[,\s]', '', val)  # Remove commas and spaces
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
        
        # Type determination
        if percentage_count > total_checked * 0.3:
            column_types[col] = 'percentage'
        elif currency_count > total_checked * 0.3:
            column_types[col] = 'currency'
        elif numeric_ratio > 0.7:
            # Further determine if integer or float
            has_decimal = any('.' in re.sub(r'[,\s]', '', str(v)) for v in str_values[:20] if str(v).strip())
            column_types[col] = 'float' if has_decimal else 'integer'
        else:
            column_types[col] = 'text'
    
    return column_types

def clean_numeric_value(value: Any, target_type: str = 'float') -> Optional[float]:
    """
    General numeric value cleaning and conversion function
    Args:
        value: Original value
        target_type: Target type ('integer', 'float', 'percentage', 'currency', 'capacity') 
    Returns:
        Converted numeric value or None
    """
    if pd.isna(value):
        return None
        
    if is_missing_value(value):
        return None
    
    str_val = str(value).strip()
    
    try:
        # Handle percentage
        if target_type == 'percentage':
            # Remove % symbol, convert to decimal
            clean_val = re.sub(r'[%\s]', '', str_val)
            clean_val = re.sub(r'[,]', '', clean_val)  # Remove thousand separators
            return float(clean_val) / 100.0
        
        # Handle currency
        elif target_type == 'currency':
            # Remove currency symbols and thousand separators
            clean_val = re.sub(r'[$€£¥,\s]|AUD|USD|EUR|GBP', '', str_val, flags=re.IGNORECASE)
            return float(clean_val)
        
        # Handle capacity (remove unit identifiers)
        elif target_type == 'capacity':
            # Remove thousand separators, spaces and unit identifiers (like MW, mw, etc.)
            clean_val = re.sub(r'[,\s]', '', str_val)
            clean_val = re.sub(r'[a-zA-Z]+', '', clean_val)
            return float(clean_val)
        
        # Handle regular numeric values
        else:
            # Remove thousand separators and extra spaces
            clean_val = re.sub(r'[,\s]', '', str_val)
            if target_type == 'integer':
                return float(int(float(clean_val)))  # Convert to float first then int to avoid decimal issues
            else:  # float
                return float(clean_val)
                
    except (ValueError, TypeError):
        return None

def process_data_with_numeric_cleaning(df: pd.DataFrame, data_type: str = 'abs') -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    General data processing function including numeric conversion
    Args:
        df: Original DataFrame
        data_type: Data type ('abs', 'cer', 'nger')
    Returns:
        (Processed DataFrame, column type mapping)
    """
    print(f"  🔍 Detecting {data_type.upper()} numeric column types...")
    
    # 1. Detect numeric column types
    column_types = detect_numeric_columns(df)
    numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
    
    if numeric_cols:
        print(f"  ✓ Detected {len(numeric_cols)} numeric columns: {list(numeric_cols.keys())}")
        for col, col_type in list(numeric_cols.items())[:5]:
            print(f"    - {col}: {col_type}")
        if len(numeric_cols) > 5:
            print(f"    ... {len(numeric_cols)-5} more numeric columns")
    
    # 2. Convert numeric columns
    print("  🔢 Converting numeric columns...")
    df_processed = df.copy()
    converted_count = 0
    for col, col_type in column_types.items():
        if col_type != 'text' and col in df_processed.columns:
            numeric_col = f"{col}_numeric"
            df_processed[numeric_col] = df_processed[col].apply(
                lambda x: clean_numeric_value(x, col_type)
            )
            
            success_count = df_processed[numeric_col].notna().sum()
            if success_count > 0:
                converted_count += success_count
                df_processed[col] = df_processed[numeric_col]
                df_processed.drop(columns=[numeric_col], inplace=True)
    
    if converted_count > 0:
        print(f"  ✓ Numeric conversion completed: {converted_count} values successfully converted")
    else:
        print("  ⚠️ No values found that need conversion")
    
    return df_processed, column_types

def process_abs_data_with_cleaning(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Convenience function for processing ABS data
    Args:
        df: Original DataFrame
    Returns:
        (Processed DataFrame, column type mapping)
    """
    return process_data_with_numeric_cleaning(df, 'abs')

# =============================================================================
# General Helper Functions
# =============================================================================

def standardize_fuel_type(value: Any) -> Optional[str]:
    """
    General fuel type standardization function
    Args:
        value: Original fuel type value
    Returns:
        Standardized fuel type name
    """
    if is_missing_value(value):
        return None
    
    val_str = str(value).strip().lower()
    
    # Unified fuel type mapping
    fuel_mapping = {
        # Renewable energy
        'solar': 'Solar',
        'wind': 'Wind',
        'hydro': 'Hydro',
        'biomass': 'Biomass',
        'biofuel': 'Biofuel',
        'bagasse': 'Bagasse',
        'wood': 'Biomass',
        
        # Fossil fuels
        'coal': 'Coal',
        'black coal': 'Black Coal',
        'brown coal': 'Brown Coal',
        'gas': 'Natural Gas',
        'natural gas': 'Natural Gas',
        'diesel': 'Diesel',
        
        # Special fuels
        'coal seam methane': 'Coal Seam Gas',
        'coal seam gas': 'Coal Seam Gas',
        'waste coal mine gas': 'Coal Mine Gas',
        'coal mine gas': 'Coal Mine Gas',
        'landfill gas': 'Landfill Gas',
        
        # Energy storage
        'battery': 'Battery Storage',
        'battery storage': 'Battery Storage',
    }
    
    # Direct match
    if val_str in fuel_mapping:
        return fuel_mapping[val_str]
    
    # Partial match
    for key, value in fuel_mapping.items():
        if key in val_str:
            return value
    
    # Default return title case format
    return str(value).strip().title()

def clean_facility_name(value: Any, name_type: str = 'facility') -> Optional[str]:
    """
    General facility/power station name cleaning function
    Args:
        value: Original name value
        name_type: Name type ('facility', 'station', 'project')
    Returns:
        Cleaned name
    """
    if is_missing_value(value):
        return None
    
    name = str(value).strip()
    
    # Skip special summary rows
    if name.lower() in ['corporate total', 'facility', 'total', 'summary']:
        return name
    
    if name_type == 'station':
        # CER power station name special handling: remove redundant descriptive suffixes
        patterns_to_remove = [
            r'\s*-\s*(Solar|Wind|Gas|Hydro|Battery)\s*(w\s*SGU)?\s*-\s*[A-Z]{2,3}$',
            r'\s*-\s*(Solar|Wind|Gas|Hydro|Battery)$',
            r'\s*w\s*SGU\s*$',
            r'\s*wSGU\s*$'
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # General cleaning
    # Standardize separators
    name = re.sub(r'\s*-\s*', ' - ', name)
    name = re.sub(r'\s*,\s*', ', ', name)
    
    # Standardize bracket format
    name = re.sub(r'\s*\(\s*([^)]+)\s*\)\s*', r' (\1)', name)
    
    # Clean extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

# =============================================================================
# CER Data Cleaning Functions (originally cer_data_cleaner.py)
# =============================================================================

def normalize_cer_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize CER data column names
    Args:
        df: Original DataFrame
    Returns:
        DataFrame with normalized column names
    """
    df_normalized = df.copy()
    
    # Column name mapping rules
    column_mappings = {
        # Basic information columns
        'accreditation code': 'accreditation_code',
        'power station name': 'power_station_name',
        'project name': 'project_name',
        'state ': 'state',  # Handle trailing spaces
        'state': 'state',
        'postcode': 'postcode',
        
        # Capacity related
        'installed capacity (mw)': 'installed_capacity_mw',
        'mw capacity': 'mw_capacity',
        
        # Fuel type
        'fuel source (s)': 'fuel_source',
        'fuel source(s)': 'fuel_source',  # Handle no-space variant
        'fuel sources': 'fuel_source',    # Plural variant seen on some pages
        'fuel source': 'fuel_source',
        
        # Date related
        'accreditation start date': 'accreditation_start_date',
        'approval date': 'approval_date',
        'committed date (month/year)': 'committed_date',
    }
    
    # Create new column name mapping
    new_columns = {}
    for col in df.columns:
        # Clean column name: remove leading/trailing spaces, convert to lowercase
        clean_col = str(col).strip().lower()
        
        # Find mapping
        if clean_col in column_mappings:
            new_columns[col] = column_mappings[clean_col]
        else:
            # Default normalization: spaces to underscores, remove special characters
            normalized = re.sub(r'[^\w\s]', '', clean_col)  # Remove special characters
            normalized = re.sub(r'\s+', '_', normalized)    # Spaces to underscores
            normalized = re.sub(r'_+', '_', normalized)     # Merge multiple underscores
            normalized = normalized.strip('_')              # Remove leading/trailing underscores
            new_columns[col] = normalized
    
    # Rename columns
    df_normalized = df_normalized.rename(columns=new_columns)
    
    print(f"  ✓ CER column name normalization completed: {len(new_columns)} columns")
    
    # Show major column name changes
    important_changes = [f"{old_col} → {new_col}" for old_col, new_col in new_columns.items() 
                        if old_col != new_col and any(key in old_col.lower() for key in ['state', 'power', 'capacity', 'fuel'])]
    
    if important_changes:
        print("  Major column name changes:")
        for change in important_changes[:5]:
            print(f"    - {change}")
    
    return df_normalized

def process_cer_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process CER data time columns, split MMM-YYYY format into year and month columns
    Args:
        df: DataFrame
    Returns:
        DataFrame with added year and month columns
    """
    df_processed = df.copy()
    
    # Use general month mapping
    month_abbr_to_num = MONTH_ABBR_TO_NUM
    
    # Find columns containing dates
    date_columns = [col for col in df_processed.columns 
                   if any(keyword in col.lower() for keyword in ['date', 'committed'])]
    
    processed_count = 0
    
    for date_col in date_columns:
        # Create year and month columns
        year_col = f"{date_col}_year"
        month_col = f"{date_col}_month"
        
        def parse_mmm_yyyy(date_str):
            """Parse MMM-YYYY format dates"""
            if pd.isna(date_str) or not date_str:
                return None, None
            
            try:
                date_str = str(date_str).strip().lower()
                if '-' in date_str:
                    parts = date_str.split('-')
                    if len(parts) == 2:
                        month_abbr = parts[0].strip()
                        year_str = parts[1].strip()
                        
                        # Convert month
                        month_num = month_abbr_to_num.get(month_abbr)
                        if month_num is None:
                            return None, None
                        
                        # Convert year
                        try:
                            year_num = int(year_str)
                            return year_num, month_num
                        except ValueError:
                            return None, None
                return None, None
            except Exception:
                return None, None
        
        # Apply parsing function
        parsed_dates = df_processed[date_col].apply(parse_mmm_yyyy)
        
        # Split years and months
        years = [item[0] if item[0] is not None else None for item in parsed_dates]
        months = [item[1] if item[1] is not None else None for item in parsed_dates]
        
        df_processed[year_col] = years
        df_processed[month_col] = months
        
        # Count successful conversions
        success_count = sum(1 for y, m in zip(years, months) if y is not None and m is not None)
        if success_count > 0:
            processed_count += success_count
            print(f"  ✓ Time column processing: {date_col} → {year_col}, {month_col} ({success_count} records)")
    
    if processed_count == 0:
        print("  ⚠️ No time columns found that need processing")
    else:
        print(f"  ✓ CER time processing completed: {processed_count} time records processed")
    
    return df_processed

def convert_cer_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert numeric columns in CER data
    Args:
        df: DataFrame
    Returns:
        DataFrame with converted numeric columns
    """
    df_processed = df.copy()
    
    # Identify numeric columns that need conversion (capacity related)
    capacity_columns = [col for col in df_processed.columns 
                       if any(keyword in col.lower() for keyword in ['capacity', 'mw', 'installed'])]
    
    if not capacity_columns:
        print("  ⚠️ No capacity columns found that need conversion")
        return df_processed
    
    converted_count = 0
    
    for col in capacity_columns:
        # Convert numeric values (using general function)
        original_values = df_processed[col].copy()
        df_processed[col] = df_processed[col].apply(lambda x: clean_numeric_value(x, 'capacity'))
        
        # Count successful conversions
        success_count = df_processed[col].notna().sum()
        original_count = original_values.notna().sum()
        
        if success_count > 0:
            converted_count += success_count
            print(f"  ✓ Numeric conversion: {col} ({success_count}/{original_count} records)")
    
    if converted_count > 0:
        print(f"  ✓ CER numeric conversion completed: {converted_count} values converted")
    else:
        print("  ⚠️ No values successfully converted")
    
    return df_processed

def process_cer_data_with_cleaning(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    Process CER data including column name normalization, time processing and numeric conversion
    Args:
        df: Original DataFrame
        table_type: Table type (e.g., "approved_power_stations")
    Returns:
        Processed DataFrame
    """
    print(f"  🧹 Starting CER data cleaning: {table_type}")
    
    # 1. Column name normalization
    print("  📝 Normalizing column names...")
    df_processed = normalize_cer_column_names(df)
    
    # 2. Time processing
    print("  🕐 Processing time columns...")
    df_processed = process_cer_time_columns(df_processed)
    
    # 3. Numeric conversion
    print("  🔢 Converting numeric columns...")
    df_processed = convert_cer_numeric_columns(df_processed)
    
    print(f"  ✓ CER data cleaning completed: {table_type} ({df_processed.shape})")
    
    return df_processed

# =============================================================================
# Data Quality Repair Functions (originally data_quality_fixer.py)
# =============================================================================

def fix_missing_values(df: pd.DataFrame, missing_indicators: List[str] = None) -> pd.DataFrame:
    """
    Unified handling of missing value indicators
    Args:
        df: Original DataFrame
        missing_indicators: List of missing value indicators
    Returns:
        Repaired DataFrame
    """
    if missing_indicators is None:
        missing_indicators = MISSING_VALUE_INDICATORS
    
    df_fixed = df.copy()
    
    # Count repair statistics
    fix_count = 0
    
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':  # Only process text columns
            for indicator in missing_indicators:
                # Exact match missing value indicators
                mask = df_fixed[col].astype(str).str.strip() == indicator
                count = mask.sum()
                if count > 0:
                    df_fixed.loc[mask, col] = None
                    fix_count += count
    
    print(f"  ✓ Missing value repair: {fix_count} missing value indicators repaired")
    return df_fixed

def parse_date_flexible(date_str: str) -> Optional[Tuple[int, int, int]]:
    """
    Flexible parsing of multiple date formats
    Args:
        date_str: Date string
    Returns:
        (year, month, day) tuple, returns None if parsing fails
    """
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    
    # Use general month mapping
    month_abbr = MONTH_ABBR_TO_NUM
    
    try:
        # Format 1: DD/MM/YYYY
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
        
        # Format 2: MMM-YYYY
        elif '-' in date_str and len(date_str.split('-')) == 2:
            parts = date_str.split('-')
            month_str, year_str = parts[0].strip().lower(), parts[1].strip()
            if month_str in month_abbr:
                year, month = int(year_str), month_abbr[month_str]
                return (year, month, 1)  # Default to beginning of month
        
        # Format 3: YYYY-MM-DD
        elif '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # Year first
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
        
        # Format 4: DD-MM-YYYY
        elif '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            if len(parts[2]) == 4:  # Year last
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                return (year, month, day)
    
    except (ValueError, IndexError):
        pass
    
    return None

def fix_date_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix multiple date formats
    Args:
        df: Data DataFrame
    Returns:
        Repaired DataFrame
    """
    df_fixed = df.copy()
    
    print("  📅 Fixing date formats...")
    
    # Find all date columns
    date_columns = [col for col in df_fixed.columns if 'date' in col.lower()]
    
    total_fixed = 0
    
    for date_col in date_columns:
        if date_col not in df_fixed.columns:
            continue
            
        print(f"    Processing date column: {date_col}")
        
        # Create standardized date columns
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
                
                # Generate ISO format date
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
        
        # Add new columns
        df_fixed[year_col] = years
        df_fixed[month_col] = months
        df_fixed[day_col] = days
        df_fixed[iso_col] = iso_dates
        
        total_fixed += success_count
        print(f"      ✓ Successfully parsed {success_count}/{len(df_fixed)} dates")
    
    if total_fixed > 0:
        print(f"  ✓ Date format repair completed: {total_fixed} date values repaired")
    else:
        print(f"  ⚠️ No date columns found that need repair")
    
    return df_fixed

def fix_specific_data_issues(df: pd.DataFrame, data_type: str, **kwargs) -> pd.DataFrame:
    """
    Fix specific issues for particular data types
    Args:
        df: Data DataFrame
        data_type: Data type ('nger', 'cer')
        **kwargs: Additional parameters
    Returns:
        Repaired DataFrame
    """
    df_fixed = df.copy()
    print(f"  🔧 Fixing {data_type.upper()} specific issues...")
    
    # 1. Unified missing value handling
    df_fixed = fix_missing_values(df_fixed)
    
    if data_type.lower() == 'nger':
        # Standardize boolean fields
        if 'gridconnected' in df_fixed.columns:
            def standardize_grid_connected(value):
                if pd.isna(value) or value is None:
                    return None
                val_str = str(value).strip().lower()
                if val_str in ['on', 'connected', 'yes', 'true', '1']:
                    return 'Connected'
                elif val_str in ['off', 'disconnected', 'no', 'false', '0']:
                    return 'Disconnected'
                return None
            
            original_count = df_fixed['gridconnected'].notna().sum()
            df_fixed['gridconnected'] = df_fixed['gridconnected'].apply(standardize_grid_connected)
            fixed_count = df_fixed['gridconnected'].notna().sum()
            print(f"    - gridconnected field standardization: {original_count} → {fixed_count}")
        
        # Standardize fuel types and facility names
        if 'primaryfuel' in df_fixed.columns:
            df_fixed['primaryfuel'] = df_fixed['primaryfuel'].apply(standardize_fuel_type)
            print(f"    - primaryfuel field standardization completed")
        
        if 'facilityname' in df_fixed.columns:
            df_fixed['facilityname'] = df_fixed['facilityname'].apply(lambda x: clean_facility_name(x, 'facility'))
            print(f"    - facilityname field cleaning completed")
    
    elif data_type.lower() == 'cer':
        table_type = kwargs.get('table_type', 'unknown')
        
        # Skip adding *_fixed and *_iso date columns for CER as requested
        
        # Clean power station/project names
        name_columns = [col for col in ['power_station_name', 'Power station name', 'project_name', 'Project Name'] 
                       if col in df_fixed.columns]
        
        for name_col in name_columns:
            df_fixed[name_col] = df_fixed[name_col].apply(lambda x: clean_facility_name(x, 'station'))
            print(f"    - {name_col} field cleaning completed")
        
        # Standardize fuel types
        fuel_columns = [col for col in ['fuel_source', 'Fuel Source', 'Fuel Source (s)'] 
                       if col in df_fixed.columns]
        
        for fuel_col in fuel_columns:
            df_fixed[fuel_col] = df_fixed[fuel_col].apply(standardize_fuel_type)
            print(f"    - {fuel_col} field standardization completed")
    
    return df_fixed

def process_data_quality_fixes(df: pd.DataFrame, data_type: str, **kwargs) -> pd.DataFrame:
    """
    Unified data quality repair entry function
    Args:
        df: Original DataFrame
        data_type: Data type ('nger', 'cer', 'abs')
        **kwargs: Additional parameters
    Returns:
        Repaired DataFrame
    """
    print(f"  🔧 Starting data quality repair: {data_type.upper()}")
    
    if data_type.lower() in ['nger', 'cer']:
        return fix_specific_data_issues(df, data_type, **kwargs)
    else:
        # ABS data or other types only handle missing values
        return fix_missing_values(df)