#!/usr/bin/env python3
"""Database configuration and operations"""

# Standard library imports
import threading
from datetime import datetime
from typing import List

# Third-party library imports
import pandas as pd
import psycopg2
import psycopg2.pool
from psycopg2.extras import execute_values

# Local module imports
from data_cleaner import *
from excel_utils import get_merged_cells, read_merged_headers
from state_standardizer import standardize_dataframe_states, standardize_state_name


DB_CONFIG = {
    'host': 'localhost', 'port': 5432, 'user': 'postgres', 
    'password': 'postgre', 'database': 'postgres'
}

# Global connection pool
_connection_pool = None
_pool_lock = threading.Lock()

# Connection tracking (for debugging)
_active_connections = set()
_connections_lock = threading.Lock()

# Schema migration flags (avoid repeating expensive ALTERs per process lifetime)
_nger_schema_migrated = False
# Note: CER schema migration is handled during table creation; no per-insert cache needed

# Shared geocoding-related column definitions to avoid duplication
GEOCODE_FIELDS = {
    'lat': 'NUMERIC',
    'lon': 'NUMERIC',
    'formatted_address': 'VARCHAR',
    'place_id': 'VARCHAR',
    'postcode': 'VARCHAR',
    'bbox_south': 'NUMERIC',
    'bbox_north': 'NUMERIC',
    'bbox_west': 'NUMERIC',
    'bbox_east': 'NUMERIC'
}

def get_connection_pool(minconn=1, maxconn=10):
    """Get database connection pool (singleton pattern)"""
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
                    print(f"PostgreSQL connection pool created successfully: {minconn}-{maxconn} connections")
                    # Try to enable PostGIS extension (will be ignored if already enabled)
                    try:
                        _conn = _connection_pool.getconn()
                        if _conn:
                            with _conn.cursor() as _cur:
                                _cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                                _conn.commit()
                                print("PostGIS extension enabled or already exists")
                        if _conn:
                            _connection_pool.putconn(_conn)
                    except Exception as ee:
                        print(f"Warning: Failed to enable PostGIS extension: {ee}")
                except Exception as e:
                    print(f"PostgreSQL connection pool creation failed: {e}")
                    return None
    
    return _connection_pool

def test_connection(conn):
    """Test if connection is valid"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except:
        return False

def track_connection(conn):
    """Track connection"""
    with _connections_lock:
        _active_connections.add(id(conn))

def get_db_connection():
    """Get database connection (from connection pool)"""
    pool = get_connection_pool()
    if not pool:
        return None
    
    try:
        conn = pool.getconn()
        if not conn:
            return None
            
        # Test if connection is valid
        if test_connection(conn):
            track_connection(conn)
            return conn
        
        # Connection invalid, try to get a new one
        print("Connection test failed, trying to get a new one")
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
        print(f"Failed to get connection from pool: {e}")
        return None

def return_db_connection(conn):
    """Return database connection to connection pool"""
    if not conn:
        return
    
    # Check if connection is tracked (prevent returning connections not from pool)
    conn_id = id(conn)
    with _connections_lock:
        if conn_id not in _active_connections:
            print("Attempting to return untracked connection, closing directly")
            safe_close_connection(conn)
            return
        _active_connections.discard(conn_id)
    
    if not _connection_pool:
        print("Connection pool does not exist, cannot return connection")
        safe_close_connection(conn)
        return
    
    try:
        # Check if connection is still valid
        if not test_connection(conn):
            print("Connection has expired, closing directly")
            safe_close_connection(conn)
            return
        
        # Return connection to pool
        _connection_pool.putconn(conn)
    except Exception as e:
        print(f"Failed to return connection to pool: {e}")
        safe_close_connection(conn)

def safe_close_connection(conn):
    """Safely close connection"""
    try:
        conn.close()
    except:
        pass


def close_connection_pool():
    """Close connection pool"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        
        # Clear connection tracking
        with _connections_lock:
            _active_connections.clear()
        
        print("Database connection pool closed")


def table_exists(cursor, table_name: str) -> bool:
    """Check if table exists"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]

def column_exists(cursor, table_name: str, column_name: str) -> bool:
    """Check if column exists"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s 
            AND column_name = %s
        );
    """, (table_name, column_name))
    return cursor.fetchone()[0]

def add_column_if_not_exists(cursor, table_name: str, column_name: str, column_type: str) -> bool:
    """Add column if it doesn't exist"""
    try:
        if not column_exists(cursor, table_name, column_name):
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            print(f"  Added column: {table_name}.{column_name} ({column_type})")
            return True
        return False
    except Exception as e:
        print(f"  Warning: Failed to add column: {table_name}.{column_name} - {e}")
        return False

def is_valid_value(value) -> bool:
    """Check if value is valid (not null, not NaN, etc.)"""
    if value is None or pd.isna(value):
        return False
    str_val = str(value).strip()
    return str_val != '' and str_val.lower() not in {'nan', 'none', 'null', '-'}

def clean_value(value, max_length: int = None) -> str:
    """Clean value, return None or cleaned string"""
    if not is_valid_value(value):
        return None
    str_val = str(value).strip()
    if max_length and len(str_val) > max_length:
        return str_val[:max_length]
    return str_val

def create_table_safe(cursor, table_name: str, create_sql: str) -> bool:
    """Safely create table"""
    try:
        if not table_exists(cursor, table_name):
            cursor.execute(create_sql)
            print(f"Table created successfully: {table_name}")
            return True
        else:
            print(f"Table already exists: {table_name}")
            return True
    except Exception as e:
        print(f"Table creation failed: {table_name} - {e}")
        return False

def create_nger_table_impl(cursor):
    """Create NGER table implementation (using normalized column names)"""
    
    # Define NGER table column structure
    column_definitions = {
        'year_label': 'VARCHAR',
        'start_year': 'INTEGER', 
        'stop_year': 'INTEGER',
        'facility_name': 'VARCHAR',
        'state': 'VARCHAR',
        'facility_type': 'VARCHAR',
        'primary_fuel': 'VARCHAR',
        'reporting_entity': 'VARCHAR',
        'electricity_production_gj': 'NUMERIC',
        'electricity_production_mwh': 'NUMERIC',
        'emission_intensity_tco2e_mwh': 'NUMERIC',
        'scope1_emissions_tco2e': 'NUMERIC',
        'scope2_emissions_tco2e': 'NUMERIC',
        'total_emissions_tco2e': 'NUMERIC',
        'grid_info': 'VARCHAR',
        'grid_connected': 'BOOLEAN',
        'important_notes': 'TEXT',
        'lat': 'NUMERIC',
        'lon': 'NUMERIC',
        'formatted_address': 'VARCHAR',
        'place_id': 'VARCHAR',
        'postcode': 'VARCHAR',
        'bbox_south': 'NUMERIC',
        'bbox_north': 'NUMERIC',
        'bbox_west': 'NUMERIC',
        'bbox_east': 'NUMERIC'
    }
    
    create_sql = create_table_sql_with_normalized_columns('nger_unified', column_definitions)
    ok = create_table_safe(cursor, 'nger_unified', create_sql)
    if not ok:
        return False
    
    print(f"NGER table creation completed (normalized column names)")
    
    # Perform in-place migration for existing tables (types and dropped columns)
    try:
        migrate_nger_unified_schema(cursor)
    except Exception as e:
        print(f"Warning: NGER schema migration step failed: {e}")
    
    # Ensure geometry column
    try:
        ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
    except Exception as e:
        print(f"Warning: NGER geometry column processing failed: {e}")
    return True

def create_nger_table(conn) -> bool:
    """Create NGER table (called in single thread)"""
    try:
        cursor = conn.cursor()
        try:
            result = create_nger_table_impl(cursor)
            if result is not False:
                conn.commit()
            return result
        finally:
            try:
                cursor.close()
            except Exception:
                pass
    except Exception as e:
        print(f"NGER table creation failed: {e}")
        conn.rollback()
        return False

def create_cer_tables_impl(cursor):
    """Create CER tables implementation (using normalized column names)"""
    
    cer_table_types = ['approved_power_stations', 'committed_power_stations', 'probable_power_stations']
    
    for table_type in cer_table_types:
        # Basic column structure
        column_definitions = {
            'accreditation_code': 'VARCHAR',
            'power_station_name': 'VARCHAR',
            'project_name': 'VARCHAR', 
            'state': 'VARCHAR',
            'postcode': 'VARCHAR',
            'installed_capacity_mw': 'NUMERIC',
            'mw_capacity': 'NUMERIC',
            'fuel_source': 'VARCHAR',
            'accreditation_start_date': 'DATE',
            'approval_date': 'DATE',
            'committed_date': 'VARCHAR',
            'committed_date_year': 'INTEGER',
            'committed_date_month': 'INTEGER',
            'accreditation_start_date_year': 'INTEGER',
            'accreditation_start_date_month': 'INTEGER',
            'approval_date_year': 'INTEGER',
            'approval_date_month': 'INTEGER',
            # Geocoding fields
            'lat': 'NUMERIC',
            'lon': 'NUMERIC',
            'formatted_address': 'VARCHAR',
            'place_id': 'VARCHAR',
            'bbox_south': 'NUMERIC',
            'bbox_north': 'NUMERIC',
            'bbox_west': 'NUMERIC',
            'bbox_east': 'NUMERIC'
        }

        # Remove unwanted columns specifically for approved table
        if table_type == 'approved_power_stations':
            for k in [
                'project_name', 'installed_capacity_mw', 'mw_capacity', 'committed_date',
                'committed_date_year', 'committed_date_month', 'accreditation_start_date_year',
                'accreditation_start_date_month', 'approval_date_year', 'approval_date_month',
                # Per requirement: drop these three columns from approved table
                'fuel_source', 'accreditation_start_date', 'approval_date'
            ]:
                if k in column_definitions:
                    column_definitions.pop(k)
		# Remove specified columns for committed table per requirement
        if table_type == 'committed_power_stations':
            for k in [
                'accreditation_code', 'power_station_name', 'installed_capacity_mw',
                'accreditation_start_date', 'approval_date',
                'accreditation_start_date_year', 'accreditation_start_date_month',
                'approval_date_year', 'approval_date_month'
            ]:
                if k in column_definitions:
                    column_definitions.pop(k)
                    
        # Remove specified columns for probable table per requirement
        if table_type == 'probable_power_stations':
            for k in [
                'accreditation_code', 'power_station_name', 'installed_capacity_mw',
                'accreditation_start_date', 'approval_date',
                'accreditation_start_date_year', 'accreditation_start_date_month',
                'committed_date', 'committed_date_year', 'committed_date_month',
                'approval_date_year', 'approval_date_month'
            ]:
                if k in column_definitions:
                    column_definitions.pop(k)
        
        normalized_table_name = normalize_db_column_name(f"cer_{table_type}")
        create_sql = create_table_sql_with_normalized_columns(normalized_table_name, column_definitions)
        
        if not create_table_safe(cursor, normalized_table_name, create_sql):
            return False
        
        print(f"CER table creation completed (normalized column names): {normalized_table_name}")

    # After ensuring tables exist, migrate schema for approved/committed tables to enforce VARCHAR and drop unwanted columns
    try:
        migrate_cer_approved_schema(cursor)
        migrate_cer_committed_schema(cursor)
        migrate_cer_probable_schema(cursor)
        drop_unwanted_columns_for_cer_approved(cursor)
        drop_specified_columns_for_cer_committed(cursor)
        drop_specified_columns_for_cer_probable(cursor)
    except Exception as e:
        print(f"Warning: CER schema migration/drop step failed: {e}")
    
    return True

def create_cer_tables(conn) -> bool:
    """Create CER tables (called in single thread)"""
    try:
        # Enforce short transaction for DDL so later rollbacks won't drop created tables
        prev_autocommit = getattr(conn, 'autocommit', False)
        try:
            # If we're currently inside a transaction, roll it back before toggling autocommit
            try:
                from psycopg2 import extensions as _pgext
                if hasattr(conn, 'get_transaction_status'):
                    status = conn.get_transaction_status()
                    if status in (_pgext.TRANSACTION_STATUS_INTRANS, _pgext.TRANSACTION_STATUS_INERROR):
                        conn.rollback()
            except Exception:
                # Best-effort rollback check
                try:
                    conn.rollback()
                except Exception:
                    pass

            conn.autocommit = True
            cursor = conn.cursor()
            try:
                result = create_cer_tables_impl(cursor)
                return result
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        finally:
            try:
                conn.autocommit = prev_autocommit
            except Exception:
                pass
    except Exception as e:
        print(f"CER table creation failed: {e}")
        return False


def create_all_abs_tables(conn, file_path: str) -> bool:
    """Pre-create all ABS tables (called in single thread)"""
    try:
        cursor = conn.cursor()
        
        # Geographic level definitions
        levels = {
            "Table 1": {"desc": "State Level", "level": 0},
            "Table 2": {"desc": "Local Government Level", "level": 1}
        }
        
        for sheet_name in ["Table 1", "Table 2"]:
            level_info = levels[sheet_name]
            print(f"Pre-creating ABS tables: {sheet_name}({level_info['desc']})...")
            
            try:
                merged_cells = get_merged_cells(file_path, sheet_name)
                df = read_merged_headers(file_path, sheet_name)
                print(f"Found {len(merged_cells)} merged cells requiring table creation")
                
                for cell in merged_cells:
                    start_col, end_col = cell['start_col'] - 1, cell['end_col']
                    # Safe column range (prevent out of bounds)
                    total_cols = len(df.columns)
                    if total_cols < 3:
                        print(f"✗Skip: Less than 3 columns, cannot build basic columns Code/Label/Year -> {cell['value']}")
                        continue
                    start_col_safe = max(0, min(start_col, total_cols))
                    end_col_safe = max(start_col_safe, min(end_col, total_cols))
                    if start_col_safe >= end_col_safe:
                        print(f"✗Skip: Invalid column range [{start_col},{end_col}) -> [{start_col_safe},{end_col_safe}) : {cell['value']}")
                        continue
                    # Only use columns within the merged cell range
                    selected_cols = list(df.columns[start_col_safe:end_col_safe])
                    
                    # Create table
                    try:
                        clean_table = normalize_db_column_name(cell['value'])
                        
                        # Create table using normalized column names and type detection
                        
                        # Detect column types using only the merged-range columns
                        subset_df = df.iloc[:, list(range(start_col_safe, end_col_safe))]
                        subset_df.columns = selected_cols
                        column_types = detect_numeric_columns(subset_df, start_col=0)
                        
                        # Create normalized column name list (same length and order as selected_cols)
                        normalized_cols = normalize_column_mapping(selected_cols)
                        
                        # Build column definitions dictionary from merged-range columns only
                        column_definitions = {}
                        
                        # Add data columns (only merged-range columns)
                        for col, normalized_col in zip(selected_cols, normalized_cols):
                            
                            # Set SQL type based on detected type
                            col_type = column_types.get(col, 'text')
                            if col_type in ['integer']:
                                sql_type = 'INTEGER'
                            elif col_type in ['float', 'percentage', 'currency']:
                                sql_type = 'NUMERIC'
                            else:
                                sql_type = 'TEXT'
                            
                            # Force ABS Code column to be INTEGER
                            if normalized_col == 'code':
                                column_definitions[normalized_col] = 'INTEGER'
                            else:
                                column_definitions[normalized_col] = sql_type
                        
                        # Create table (add prefix for ABS tables)
                        normalized_table_name = normalize_db_column_name(f"abs_{cell['value']}")  # Use unified function
                        create_sql = create_table_sql_with_normalized_columns(
                            normalized_table_name, 
                            column_definitions
                        )
                        
                        if not create_table_safe(cursor, normalized_table_name, create_sql):
                            print(f"ABS table creation failed: {cell['value']}")
                            return False
                        else:
                            # Report column name normalization and type detection results
                            print_column_mapping_report(selected_cols, normalized_cols)
                            numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
                            if numeric_cols:
                                print(f"  {cell['value']}: Detected {len(numeric_cols)} numeric columns")
                    except Exception as e:
                        print(f"ABS table creation failed: {cell['value']} - {e}")
                        return False
                
                print(f"ABS table pre-creation completed: {sheet_name} - {len(merged_cells)} tables")
                
            except Exception as e:
                print(f"ABS table pre-creation failed: {sheet_name} - {e}")
                return False
        
        # Commit transaction after successful creation to ensure tables actually exist
        conn.commit()
        return True
        
    except Exception as e:
        print(f"ABS table pre-creation failed: {e}")
        conn.rollback()
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass





def batch_insert(cursor, insert_sql: str, data: List[tuple], batch_size: int = 10000) -> None:
    """High-performance batch insert using execute_values."""
    if not data:
        return
    for i in range(0, len(data), batch_size):
        chunk = data[i:i + batch_size]
        # Transform INSERT ... VALUES (... placeholders ...) into INSERT ... VALUES %s
        try:
            values_pos = insert_sql.upper().rindex("VALUES")
            base_insert = insert_sql[:values_pos] + "VALUES %s"
        except ValueError:
            # Fallback: assume standard form and append VALUES %s
            base_insert = insert_sql.split("(")[0].strip() + " VALUES %s"
        execute_values(cursor, base_insert, chunk, page_size=min(1000, batch_size))

def prepare_insert_sql(table_name: str, columns: List[str]) -> str:
    """Prepare insert SQL statement"""
    placeholders = ', '.join(['%s'] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"


# Specialized functions
def save_nger_data(conn, year_label: str, df: pd.DataFrame) -> bool:
    """Save NGER data"""
    try:
        cursor = conn.cursor()
        
        # Ensure schema types and dropped columns are applied (once per process)
        global _nger_schema_migrated
        if not _nger_schema_migrated:
            try:
                migrate_nger_unified_schema(cursor)
                _nger_schema_migrated = True
            except Exception as e:
                print(f"  Warning: NGER schema migrate on insert failed: {e}")
        
        # Standardize state names
        print(f"  Standardizing NGER state names...")
        standardize_dataframe_states(df, 'state')
        
        # Use normalized column name mappings
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
        
        # Note: original->db column mapping no longer includes controlling_corporation
        column_name_mapping = {
            'facilityname': 'facility_name',
            'primaryfuel': 'primary_fuel',
            'reportingentity': 'reporting_entity'
        }
        
        # Ensure geocoding columns exist (even if table already exists)
        geocode_fields = GEOCODE_FIELDS

        for col_name, col_type in geocode_fields.items():
            add_column_if_not_exists(cursor, 'nger_unified', col_name, col_type)

        # Unified boolean parsing function (for grid_connected field)
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

        # Max length constraints for VARCHAR fields
        varchar_max_lengths = {
            'year_label': 32,
            'facilityname': 255,
            'state': 64,
            'primaryfuel': 128,
            'reportingentity': 255,
            'facility_type': 128,
            'grid_info': 128,
            'formatted_address': 255,
            'place_id': 128,
            'postcode': 16
        }

        data = []
        for _, row in df.iterrows():
            row_data = [clean_value(year_label, max_length=varchar_max_lengths['year_label'])]
            
            # Add time columns
            start_year = row.get('start_year') if 'start_year' in df.columns else None
            stop_year = row.get('stop_year') if 'stop_year' in df.columns else None
            row_data.append(start_year)
            row_data.append(stop_year)
            
            # Basic columns (using normalized column names)
            basic_columns = ['facilityname', 'state', 'primaryfuel', 'reportingentity']
            for col in basic_columns:
                value = row.get(col) if col in df.columns else None
                max_len = varchar_max_lengths.get(col)
                row_data.append(clean_value(value, max_length=max_len))
            
            # Mapping columns
            for target_col, source_cols in mappings.items():
                value = None
                for source_col in source_cols:
                    if source_col in df.columns:
                        val = row.get(source_col)
                        if is_valid_value(val):
                            if target_col == 'grid_connected':
                                value = _parse_bool(val)
                            elif target_col.endswith(('_gj', '_mwh', '_tco2e')):
                                try:
                                    value = float(str(val).replace(',', ''))
                                except:
                                    value = None
                            else:
                                # Truncate text-mapped VARCHAR targets
                                max_len = varchar_max_lengths.get(target_col, None)
                                value = clean_value(val, max_length=max_len)
                            break
                row_data.append(value)

            # Append geocoding column values
            for field in geocode_fields.keys():
                value = row.get(field)
                if field in ['lat', 'lon', 'bbox_south', 'bbox_north', 'bbox_west', 'bbox_east'] and is_valid_value(value):
                    try:
                        row_data.append(float(value))
                    except:
                        row_data.append(None)
                else:
                    row_data.append(clean_value(value, max_length=varchar_max_lengths.get(field)))
            data.append(tuple(row_data))
        
        # Use normalized column names
        cols = ['year_label', 'start_year', 'stop_year', 'facility_name', 'state', 'primary_fuel', 'reporting_entity',
                'facility_type', 'electricity_production_gj', 'electricity_production_mwh',
                'emission_intensity_tco2e_mwh', 'scope1_emissions_tco2e', 'scope2_emissions_tco2e',
                'total_emissions_tco2e', 'grid_info', 'grid_connected', 'important_notes'] + list(geocode_fields.keys())
        
        # Batch insert
        insert_sql = prepare_insert_sql('nger_unified', cols)
        batch_insert(cursor, insert_sql, data)
        
        # Generate/update geom column after insertion
        try:
            ensure_geometry_column_and_index(cursor, 'nger_unified', 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, 'nger_unified',
                                                'bbox_west', 'bbox_south', 'bbox_east', 'bbox_north')
        except Exception as e:
            print(f"  Warning: NGER geometry column update failed: {e}")

        conn.commit()
        print(f"  NGER data insertion successful: {len(data)} rows -> nger_unified table")
        return True
        
    except Exception as e:
        print(f"  NGER data insertion failed: {e}")
        conn.rollback()
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass

def migrate_nger_unified_schema(cursor) -> None:
    """Ensure nger_unified schema matches latest requirements: set VARCHAR types and drop obsolete columns."""
    try:
        # Change column types to VARCHAR
        varchar_columns = [
            'year_label', 'facility_name', 'state', 'facility_type', 'primary_fuel',
            'reporting_entity', 'grid_info', 'formatted_address', 'place_id', 'postcode'
        ]
        for col in varchar_columns:
            try:
                cursor.execute(f"ALTER TABLE nger_unified ALTER COLUMN {col} TYPE VARCHAR")
            except Exception as e:
                # Ignore errors where column doesn't exist yet or already of desired type
                pass
        # Drop obsolete column if exists
        try:
            cursor.execute("ALTER TABLE nger_unified DROP COLUMN IF EXISTS controlling_corporation")
            print("  Dropped column: nger_unified.controlling_corporation (if existed)")
        except Exception:
            pass
    except Exception as e:
        # Surface minimal warning; do not fail caller
        print(f"  Warning: migrate_nger_unified_schema encountered an error: {e}")

def migrate_cer_approved_schema(cursor) -> None:
    """Ensure cer_approved_power_stations schema has VARCHAR types for key text columns."""
    try:
        table_name = 'cer_approved_power_stations'
        varchar_columns = [
            'accreditation_code', 'power_station_name', 'state', 'postcode', 'formatted_address', 'place_id'
        ]
        for col in varchar_columns:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE VARCHAR")
            except Exception:
                # Ignore if column doesn't exist yet or type is already VARCHAR-compatible
                pass
        # These columns are being dropped for approved table; no type/cleanup needed
    except Exception as e:
        print(f"  Warning: migrate_cer_approved_schema encountered an error: {e}")

def drop_unwanted_columns_for_cer_approved(cursor) -> None:
    """Drop specified columns from cer_approved_power_stations if they exist."""
    try:
        table = 'cer_approved_power_stations'
        cols = [
            'project_name', 'installed_capacity_mw', 'mw_capacity', 'committed_date',
            'committed_date_year', 'committed_date_month', 'accreditation_start_date_year',
            'accreditation_start_date_month', 'approval_date_year', 'approval_date_month',
            # Per requirement: also drop these three columns
            'fuel_source', 'accreditation_start_date', 'approval_date'
        ]
        for col in cols:
            try:
                cursor.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}")
            except Exception:
                pass
    except Exception as e:
        print(f"  Warning: drop_unwanted_columns_for_cer_approved encountered an error: {e}")

def migrate_cer_committed_schema(cursor) -> None:
    """Ensure cer_committed_power_stations schema has VARCHAR types for key text columns."""
    try:
        table_name = 'cer_committed_power_stations'
        varchar_columns = [
            'project_name', 'state', 'postcode', 'fuel_source', 'committed_date'
        ]
        for col in varchar_columns:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE VARCHAR")
            except Exception:
                # Ignore if column doesn't exist yet or type is already VARCHAR-compatible
                pass
    except Exception as e:
        print(f"  Warning: migrate_cer_committed_schema encountered an error: {e}")

def migrate_cer_probable_schema(cursor) -> None:
    """Ensure cer_probable_power_stations schema has VARCHAR types for key text columns."""
    try:
        table_name = 'cer_probable_power_stations'
        varchar_columns = [
            'project_name', 'state', 'postcode', 'fuel_source', 'formatted_address', 'place_id'
        ]
        for col in varchar_columns:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE VARCHAR")
            except Exception:
                # Ignore if column doesn't exist yet or type is already VARCHAR-compatible
                pass
    except Exception as e:
        print(f"  Warning: migrate_cer_probable_schema encountered an error: {e}")

def drop_specified_columns_for_cer_committed(cursor) -> None:
    """Drop specified columns from cer_committed_power_stations if they exist."""
    try:
        table = 'cer_committed_power_stations'
        cols = [
            'accreditation_code', 'power_station_name', 'installed_capacity_mw',
            'accreditation_start_date', 'approval_date',
            'accreditation_start_date_year', 'accreditation_start_date_month',
            'approval_date_year', 'approval_date_month'
        ]
        for col in cols:
            try:
                cursor.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}")
            except Exception:
                pass
    except Exception as e:
        print(f"  Warning: drop_specified_columns_for_cer_committed encountered an error: {e}")

def drop_specified_columns_for_cer_probable(cursor) -> None:
    """Drop specified columns from cer_probable_power_stations if they exist."""
    try:
        table = 'cer_probable_power_stations'
        cols = [
            'accreditation_code', 'power_station_name', 'installed_capacity_mw',
            'accreditation_start_date', 'approval_date',
            'accreditation_start_date_year', 'accreditation_start_date_month',
            'committed_date', 'committed_date_year', 'committed_date_month',
            # Include potential typo variants seen in source data
            'comitted_date_year', 'comitted_date_month',
            'approval_date_year', 'approval_date_month'
        ]
        for col in cols:
            try:
                cursor.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}")
            except Exception:
                pass
    except Exception as e:
        print(f"  Warning: drop_specified_columns_for_cer_probable encountered an error: {e}")

def save_cer_data(conn, table_type: str, df: pd.DataFrame) -> bool:
    """Save CER data (using normalized column names, table already exists)"""
    try:
        cursor = conn.cursor()
        normalized_table_name = normalize_db_column_name(f"cer_{table_type}")

        # Ensure CER tables exist (idempotent). This prevents "relation does not exist" errors
        # if insertion runs before the one-time table creation step.
        try:
            if not table_exists(cursor, normalized_table_name):
                # Force short transaction for DDL to avoid being rolled back later
                prev_autocommit = getattr(conn, 'autocommit', False)
                try:
                    # Ensure no active transaction before toggling autocommit
                    try:
                        from psycopg2 import extensions as _pgext
                        if hasattr(conn, 'get_transaction_status'):
                            status = conn.get_transaction_status()
                            if status in (_pgext.TRANSACTION_STATUS_INTRANS, _pgext.TRANSACTION_STATUS_INERROR):
                                conn.rollback()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    conn.autocommit = True
                    tmp_cur = conn.cursor()
                    try:
                        create_cer_tables_impl(tmp_cur)
                    finally:
                        try:
                            tmp_cur.close()
                        except Exception:
                            pass
                finally:
                    try:
                        conn.autocommit = prev_autocommit
                    except Exception:
                        pass
        except Exception as ee:
            print(f"  Warning: Ensuring CER tables failed: {ee}")
        
        # Standardize state names
        print(f"  Standardizing CER state names...")
        standardize_dataframe_states(df, 'state')

        # Normalize critical approved-table aliases to canonical columns if present
        # This guards against slight header variations leading to NULL inserts.
        try:
            alias_groups = {
                'fuel_source': ['fuel_source', 'Fuel Source (s)', 'Fuel Source', 'fuel sources', 'fuel source (s)', 'fuel source'],
                'accreditation_start_date': ['accreditation_start_date', 'Accreditation start date', 'accreditation start date'],
                'approval_date': ['approval_date', 'Approval date', 'approval date']
            }
            df_alias_fixed = df.copy()
            for canonical, candidates in alias_groups.items():
                if canonical not in df_alias_fixed.columns:
                    for cand in candidates:
                        if cand in df_alias_fixed.columns:
                            df_alias_fixed[canonical] = df_alias_fixed[cand]
                            break
                # If canonical exists but is entirely empty while candidates have data, backfill
                if canonical in df_alias_fixed.columns:
                    if df_alias_fixed[canonical].isna().all() or df_alias_fixed[canonical].astype(str).str.strip().isin(['', 'nan', 'None']).all():
                        for cand in candidates:
                            if cand in df_alias_fixed.columns and not (df_alias_fixed[cand].astype(str).str.strip().isin(['', 'nan', 'None']).all()):
                                df_alias_fixed[canonical] = df_alias_fixed[cand]
                                break
            df = df_alias_fixed
        except Exception as _ee:
            # Non-fatal: continue with original df if alias normalization fails
            pass
        
        # Original columns and geocoding columns
        geocode_fields = GEOCODE_FIELDS
        geocode_column_names = set(geocode_fields.keys())
        original_cols = [col for col in df.columns if col not in geocode_column_names]

        # For approved table, filter out the three dropped columns so we don't recreate them
        if table_type == 'approved_power_stations':
            cols_to_remove = {
                'fuel_source', 'accreditation_start_date', 'approval_date'
            }
            original_cols = [c for c in original_cols if c not in cols_to_remove]

        # For committed table, filter out specified columns per requirement
        if table_type == 'committed_power_stations':
            cols_to_remove = {
                'accreditation_code', 'power_station_name', 'installed_capacity_mw',
                'accreditation_start_date', 'approval_date',
                'accreditation_start_date_year', 'accreditation_start_date_month',
                'approval_date_year', 'approval_date_month'
            }
            original_cols = [c for c in original_cols if c not in cols_to_remove]
        # For probable table, filter out specified columns per requirement
        if table_type == 'probable_power_stations':
            cols_to_remove = {
                'accreditation_code', 'power_station_name', 'installed_capacity_mw',
                'accreditation_start_date', 'approval_date',
                'accreditation_start_date_year', 'accreditation_start_date_month',
                'committed_date', 'committed_date_year', 'committed_date_month',
                # Include potential typo variants seen in source data
                'comitted_date_year', 'comitted_date_month',
                'approval_date_year', 'approval_date_month'
            }
            original_cols = [c for c in original_cols if c not in cols_to_remove]
        
        # Prepare column information for data insertion
        used_names = {'id'}
        clean_original_cols = []
        original_to_clean = {}
        for col in original_cols:
            clean_col = normalize_db_column_name(col)
            # Avoid conflicts with geocoding fields
            if clean_col in ['postcode', 'state_full', 'country', 'locality']:
                clean_col = f"original_{clean_col}"
            # Ensure uniqueness
            original_name = clean_col
            counter = 1
            while clean_col in used_names:
                clean_col = f"{original_name}_{counter}"
                counter += 1
            used_names.add(clean_col)
            clean_original_cols.append(clean_col)
            original_to_clean[col] = clean_col
        
        # Dynamically add columns to table
        all_columns = clean_original_cols + list(geocode_fields.keys())
        for col_name in all_columns:
            # Determine column type
            if col_name in geocode_fields:
                col_type = geocode_fields[col_name]
            else:
                col_type = 'TEXT'  # Default to TEXT type
            
            add_column_if_not_exists(cursor, normalized_table_name, col_name, col_type)
        
        # Max length constraints for VARCHAR fields relevant to CER
        varchar_max_lengths = {
            'accreditation_code': 64,
            'power_station_name': 255,
            'project_name': 255,
            'state': 64,
            'postcode': 16,
            'fuel_source': 128,
            'formatted_address': 255,
            'place_id': 128
        }

        # Convert CER date-like strings to datetime.date for DATE columns
        def _to_date(value):
            if not is_valid_value(value):
                return None
            s = str(value).strip()
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%b %Y", "%Y/%m/%d", "%Y.%m.%d"]:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.date()
                except Exception:
                    continue
            # Try pandas to_datetime as a last resort
            try:
                dt = pd.to_datetime(s, errors='coerce', dayfirst=True)
                if pd.notna(dt):
                    return dt.date()
            except Exception:
                pass
            return None

        # Prepare data
        data = []
        
        for _, row in df.iterrows():
            row_data = []
            for col in original_cols:
                val = row.get(col)
                target_col = original_to_clean.get(col)
                max_len = varchar_max_lengths.get(target_col)
                # Convert specific known columns to DATE compatible values
                if original_to_clean.get(col) in ['accreditation_start_date', 'approval_date'] and table_type != 'approved_power_stations':
                    row_data.append(_to_date(val))
                else:
                    row_data.append(clean_value(val, max_length=max_len))
            
            for field in geocode_fields.keys():
                value = row.get(field)
                if field in ['lat', 'lon', 'bbox_south', 'bbox_north', 'bbox_west', 'bbox_east'] and is_valid_value(value):
                    try:
                        row_data.append(float(value))
                    except:
                        row_data.append(None)
                else:
                    row_data.append(clean_value(value, max_length=varchar_max_lengths.get(field)))
            
            data.append(tuple(row_data))
        
        # Insert
        insert_sql = prepare_insert_sql(normalized_table_name, all_columns)
        batch_insert(cursor, insert_sql, data)
        
        # Create/update geom column for CER table
        try:
            ensure_geometry_column_and_index(cursor, normalized_table_name, 'lat', 'lon', 'geom')
            ensure_area_and_bbox_geometries(cursor, normalized_table_name,
                                            'bbox_west', 'bbox_south', 'bbox_east', 'bbox_north')
        except Exception as e:
            print(f"  Warning: CER geometry column update failed: {e}")

        conn.commit()
        print(f"  CER data insertion successful: {normalized_table_name} ({len(data)} rows, with geocoding)")
        return True
        
    except Exception as e:
        print(f"  CER data insertion failed: {e}")
        conn.rollback()
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass

def create_abs_table(conn, merged_cell_value: str, columns: List[str]) -> str:
    """Create ABS table (table already exists, just return table name)"""
    clean_table = normalize_db_column_name(f"abs_{merged_cell_value}")
    print(f"ABS table already exists: {clean_table}")
    return clean_table

def create_abs_table_with_types(conn, merged_cell_value: str, columns: List[str], column_types: dict) -> str:
    """Create ABS table (based on pre-detected column types, table already exists in pre-creation stage)"""
    clean_table = normalize_db_column_name(f"abs_{merged_cell_value}")
    print(f"ABS table already exists (with types): {clean_table}")
    return clean_table

 

def insert_abs_data_cleaned(conn, table_name: str, df: pd.DataFrame, geo_level: int = None, column_types: dict = None) -> bool:
    """Insert cleaned ABS data using only merged-range columns (no fixed Code/Label/Year).

    Critical: Align insertion column normalization with table creation by using
    normalize_column_mapping (position-preserving, unique) so that columns like
    "number_of_business_exits_with_turnover_of_zero_to_less_than" consistently map
    to the same normalized names created earlier. This prevents accidental new
    columns and NULL inserts due to name mismatches.
    """
    try:
        cursor = conn.cursor()
        
        print(f"  💾Inserting cleaned ABS data to: {table_name}")
        
        # Prepare column name mapping for all columns in df (must mirror table creation)
        # Use equal-length, order-preserving normalization to avoid mismatches.
        normalized_cols = normalize_column_mapping(list(df.columns))
        original_to_clean = {orig: norm for orig, norm in zip(df.columns, normalized_cols)}
        cols = normalized_cols

        # Before insertion, ensure all columns exist in target table (prevent missing column errors due to inconsistent column name mapping)
        try:
            for clean_col in cols:
                if not column_exists(cursor, table_name, clean_col):
                    # Infer column type: prioritize passed column_types (based on original column name), otherwise guess based on column name
                    sql_type = 'TEXT'
                    # Reverse lookup original column name for type hints
                    source_col = next((orig for orig, mapped in original_to_clean.items() if mapped == clean_col), None)
                    if column_types and source_col and source_col in column_types:
                        ct = column_types[source_col]
                        if ct in ['integer']:
                            sql_type = 'INTEGER'
                        elif ct in ['float', 'percentage', 'currency']:
                            sql_type = 'NUMERIC'
                        else:
                            sql_type = 'TEXT'
                    else:
                        # Heuristic inference based on column name
                        lc = clean_col.lower()
                        if any(k in lc for k in ['percent', 'rate', 'ratio']):
                            sql_type = 'NUMERIC'
                        elif any(k in lc for k in ['count', 'number', 'total', 'year']):
                            sql_type = 'INTEGER'
                        else:
                            sql_type = 'TEXT'
                    # Force specific ABS key columns to VARCHAR
                    if clean_col.lower() in ['code', 'label']:
                        sql_type = 'VARCHAR'
                    add_column_if_not_exists(cursor, table_name, clean_col, sql_type)
            # Ensure geographic_level exists if we were provided a geo_level
            if geo_level is not None and not column_exists(cursor, table_name, 'geographic_level'):
                add_column_if_not_exists(cursor, table_name, 'geographic_level', 'INTEGER')
        except Exception as ee:
            print(f"  Warning: ABS column validation/supplementation failed: {ee}")
        
        # Prepare insertion data (data already cleaned)
        data = []
        for _, row in df.iterrows():
            row_data = []
            
            # Insert values for all columns directly (already cleaned)
            for col in df.columns:
                value = row[col]
                if pd.isna(value):
                    row_data.append(None)
                else:
                    # Coerce ABS Code to integer if possible
                    if str(col).strip().lower() == 'code':
                        try:
                            code_val = int(str(value).strip().split('.')[0])
                        except Exception:
                            code_val = None
                        row_data.append(code_val)
                    else:
                        row_data.append(value)
            # Append geographic_level constant per row if provided
            if geo_level is not None:
                row_data.append(int(geo_level))
            
            data.append(tuple(row_data))
        
        # If geo_level provided, include geographic_level in the insert column list
        insert_columns = list(cols)
        if geo_level is not None:
            insert_columns.append('geographic_level')
        insert_sql = prepare_insert_sql(table_name, insert_columns)
        batch_insert(cursor, insert_sql, data, 10000)
        
        conn.commit()
        
        # Simplified statistics report
        print(f"  ABS data insertion successful: {len(data)} rows (pre-cleaned)")
        
        return True
        
    except Exception as e:
        print(f"  ABS data insertion failed: {e}")
        conn.rollback()
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass

# =============================================================================
# PostGIS/Geometry helper functions
# =============================================================================

def geometry_column_exists(cursor, table_name: str, geom_col: str = 'geom') -> bool:
    """Check if geometry column exists."""
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
    """Ensure geometry(Point,4326) column exists and is populated from lat/lon, create GiST index."""
    # 1) Add geometry column
    if not geometry_column_exists(cursor, table_name, geom_col):
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {geom_col} geometry(Point, 4326);")
            print(f"  Added geometry column: {table_name}.{geom_col}")
        except Exception as e:
            # If failed due to extension not enabled, upper layer should have tried to enable it
            raise e
    # 2) Update geom with lat/lon (only null values)
    update_sql = f"""
        UPDATE {table_name}
        SET {geom_col} = ST_SetSRID(ST_MakePoint(NULLIF({lon_col}::text,'')::double precision,
                                                 NULLIF({lat_col}::text,'')::double precision), 4326)
        WHERE {geom_col} IS NULL AND {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL;
    """
    cursor.execute(update_sql)
    # 3) Create GiST index (if not exists)
    index_name = f"{table_name}_{geom_col}_gist"
    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST ({geom_col});")
    except Exception as e:
        # Compatible with older Postgres versions without IF NOT EXISTS: ignore already exists error
        try:
            cursor.execute(f"SELECT 1 FROM pg_class WHERE relname = %s;", (index_name,))
            exists = bool(cursor.fetchone())
            if not exists:
                cursor.execute(f"CREATE INDEX {index_name} ON {table_name} USING GIST ({geom_col});")
        except Exception:
            pass
    print(f"  Geometry index ensured: {index_name}")

def ensure_area_and_bbox_geometries(cursor, table_name: str,
                                    bbox_w_col: str = 'bbox_west', bbox_s_col: str = 'bbox_south',
                                    bbox_e_col: str = 'bbox_east', bbox_n_col: str = 'bbox_north',
                                    bbox_geom_col: str = 'geom_bbox') -> None:
    """Ensure bbox polygon geometry column exists and is populated, create GiST index."""
    # bbox polygon column
    if not geometry_column_exists(cursor, table_name, bbox_geom_col):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {bbox_geom_col} geometry(Polygon, 4326);")
        print(f"  Added geometry column: {table_name}.{bbox_geom_col}")

    # Populate bbox polygon with bbox (only null values)
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

    # Index
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
    print(f"  Geometry index ensured: {index_name}")


def create_proximity_join():
    """Create table with proximity matches (within 5km)"""
    conn = get_db_connection()
    if not conn:
        print("Database connection failed")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS nger_cer_proximity_matches")
        
        # Create proximity matches table
        create_sql = """
        CREATE TABLE nger_cer_proximity_matches AS
        SELECT 
            n.id as nger_id,
            c.id as cer_id,
            'proximity_1km' as match_type,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(n.lon, n.lat), 4326),
                ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)
            ) * 111000 as distance_meters
        FROM nger_unified n
        CROSS JOIN cer_approved_power_stations c
        WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL 
        AND c.lat IS NOT NULL AND c.lon IS NOT NULL
        AND n.state = c.state
        AND ST_DWithin(
            ST_SetSRID(ST_MakePoint(n.lon, n.lat), 4326),
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326),
            0.01
        );
        """
        
        cursor.execute(create_sql)
        conn.commit()
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM nger_cer_proximity_matches")
        count = cursor.fetchone()[0]
        print(f"✓ Created nger_cer_proximity_matches table with {count} records")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to create proximity join table: {e}")
        conn.rollback()
        return False
    finally:
        return_db_connection(conn)

if __name__ == "__main__":
    create_proximity_join()