#!/usr/bin/env python3
"""Data acquisition and processing tools"""

# Standard library imports
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Third-party library imports
import pandas as pd
import requests
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Local module imports
from data_cleaner import *
from database_utils import *
from excel_utils import get_merged_cells, read_merged_headers
from geocoding import (
    add_geocoding_to_cer_data,
    add_geocoding_to_nger_data,
    initialize_geocoding_cache,
    save_global_cache
)
from time_format_utils import process_abs_time_format, process_nger_time_format

# Configuration
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "COMP5339-Assignment1/1.0"})


# ============================================================================
# Data Acquisition
# ============================================================================

def download_nger_year(year_data, results_queue):
    """Download NGER data"""
    thread_id = threading.get_ident()
    year_label, url = year_data
    
    try:
        print(f"[Thread {thread_id}] Downloading NGER data: {year_label}...")
        resp = requests.get(url, timeout=120, headers={"User-Agent": "COMP5339-Assignment1/1.0"})
        resp.raise_for_status()
        
        if "json" in resp.headers.get("Content-Type", "") or resp.text.strip().startswith("["):
            data = resp.json()
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            # Normalize column names to lowercase, strip whitespace to avoid field mapping issues
            try:
                df.columns = [str(c).strip().lower() for c in df.columns]
            except Exception:
                pass
            
            # Process database operations directly in thread
            try:
                # 1. Data quality fixes (before database insertion)
                if df is not None and not df.empty:
                    df = process_data_quality_fixes(df, 'nger')
                    print(f"  NGER data quality fixes completed: {year_label}")
                
                # 2. Time format conversion
                if df is not None and not df.empty:
                    df = process_nger_time_format(df, year_label)
                    
                # 3. Geocoding enhancement
                if df is not None and not df.empty:
                    df = add_geocoding_to_nger_data(df, max_workers=10)
            except Exception as e:
                print(f"  Warning: NGER data processing failed, continuing with raw data: {e}")

            conn = get_db_connection()
            if conn and df is not None and not df.empty:
                if save_nger_data(conn, year_label, df):
                    results_queue.put((year_label, True, None))
                    print(f"[Thread {thread_id}] NGER data download and database insertion completed: {year_label}")
                else:
                    results_queue.put((year_label, False, "Database insertion failed"))
            else:
                results_queue.put((year_label, False, "Database connection failed"))
        else:
            results_queue.put((year_label, False, "Format error"))
    except Exception as e:
        results_queue.put((year_label, False, str(e)))
        print(f"[Thread {thread_id}] NGER data download failed: {year_label}: {e}")
    finally:
        if 'conn' in locals() and conn:
            return_db_connection(conn)


def fetch_nger_data(max_workers=1):
    """Multi-threaded NGER data acquisition"""
    try:
        table = pd.read_csv(DATA_DIR / "nger_data_api_links.csv")
        year_col = next((c for c in ["year_label", "year"] if c in table.columns), None)
        url_col = next((c for c in ["url", "api_url", "link"] if c in table.columns), None)
        
        if not year_col or not url_col:
            return False
        
        tasks = [(str(row[year_col]).strip(), str(row[url_col]).strip()) 
                 for _, row in table.iterrows() 
                 if str(row[url_col]).lower() != "nan"]
        
        print(f"Starting multi-threaded NGER data download ({max_workers} threads): {len(tasks)} year files")
        
        results_queue = queue.Queue()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_nger_year, task, results_queue) for task in tasks]
            [f.result() for f in as_completed(futures)]
        
        # Process results
        success_count = 0
        while not results_queue.empty():
            item_label, success, error = results_queue.get()
            if error: 
                print(f"{item_label}: {error}")
                continue
            if success:
                success_count += 1
        
        print(f"NGER data processing completed: {success_count}/{len(tasks)} tasks successful")
        return success_count > 0
        
    except Exception as e:
        print(f"NGER data processing failed: {e}")
        return False

def fetch_abs_data():
    """Download ABS data"""
    url = "https://www.abs.gov.au/methodologies/data-region-methodology/2011-24/14100DO0003_2011-24.xlsx"
    filepath = DATA_DIR / "14100DO0003_2011-24.xlsx"
    
    try:
        response = SESSION.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk: f.write(chunk)
        print("ABS data download successful")
        return filepath
    except Exception as e:
        print(f"ABS data download failed: {e}")
        return None

# ============================================================================
# CER Web Scraping
# ============================================================================

def setup_driver():
    """Setup WebDriver"""
    options = Options()
    for arg in ["--headless", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]:
        options.add_argument(arg)
    options.add_argument("--window-size=1920,1080")
    
    try:
        return webdriver.Chrome(options=options)
    except Exception as e:
        print(f"WebDriver: {e}")
        return None

def identify_table(df):
    """Identify table type"""
    cols = ' '.join(df.columns).lower()
    if 'accreditation code' in cols and 'power station name' in cols:
        return "approved_power_stations"
    elif 'project name' in cols and 'committed date' in cols:
        return "committed_power_stations"
    elif 'project name' in cols and 'mw capacity' in cols:
        return "probable_power_stations"
    return None

def parse_table(table_element):
    """Parse table"""
    try:
        rows = []
        table_rows = table_element.find_elements(By.TAG_NAME, "tr")
        if not table_rows:
            return pd.DataFrame()
        
        headers = [th.text.strip() for th in table_rows[0].find_elements(By.TAG_NAME, "th")]
        if not headers:
            return pd.DataFrame()
        
        for row in table_rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if cells:
                row_data = [cell.text.strip() for cell in cells]
                if len(row_data) == len(headers):
                    rows.append(row_data)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=headers)
        
        # Filter empty columns
        keep_cols = []
        for i, col in enumerate(df.columns):
            col_name = str(col).strip()
            if col_name and col_name.lower() not in ['', 'nan', 'none']:
                col_data = df.iloc[:, i].astype(str).str.strip()
                if not col_data.isin(['', 'nan', 'None']).all():
                    keep_cols.append(col)
        
        return df[keep_cols] if keep_cols else pd.DataFrame()
    except Exception as e:
        print(f"Parsing error: {e}")
        return pd.DataFrame()

def get_max_pages(table_element):
    """Get maximum number of pages"""
    try:
        container = table_element.find_element(By.XPATH, "ancestor::*[.//table][1]")
        elements = container.find_elements(By.XPATH, ".//*[contains(text(), 'Showing') and contains(text(), 'of')]")
        if elements:
            match = re.search(r'showing\s*(\d+)\s*to\s*(\d+)\s*of\s*(\d+)', elements[0].text, re.IGNORECASE)
            if match:
                start, end, total = map(int, match.groups())
                return (total + (end - start)) // (end - start + 1)
        return 10
    except:
        return 10

def scrape_paginated_table(driver, table_element, table_type):
    """Scrape paginated table"""
    max_pages, frames, page = get_max_pages(table_element), [], 1
    print(f"{table_type}(max {max_pages} pages)")
    
    while page <= max_pages:
        try:
            df = parse_table(table_element)
            if not df.empty: 
                frames.append(df)
                print(f"  Page {page}: {len(df)} rows")
            
            if page < max_pages:
                try:
                    container = table_element.find_element(By.XPATH, "ancestor::*[.//table][1]")
                    next_btn = container.find_element(By.XPATH, ".//button[contains(text(), '›') or contains(text(), 'Next')]")
                    if next_btn.is_displayed() and next_btn.is_enabled():
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(3)
                        page += 1
                    else: break
                except: break
            else: break
                
        except StaleElementReferenceException:
            time.sleep(2)
            try:
                tables = driver.find_elements(By.TAG_NAME, "table")
                table_element = tables[0]
            except:
                break
        except Exception as e:
            print(f"  Page {page} error: {e}")
            break
    
    if frames:
        result = pd.concat(frames, ignore_index=True).drop_duplicates()
        print(f"  {len(result)} rows")
        return result
    return pd.DataFrame()

def fetch_cer_data(max_workers=1):
    """Scrape CER data"""
    driver = setup_driver()
    if not driver:
        return False
    
    conn = get_db_connection()
    if not conn:
        driver.quit()
        return False
    
    try:
        driver.get("https://cer.gov.au/markets/reports-and-data/large-scale-renewable-energy-data")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(5)
        
        tables = driver.find_elements(By.TAG_NAME, "table")
        targets = ["approved_power_stations", "committed_power_stations", "probable_power_stations"]
        success_count = 0
        
        print(f"Found CER web tables: {len(tables)} tables")
        
        for i, table in enumerate(tables):
            try:
                df_temp = parse_table(table)
                if df_temp.empty: continue
                
                table_type = identify_table(df_temp)
                if table_type not in targets: continue
                
                print(f"\nStarting CER table processing: {table_type}...")
                df_result = scrape_paginated_table(driver, table, table_type)
                if df_result.empty: continue
                
                # Data cleaning: column normalization, time processing, value conversion (before geocoding)
                print(f"  Starting CER data cleaning: {table_type}...")
                
                # 1. Basic data cleaning (column normalization, time processing, value conversion)
                df_cleaned = process_cer_data_with_cleaning(df_result, table_type)
                
                # 2. Data quality fixes (missing values, date format standardization)
                df_cleaned = process_data_quality_fixes(df_cleaned, 'cer', table_type=table_type)
                
                print(f"  CER data cleaning and fixes completed: {table_type}")
                
                print(f"  Multi-threaded geocoding for CER data...")
                df_geocoded = add_geocoding_to_cer_data(df_cleaned, table_type, max_workers)
                
                if save_cer_data(conn, table_type, df_geocoded):
                    success_count += 1
                    print(f"  CER table processing completed: {table_type}")
                    
            except Exception as e:
                print(f"  CER table {i+1} processing failed: {e}")
        
        print(f"CER data processing completed: {success_count} tables successfully inserted into database")
        return success_count > 0
        
    except Exception as e:
        print(f"CER data processing failed: {e}")
        return False
    finally:
        if conn:
            return_db_connection(conn)
        driver.quit()

# ============================================================================
# Excel Processing
# ============================================================================


def process_abs_merged_cell_with_db(args):
    """Process ABS merged cells and insert into database (data pre-cleaned)"""
    thread_id = threading.get_ident()
    cell, df_cleaned, level_info, db_config, column_types = args
    
    try:
        print(f"[Thread {thread_id}] Processing ABS merged cell: {cell['value']}")
        conn = get_db_connection()
        
        if not conn:
            return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': 'Database connection failed'}
        
        start_col, end_col = cell['start_col'] - 1, cell['end_col']
        selected_cols = ['Code', 'Label', 'Year'] + list(df_cleaned.columns[start_col:end_col])
        subset_df = df_cleaned[selected_cols].copy()
        
        # Extract column type information for this subset
        subset_column_types = {col: column_types.get(col, 'text') for col in selected_cols[3:]}
        
        table_name = create_abs_table_with_types(conn, cell['value'], selected_cols, subset_column_types)
        if table_name and insert_abs_data_cleaned(conn, table_name, subset_df, level_info['level'], subset_column_types):
            print(f"[Thread {thread_id}] ABS data insertion successful: {cell['value']}")
            return {'success': True, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': None}
        else:
            print(f"[Thread {thread_id}] ABS data insertion failed: {cell['value']}")
            return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': 'Insertion failed'}
        
    except Exception as e:
        print(f"[Thread {thread_id}] ABS cell processing failed: {cell['value']}: {e}")
        return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': str(e)}
    finally:
        if 'conn' in locals() and conn:
            return_db_connection(conn)

def run_threading_tasks(tasks, task_func, max_workers, operation_name):
    """Generic function for running multi-threaded tasks"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(task_func, task): task[0]['value'] if isinstance(task[0], dict) else str(task[0]) for task in tasks}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                task_name = futures[future]
                print(f"Thread exception {task_name}: {e}")
                results.append({'success': False, 'cell_name': task_name, 'error': str(e)})
    
    success_count = sum(1 for r in results if r['success'])
    print(f"{operation_name} processing completed: {success_count}/{len(tasks)} tasks successful")
    return results

def process_abs_data(file_path: str, max_workers=4):
    """Multi-threaded ABS data processing"""
    try:
        # Geographic level definitions
        levels = {
            "Table 1": {"desc": "State level", "level": 0},
            "Table 2": {"desc": "Local government level", "level": 1}
        }
        
        for sheet_name in ["Table 1", "Table 2"]:
            level_info = levels[sheet_name]
            print(f"\nStarting ABS table processing: {sheet_name}({level_info['desc']})...")
            
            merged_cells = get_merged_cells(file_path, sheet_name)
            df = read_merged_headers(file_path, sheet_name)
            print(f"Found {len(merged_cells)} merged cells, {df.shape[0]} rows of data")
            
            # Process ABS time format (validation)
            df = process_abs_time_format(df)
            
            # Data cleaning: value conversion and LGA standardization (before database insertion)
            print(f"  Starting {sheet_name} data cleaning...")
            df_cleaned, column_types = process_abs_data_with_cleaning(df)
            
            # Statistics on cleaning results
            numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
            if numeric_cols:
                print(f"  {sheet_name}: Detected and converted {len(numeric_cols)} numeric columns")
            print(f"  {sheet_name} data cleaning completed")
            
            tasks = [(cell, df_cleaned, level_info, DB_CONFIG, column_types) for cell in merged_cells]
            print(f"Using {max_workers} threads for parallel ABS data processing...")
            
            results = run_threading_tasks(tasks, process_abs_merged_cell_with_db, max_workers, f"ABS table {sheet_name}")
            
        
        print("ABS data processing completed")
        return True
        
    except Exception as e:
        print(f"ABS data processing failed: {e}")
        return False

# ============================================================================
# Main Function
# ============================================================================

def create_table_direct(table_name: str, create_func, *args):
    """Create table directly"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            print("Database connection failed")
            return False
        
        if args:
            return create_func(conn, *args)
        else:
            return create_func(conn)
    except Exception as e:
        print(f"Failed to create {table_name}: {e}")
        return False
    finally:
        if conn:
            return_db_connection(conn)

def print_final_results(results):
    """Print final results"""
    success = sum(results)
    print(f"\n{'='*50}")
    print(f"Data processing system execution completed: {success}/3 modules successful")
    nger_symbol = 'OK' if results[0] else 'FAIL'
    abs_symbol = 'OK' if results[1] else 'FAIL'
    cer_symbol = 'OK' if results[2] else 'FAIL'
    print(f"NGER: {nger_symbol}  |  ABS: {abs_symbol}  |  CER: {cer_symbol}")
    print(f"{'='*50}")

def main():
    """Main function"""
    print("=" * 50)
    print("Data Acquisition and Processing System")
    print("NGER Data + ABS Economic Data + CER Power Station Data")
    print("=" * 50)
    
    # Initialize geocoding cache (preload cache to reduce API calls and disk I/O competition)
    try:
        initialize_geocoding_cache(str(DATA_DIR / "geocoding_cache.json"))
    except Exception as e:
        print(f"Warning: Geocoding cache initialization failed: {e}")
    
    # Initialize connection pool
    pool = get_connection_pool(minconn=2, maxconn=15)
    if not pool:
        print("Database connection pool initialization failed")
        return
    
    try:
        print("First create data tables, then acquire and process data...")
        
        # Create NGER tables
        print("\n" + "=" * 20 + " 1. Create NGER Tables " + "=" * 20)
        nger_table_ok = create_table_direct("NGER表", create_nger_table)
        if not nger_table_ok:
            print("Failed to prepare NGER tables")
            return
        
        # NGER data acquisition and processing
        print("\n" + "=" * 20 + " 2. NGER Data Acquisition and Processing " + "=" * 20)
        nger_ok = fetch_nger_data(max_workers=10)
        
        # Create CER tables
        print("\n" + "=" * 20 + " 3. Create CER Tables " + "=" * 20)
        cer_table_ok = create_table_direct("CER表", create_cer_tables)
        if not cer_table_ok:
            print("Failed to prepare CER tables")
            return
        
        # CER power station data acquisition and processing
        print("\n" + "=" * 20 + " 4. CER Power Station Data Acquisition and Processing " + "=" * 20)
        cer_ok = fetch_cer_data()
    
        # ABS data processing
        abs_file = fetch_abs_data()
        if abs_file:
            print("\n" + "=" * 20 + " 5. Create ABS Tables " + "=" * 20)
            conn = get_db_connection()
            if not conn:
                print("Database connection failed")
                return
            
            try:
                abs_table_ok = create_all_abs_tables(conn, str(abs_file))
                if not abs_table_ok:
                    print("ABS table preparation failed")
                    return
            except Exception as e:
                print(f"ABS table creation failed: {e}")
                return
            finally:
                return_db_connection(conn)
            
            print("\n" + "=" * 20 + " 6. ABS Economic Data Acquisition and Processing " + "=" * 20)
            abs_ok = process_abs_data(str(abs_file), max_workers=10)
        else:
            abs_ok = False
        
        # Create proximity matches
        print("\n7. Creating proximity matches (1km)...")
        if create_proximity_join():
            print("Proximity matches created successfully")
        else:
            print("Proximity matches creation failed")

        # Print final results
        print_final_results([nger_ok, abs_ok, cer_ok])
        
        # Save geocoding cache
        print("Saving geocoding cache...")
        try:
            save_global_cache()
            print("Geocoding cache saved")
        except Exception as e:
            print(f"Failed to save geocoding cache: {e}")
        
    finally:
        # Close connection pool
        close_connection_pool()

if __name__ == "__main__":
    main()
