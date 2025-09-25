#!/usr/bin/env python3
"""数据获取和处理工具"""

# 标准库导入
from pathlib import Path
import time
import re
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# 第三方库导入
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException

# 本地模块导入
from database_config import *
from geocoding import add_geocoding_to_cer_data, add_geocoding_to_nger_data, save_global_cache, initialize_geocoding_cache
from excel_utils import get_merged_cells, read_merged_headers
from time_format_utils import process_nger_time_format, process_cer_time_format, process_abs_time_format
from data_cleaner import *

# 配置
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "COMP5339-Assignment1/1.0"})


# ============================================================================
# 数据获取
# ============================================================================

def download_nger_year(year_data, results_queue):
    """下载NGER数据"""
    thread_id = threading.get_ident()
    year_label, url = year_data
    
    try:
        print(f"[线程{thread_id}] 下载NGER数据: {year_label}...")
        resp = requests.get(url, timeout=120, headers={"User-Agent": "COMP5339-Assignment1/1.0"})
        resp.raise_for_status()
        
        if "json" in resp.headers.get("Content-Type", "") or resp.text.strip().startswith("["):
            data = resp.json()
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            # 统一列名为小写，去除首尾空格，避免字段映射缺失
            try:
                df.columns = [str(c).strip().lower() for c in df.columns]
            except Exception:
                pass
            
            # 在线程中直接处理数据库操作
            try:
                # 1. 数据质量修复（入库前）
                if df is not None and not df.empty:
                    df_before = df.copy()
                    df = process_data_quality_fixes(df, 'nger')
                    print(f"  ✓NGER数据质量修复完成: {year_label}")
                
                # 2. 时间格式转换
                if df is not None and not df.empty:
                    df = process_nger_time_format(df, year_label)
                    
                # 3. 地理编码增强
                if df is not None and not df.empty:
                    df = add_geocoding_to_nger_data(df, max_workers=1)
            except Exception as e:
                print(f"  ⚠NGER数据处理失败，继续入库原始数据: {e}")

            conn = get_db_connection()
            if conn and df is not None and not df.empty:
                if save_nger_data(conn, year_label, df):
                    results_queue.put((year_label, True, None))
                    print(f"✓[线程{thread_id}] NGER数据下载和入库完成: {year_label}")
                else:
                    results_queue.put((year_label, False, "数据库入库失败"))
            else:
                results_queue.put((year_label, False, "数据库连接失败"))
        else:
            results_queue.put((year_label, False, "格式错误"))
    except Exception as e:
        results_queue.put((year_label, False, str(e)))
        print(f"✗[线程{thread_id}] NGER数据下载失败: {year_label}: {e}")
    finally:
        if 'conn' in locals() and conn:
            return_db_connection(conn)

def process_threading_results(results_queue, total_tasks, operation_name):
    """处理多线程结果"""
    success_count = 0
    while not results_queue.empty():
        item_label, success, error = results_queue.get()
        if error: 
            print(f"✗{item_label}: {error}")
            continue
        if success:
            success_count += 1
    
    print(f"✓{operation_name}处理完成: {success_count}/{total_tasks} 个任务成功")
    return success_count > 0

def fetch_nger_data(max_workers=1):
    """多线程获取NGER数据"""
    try:
        table = pd.read_csv(DATA_DIR / "nger_data_api_links.csv")
        year_col = next((c for c in ["year_label", "year"] if c in table.columns), None)
        url_col = next((c for c in ["url", "api_url", "link"] if c in table.columns), None)
        
        if not year_col or not url_col:
            return False
        
        tasks = [(str(row[year_col]).strip(), str(row[url_col]).strip()) 
                 for _, row in table.iterrows() 
                 if str(row[url_col]).lower() != "nan"]
        
        print(f"开始多线程下载NGER数据 ({max_workers}线程): {len(tasks)}个年份文件")
        
        results_queue = queue.Queue()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_nger_year, task, results_queue) for task in tasks]
            [f.result() for f in as_completed(futures)]
        
        return process_threading_results(results_queue, len(tasks), "NGER数据")
        
    except Exception as e:
        print(f"✗NGER数据处理失败: {e}")
        return False

def fetch_abs_data():
    """下载ABS数据"""
    url = "https://www.abs.gov.au/methodologies/data-region-methodology/2011-24/14100DO0003_2011-24.xlsx"
    filepath = DATA_DIR / "14100DO0003_2011-24.xlsx"
    
    try:
        response = SESSION.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk: f.write(chunk)
        print("✓ABS数据下载成功")
        return filepath
    except Exception as e:
        print(f"✗ABS数据下载失败: {e}")
        return None

# ============================================================================
# CER爬取
# ============================================================================

def setup_driver():
    """设置WebDriver"""
    options = Options()
    for arg in ["--headless", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]:
        options.add_argument(arg)
    options.add_argument("--window-size=1920,1080")
    
    try:
        return webdriver.Chrome(options=options)
    except Exception as e:
        print(f"✗WebDriver: {e}")
        return None

def identify_table(df):
    """识别表类型"""
    cols = ' '.join(df.columns).lower()
    if 'accreditation code' in cols and 'power station name' in cols:
        return "approved_power_stations"
    elif 'project name' in cols and 'committed date' in cols:
        return "committed_power_stations"
    elif 'project name' in cols and 'mw capacity' in cols:
        return "probable_power_stations"
    return None

def parse_table(table_element):
    """解析表格"""
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
        
        # 过滤空列
        keep_cols = []
        for i, col in enumerate(df.columns):
            col_name = str(col).strip()
            if col_name and col_name.lower() not in ['', 'nan', 'none']:
                col_data = df.iloc[:, i].astype(str).str.strip()
                if not col_data.isin(['', 'nan', 'None']).all():
                    keep_cols.append(col)
        
        return df[keep_cols] if keep_cols else pd.DataFrame()
    except Exception as e:
        print(f"✗解析: {e}")
        return pd.DataFrame()

def get_max_pages(table_element):
    """获取最大页数"""
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
    """爬取分页表"""
    max_pages, frames, page = get_max_pages(table_element), [], 1
    print(f"{table_type}(最大{max_pages}页)")
    
    while page <= max_pages:
        try:
            df = parse_table(table_element)
            if not df.empty: 
                frames.append(df)
                print(f"  第{page}页: {len(df)}行")
            
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
            print(f"  页{page}错误: {e}")
            break
    
    if frames:
        result = pd.concat(frames, ignore_index=True).drop_duplicates()
        print(f"  ✓{len(result)}行")
        return result
    return pd.DataFrame()

def fetch_cer_data(max_workers=1):
    """爬取CER数据"""
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
        
        print(f"发现CER网页表格: {len(tables)}个")
        
        for i, table in enumerate(tables):
            try:
                df_temp = parse_table(table)
                if df_temp.empty: continue
                
                table_type = identify_table(df_temp)
                if table_type not in targets: continue
                
                print(f"\n开始处理CER表格: {table_type}...")
                df_result = scrape_paginated_table(driver, table, table_type)
                if df_result.empty: continue
                
                # 数据清理：列名规范化、时间处理、数值转换（在地理编码前完成）
                print(f"  🧹开始CER数据清理: {table_type}...")
                
                # 1. 基础数据清理（列名规范化、时间处理、数值转换）
                df_cleaned = process_cer_data_with_cleaning(df_result, table_type)
                
                # 2. 数据质量修复（缺失值、日期格式统一）
                df_fixed = process_data_quality_fixes(df_cleaned, 'cer', table_type=table_type)
                
                print(f"  ✓CER数据清理和修复完成: {table_type}")
                df_time_processed = df_fixed
                
                print(f"  对CER数据进行多线程地理编码...")
                df_geocoded = add_geocoding_to_cer_data(df_time_processed, table_type, max_workers)
                
                if save_cer_data(conn, table_type, df_geocoded):
                    success_count += 1
                    print(f"  ✓CER表格处理完成: {table_type}")
                    
            except Exception as e:
                print(f"  ✗CER表格{i+1}处理失败: {e}")
        
        print(f"✓CER数据处理完成: {success_count}个表格成功入库")
        return success_count > 0
        
    except Exception as e:
        print(f"✗CER数据处理失败: {e}")
        return False
    finally:
        if conn:
            return_db_connection(conn)
        driver.quit()

# ============================================================================
# Excel处理
# ============================================================================


def process_abs_merged_cell_with_db(args):
    """处理ABS单元格并入库（数据已预清理）"""
    thread_id = threading.get_ident()
    cell, df_cleaned, level_info, db_config, column_types = args
    
    try:
        print(f"[线程{thread_id}] 处理ABS合并单元格: {cell['value']}")
        conn = get_db_connection()
        
        if not conn:
            return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': '数据库连接失败'}
        
        start_col, end_col = cell['start_col'] - 1, cell['end_col']
        selected_cols = ['Code', 'Label', 'Year'] + list(df_cleaned.columns[start_col:end_col])
        subset_df = df_cleaned[selected_cols].copy()
        
        # 提取该子集相关的列类型信息
        subset_column_types = {col: column_types.get(col, 'text') for col in selected_cols[3:]}
        
        table_name = create_abs_table_with_types(conn, cell['value'], selected_cols, subset_column_types)
        if table_name and insert_abs_data_cleaned(conn, table_name, subset_df, level_info['level'], subset_column_types):
            print(f"✓[线程{thread_id}] ABS数据入库成功: {cell['value']}")
            return {'success': True, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': None}
        else:
            print(f"✗[线程{thread_id}] ABS数据入库失败: {cell['value']}")
            return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': '入库失败'}
        
    except Exception as e:
        print(f"✗[线程{thread_id}] ABS单元格处理失败: {cell['value']}: {e}")
        return {'success': False, 'cell_name': cell['value'], 'thread_id': thread_id, 'error': str(e)}
    finally:
        if 'conn' in locals() and conn:
            return_db_connection(conn)

def run_threading_tasks(tasks, task_func, max_workers, operation_name):
    """运行多线程任务的通用函数"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(task_func, task): task[0]['value'] if isinstance(task[0], dict) else str(task[0]) for task in tasks}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                task_name = futures[future]
                print(f"✗线程异常{task_name}: {e}")
                results.append({'success': False, 'cell_name': task_name, 'error': str(e)})
    
    success_count = sum(1 for r in results if r['success'])
    print(f"✓{operation_name}处理完成: {success_count}/{len(tasks)} 个任务成功")
    return results

def process_abs_data(file_path: str, max_workers=4):
    """多线程处理ABS数据"""
    try:
        # 地理级别定义
        levels = {
            "Table 1": {"desc": "州级", "level": 0},
            "Table 2": {"desc": "地方政府级", "level": 1}
        }
        
        for sheet_name in ["Table 1", "Table 2"]:
            level_info = levels[sheet_name]
            print(f"\n开始处理ABS表格: {sheet_name}({level_info['desc']})...")
            
            merged_cells = get_merged_cells(file_path, sheet_name)
            df = read_merged_headers(file_path, sheet_name)
            print(f"发现{len(merged_cells)}个合并单元格, 数据{df.shape[0]}行")
            
            # 处理ABS时间格式（验证）
            df = process_abs_time_format(df)
            
            # 数据清理：数值转换和LGA标准化（在入库前完成）
            print(f"  🧹开始{sheet_name}数据清理...")
            df_cleaned, column_types = process_abs_data_with_cleaning(df)
            
            # 统计清理结果
            numeric_cols = {k: v for k, v in column_types.items() if v != 'text'}
            if numeric_cols:
                print(f"  📊{sheet_name}: 检测并转换了{len(numeric_cols)}个数值列")
            print(f"  ✓{sheet_name}数据清理完成")
            
            tasks = [(cell, df_cleaned, level_info, DB_CONFIG, column_types) for cell in merged_cells]
            print(f"使用{max_workers}个线程并行处理ABS数据...")
            
            results = run_threading_tasks(tasks, process_abs_merged_cell_with_db, max_workers, f"ABS表格{sheet_name}")
            
            # 线程统计
            thread_stats = {}
            for r in results:
                tid = r.get('thread_id', 'Unknown')
                if tid not in thread_stats:
                    thread_stats[tid] = {'success': 0, 'failed': 0}
                if r['success']:
                    thread_stats[tid]['success'] += 1
                else:
                    thread_stats[tid]['failed'] += 1
            
            print("ABS数据处理线程执行统计:")
            for tid, stats in thread_stats.items():
                print(f"  线程{tid}: {stats['success']}/{stats['success']+stats['failed']}")
        
        print("✓ABS数据处理全部完成")
        return True
        
    except Exception as e:
        print(f"✗ABS数据处理失败: {e}")
        return False

# ============================================================================
# 主函数
# ============================================================================

def create_table_direct(table_name: str, create_func, *args):
    """直接创建表"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            print("✗数据库连接失败")
            return False
        
        if args:
            return create_func(conn, *args)
        else:
            return create_func(conn)
    except Exception as e:
        print(f"✗创建{table_name}失败: {e}")
        return False
    finally:
        if conn:
            return_db_connection(conn)

def print_final_results(results):
    """打印最终结果"""
    success = sum(results)
    print(f"\n{'='*50}")
    print(f"✓数据处理系统执行完成: {success}/3 个模块成功")
    nger_symbol = '✓' if results[0] else '✗'
    abs_symbol = '✓' if results[1] else '✗'
    cer_symbol = '✓' if results[2] else '✗'
    print(f"NGER: {nger_symbol}  |  ABS: {abs_symbol}  |  CER: {cer_symbol}")
    print(f"{'='*50}")

def main():
    """主函数"""
    print("=" * 50)
    print("数据获取和处理系统")
    print("NGER数据 + ABS经济数据 + CER电站数据")
    print("=" * 50)
    
    # 初始化地理编码缓存（提前加载缓存，减少API调用与磁盘I/O竞争）
    try:
        initialize_geocoding_cache(str(DATA_DIR / "geocoding_cache.json"))
    except Exception as e:
        print(f"⚠地理编码缓存初始化失败: {e}")
    
    # 初始化连接池
    pool = get_connection_pool(minconn=2, maxconn=15)
    if not pool:
        print("✗数据库连接池初始化失败")
        return
    
    try:
        print("先创建数据表，再获取、处理数据...")
        
        # # 创建NGER表
        # print("\n" + "=" * 20 + " 1. 创建NGER表 " + "=" * 20)
        # nger_table_ok = create_table_direct("NGER表", create_nger_table)
        # if not nger_table_ok:
        #     print("✗NGER表准备失败")
        #     return
        
        # # NGER数据获取和处理
        # print("\n" + "=" * 20 + " 2. NGER数据获取和处理 " + "=" * 20)
        # nger_ok = fetch_nger_data(max_workers=1)
        
        # # 创建CER表
        # print("\n" + "=" * 20 + " 3. 创建CER表 " + "=" * 20)
        # cer_table_ok = create_table_direct("CER表", create_cer_tables)
        # if not cer_table_ok:
        #     print("✗CER表准备失败")
        #     return
        
        # # CER电站数据获取和处理
        # print("\n" + "=" * 20 + " 4. CER电站数据获取和处理 " + "=" * 20)
        # cer_ok = fetch_cer_data()
    
        # ABS数据处理
        abs_file = fetch_abs_data()
        if abs_file:
            print("\n" + "=" * 20 + " 5. 创建ABS表 " + "=" * 20)
            abs_table_ok = create_table_direct("ABS表", create_all_abs_tables, str(abs_file))
            if not abs_table_ok:
                print("✗ABS表准备失败")
                return
            
            print("\n" + "=" * 20 + " 6. ABS经济数据获取和处理 " + "=" * 20)
            abs_ok = process_abs_data(str(abs_file), max_workers=10)
        else:
            abs_ok = False
        
        # 打印最终结果
        # print_final_results([nger_ok, abs_ok, cer_ok])
        
        # 保存地理编码缓存
        print("正在保存地理编码缓存...")
        try:
            save_global_cache()
            print("✓地理编码缓存已保存")
        except Exception as e:
            print(f"✗保存地理编码缓存失败: {e}")
        
    finally:
        # 关闭连接池
        close_connection_pool()

if __name__ == "__main__":
    main()