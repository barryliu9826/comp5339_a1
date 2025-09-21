#!/usr/bin/env python3
"""Excel处理工具"""

import pandas as pd
import openpyxl


def read_merged_headers(file_path: str, sheet_name: str) -> pd.DataFrame:
    """读取合并表头"""
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]
    merged_ranges = list(ws.merged_cells.ranges)
    
    column_names = []
    for col in range(1, ws.max_column + 1):
        parts = []
        for row in [6, 7]:
            merged_cell = next((r for r in merged_ranges 
                              if r.min_row <= row <= r.max_row and r.min_col <= col <= r.max_col), None)
            
            if merged_cell:
                cell_value = ws.cell(merged_cell.min_row, merged_cell.min_col).value
            else:
                cell_value = ws.cell(row, col).value
            
            if cell_value and str(cell_value).strip():
                part = str(cell_value).strip()
                if part not in parts:
                    parts.append(part)
        
        column_names.append(" - ".join(parts) if parts else f"Column_{col}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, skiprows=7)
    df.columns = column_names[:len(df.columns)]
    return df


def get_merged_cells(file_path: str, sheet_name: str):
    """获取合并单元格"""
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]
    merged_ranges = list(ws.merged_cells.ranges)
    
    cells = []
    for merged_range in merged_ranges:
        if merged_range.min_row == 6:  # 只处理第6行的合并单元格
            cell_value = ws.cell(merged_range.min_row, merged_range.min_col).value
            if cell_value and str(cell_value).strip():
                cells.append({
                    'value': str(cell_value).strip(),
                    'start_col': merged_range.min_col,
                    'end_col': merged_range.max_col + 1
                })
    
    return cells