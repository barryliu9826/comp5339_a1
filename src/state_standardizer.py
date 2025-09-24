#!/usr/bin/env python3
"""州名标准化工具"""

import re
from typing import Dict, Optional, Union

# 澳大利亚州名标准化映射表
STATE_MAPPING = {
    # 英文缩写 (标准格式)
    'NSW': 'NSW',
    'VIC': 'VIC', 
    'QLD': 'QLD',
    'SA': 'SA',
    'WA': 'WA',
    'TAS': 'TAS',
    'NT': 'NT',
    'ACT': 'ACT',
    
    # 全名映射到缩写
    'New South Wales': 'NSW',
    'Victoria': 'VIC',
    'Queensland': 'QLD',
    'South Australia': 'SA',
    'Western Australia': 'WA',
    'Tasmania': 'TAS',
    'Northern Territory': 'NT',
    'Australian Capital Territory': 'ACT',
    
    # ABS数字代码映射到缩写
    '1': 'NSW',
    '2': 'VIC',
    '3': 'QLD',
    '4': 'SA',
    '5': 'WA',
    '6': 'TAS',
    '7': 'NT',
    '8': 'ACT',
    '9': 'OT',  # Other Territories
    
    # 其他可能的变体
    'New South Wales, Australia': 'NSW',
    'Victoria, Australia': 'VIC',
    'Queensland, Australia': 'QLD',
    'South Australia, Australia': 'SA',
    'Western Australia, Australia': 'WA',
    'Tasmania, Australia': 'TAS',
    'Northern Territory, Australia': 'NT',
    'Australian Capital Territory, Australia': 'ACT',
    
    # 小写变体
    'nsw': 'NSW',
    'vic': 'VIC',
    'qld': 'QLD',
    'sa': 'SA',
    'wa': 'WA',
    'tas': 'TAS',
    'nt': 'NT',
    'act': 'ACT',
    
    # 无效值
    '-': None,
    'N/A': None,
    'NA': None,
    'nan': None,
    'None': None,
    '': None,
    None: None,
}

# 反向映射：从标准缩写到全名（用于显示）
STATE_FULL_NAMES = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland', 
    'SA': 'South Australia',
    'WA': 'Western Australia',
    'TAS': 'Tasmania',
    'NT': 'Northern Territory',
    'ACT': 'Australian Capital Territory',
    'OT': 'Other Territories'
}

def standardize_state_name(state_input: Union[str, int, None]) -> Optional[str]:
    """
    将各种格式的州名标准化为英文缩写格式
    
    Args:
        state_input: 输入的州名，可能是字符串、数字或None
        
    Returns:
        标准化的州名缩写，如果无法识别则返回None
        
    Examples:
        >>> standardize_state_name('New South Wales')
        'NSW'
        >>> standardize_state_name('1')
        'NSW'
        >>> standardize_state_name('nsw')
        'NSW'
        >>> standardize_state_name('-')
        None
    """
    if state_input is None:
        return None
    
    # 转换为字符串并清理
    state_str = str(state_input).strip()
    
    # 处理空字符串
    if not state_str or state_str.lower() in {'', 'nan', 'none', 'null'}:
        return None
    
    # 直接查找映射
    if state_str in STATE_MAPPING:
        return STATE_MAPPING[state_str]
    
    # 尝试大小写不敏感的查找
    try:
        state_lower = state_str.lower()
        for key, value in STATE_MAPPING.items():
            if key.lower() == state_lower:
                return value
    except AttributeError:
        # 如果state_str不是字符串，直接返回None
        return None
    
    # 尝试部分匹配（处理可能包含额外信息的字符串）
    try:
        for key, value in STATE_MAPPING.items():
            if key.lower() in state_lower or state_lower in key.lower():
                return value
    except AttributeError:
        # 如果state_str不是字符串，直接返回None
        return None
    
    # 如果都无法匹配，返回None
    return None

def get_state_full_name(state_abbrev: str) -> Optional[str]:
    """
    获取州名缩写的全名
    
    Args:
        state_abbrev: 州名缩写
        
    Returns:
        州名全名，如果无法找到则返回None
    """
    return STATE_FULL_NAMES.get(state_abbrev)

def standardize_dataframe_states(df, state_column: str = 'state') -> None:
    """
    标准化DataFrame中的州名列
    
    Args:
        df: pandas DataFrame
        state_column: 州名列名
    """
    if state_column in df.columns:
        df[state_column] = df[state_column].apply(standardize_state_name)

def get_state_statistics(df, state_column: str = 'state') -> Dict:
    """
    获取州名统计信息
    
    Args:
        df: pandas DataFrame
        state_column: 州名列名
        
    Returns:
        包含州名统计信息的字典
    """
    if state_column not in df.columns:
        return {'error': f'Column {state_column} not found'}
    
    # 标准化州名
    standardized = df[state_column].apply(standardize_state_name)
    
    # 统计
    stats = {
        'total_records': len(df),
        'valid_states': standardized.notna().sum(),
        'invalid_states': standardized.isna().sum(),
        'state_counts': standardized.value_counts().to_dict(),
        'unique_states': sorted(standardized.dropna().unique())
    }
    
    return stats

def validate_state_data(df, state_column: str = 'state') -> Dict:
    """
    验证州名数据的质量
    
    Args:
        df: pandas DataFrame
        state_column: 州名列名
        
    Returns:
        验证结果字典
    """
    if state_column not in df.columns:
        return {'valid': False, 'error': f'Column {state_column} not found'}
    
    # 获取原始州名
    original_states = df[state_column].dropna().unique()
    
    # 标准化州名
    standardized = df[state_column].apply(standardize_state_name)
    valid_states = standardized.dropna().unique()
    
    # 找出无法标准化的州名
    invalid_states = []
    for state in original_states:
        if standardize_state_name(state) is None:
            invalid_states.append(state)
    
    validation_result = {
        'valid': len(invalid_states) == 0,
        'total_unique_original': len(original_states),
        'total_unique_standardized': len(valid_states),
        'invalid_states': invalid_states,
        'valid_states': sorted(valid_states),
        'coverage_rate': len(valid_states) / len(original_states) if len(original_states) > 0 else 0
    }
    
    return validation_result
