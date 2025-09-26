#!/usr/bin/env python3
"""State name standardization tool"""

# Standard library imports
from typing import Dict, Optional, Union

# Australian state name standardization mapping table
STATE_MAPPING = {
    # Full names mapped to abbreviations
    'New South Wales': 'NSW',
    'Victoria': 'VIC',
    'Queensland': 'QLD',
    'South Australia': 'SA',
    'Western Australia': 'WA',
    'Tasmania': 'TAS',
    'Northern Territory': 'NT',
    'Australian Capital Territory': 'ACT',
    
    # ABS numeric codes mapped to abbreviations
    '1': 'NSW',
    '2': 'VIC',
    '3': 'QLD',
    '4': 'SA',
    '5': 'WA',
    '6': 'TAS',
    '7': 'NT',
    '8': 'ACT',
    '9': 'OT',  # Other Territories
    
    # Other possible variants
    'New South Wales, Australia': 'NSW',
    'Victoria, Australia': 'VIC',
    'Queensland, Australia': 'QLD',
    'South Australia, Australia': 'SA',
    'Western Australia, Australia': 'WA',
    'Tasmania, Australia': 'TAS',
    'Northern Territory, Australia': 'NT',
    'Australian Capital Territory, Australia': 'ACT',
    
    # Lowercase variants
    'nsw': 'NSW',
    'vic': 'VIC',
    'qld': 'QLD',
    'sa': 'SA',
    'wa': 'WA',
    'tas': 'TAS',
    'nt': 'NT',
    'act': 'ACT',
    
    # Invalid values
    '-': None,
    'N/A': None,
    'NA': None,
    'nan': None,
    'None': None,
    '': None,
    None: None,
}

# Reverse mapping: from standard abbreviations to full names (for display)
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
    Standardize various formats of state names to English abbreviations
    Args:
        state_input: Input state name, can be string, number, or None
    Returns:
        Standardized state name abbreviation, returns None if unrecognizable
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
    
    # Convert to string and clean
    state_str = str(state_input).strip()
    
    # Handle empty strings
    if not state_str or state_str.lower() in {'', 'nan', 'none', 'null'}:
        return None
    
    # Direct mapping lookup
    if state_str in STATE_MAPPING:
        return STATE_MAPPING[state_str]
    
    # Try case-insensitive lookup and partial matching
    state_lower = state_str.lower()
    for key, value in STATE_MAPPING.items():
        if key.lower() == state_lower or (key.lower() in state_lower or state_lower in key.lower()):
            return value
    
    # If no match found, return None
    return None

def get_state_full_name(state_abbrev: str) -> Optional[str]:
    """
    Get the full name of a state abbreviation
    Args:
        state_abbrev: State name abbreviation
    Returns:
        Full state name, returns None if not found
    """
    return STATE_FULL_NAMES.get(state_abbrev)

def standardize_dataframe_states(df, state_column: str = 'state') -> None:
    """
    Standardize the state column in a DataFrame
    Args:
        df: pandas DataFrame
        state_column: State column name
    """
    if state_column in df.columns:
        df[state_column] = df[state_column].apply(standardize_state_name)

def get_state_statistics(df, state_column: str = 'state') -> Dict:
    """
    Get state name statistics
    Args:
        df: pandas DataFrame
        state_column: State column name
    Returns:
        Dictionary containing state name statistics
    """
    if state_column not in df.columns:
        return {'error': f'Column {state_column} not found'}
    
    # Standardize state names
    standardized = df[state_column].apply(standardize_state_name)
    
    # Statistics
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
    Validate the quality of state name data
    Args:
        df: pandas DataFrame
        state_column: State column name
    Returns:
        Validation result dictionary
    """
    if state_column not in df.columns:
        return {'valid': False, 'error': f'Column {state_column} not found'}
    
    # Get original state names
    original_states = df[state_column].dropna().unique()
    
    # Standardize state names
    standardized = df[state_column].apply(standardize_state_name)
    valid_states = standardized.dropna().unique()
    
    # Find unstandardizable state names (avoid duplicate standardize_state_name calls)
    invalid_states = [state for state in original_states if standardize_state_name(state) is None]
    
    validation_result = {
        'valid': len(invalid_states) == 0,
        'total_unique_original': len(original_states),
        'total_unique_standardized': len(valid_states),
        'invalid_states': invalid_states,
        'valid_states': sorted(valid_states),
        'coverage_rate': len(valid_states) / len(original_states) if len(original_states) > 0 else 0
    }
    
    return validation_result