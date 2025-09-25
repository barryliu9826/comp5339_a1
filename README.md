# Australian Economic Data Acquisition and Processing System

## Project Overview

This is a comprehensive data acquisition, processing, and analysis system for automatically downloading, processing, storing, and visualizing Australian economic and energy data. The system integrates three main data sources, stores all data uniformly in a PostgreSQL database, and provides powerful data cleaning and exploratory data analysis (EDA) capabilities.

## Data Sources

### 1. NGER Data (National Greenhouse and Energy Reporting)
- **Data Years**: 2013-14 to 2023-24 (11 years)
- **Data Format**: JSON API responses
- **Storage Method**: Separate tables by year (`nger_2013_14`, `nger_2014_15`, ...)
- **Data Volume**: Approximately 400-800 facility records per year

### 2. ABS Data (Australian Bureau of Statistics)
- **Data File**: `14100DO0003_2011-24.xlsx`
- **Data Content**: Australian economic and industry data
- **Geographic Levels**: 
  - **State Level Data** (geographic_level = 0): State, territory, and statistical area level
  - **Local Government Level Data** (geographic_level = 1): Local government area level
- **Storage Method**: Separate tables by business type, each containing data from both geographic levels
- **Data Volume**: State level ~29,097 rows, local government level ~5,477 rows

### 3. CER Data (Clean Energy Regulator)
- **Data Source**: CER official website table data
- **Data Types**: 
  - `cer_approved_power_stations`: Approved power stations (~280 records)
  - `cer_committed_power_stations`: Committed power stations (~35 records)
  - `cer_probable_power_stations`: Probable power stations (~49 records)
- **Acquisition Method**: Selenium automated web scraping

## System Architecture

### Core Files

```
src/
├── data_acquisition_processor.py  # Main processing program (517 lines)
├── database_config.py            # Database configuration and operations (1044 lines)
├── geocoding.py                  # Geocoding and caching (665 lines)
├── excel_utils.py                # Excel processing tools (86 lines)
├── state_standardizer.py         # State name standardization tools (189 lines)
├── time_format_utils.py          # Time format processing tools (86 lines)
├── data_cleaner.py               # Unified data cleaning module (1169 lines)
└── eda_visualization.py          # Exploratory data analysis visualization (215 lines)

data/
├── nger_data_api_links.csv       # NGER API links
├── 14100DO0003_2011-24.xlsx     # ABS Excel data
├── geocoding_cache.json         # Geocoding persistent cache
└── eda/                          # EDA visualization output directory
    ├── abs_overview_geographic_level.png
    ├── cer_map_categories.png
    └── nger_map_by_fuel.png
```

### Technology Stack

- **Python 3.11+** (conda environment: comp5339)
- **Data Processing**: pandas>=2.3.0, openpyxl>=3.1.0, numpy>=2.3.0
- **Data Visualization**: matplotlib>=3.9.0, seaborn>=0.13.2
- **Network Requests**: requests>=2.32.0
- **Web Scraping**: selenium>=4.35.0 (Chrome WebDriver)
- **Database**: PostgreSQL (psycopg2-binary>=2.9.0)
- **Geocoding**: Nominatim API (via requests)
- **Excel Processing**: openpyxl (merged cell parsing)
- **Type Hints**: typing-extensions>=4.0.0

## Database Design

### Connection Configuration
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres', 
    'password': 'postgre',
    'database': 'postgres'
}
```

### Table Structure Design

#### NGER Tables (separate tables by year)
```sql
CREATE TABLE nger_2023_24 (
    id SERIAL PRIMARY KEY,
    facility_name TEXT,
    state TEXT,
    postcode TEXT,
    -- Other dynamic columns...
);
```

#### ABS Tables (separate tables by business type)
```sql
CREATE TABLE business_entries__year_ended_30_june (
    id SERIAL PRIMARY KEY,
    code TEXT,                    -- Region code
    label TEXT,                   -- Region name
    year INTEGER,                 -- Year
    geographic_level INTEGER,     -- Geographic level (0=state, 1=local government)
    -- Business data columns...
);
```

#### CER Tables (separate tables by power station type)
```sql
CREATE TABLE cer_approved_power_stations (
    id SERIAL PRIMARY KEY,
    accreditation_code TEXT,
    power_station_name TEXT,
    state TEXT,
    postcode TEXT
);
```

### Geographic Level Encoding

| Code | Meaning | Data Source | Record Count |
|------|---------|-------------|--------------|
| 0 | State Level | Table 1 | ~29,097 rows |
| 1 | Local Government Level | Table 2 | ~5,477 rows |

## Usage Instructions

### Environment Setup

```bash
# Activate conda environment
conda activate comp5339

# Ensure PostgreSQL is running
# Username: postgres, Password: postgre, Port: 5432
```

### Using pip to install dependencies (Recommended)

Use built-in `venv` and `pip` to install all dependencies:

```bash
# 1) Create and activate virtual environment (macOS/Linux)
python3 -m venv .venv
source .venv/bin/activate

# 2) Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 3) Run the program
python src/data_acquisition_processor.py
```

### Using conda environment (Optional)

```bash
# Activate conda environment
conda activate comp5339

# Install dependencies
pip install -r requirements.txt

# Run the program
python src/data_acquisition_processor.py
```

### Development Environment Requirements

- **Python 3.11+** (recommended)
- **Google Chrome**: Selenium 4.20+ uses Selenium Manager by default to automatically download drivers, no manual chromedriver configuration needed
- **PostgreSQL**: Locally available with account configuration matching `DB_CONFIG` in `src/database_config.py`
- **Network Environment**: If network is restricted, geocoding (Nominatim) may fail or be slow; can retry multiple times or run database-only processes offline

### Running Data Acquisition

```bash
# Run complete data acquisition and processing workflow
python src/data_acquisition_processor.py
```

### Running EDA Visualization Analysis

```bash
# Generate exploratory data analysis charts
python src/eda_visualization.py
```

This will generate the following visualization charts and save them to the `data/eda/` directory:
- **CER Power Station Distribution Map** (`cer_map_categories.png`): Shows approved, committed, and probable power station locations by category
- **NGER Facility Distribution Map** (`nger_map_by_fuel.png`): Shows geographic distribution of NGER facilities by primary fuel type
- **ABS Geographic Level Overview** (`abs_overview_geographic_level.png`): Shows distribution of state-level and local government-level data in ABS data

### Data Query Examples

```sql
-- Query 2023 state-level business data
SELECT * FROM business_entries__year_ended_30_june 
WHERE geographic_level = 0 AND year = 2023;

-- Count data by different levels
SELECT 
    CASE geographic_level 
        WHEN 0 THEN 'State Level'
        WHEN 1 THEN 'Local Government Level'
    END as level_name,
    COUNT(*) as count
FROM business_entries__year_ended_30_june 
GROUP BY geographic_level;

-- Query NGER facilities for specific year
SELECT facility_name, state, postcode 
FROM nger_2023_24 
WHERE state = 'NSW';

-- Query approved power stations
SELECT power_station_name, state 
FROM cer_approved_power_stations 
WHERE state = 'NSW';
```

## System Features

### 🚀 Performance Optimization
- **Connection Sharing**: All data sources share a single database connection
- **Batch Insertion**: 10,000 records per batch, optimized for large data processing
- **Numeric Encoding**: Uses INTEGER type instead of TEXT, improving query performance
- **Smart Deduplication**: Automatically handles duplicate column names and empty columns
- **Geocoding Cache**: Memory + file dual-layer cache, avoiding duplicate calls, improving speed
- **Data Cleaning Optimization**: Unified data cleaning module, improving data processing efficiency

### 🛡️ Error Handling
- **Connection Management**: Automatic retry and connection recovery
- **Transaction Rollback**: Automatic rollback on operation failure
- **Data Validation**: Automatically filters invalid data and empty columns
- **Exception Logging**: Detailed error information and processing status
- **Cache Safety**: Direct overwrite when saving cache, no longer creates .backup files

### 🔧 Code Quality
- **Modular Design**: Separation of database operations and business logic
- **Function Reuse**: Unified column name cleaning and data processing logic
- **Type Safety**: Strict type annotations and parameter validation
- **Complete Documentation**: Detailed function documentation and usage instructions
- **Data Visualization**: Professional EDA visualization module supporting multiple chart types

## Data Acquisition Status

### ✅ Completed
- [x] NGER data download (2013-2024, 11 files)
- [x] ABS economic data download and processing
- [x] CER website data scraping
- [x] PostgreSQL database integration
- [x] Table structure optimization and unification
- [x] Batch data insertion optimization
- [x] Connection sharing and performance optimization
- [x] Code modularization and simplification
- [x] Unified data cleaning module
- [x] Exploratory data analysis visualization

### 📊 Data Statistics
- **NGER Tables**: 11 tables (by year: 2013-14 to 2023-24)
- **ABS Tables**: 15 tables (by business type, containing both geographic levels)
- **CER Tables**: 3 tables (by power station type: approved, committed, probable)
- **Total Data Volume**: Approximately 600,000+ records
- **Geocoding Cache**: Automatic caching, avoiding duplicate API calls
- **EDA Visualization**: 3 professional chart types, supporting geographic distribution and statistical analysis

## Project File Descriptions

### Core Processing Files
- **`src/data_acquisition_processor.py`** (517 lines): Main data acquisition and processing program
  - NGER data download and JSON processing
  - ABS Excel file reading and merged cell parsing
  - CER website data scraping and pagination handling
  - Unified PostgreSQL data storage
  - Multi-threaded concurrent processing optimization

- **`src/database_config.py`** (1044 lines): Database configuration and operations module
  - Database connection pool management
  - Table creation and data insertion functions
  - Column name cleaning and deduplication logic
  - Batch insertion optimization
  - Thread-safe database operations

- **`src/geocoding.py`** (665 lines): Geocoding and caching module
  - Nominatim API integration
  - Memory + file dual-layer cache
  - Multi-threaded geocoding processing
  - Thread-safe cache management

- **`src/excel_utils.py`** (86 lines): Excel processing tools module
  - Merged cell parsing
  - Multi-level header processing
  - Dynamic column name generation

- **`src/state_standardizer.py`** (189 lines): State name standardization tools module
  - Australian state name standardization mapping
  - Supports English full names, abbreviations, numeric code conversion
  - DataFrame batch state name standardization processing
  - Error handling and data cleaning

- **`src/time_format_utils.py`** (86 lines): Time format processing tools module
  - NGER year label splitting (e.g., "2023-24" → start_year, stop_year)
  - CER time format standardization processing
  - ABS time data format unification
  - Automatic recognition and conversion of multiple time formats

- **`src/data_cleaner.py`** (1169 lines): Unified data cleaning module
  - Comprehensive data cleaning and normalization functions
  - Missing value handling and data type conversion
  - String cleaning and standardization
  - Data quality validation and repair
  - Unified cleaning workflow supporting multiple data sources

- **`src/eda_visualization.py`** (215 lines): Exploratory data analysis visualization module
  - CER power station geographic distribution visualization
  - NGER facility distribution by fuel type
  - ABS data geographic level statistical analysis
  - Professional matplotlib and seaborn chart generation
  - Automatic saving of high-resolution charts to specified directory

### Data Files
- **`data/nger_data_api_links.csv`**: NGER data API links
- **`data/14100DO0003_2011-24.xlsx`**: ABS economic data Excel file
- **`data/geocoding_cache.json`**: Geocoding persistent cache file
- **`data/eda/`**: EDA visualization output directory
  - `abs_overview_geographic_level.png`: ABS geographic level distribution chart
  - `cer_map_categories.png`: CER power station category distribution chart
  - `nger_map_by_fuel.png`: NGER facility fuel type distribution chart

## Technical Highlights

### 1. Excel Merged Cell Processing
- Automatically identifies and parses merged cells in Excel
- Supports multi-level header structures
- Dynamically generates column names and table structures

### 2. Web Data Scraping
- Selenium automated scraping of dynamic web content
- Intelligent pagination detection and processing
- Automatic table type identification

### 3. Database Optimization
- Dynamic table structure generation
- Batch data insertion optimization
- Intelligent column name cleaning and deduplication
- Connection pool management

### 4. Code Architecture
- Modular design with separated responsibilities
- Unified error handling mechanism
- Extensible data source integration architecture
- Complete logging and monitoring

### 5. Geocoding and Caching
- Uses Nominatim API (respects rate limits, ~1.1 seconds/request)
- `src/geocoding.py` provides geocoding and caching (memory + JSON persistence)
- Thread-safe global cache in multi-threaded environments
- Automatically saves `data/geocoding_cache.json` at end of main process

### 6. Data Cleaning and Quality Assurance
- `src/data_cleaner.py` provides unified data cleaning and normalization functions
- Supports cleaning workflows for multiple data sources
- Automatically handles missing values, data type conversion, and string standardization
- Data quality validation and repair mechanisms

### 7. Exploratory Data Analysis Visualization
- `src/eda_visualization.py` provides professional data visualization functions
- Supports geographic distribution maps, statistical charts, and other visualization types
- Uses matplotlib and seaborn to generate high-quality charts
- Automatically saves high-resolution images to specified directory

## Development History

### Major Milestones
1. **Data Acquisition**: Implemented automatic download from three data sources
2. **Excel Processing**: Solved complex merged cell parsing problems
3. **Database Integration**: Unified data storage to PostgreSQL
4. **Performance Optimization**: Batch insertion and connection sharing optimization
5. **Code Refactoring**: Modularization and simplification optimization
6. **Business Optimization**: Numeric encoding and table structure unification
7. **Data Cleaning**: Unified data cleaning and quality assurance module
8. **Data Visualization**: Exploratory data analysis and visualization functionality

### Technical Challenge Solutions
- ✅ NGER data download loop issue fix
- ✅ Excel merged cell multi-level header parsing
- ✅ CER website dynamic content scraping and pagination handling
- ✅ PostgreSQL batch insertion performance optimization
- ✅ Column name duplication and special character handling
- ✅ Database connection management and transaction processing
- ✅ Unified data cleaning and quality assurance mechanism
- ✅ Professional data visualization and chart generation

## Future Extensions

### Features to Implement
- [ ] Data quality monitoring and reporting
- [ ] Automated data update scheduling
- [ ] Data visualization dashboard
- [ ] API interface development

### Performance Optimization Opportunities
- [ ] Database index optimization
- [ ] Query performance tuning
- [ ] Concurrent processing optimization

## Geocoding and Cache Usage Instructions

### Automatic Integration (Recommended)

Running the main processing program will automatically geocode CER tables and write to cache:

```bash
python src/data_acquisition_processor.py
```

### Using in Code

```python
from geocoding import Geocoder, save_global_cache

geocoder = Geocoder(use_persistent_cache=True)
result = geocoder.geocode_query("Sydney, NSW, Australia")

# Can manually save before exit (main process already auto-saves)
save_global_cache()
```

### Cache File

- Location: `data/geocoding_cache.json`
- Save Strategy: Direct overwrite save, no longer generates `.backup` files

## Project Status

### Current Version Features
- ✅ Complete eight-module architecture (data acquisition, database, geocoding, Excel processing, state standardization, time format processing, data cleaning, EDA visualization)
- ✅ Multi-threaded concurrent processing optimization
- ✅ Geocoding cache system
- ✅ Excel merged cell intelligent parsing
- ✅ Database connection pool management
- ✅ Thread-safe operation design
- ✅ State name standardization and time format unified processing
- ✅ Unified data cleaning and quality assurance
- ✅ Professional data visualization and EDA analysis

### Recent Updates (September 26, 2024)
- 🆕 Added `data_cleaner.py` module (1169 lines): Unified data cleaning and quality assurance module
- 🆕 Added `eda_visualization.py` module (215 lines): Exploratory data analysis visualization module
- 🆕 Added `state_standardizer.py` module (189 lines): Australian state name standardization tool
- 🆕 Added `time_format_utils.py` module (86 lines): Time format processing tool
- 📝 Expanded to eight-module architecture, enhanced data processing and visualization capabilities
- 📝 Updated file line count statistics for all modules
- 📝 Improved module function descriptions and technical documentation
- 📝 Updated requirements.txt to include data visualization dependencies
- 📝 Added EDA visualization output directory and chart descriptions

## Contact Information

- **Project**: COMP5339 Assignment 1
- **Environment**: conda comp5339 or Python venv
- **Database**: PostgreSQL localhost:5432

---

*Last updated: September 26, 2024*
