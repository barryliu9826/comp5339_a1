# Australian Energy and Economic Data Analysis System

A comprehensive data analysis system for processing and analyzing Australian energy infrastructure and economic data from multiple government sources including NGER (National Greenhouse and Energy Reporting), CER (Clean Energy Regulator), and ABS (Australian Bureau of Statistics).

## Project Overview

This project provides a complete data pipeline for acquiring, processing, geocoding, and analyzing Australian energy and economic datasets. It features automated data collection, spatial analysis capabilities, and comprehensive visualization tools for understanding the relationship between energy infrastructure and regional economic indicators.

## Data Sources

- **NGER (National Greenhouse and Energy Reporting)**: Power facility emissions and energy production data
- **CER (Clean Energy Regulator)**: Approved, committed, and probable renewable energy power stations
- **ABS (Australian Bureau of Statistics)**: Regional economic and demographic data including:
  - Agricultural commodities
  - Building approvals
  - Business entries/exits
  - Employment statistics
  - Property transfers
  - Personal insolvencies
  - Dwelling stock estimates
  - Industry employment data
  - Business turnover analysis
  - Gross agricultural production values

## System Architecture

### Database
- **PostgreSQL with PostGIS extension** for spatial data processing
- **20+ business tables** storing structured data from multiple sources
- **Spatial indexing** for efficient geographic queries
- **Proximity matching** between NGER facilities and CER power stations

### Core Components

#### Data Acquisition (`data_processor.py`)
- Automated web scraping using Selenium
- Multi-threaded data collection
- Excel file processing with merged cell handling
- Robust error handling and retry mechanisms

#### Data Processing (`data_cleaner.py`)
- Data cleaning and standardization
- State name normalization
- Time format processing
- Data type conversion and validation

#### Geocoding (`geocoding.py`)
- Google Maps API integration
- Intelligent caching system
- Rate limiting and quota management
- Batch processing capabilities

#### Database Operations (`database_utils.py`)
- Connection pooling for performance
- Schema migration management
- Bulk data insertion
- Spatial data operations

#### Visualization (`eda_visualization.py`)
- Interactive data exploration
- Geographic mapping
- Statistical analysis charts
- Export capabilities

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 12+ with PostGIS extension
- Google Maps API key (for geocoding)

### Installation

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL database**
   ```sql
   CREATE DATABASE your_database_name;
   CREATE EXTENSION postgis;
   ```

3. **Configure database connection**
   Update the `DB_CONFIG` in `src/database_utils.py`:
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'port': 5432,
       'user': 'your_username',
       'password': 'your_password',
       'database': 'your_database_name'
   }
   ```

4. **Set up Google Maps API key**
   Update the `HARDCODED_GOOGLE_MAPS_API_KEY` in `src/geocoding.py` with your API key.

### Usage

1. **Run data acquisition**
   ```bash
   python src/data_processor.py
   ```

2. **Generate visualizations**
   ```bash
   python src/eda_visualization.py
   ```

## Project Structure

```
comp5339_a1/
├── src/                          # Source code
│   ├── data_processor.py             # Data collection
│   ├── data_cleaner.py               # Data cleaning
│   ├── database_utils.py             # Database operations
│   ├── eda_visualization.py          # Visualization
│   ├── excel_utils.py                # Excel processing
│   ├── geocoding.py                  # Geocoding services
│   ├── state_standardizer.py         # State normalization
│   └── time_format_utils.py          # Time processing
├── data/                         # Data directory
│   ├── eda/                      # Generated visualizations
│   ├── geocoding_cache.json      # Geocoding cache
│   ├── nger_data_api_links.csv   # Data source links
│   └── *.xlsx                    # Raw data files
└── requirements.txt              # Python dependencies
```

## Key Features

### Data Acquisition
- **Automated scraping** of government data sources
- **Multi-threaded processing** for improved performance
- **Error handling** with retry mechanisms
- **Progress tracking** for long-running operations

### Geocoding
- **Google Maps API integration** for accurate geocoding
- **Intelligent caching** to minimize API calls
- **Rate limiting** to respect API quotas
- **Batch processing** for efficiency

### Spatial Analysis
- **PostGIS integration** for advanced spatial queries
- **Proximity matching** between facilities
- **Geographic visualization** capabilities
- **Spatial indexing** for performance

### Data Visualization
- **Interactive charts** and graphs
- **Geographic mapping** with fuel type analysis
- **Statistical analysis** tools
- **Export capabilities** for reports

## Database Schema

The system uses a comprehensive PostgreSQL schema with 20+ tables:

- **NGER Tables**: Power facility data with emissions and production metrics
- **CER Tables**: Renewable energy power station data (approved, committed, probable)
- **ABS Tables**: Economic and demographic data by region
- **Proximity Tables**: Spatial relationships between facilities

See `Complete_Database_Schema.md` for detailed schema documentation.

## Configuration

### Database Configuration
Update `DB_CONFIG` in `database_utils.py` to match your PostgreSQL setup.

### API Configuration
Set your Google Maps API key in `geocoding.py` for geocoding functionality.

### Data Sources
Configure data source URLs in `nger_data_api_links.csv` for automated data collection.

## Sample Visualizations

The system generates various visualizations including:
- Geographic distribution of power facilities by fuel type
- Proximity analysis between NGER and CER facilities

## Data Analysis Capabilities

- **Spatial Analysis**: Find facilities within specific distances
- **Economic Correlation**: Analyze relationships between energy infrastructure and economic indicators
- **Temporal Analysis**: Track changes over time
- **Regional Comparison**: Compare different states and territories

## Important Notes

- **API Quotas**: The Google Maps API has usage limits. The system includes rate limiting to manage this.
- **Data Freshness**: Government data sources may update periodically. The system can be re-run to collect latest data.
- **Geographic Accuracy**: Geocoding accuracy depends on address quality in source data.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request