# Complete Database Schema Documentation

## Database Overview
- **Database Type**: PostgreSQL with PostGIS extension
- **Total Tables**: 20 business tables + 1 system table (spatial_ref_sys)
- **Main Data Sources**: NGER (National Greenhouse and Energy Reporting), CER (Clean Energy Regulator), ABS (Australian Bureau of Statistics)

---

## 1. NGER Tables (nger_unified)

**Purpose**: Stores National Greenhouse and Energy Reporting data, including detailed facility information, emissions data, and geographic information

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| id | integer | NOT NULL | Primary key, auto-increment |
| year_label | character varying | NULL | Year label |
| start_year | integer | NULL | Start year |
| stop_year | integer | NULL | Stop year |
| facility_name | character varying | NULL | Facility name |
| state | character varying | NULL | State/Territory |
| facility_type | character varying | NULL | Facility type |
| primary_fuel | character varying | NULL | Primary fuel |
| reporting_entity | character varying | NULL | Reporting entity |
| electricity_production_gj | numeric | NULL | Electricity production (GJ) |
| electricity_production_mwh | numeric | NULL | Electricity production (MWh) |
| emission_intensity_tco2e_mwh | numeric | NULL | Emission intensity (tCO2e/MWh) |
| scope1_emissions_tco2e | numeric | NULL | Scope 1 emissions (tCO2e) |
| scope2_emissions_tco2e | numeric | NULL | Scope 2 emissions (tCO2e) |
| total_emissions_tco2e | numeric | NULL | Total emissions (tCO2e) |
| grid_info | character varying | NULL | Grid information |
| grid_connected | boolean | NULL | Grid connected status |
| important_notes | text | NULL | Important notes |
| lat | numeric | NULL | Latitude |
| lon | numeric | NULL | Longitude |
| formatted_address | character varying | NULL | Formatted address |
| place_id | character varying | NULL | Place ID |
| postcode | character varying | NULL | Postcode |
| bbox_south | numeric | NULL | Bounding box south |
| bbox_north | numeric | NULL | Bounding box north |
| bbox_west | numeric | NULL | Bounding box west |
| bbox_east | numeric | NULL | Bounding box east |
| geom | USER-DEFINED | NULL | Point geometry (PostGIS) |
| geom_bbox | USER-DEFINED | NULL | Bounding box geometry (PostGIS) |

---

## 2. CER Table Group

### 2.1 CER Approved Power Stations (cer_approved_power_stations)

**Purpose**: Stores approved clean energy power station data

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| id | integer | NOT NULL | Primary key, auto-increment |
| accreditation_code | character varying | NULL | Accreditation code |
| power_station_name | character varying | NULL | Power station name |
| state | character varying | NULL | State/Territory |
| postcode | character varying | NULL | Postcode |
| lat | numeric | NULL | Latitude |
| lon | numeric | NULL | Longitude |
| formatted_address | character varying | NULL | Formatted address |
| place_id | character varying | NULL | Place ID |
| bbox_south | numeric | NULL | Bounding box south |
| bbox_north | numeric | NULL | Bounding box north |
| bbox_west | numeric | NULL | Bounding box west |
| bbox_east | numeric | NULL | Bounding box east |
| geom | USER-DEFINED | NULL | Point geometry (PostGIS) |
| geom_bbox | USER-DEFINED | NULL | Bounding box geometry (PostGIS) |

### 2.2 CER Committed Power Stations (cer_committed_power_stations)

**Purpose**: Stores committed clean energy power station data

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| id | integer | NOT NULL | Primary key, auto-increment |
| project_name | character varying | NULL | Project name |
| state | character varying | NULL | State/Territory |
| postcode | character varying | NULL | Postcode |
| mw_capacity | numeric | NULL | Capacity (MW) |
| fuel_source | character varying | NULL | Fuel source |
| committed_date | character varying | NULL | Committed date |
| committed_date_year | integer | NULL | Committed date year |
| committed_date_month | integer | NULL | Committed date month |
| lat | numeric | NULL | Latitude |
| lon | numeric | NULL | Longitude |
| formatted_address | character varying | NULL | Formatted address |
| place_id | character varying | NULL | Place ID |
| bbox_south | numeric | NULL | Bounding box south |
| bbox_north | numeric | NULL | Bounding box north |
| bbox_west | numeric | NULL | Bounding box west |
| bbox_east | numeric | NULL | Bounding box east |
| geom | USER-DEFINED | NULL | Point geometry (PostGIS) |
| geom_bbox | USER-DEFINED | NULL | Bounding box geometry (PostGIS) |

### 2.3 CER Probable Power Stations (cer_probable_power_stations)

**Purpose**: Stores probable clean energy power station data

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| id | integer | NOT NULL | Primary key, auto-increment |
| project_name | character varying | NULL | Project name |
| state | character varying | NULL | State/Territory |
| postcode | character varying | NULL | Postcode |
| mw_capacity | numeric | NULL | Capacity (MW) |
| fuel_source | character varying | NULL | Fuel source |
| lat | numeric | NULL | Latitude |
| lon | numeric | NULL | Longitude |
| formatted_address | character varying | NULL | Formatted address |
| place_id | character varying | NULL | Place ID |
| bbox_south | numeric | NULL | Bounding box south |
| bbox_north | numeric | NULL | Bounding box north |
| bbox_west | numeric | NULL | Bounding box west |
| bbox_east | numeric | NULL | Bounding box east |
| geom | USER-DEFINED | NULL | Point geometry (PostGIS) |
| geom_bbox | USER-DEFINED | NULL | Bounding box geometry (PostGIS) |

---

## 3. ABS Table Group (Australian Bureau of Statistics Data)

All ABS tables have the following common field structure:

### Common Fields
| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| id | integer | NOT NULL | Primary key, auto-increment |
| code | character varying | NULL | Area code |
| label | character varying | NULL | Area label |
| year | integer | NULL | Year |
| geographic_level | integer | NULL | Geographic level (0=State, 1=Local Government) |

### 3.1 ABS Agricultural Commodities (abs_agricultural_commodities_year_ended_30_june)

**Purpose**: Agricultural commodities data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| area_of_holding_total_area_ha | numeric | Total holding area (hectares) |
| dairy_cattle_total_no | numeric | Total dairy cattle number |
| meat_cattle_total_no | numeric | Total meat cattle number |
| sheep_and_lambs_total_no | numeric | Total sheep and lambs number |
| pigs_total_no | numeric | Total pigs number |
| meat_chickens_total_no | numeric | Total meat chickens number |
| broadacre_crops_total_area_ha | numeric | Total broadacre crops area (hectares) |
| vegetables_total_area_ha | numeric | Total vegetables area (hectares) |
| orchard_fruit_trees_and_nut_trees_produce_intended_for_sale | numeric | Orchard fruit trees and nut trees produce intended for sale |
| agricultural_production_total_gross_value_dollar_m | numeric | Agricultural production total gross value (million AUD) |

### 3.2 ABS Building Approvals (abs_building_approvals_year_ended_30_june)

**Purpose**: Building approvals data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| private_sector_houses_no | integer | Private sector houses number |
| private_sector_dwellings_excluding_houses_no | integer | Private sector dwellings excluding houses number |
| total_private_sector_dwelling_units_no | integer | Total private sector dwelling units number |
| total_dwelling_units_no | integer | Total dwelling units number |
| value_of_private_sector_houses_dollar_m | integer | Value of private sector houses (million AUD) |
| value_of_private_sector_dwellings_excluding_houses_dollar_m | integer | Value of private sector dwellings excluding houses (million AUD) |
| total_value_of_private_sector_dwelling_units_dollar_m | integer | Total value of private sector dwelling units (million AUD) |
| value_of_residential_building_dollar_m | integer | Value of residential building (million AUD) |
| value_of_nonresidential_building_dollar_m | integer | Value of non-residential building (million AUD) |
| value_of_total_building_dollar_m | integer | Value of total building (million AUD) |
| number_of_established_house_transfers_no | integer | Number of established house transfers |

### 3.3 ABS Business Entries/Exits (abs_business_entries_year_ended_30_june)

**Purpose**: Business entries and exits data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_nonemploying_business_entries | integer | Number of non-employing business entries |
| number_of_employing_business_entries_14_employees | integer | Number of employing business entries (1-4 employees) |
| number_of_employing_business_entries_519_employees | integer | Number of employing business entries (5-19 employees) |
| number_of_employing_business_entries_20_or_more_employees | integer | Number of employing business entries (20 or more employees) |
| total_number_of_business_entries | integer | Total number of business entries |
| number_of_nonemploying_business_exits | integer | Number of non-employing business exits |

### 3.4 ABS Business Entries by Turnover (abs_business_entries_by_turnover_year_ended_30_june)

**Purpose**: Business entries data by turnover category

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_business_entries_with_turnover_of_zero_to_less_tha | integer | Number of business entries with turnover $0 to <$50k |
| number_of_business_entries_with_turnover_of_dollar_50k_to_le | integer | Number of business entries with turnover $50k to <$200k |
| number_of_business_entries_with_turnover_of_dollar_200k_to_l | integer | Number of business entries with turnover $200k to <$2m |
| number_of_business_entries_with_turnover_of_dollar_2m_to_les | integer | Number of business entries with turnover $2m to <$5m |
| number_of_business_entries_with_turnover_of_dollar_5m_to_les | integer | Number of business entries with turnover $5m to <$10m |
| number_of_business_entries_with_turnover_of_dollar_10m_or_mo | integer | Number of business entries with turnover $10m or more |
| number_of_business_exits_with_turnover_of_zero_to_less_than | integer | Number of business exits with turnover $0 to <$50k |

### 3.5 ABS Business Exits by Turnover (abs_business_exits_by_turnover_year_ended_30_june)

**Purpose**: Business exits data by turnover category

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_business_exits_with_turnover_of_zero_to_less_than | integer | Number of business exits with turnover $0 to <$50k |
| number_of_business_exits_with_turnover_of_dollar_50k_to_less | integer | Number of business exits with turnover $50k to <$200k |
| number_of_business_exits_with_turnover_of_dollar_200k_to_les | integer | Number of business exits with turnover $200k to <$2m |
| number_of_business_exits_with_turnover_of_dollar_2m_to_less | integer | Number of business exits with turnover $2m to <$5m |
| number_of_business_exits_with_turnover_of_dollar_5m_to_less | integer | Number of business exits with turnover $5m to <$10m |
| number_of_business_exits_with_turnover_of_dollar_10m_or_more | integer | Number of business exits with turnover $10m or more |

### 3.6 ABS Business Exits (abs_business_exits_year_ended_30_june)

**Purpose**: Business exits data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_nonemploying_business_exits | integer | Number of non-employing business exits |
| number_of_employing_business_exits_14_employees | integer | Number of employing business exits (1-4 employees) |
| number_of_employing_business_exits_519_employees | integer | Number of employing business exits (5-19 employees) |
| number_of_employing_business_exits_20_or_more_employees | integer | Number of employing business exits (20 or more employees) |
| total_number_of_business_exits | integer | Total number of business exits |
| agriculture_forestry_and_fishing_no | integer | Agriculture, forestry and fishing number |

### 3.7 ABS Debtors Entering Personal Insolvencies (abs_debtors_entering_personal_insolvencies_year_ended_30_jun)

**Purpose**: Personal insolvency debtors data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| debtors_entering_business_related_personal_insolvencies_no | integer | Debtors entering business-related personal insolvencies number |
| debtors_entering_nonbusiness_related_personal_insolvencies_n | integer | Debtors entering non-business-related personal insolvencies number |
| total_debtors_entering_personal_insolvencies_no | integer | Total debtors entering personal insolvencies number |
| managers_no | integer | Managers number |

### 3.8 ABS Estimated Dwelling Stock (abs_estimated_dwelling_stock_as_at_june_quarter)

**Purpose**: Estimated dwelling stock data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| houses_additions_no | integer | Houses additions number |
| houses_removals_no | integer | Houses removals number |
| houses_total_no | integer | Houses total number |
| townhouses_additions_no | integer | Townhouses additions number |
| townhouses_removals_no | integer | Townhouses removals number |
| townhouses_total_no | integer | Townhouses total number |
| apartments_additions_no | integer | Apartments additions number |
| apartments_removals_no | integer | Apartments removals number |
| apartments_total_no | integer | Apartments total number |
| total_dwelling_additions_no | integer | Total dwelling additions number |
| total_dwelling_removals_no | integer | Total dwelling removals number |
| total_dwellings_no | integer | Total dwellings number |

### 3.9 ABS Gross Value of Agricultural Production (abs_gross_value_of_agricultural_production_year_ended_30_jun)

**Purpose**: Gross value of agricultural production data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| agricultural_production_total_gross_value_dollar_m | numeric | Agricultural production total gross value (million AUD) |
| crops_total_gross_value_dollar_m | numeric | Crops total gross value (million AUD) |
| livestock_slaughtered_and_other_disposals_total_gross_value | numeric | Livestock slaughtered and other disposals total gross value |
| agriculture_forestry_and_fishing_percent | numeric | Agriculture, forestry and fishing percentage |

### 3.10 ABS Industry of Employment (abs_industry_of_employment_persons_aged_15_years_and_over_ce)

**Purpose**: Employment by industry data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| agriculture_forestry_and_fishing_percent | numeric | Agriculture, forestry and fishing percentage |
| mining_percent | numeric | Mining percentage |
| manufacturing_percent | numeric | Manufacturing percentage |
| electricity_gas_water_and_waste_services_percent | numeric | Electricity, gas, water and waste services percentage |
| construction_percent | numeric | Construction percentage |
| wholesale_trade_percent | numeric | Wholesale trade percentage |
| retail_trade_percent | numeric | Retail trade percentage |
| accommodation_and_food_services_percent | numeric | Accommodation and food services percentage |
| transport_postal_and_warehousing_percent | numeric | Transport, postal and warehousing percentage |
| information_media_and_telecommunications_percent | numeric | Information, media and telecommunications percentage |
| financial_and_insurance_services_percent | numeric | Financial and insurance services percentage |
| rental_hiring_and_real_estate_services_percent | numeric | Rental, hiring and real estate services percentage |
| professional_scientific_and_technical_services_percent | numeric | Professional, scientific and technical services percentage |
| administrative_and_support_services_percent | numeric | Administrative and support services percentage |
| public_administration_and_safety_percent | numeric | Public administration and safety percentage |
| education_and_training_percent | numeric | Education and training percentage |
| health_care_and_social_assistance_percent | numeric | Health care and social assistance percentage |
| arts_and_recreation_services_percent | numeric | Arts and recreation services percentage |
| other_services_percent | numeric | Other services percentage |
| industry_of_employment_inadequately_described_or_not_stated | numeric | Industry of employment inadequately described or not stated |
| total_persons_employed_aged_15_years_and_over_no | integer | Total persons employed aged 15 years and over number |

### 3.11 ABS Number of Businesses at 30 June (abs_number_of_businesses_at_30_june)

**Purpose**: Number of businesses at 30 June data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_nonemploying_businesses | integer | Number of non-employing businesses |
| number_of_employing_businesses_14_employees | integer | Number of employing businesses (1-4 employees) |
| number_of_employing_businesses_519_employees | integer | Number of employing businesses (5-19 employees) |
| number_of_employing_businesses_20_or_more_employees | integer | Number of employing businesses (20 or more employees) |
| total_number_of_businesses | integer | Total number of businesses |
| number_of_nonemploying_business_entries | integer | Number of non-employing business entries |

### 3.12 ABS Number of Businesses by Industry (abs_number_of_businesses_by_industry_at_30_june)

**Purpose**: Number of businesses by industry data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| agriculture_forestry_and_fishing_no | integer | Agriculture, forestry and fishing number |
| mining_no | integer | Mining number |
| manufacturing_no | integer | Manufacturing number |
| electricity_gas_water_and_waste_services_no | integer | Electricity, gas, water and waste services number |
| construction_no | integer | Construction number |
| wholesale_trade_no | integer | Wholesale trade number |
| retail_trade_no | integer | Retail trade number |
| accommodation_and_food_services_no | integer | Accommodation and food services number |
| transport_postal_and_warehousing_no | integer | Transport, postal and warehousing number |
| information_media_and_telecommunications_no | integer | Information, media and telecommunications number |
| financial_and_insurance_services_no | integer | Financial and insurance services number |
| rental_hiring_and_real_estate_services_no | integer | Rental, hiring and real estate services number |
| professional_scientific_and_technical_services_no | integer | Professional, scientific and technical services number |
| administrative_and_support_services_no | integer | Administrative and support services number |
| public_administration_and_safety_no | integer | Public administration and safety number |
| education_and_training_no | integer | Education and training number |
| health_care_and_social_assistance_no | integer | Health care and social assistance number |
| arts_and_recreation_services_no | integer | Arts and recreation services number |
| other_services_no | integer | Other services number |
| currently_unknown_no | integer | Currently unknown number |
| number_of_businesses_with_turnover_of_zero_to_less_than_doll | integer | Number of businesses with turnover $0 to <$50k |

### 3.13 ABS Number of Businesses by Turnover (abs_number_of_businesses_by_turnover_at_30_june)

**Purpose**: Number of businesses by turnover data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_businesses_with_turnover_of_zero_to_less_than_doll | integer | Number of businesses with turnover $0 to <$50k |
| number_of_businesses_with_turnover_of_dollar_50k_to_less_tha | integer | Number of businesses with turnover $50k to <$200k |
| number_of_businesses_with_turnover_of_dollar_200k_to_less_th | integer | Number of businesses with turnover $200k to <$2m |
| number_of_businesses_with_turnover_of_dollar_2m_to_less_than | integer | Number of businesses with turnover $2m to <$5m |
| number_of_businesses_with_turnover_of_dollar_5m_to_less_than | integer | Number of businesses with turnover $5m to <$10m |
| number_of_businesses_with_turnover_of_dollar_10m_or_more | integer | Number of businesses with turnover $10m or more |
| number_of_business_entries_with_turnover_of_zero_to_less_tha | integer | Number of business entries with turnover $0 to <$50k |

### 3.14 ABS Occupations of Debtors (abs_occupations_of_debtors_entering_personal_insolvencies_ye)

**Purpose**: Personal insolvency debtors by occupation data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| managers_no | integer | Managers number |
| professionals_no | integer | Professionals number |
| technicians_and_trades_workers_no | integer | Technicians and trades workers number |
| community_and_personal_service_workers_no | integer | Community and personal service workers number |
| clerical_and_administrative_workers_no | integer | Clerical and administrative workers number |
| sales_workers_no | integer | Sales workers number |
| machinery_operators_and_drivers_no | integer | Machinery operators and drivers number |
| labourers_no | integer | Labourers number |
| debtors_with_other_or_unknown_occupations_no | integer | Debtors with other or unknown occupations number |

### 3.15 ABS Residential Property Transfers (abs_residential_property_transfers_year_ended_30_june)

**Purpose**: Residential property transfers data

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| number_of_established_house_transfers_no | integer | Number of established house transfers |
| median_price_of_established_house_transfers_dollar | integer | Median price of established house transfers (AUD) |
| number_of_attached_dwelling_transfers_no | integer | Number of attached dwelling transfers |
| median_price_of_attached_dwelling_transfers_dollar | integer | Median price of attached dwelling transfers (AUD) |
| debtors_entering_business_related_personal_insolvencies_no | integer | Debtors entering business-related personal insolvencies number |

---

## 4. Relationship Tables

### 4.1 NGER CER Proximity Matches (nger_cer_proximity_matches)

**Purpose**: Stores spatial proximity relationships between NGER facilities and CER power stations

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| nger_id | integer | NULL | NGER table foreign key, references nger_unified.id |
| cer_id | integer | NULL | CER table foreign key, references cer_approved_power_stations.id |
| match_type | text | NULL | Match type, fixed value 'proximity_1km' |
| distance_meters | double precision | NULL | Distance (meters) |

---

## Table Relationships Summary

### Foreign Key Relationships
- `nger_cer_proximity_matches.nger_id` → `nger_unified.id`
- `nger_cer_proximity_matches.cer_id` → `cer_approved_power_stations.id`

### Spatial Relationships
- All tables with geographic information have PostGIS geometry fields
- Use spatial indexes for efficient spatial queries
- Support spatial joins based on distance

### Data Sources
- **NGER Tables**: National Greenhouse and Energy Reporting data
- **CER Tables**: Clean Energy Regulator power station data
- **ABS Tables**: Australian Bureau of Statistics economic data
- **Proximity Table**: Spatial distance-based relationship table

### Indexes
- All tables have primary key indexes (id)
- Geographic fields have GiST spatial indexes (geom, geom_bbox)
- Foreign key fields have indexes to support join queries
