#!/usr/bin/env python3
"""EDA visualization: NGER, CER, and ABS data from database"""

from pathlib import Path
from typing import List
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from database_config import get_db_connection, return_db_connection

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "eda"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_df(sql: str) -> pd.DataFrame:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Database connection failed")
    try:
        df = pd.read_sql(sql, conn)
        return df
    finally:
        return_db_connection(conn)

def plot_save(fig, filename: str):
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, dpi=160)
    plt.close(fig)

def _filter_australia_coords(df):
    """Filter coordinates to Australia bounds"""
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])
    return df[(df["lon"].between(110, 155)) & (df["lat"].between(-45, -5))]

def _setup_map_axes(ax, title, xlabel="Longitude", ylabel="Latitude"):
    """Setup common map axes properties"""
    ax.set_xlim(110, 155)
    ax.set_ylim(-45, -5)
    ax.set_aspect('equal', adjustable='box')
    ax.grid(True, ls=":", alpha=0.4)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

def eda_cer_map():
    """Plot CER power station locations by category"""
    queries = [
        ("approved", "SELECT lat, lon FROM cer_approved_power_stations"),
        ("committed", "SELECT lat, lon FROM cer_committed_power_stations"),
        ("probable", "SELECT lat, lon FROM cer_probable_power_stations"),
    ]

    frames = []
    for label, sql in queries:
        try:
            df = fetch_df(sql)
            if not df.empty and {"lat", "lon"}.issubset(df.columns):
                df["category"] = label
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return

    df_all = pd.concat(frames, ignore_index=True)
    aus = _filter_australia_coords(df_all)
    if aus.empty:
        return

    palette = {"approved": "#1b9e77", "committed": "#d95f02", "probable": "#7570b3"}

    fig, ax = plt.subplots(figsize=(8.8, 7))
    for cat, group in aus.groupby("category"):
        ax.scatter(group["lon"], group["lat"], s=18, alpha=0.7, label=cat, c=palette.get(cat, None))

    _setup_map_axes(ax, "CER Power Station Distribution (Approved / Committed / Probable)")
    ax.legend(title="Category")
    plot_save(fig, "cer_map_categories.png")

def eda_nger_map():
    """Plot NGER facility locations by primary fuel type"""
    try:
        df = fetch_df("SELECT lat, lon, primary_fuel FROM nger_unified WHERE lat IS NOT NULL AND lon IS NOT NULL")
    except Exception:
        return

    if df.empty:
        return

    aus = _filter_australia_coords(df)
    if aus.empty:
        return

    aus["primary_fuel"] = aus["primary_fuel"].fillna("Unknown").astype(str).str.strip()
    counts = aus["primary_fuel"].value_counts()
    top_fuels = counts.head(6).index.tolist()
    aus["fuel_cat"] = aus["primary_fuel"].where(aus["primary_fuel"].isin(top_fuels), other="Other")

    palette = {fuel: color for fuel, color in zip(
        top_fuels + ["Other"],
        ["#1b9e77", "#d95f02", "#7570b3", "#e7298a", "#66a61e", "#e6ab02", "#999999"]
    )}

    fig, ax = plt.subplots(figsize=(8.8, 7))
    for fuel, group in aus.groupby("fuel_cat"):
        ax.scatter(group["lon"], group["lat"], s=16, alpha=0.7, label=fuel, c=palette.get(fuel, "#999999"))

    _setup_map_axes(ax, "NGER Facility Distribution (by Primary Fuel Type)")
    ax.legend(title="Primary Fuel", loc="best", fontsize=9)
    plot_save(fig, "nger_map_by_fuel.png")

def list_abs_tables() -> List[str]:
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public' AND table_name LIKE 'abs_%'
        ORDER BY table_name
    """
    df = fetch_df(sql)
    return df["table_name"].tolist() if not df.empty else []

def _clean_table_name(name):
    """Clean ABS table names for display"""
    # Remove common suffixes
    suffixes = ['_year_ended_30_june', '_year_ended_30_jun', '_as_at_june_quarter', '_at_30_june',
               '_entering_personal_insolvencies', '_aged_15_years_and_over', '_gross_value_of_',
               '_number_of_', '_residential_property_', '_estimated_dwelling_', '_building_',
               '_agricultural_', '_business_']
    
    clean = name.replace('abs_', '')
    for suffix in suffixes:
        clean = clean.replace(suffix, '')
    
    # Specific mappings to avoid duplicates
    mappings = {
        'business_entries_by_turnover': 'entries_by_turnover',
        'business_entries': 'entries',
        'business_exits_by_turnover': 'exits_by_turnover',
        'business_exits': 'exits',
        'debtors_entering': 'debtors',
        'occupations_of_debtors': 'occupations',
        'industry_of_employment': 'employment',
        'gross_value_of_agricultural': 'agricultural_value',
        'number_of_businesses_by_industry': 'businesses_by_industry',
        'number_of_businesses_by_turnover': 'businesses_by_turnover',
        'number_of_businesses': 'businesses',
        'residential_property': 'property',
        'estimated_dwelling': 'dwellings',
        'building_approvals': 'approvals',
        'agricultural_commodities': 'commodities'
    }
    
    for pattern, replacement in mappings.items():
        if pattern in clean:
            return replacement
    
    return clean

def eda_abs_overview():
    """ABS geographic level distribution visualization"""
    tables = list_abs_tables()
    if not tables:
        return

    try:
        lvl_records = []
        for t in tables:
            sql = f"SELECT geographic_level AS lvl, COUNT(*) AS n FROM {t} WHERE geographic_level IS NOT NULL GROUP BY geographic_level"
            df = fetch_df(sql)
            if not df.empty:
                df["table"] = t
                lvl_records.append(df)
        
        if lvl_records:
            df_lvl = pd.concat(lvl_records, ignore_index=True)
            df_lvl['table_clean'] = df_lvl['table'].apply(_clean_table_name)
            df_lvl['level_label'] = df_lvl['lvl'].map({0: 'State Level', 1: 'Local Government Level'})
            
            fig, ax = plt.subplots(figsize=(18, 10))
            sns.barplot(data=df_lvl, x="table_clean", y="n", hue="level_label", ax=ax)
            ax.set_title("ABS Geographic Level Distribution", fontsize=20, pad=30)
            ax.set_xlabel("Table", fontsize=18)
            ax.set_ylabel("Rows", fontsize=18)
            ax.tick_params(axis='x', rotation=45, labelsize=14)
            ax.tick_params(axis='y', labelsize=14)
            ax.legend(fontsize=14)
            plot_save(fig, "abs_overview_geographic_level.png")
    except Exception as e:
        print(f"ABS overview failed: {e}")

def main():
    sns.set_theme(style="whitegrid")
    print(f"Output directory: {OUTPUT_DIR}")
    
    eda_cer_map()
    eda_nger_map()
    eda_abs_overview()
    
    print("EDA charts generated in data/eda/")

if __name__ == "__main__":
    main()