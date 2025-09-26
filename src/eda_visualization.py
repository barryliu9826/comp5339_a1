#!/usr/bin/env python3
"""EDA visualization: NGER data from database"""

# Standard library imports
import warnings
from pathlib import Path
from typing import List

# Third-party library imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Local module imports
from database_utils import get_db_connection, return_db_connection

warnings.filterwarnings('ignore')
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

def analyze_nger_cer_proximity_fuel_types():
    """Analyze fuel types in NGER-CER proximity matches - merged from proximity_eda.py"""
    print("Starting NGER-CER Proximity Fuel Type Analysis")
    
    try:
        # Fetch data from nger_cer_proximity_matches table
        df = fetch_df("SELECT primary_fuel FROM nger_cer_proximity_matches WHERE primary_fuel IS NOT NULL")
        
        if df.empty:
            print("No proximity fuel data available")
            return
            
        print(f"Proximity fuel data records: {len(df):,}")
        
        # Count fuel types
        fuel_counts = df['primary_fuel'].value_counts()
        print(f"Unique fuel types in proximity matches: {len(fuel_counts)}")
        
        # Create pie chart (matching the style from proximity_eda.py)
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Calculate percentages and group small ones into "Others"
        total_count = fuel_counts.sum()
        fuel_percentages = (fuel_counts / total_count * 100)
        
        # Separate fuel types >= 2% and < 2%
        major_fuels = fuel_counts[fuel_percentages >= 3.0]
        minor_fuels = fuel_counts[fuel_percentages < 3.0]
        
        # Create data for pie chart
        pie_data = major_fuels.copy()
        if not minor_fuels.empty:
            pie_data["Others"] = minor_fuels.sum()
        
        # Create pie chart with same styling as proximity_eda.py
        wedges, texts, autotexts = ax.pie(
            pie_data.values, 
            labels=pie_data.index, 
            autopct="%1.1f%%", 
            startangle=90
        )        
        ax.set_title('Primary Fuel Types in Proximity Matches', fontsize=14, fontweight='bold')
        
        # Add legend with counts
        ax.legend(wedges, [f'{label}: {value:,}' for label, value in pie_data.items()],
                 title="Fuel Types", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.tight_layout()
        plot_save(fig, "nger_cer_proximity_fuel_types_pie_chart.png")
        
        print(f"NGER-CER proximity fuel type analysis completed")
        print(f"Visualization saved to: {OUTPUT_DIR}")
        
    except Exception as e:
        print(f"Error analyzing NGER-CER proximity fuel types: {e}")

def main():
    sns.set_theme(style="whitegrid")
    print(f"Output directory: {OUTPUT_DIR}")
    
    eda_nger_map()
    analyze_nger_cer_proximity_fuel_types()
    
    print("EDA charts generated in data/eda/")

if __name__ == "__main__":
    main()
