""" Simple routine that, given a SuperMAG inventory file, queries for stations that
    are within a requested GLon/Glat box and start_year/stop_year range.
    Also accepts fractional years (i.e. Jun 2025 would be 2025.5)
    Defaults to coords as GLON/GLAT, has option for MLON/MLAT

usage as per __main__ below,
    coords = [350, 10, -30, 60]
    daterange = [2010, 2025]
    subset = query_in_bbox(df_years, coords, daterange)

"""

import json
import os
import pandas as pd

# Function to find year and embed percentage
def get_start_stop_decimal(row):
    nonzero = [(int(year), row[year]) for year in year_cols if row[year] != 0]
    if not nonzero:
        return pd.Series([None, None])
    start_year, start_pct = min(nonzero, key=lambda x: int(x[0]))
    stop_year, stop_pct = max(nonzero, key=lambda x: int(x[0]))
    return pd.Series([start_year + start_pct / 100, stop_year + stop_pct / 100])

def query_in_bbox(df, coords, daterange, default="Geo", fetch="IAGA"):
    """ Can use default=Geo for GLON, GLAT or =Mag for MLON, MLAT
        Returns list of IAGA station ids, optionally fetch='all' returns entire dataframe
        of matching stations
    """
    if default == "Mag":
        Lon, Lat = "MLON", "MLAT"
    else:
        Lon, Lat = "GLON", "GLAT"
    GLON_min, GLON_max, GLAT_min, GLAT_max = coords
    start, stop = daterange
    if GLON_min <= GLON_max:
        glon_mask = (df[Lon] >= GLON_min) & (df[Lon] <= GLON_max)
    else:
        glon_mask = (df[Lon] >= GLON_min) | (df[Lon] <= GLON_max)

    mask = (
        glon_mask &
        (df[Lat] >= GLAT_min) & (df[Lat] <= GLAT_max) &
        (df["startyear"] >= start) &
        (df["stopyear"] <= stop)
    )
    if fetch == "all":
        return df[mask]
    else:
        return df[mask]["IAGA"].tolist()


InventoryFile = 'SuperMAG-Inventory-60s-2025-05-08.csv'
df_years = pd.read_csv(InventoryFile)
year_cols = [col for col in df_years.columns if col.isdigit()]
df_numeric = df_years[year_cols].replace('%', '', regex=True).astype(float)
df_years[['startyear', 'stopyear']] = df_numeric.apply(get_start_stop_decimal, axis=1)


if __name__ == '__main__':
    # demo run
    coords = [350, 10, -30, 60]
    daterange = [2010, 2025]
    subset = query_in_bbox(df_years, coords, daterange)
    print(f"Stations in range {coords} for times {daterange}: {subset}")
