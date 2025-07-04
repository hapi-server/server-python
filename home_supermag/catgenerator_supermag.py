""" Generates the HAPI-format catalog.json from the list of stations
  as given in SuperMAG_locations.dat,
  Then creates the per-station JSON 
     using 'SuperMAG-Inventory-60s-2025-05-08.csv' (configurable)
     to find the specific time windows each station is available.
  Also adding in the required indices to the catalog.json.

  You can update the available windows by downloading a more recent
  inventory file from https://supermag.jhuapl.edu/inventory/?fidelity=low#

"""

import json
import os
import zipfile
import pandas as pd

InventoryFile = 'SuperMAG-Inventory-60s-2025-05-08.csv'
fname = "SuperMAG_locations.dat"

indices = ["indices_all", "indices_base", "indices_dark", "indices_imf", "indices_reg", "indices_sun"]

baselines = ['baseline_all','baseline_yearly','baseline_none']

basetext = {}
basetext['baseline_all'] = "Subtract the daily variations and yearly trend (using Gjerloev, 2012)"
basetext['baseline_yearly'] = "Subtract only the yearly trend (using Gjerloev, 2012)"
basetext['baseline_none'] = "Do not subtract any baseline"



stationlist = []
with open(fname) as fin:
    for line in fin.readlines():
        stationlist.append(line.rstrip().split(',')) # is station, lat, lon

catalog_entries = [{"id": index, "title": f"Data indices for time span, {index}"} for index in indices]

station_entries = []
for baseline in baselines:
    station_entries.extend([{"id": f"{index[0].lower()}/PT1M/{baseline}", "title": f"Data for station {index[0]} at lat {index[1]} lon {index[2]} using baseline choice of {baseline}"} for index in stationlist])

data = {
    "HAPI": "3.2",
    "catalog": [],
    "status": {
        "code": 1200,
        "message": "OK request successful"
    }
}

data["catalog"].extend(catalog_entries)
data["catalog"].extend(station_entries)

with open("catalog.json","w") as fout:
    json.dump(data, fout, indent=4)
    print("Generated new catalog.json file.")
    
""" Also generate data_stations.json and indices_*.json """
zip_path = 'info_jsons.zip'
extract_to = '.'
if os.path.isfile(zip_path):
    with zipfile.ZipFile(zip_path,'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"{zip_path} unzipped successfully")
else:
    print(f"{zip_path} not found")

    
""" Also generate each station's .json file """

template = {
    "HAPI": "3.1",
    "status": {
        "code": 1200,
        "message": "OK request successful"
    },
    "parameters": [
        {
            "length": 24,
            "name": "Time",
            "type": "isotime",
            "units": "UTC"
        },
        {"name": "Field_Vector", "type": "double", "units": "nT", "size": [3], "desc": "N_geo, E_geo, Z_geo aka the N, E, A vector components in geographic mapping"},
        {"name": "mlt", "type": "double", "units": ["hours","degrees"], "fill": ["0","0"], "size": [2], "desc": "The Magnetic local time and colatitude, mlt, mcolat, in hours, degrees"},
        {"name": "sza", "type": "double", "units": "degrees", "fill": "0", "desc": "The solar zenith angle, in degrees"},
        {"name": "decl", "type": "double", "units": "degrees", "fill": "0", "desc": "The Declination from IGRF Model, in degrees"},
        {"name": "N_geo", "type": "double", "units": "nT", "fill": "0", "desc": "N_geo, the N vector component in geographic mapping."},
        {"name": "E_geo", "type": "double", "units": "nT", "fill": "0", "desc": "E_geo, the E vector component in geographic mapping."},
        {"name": "Z_geo", "type": "double", "units": "nT", "fill": "0", "desc": "Z_geo, the A vector component in geographic mapping."}
    ],
    "startDate": "2020-01-01T00:00Z",
    "stopDate": "lastday-P1D",
    "sampleStartDate": "2020-01-01T00:00Z",
    "sampleStopDate": "2020-01-01T01:00Z",
    "maxRequestDuration": "P1Y",
    "cadence": "PT1M",
    "citation": "https://supermag.jhuapl.edu/info/?page=rulesoftheroad",
    "description": """The SuperMAG data processing technique, J. W. Gjerloev, https://doi.org/10.1029/2012JA017683

    Baseline Determination
SuperMAG HAPI provides three options for the user:

1. Subtract the daily variations and yearly trend (using Gjerloev, 2012)
2. Subtract only the yearly trend (using Gjerloev, 2012)
3. Do not subtract any baseline

SuperMAG thus provides 3 different solutions. The user should use the appropriate dataset for the study. As an example a study of the Sq current or the equatorial electrojet should not subtract the daily variations since this will remove part of this signal.For more details, see https://supermag.jhuapl.edu/mag/?fidelity=low&tab=description"""
}

"""
used to be nez & geo, now returning just geo
        {"name": "N", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "N_nez, N_geo"},
        {"name": "E", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "E_nez, E_geo"},
        {"name": "Z", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "Z_nez, Z_geo"}
Also dropped 'mag' because their API gateway wasn't returning it,
        {"name": "mag", "type": "double", "units": "degrees", "fill": "0", "size": [2], "desc": "The Magnetic coordinates of the station, mlat, mlon"},

"""


# now get time brackets
df = pd.read_csv(InventoryFile)
year_columns = [col for col in df.columns if col.isdigit()]

def get_open_years(row):
    return [year for year in year_columns if row[year] != '0%']

df['Open Years'] = df.apply(get_open_years, axis=1)
open_years_df = df[['IAGA', 'Open Years']]

def get_year_span(df, iaga_id):
    row = df[df['IAGA'] == iaga_id]
    if row.empty:
        return None, None
    years = row.iloc[0]['Open Years']
    if not years:
        return None, None
    return min(years), max(years)

for baseline in baselines:
    for id in stationlist:
        start, stop = get_year_span(df, id[0])
        if start == None: continue
        fname = f"info/{id[0].lower()}/PT1M/{baseline}.json"
        os.makedirs(os.path.dirname(fname),exist_ok=True)
        with open(fname,"w") as fout:
            template2 = template
            template2["startDate"] = start + '-01-01T00:00Z'
            template2["stopDate"] = stop + '-01-01T00:00Z'
            template2["sampleStartDate"] = start + '-12-01T00:00Z'
            template2["sampleStopDate"] = start + '-12-02T00:00Z'
            template2["additionalMetadata"] = [
                { "name": "iaga", "content": id[0]},
                { "name": "x_latitude", "content": id[1]},
                { "name": "x_longitude", "content": id[2]},
                { "name": "baselines", "contentURL": "https://supermag.jhuapl.edu/mag/?fidelity=low&tab=description", "content": basetext[baseline]}
            ]

            json.dump(template2, fout, indent=4)
print(f"{len(stationlist)*len(baselines)} station info files created successfully.")
