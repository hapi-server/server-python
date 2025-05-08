""" Generates the HAPI-format catalog.json from the list of stations
  as given in SuperMAG_locations.dat
  also adding in the required indices etc
"""

import json
import os

indices = ["indices_all", "indices_base", "indices_dark", "indices_imf", "indices_reg", "indices_sun"]

fname = "SuperMAG_locations.dat"
stationlist = []
with open(fname) as fin:
    for line in fin.readlines():
        stationlist.append(line.split(',')) # is station, lat, lon

catalog_entries = [{"id": index, "title": f"Data indices for time span, {index}"} for index in indices]
station_entries = [{"id": f"{index[0]}/PT1M/xyz", "title": f"Data for station {index[0]} at lat {index[1]} lon {index[2]}"} for index in stationlist]

data = {
    "HAPI": "3.1",
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

""" Also generate each station's .json file """

template = {
    "HAPI": "3.1",
    "status": {
        "code": 1200,
        "message": "OK request successful"
    },
    "x_customRequestOptions": [
        { "name": "baseline",
          "description": "Changing the baseline subtraction from the default",
          "type": "string",
          "constraint": {
              "enum": ["system","yearly","none"]
          },
          "default": "system"
        },
        { "name": "delta",
          "description": "Changing the delta subtraction from the default",
          "type": "string",
          "constraint": {
              "enum": ["system","none","start"]
          },
          "default": "system"
        }
    ],
    "parameters": [
        {
            "length": 24,
            "name": "Time",
            "type": "isotime",
            "fill": 999999.0,
            "units": "UTC"
        },
        {"name": "ext", "type": "double", "units": "seconds", "fill": "60.0"},
        {"name": "iaga", "type": "string", "units": "TLA", "length": 3, "fill": "n/a"},
        {"name": "geo", "type": "double", "units": "degrees", "fill": "0", "size": [2], "desc": "glon, glat"},
        {"name": "mag", "type": "double", "units": "degrees", "fill": "0", "size": [2], "desc": "mlon, mlat"},
        {"name": "mlt", "type": "double", "units": "degrees", "fill": "0", "size": [2], "desc": "mlt, mcolat"},
        {"name": "sza", "type": "double", "units": "degrees", "fill": "0"},
        {"name": "N", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "N_nez, N_geo"},
        {"name": "E", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "E_nez, E_geo"},
        {"name": "Z", "type": "double", "units": "nT", "fill": "0", "size": [2], "desc": "Z_nez, Z_geo"}
    ],
    "startDate": "2020-01-01T00:00Z",
    "stopDate": "lastday-P1D",
    "sampleStartDate": "2020-01-01T00:00Z",
    "sampleStopDate": "2020-01-01T01:00Z",
    "cadence": "PT1M"
}

    
for id in stationlist:
    fname = f"info/{id[0]}/PT1M/xyz.json"
    os.makedirs(os.path.dirname(fname),exist_ok=True)
    with open(fname,"w") as fout:
        json.dump(template, fout, indent=4)
    
