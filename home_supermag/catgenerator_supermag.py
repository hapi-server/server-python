""" Generates the HAPI-format catalog.json from the list of stations
  as given in SuperMAG_locations.dat
  also adding in the required indices etc
"""

import json

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
