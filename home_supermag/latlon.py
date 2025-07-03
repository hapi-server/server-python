lat1, lat2, lon1, lon2 = 10, 20, 100, 110
fname = "SuperMAG_locations.dat"
with open(fname) as fin:
    lines = fin.readlines()
stationlist = []
for line in lines:
    station,lat,lon = line.split(',')
    lat, lon = float(lat), float(lon)
    if lat > lat1 and lat < lat2 and lon > lon1 and lon < lon2:
        stationlist.append(station)

print(f"Found {len(stationlist)} stations within {lat1},{lat2}:{lon1},{lon2}")


