from hapiclient import hapi
from hapiplot import hapiplot

server     = 'http://localhost:8000/hapi'
dataset    = "stations"
# Start and stop times
start      = '2023-12-01T00:00:00Z'
stop       = '2023-12-07T00:00:00Z'
parameters = 'IAGA'
stations, meta = hapi(server, dataset, parameters, start, stop)
print(meta)
print("First 3 stations:")
for datapair in stations[:3]:
    print(f"Station: {datapair[1]}")
    dataset = f"data_{datapair[1]}"
    parameters="geo,mag"
    try:
        data, meta = hapi(server, dataset, parameters, start, stop)
        hapiplot(data,meta)
    except Exception as e:
        print(e)

dataset = "indices_all"
parameters="SME,SML"
data, meta = hapi(server, dataset, parameters, start, stop)
hapiplot(data,meta)
