from hapiclient import hapi
from hapiplot import hapiplot

server = "http://localhost:8000/hapi"
dataset = "TTB/PT1M/xyz"
start = "2020-05-10T00:00Z"
stop = "2020-05-14T00:00Z"
parameters = ''
#data, meta = hapi(server, dataset, parameters, start, stop)
#hapiplot(data,meta)
#print(data)


import pyspedas
param_list = pyspedas.hapi(trange=[start,stop], server=server, dataset=dataset, parameters=parameters)
print(param_list)
