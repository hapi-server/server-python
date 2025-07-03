from hapiclient import hapi
from hapiplot import hapiplot

server = "https://supermag.jhuapl.edu/hapi"
dataset = "ttb/PT1M/baseline_none"
start = "2020-01-01T00:00Z"
stop = "2020-01-03T00:00Z"
parameters = ''
data, meta = hapi(server, dataset, parameters, start, stop)
hapiplot(data,meta)
