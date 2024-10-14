import hapi_parser as hp
from hapiplot import hapiplot
from io import StringIO

USE_CASE = 'supermag'
start      = '2023-12-01T00:00Z'
stop       = '2023-12-07T00:00Z'
id = "indices_all"
parameters = "SME,SML"

CFG = hp.parse_config(USE_CASE)
(status, jsondata) = hp.fetch_info_params(id,CFG.HAPI_HOME,False)
(start, stop, errorcode) = hp.clean_query_time(None,timemin=start,timemax=stop)
(status, data) = CFG.hapi_handler(id, start, stop, hp.tolist(parameters), jsondata, CFG.floc, False, None)
meta, hapidata = hp.csv_to_hapi_conv(id,parameters,CFG.HAPI_HOME,data)
print('meta=',meta)
hapiplot(hapidata,meta)


