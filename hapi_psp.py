"""
hapi_psp.py, web pass-thru reader for Parker Solar Probe

Author:
Eric Winter
Lisa Knowles
"""


# ----------------------------------------------------------------------------


# Standard modules
import urllib.request
from datetime import datetime

# Third-party modules

# Project modules
import psp_query


# #import netCDF4 as nc
# #import numpy as np
# #import pandas as pd
# import copy
# import xarray as xr
# import pandas as pd
# import time
# import os
# #
# import urllib.request
# import certifi # needed at APL for SSL
# from pandas import to_datetime # used for julian date, actually
# import json
# import re
# #from datetime import datetime, timedelta
# import datetime

# from supermag_api import *


# Module constants

# datetime-style format strings for HAPI and PSP dates.
HAPI_DATETIME_FORMAT = '%Y-%m-%dT%H:%MZ'
PSP_DATETIME_FORMAT =  '%Y-%jT%H:%M:%S.%f'

# ----------------------------------------------------------------------------


# def handle_hapi_request(
#         id, timemin, timemax, parameters, catalog, floc, stream_flag, stream
# ):
#     """Process a HAPI request.

#     Process a HAPI request.

#     Parameters
#     ----------
#     id : str
#         Identifier string for requested dataset.
#     timemin : str
#         Start datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
#     timemax : str
#         End datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
#     parameters : list of str
#         List of strings specifying requested parameters.
#     catalog : dict
#         Dataset information from catalog JSON file.
#     floc : dict
#         Site-specific information from *_config.py
#     stream_flag : bool
#         True if results should be streamed back to requestor.
#     stream : MyHandler object
#         Handler object for HAPI request.

#     Returns
#     -------
#     hapi_status, hapi_result_str : int, str
#         HAPI status code, and result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     # Initialize HAPI status code and return string.
#     hapi_status = -1
#     hapi_result_str = ""

#     # Process the query based on the HAPI endpoint requested.

#     # Return the HAPI status code and the query result string.
#     return hapi_status, hapi_result_str


# def handle_hapi_about_request(hapi_request_url: str):
#     """Process a HAPI "about" request.

#     Process a HAPI "about" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = "ABOUT"
#     return hapi_result_str


# def handle_hapi_capabilities_request(hapi_request_url: str):
#     """Process a HAPI "about" request.

#     Process a HAPI "about" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


# def handle_hapi_catalog_request(hapi_request_url: str):
#     """Process a HAPI "catalog" request.

#     Process a HAPI "catalog" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


# def handle_hapi_info_request(hapi_request_url: str):
#     """Process a HAPI "info" request.

#     Process a HAPI "info" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


def handle_hapi_data_request(
        id, timemin, timemax, parameters, catalog, floc, stream_flag, stream
):
    """Process a HAPI data request.

    Process a HAPI data request.

    Parameters
    ----------
    id : str
        Identifier string for requested dataset.
    timemin : str
        Start datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
    timemax : str
        End datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
    parameters : list of str
        List of strings specifying requested parameters.
    catalog : dict
        Dataset information from catalog JSON file.
    floc : dict
        Site-specific information from *_config.py
    stream_flag : bool
        True if results should be streamed back to requestor.
    stream : MyHandler object
        Handler object for HAPI request.

    Returns
    -------
    hapi_status, hapi_result_str : int, str
        HAPI status code, and result of HAPI request.

    Raises
    ------
    None
    """
    # Initialize HAPI status code and return string.
    hapi_status = 0
    hapi_result_str = ""

    # Create datetime objects for the start and end times, then convert to
    # PSP format.
    hapi_start_datetime = datetime.strptime(timemin, HAPI_DATETIME_FORMAT)
    hapi_end_datetime = datetime.strptime(timemax, HAPI_DATETIME_FORMAT)
    psp_start_datetime_s = hapi_start_datetime.strftime(PSP_DATETIME_FORMAT)
    psp_end_datetime_s = hapi_end_datetime.strftime(PSP_DATETIME_FORMAT)

    # Process the query.
    # collection = 'psp'
    # version = 'latest'
    # start = '2024-001T12:15:00'
    # stop = '2024-001T13:15:00'
    # stepsize = 3600.0
    # frame = 'J2000'
    # center = 'SUN'
    body = 'SPP'
    # correction = 'LT-S'
    ephemeris_json = psp_query.query_psp_ephemeris(
        start=psp_start_datetime_s, stop=psp_end_datetime_s, body=body
    )

    # Convert the native PSP REST response to CSV.

    # Return the HAPI status code and the query result string.
    return hapi_status, ephemeris_json


# ----------------------------------------------------------------------------

# def do_data_supermag(id,timemin,timemax,parameters,catalog,floc,
#                      stream_flag, stream):
#     #print("debug, got parameters: ",parameters)
#     # 'ignore' is because hapi-server uses that only for file-bsaed fetches
#     userid='superhapi'  # debug, temporarily for now

#     #timenow = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%M:%SZ')
#     #timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%M:%SZ')
#     start = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%MZ')
#     timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%MZ')
#     delta=timeend-start
#     extent = delta.total_seconds()

#     if len(floc['customOptions']) > 0:
#         parameters += floc['customOptions']
    
#     #if ( parameters!=None ):
#     #    mp= do_parameters_map( id, parameters )
#     #else:
#     #    mp= None
#     #print("debug: parameters found ",parameters)
#     final_parameters = parameters  # save for later

#     #print("debug: parameters updated ",parameters)

#     #print("debug: id is ",id)
#     if id.startswith("stations"):
#         """ note this is NOT a proper HAPI function so we do not use it
#             as it only returns a list of stations, not a time-ordered
#             function
#             To get a list of stations, construct a URL akin to:
#         https://supermag.jhuapl.edu/services/inventory.php?python&nohead&start=2018-01-18T00:00&logon=superhapi&extent=000000086400
#         """
#         # no 'parameters' used by this
#         #print("Debug:",userid,start,extent)
#         (status,magdata) = supermag_getinventory(userid,start,extent) # FORMAT='list')
#         ## add column 'window_end'
#         for i, iaga in enumerate(magdata):
#             #print(timemin, iaga, timemax)
#             line = timemin + ',' + iaga + ',' + timemax
#             magdata[i] = line
#         magdata = '\n'.join(magdata)
#         #magdata = "#Time,IAGA,window_end\n" + magdata
#         #magdata = '\',\''.join(magdata) # cheap csv-ing
#         #magdata = '\'' + magdata + '\'' # add leading and following quote
#         status=tf_to_hapicode(status,len(magdata))
#     elif id.startswith('indices'):
#         # 'parameters' is which data items to fetch, HAPI default = 'all'
#         (parameters, clean_out_later) = sm_lookup(parameters)
            
#         (status,magdata)=supermag_getindices(userid,start,extent,parameters,FORMAT='json')
#         # (note we remove 'row' because HAPI requires start as 1st var)

#         # converts to csv string with \n
#         #magdata = magdata.to_csv(header=1,index=False)
#         try:
#             magdata['tval'] = magdata['tval'].apply(sm_to_hapitimes)
#         except:
#             pass # pass when there is no valid data to parse
#         #print("Debug: all magdata = ",magdata)

#         if parameters != None:
#             # verify and fill if no data exists
#             #print("debug: catalog is ",catalog)
#             #print("debug: catalog keys are ",catalog.keys())
#             ### NOTE-- removed sm_fill_empty() BECAUSE SuperMAG data is weird
#             ###sm_fill_empty(magdata,parameters,catalog['parameters'])
#             #magdata.rename(indicesmap,inplace=True,errors='ignore')
#             #print("Debug: renamed magdata = ",magdata)

#             if len(clean_out_later) > 0:
#                 #print("Debug, removing ",clean_out_later,"\n from ",magdata.keys())
#                 magdata=magdata.drop(columns=clean_out_later,errors='ignore')
#                 #print("Debug, removed ",clean_out_later,"\n from ",magdata.keys())
#         #print("Debug: empty filled magdata = ",magdata)
            
#         magdata = magdata.to_csv(header=0,index=False,sep=',')
#         magdata = csv_removekeys(magdata) # change {k:v,k:v} to just [v,v]
#         magdata = unwind_csv_array(magdata) # change [v,v] to just v,v
#         status=tf_to_hapicode(status,len(magdata))

#     elif "/baseline_" in id: # was previously id.startswith('data'):
#         """ spec data/iaga/baseline_[all/yearly/none]/PT1M/[XYX/NEZ].json """
#         # New 'data' code, replaces prior mess
#         if "NEZ" in id:
#             vectortype = 'NEZ'
#         else:
#             vectortype = 'GEO'
#         station = id.split('/')[0] # (dataword,station)=id.split('_')
#         pattern = r'baseline_[^/]+'
#         match = re.search(pattern, id)
#         baseline = match.group()
#         """ The SuperMAG Python API expects flags N, E, Z but our
#             SuperHAPI spec renames to N_geo, E_geo, Z_geo and also
#             defaults to providing a Field_Vector = [N_geo, E_geo, Z_geo]
#             so the following code translates this, later sm_filter_data
#             will handle the returned pandas array to match the HAPI request.
#             The geomagnetic set is [N_nez, E_nez, Z_nez] in geomagnetic coords.
#             We also removed fetching individual N, E, Z in favor of the vector.
#         """
#         if 'Time' not in parameters: parameters.insert(0, 'tval')
#         parameters_munged = copy.deepcopy(parameters)
#         if parameters_munged != None:
#             if 'Field_Vector' in parameters:
#                 parameters_munged.extend(['N','E','Z'])
#                 parameters_munged.remove('Field_Vector')
#             #if 'N_geo' in parameters and 'N' not in parameters_munged:
#             #    parameters_munged[parameters_munged.index('N_geo')] = 'N'
#             #if 'E_geo' in parameters and 'E' not in parameters_munged:
#             #    parameters_munged[parameters_munged.index('E_geo')] = 'E'
#             #if 'Z_geo' in parameters and 'Z' not in parameters_munged:
#             #    parameters_munged[parameters_munged.index('Z_geo')] = 'Z'
#             #if 'N_geo' in parameters_munged: parameters_munged.remove('N_geo')
#             #if 'E_geo' in parameters_munged: parameters_munged.remove('E_geo')
#             #if 'Z_geo' in parameters_munged: parameters_munged.remove('Z_geo')
#             #if 'mlt' in parameters_munged:
#             #    i=parameters_munged.index('mlt')
#             #    parameters_munged[i:i+1] = ['mlt','mcolat']
#         else:
#             #flagstring = "&mlt&mcolat&geo&decl&sza"
#             parameters_munged = ['tval','Field_Vector','mlt','mcolat','sza','decl','N','E','Z']
#         if 'Time' in parameters_munged:
#             parameters_munged[parameters_munged.index('Time')] = 'tval'
#         flagstring = '&'.join(parameters_munged) # more than needed, will filter later
#         flagstring.replace("&Field_Vector","")

#         flagstring += f"&baseline='{baseline}'"
#         (status,magdata)=supermag_getdata(userid,start,extent,flagstring,station,FORMAT='json')
#         try:
#             magdata['tval'] = magdata['tval'].apply(sm_to_hapitimes)
#         except:
#             pass # pass when there is no valid data to parse

#         # Massive filtering needed to match parameters requested
#         if len(magdata) > 0:
#             magdata=sm_filter_data(magdata, parameters, vectortype)

#         magdata = magdata.to_csv(header=0,index=False,sep=',')
#         magdata = csv_removekeys(magdata) # change {k:v,k:v} to just [v,v]
#         magdata = unwind_csv_array(magdata) # change [v,v] to just v,v
#         #magdata = magdata.split('\n')# optional, converts csv string to list
#         status=tf_to_hapicode(status,len(magdata))
            
#     else:
#         # did not match a SuperMAG-likely keyword, so produce error message
#         status=1406 # 1406 is HAPI "unknown dataset id"
#         magdata="Error, \"" + id + "\" is not a valid query"

#     #print("Debug-- done for id,parameters: ",id,parameters)
#     return(status,magdata)


# def do_file_supermag( id, timemin, timemax, parameters):

#     # need to figure out time limits, and enforce them
#     #timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%d-%H-%M-%S')
#     #(yyyy,mo,dd,hh,mm,ss)=(int(x) for x in timemin.split('-'))
#     #timenow=datetime.datetime(yyyy,mo,dd,hh,mm,ss).timestamp()
#     #timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%d-%H-%M-%S')
#     #(yyyy,mo,dd,hh,mm,ss)=(int(x) for x in timemax.split('-'))

#     timenow = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%M:%SZ')
#     timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%M:%SZ')

#     if ( parameters!=None ):
#         mp= do_parameters_map( id, parameters )
#     else:
#         mp= None

#     # go in X-minute increments?????
#     increment = 10*60 # 10 minute increments

#     # TO DO: do we actually serve the data?
#     # TO DO: also pass parameters for subselecting data

#     mydata = ""
    
#     while timenow < timeend:
#         status=0 # gets set to 1 if this timestep works
#         if id == "polar":
#             # returns 600 vectors for that given minute as a dataframe
#             (status,mydata_df)= serve_polar_df(yyyy,dd,mo,hh,m)
#             mydata += mydata_df
#             status=tf_to_hapicode(status,len(mydata_list))
            
#         elif id == "inventory":
#             (status,mydata_list) = SuperMAGGetInventory(userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment)
#             mydata += mydata_list
#             status=tf_to_hapicode(status,len(mydata_list))
            
#         elif id == 'indices':
#             (status,magdata)=SuperMAGGetDataArray('indices',userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment,mystation)
#             mydata += magdata
#             status=tf_to_hapicode(status,len(magdata))

#         elif id == "all" or strlength(id) == 3:
#             # if a 3-digit string, is likely (?) a station request
#             if id == "all":
#                 (status,stations_list) = SuperMAGGetInventory(userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment)
#             else:
#                 stations_list=[id] # wants just 1 station
#             for mystation in stations_list:
#                 (status,magdata)=SuperMAGGetDataStruct('data',userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment,mystation)
#                 mydata += magdata
#             status=tf_to_hapicode(status,len(magdata))

#         else:
#             # did not match a HAPI-approved keyword, so produce error message
#             status=1406 # 1406 is HAPI "unknown dataset id"
#             mydata="Error, \"" + id + "\" is not a valid query"

#         #s.wfile.write(bytes(',',"utf-8"))
#         #s.wfile.write(bytes(ss[i],"utf-8"))
#         timenow = timenow + datetime.timedelta(minutes=+increment)
#         sm_data_to_csv(filename,mydata)


if __name__ == '__main__':
    pass
    # assert handle_hapi_request("http://yoyodyne.jhuapl.edu:8080/hapi/about") != ""
    # assert handle_hapi_capabilities_request() is not ""
    # assert handle_hapi_catalog_request() is not ""
    # assert handle_hapi_info_request() is not ""
    # assert handle_hapi_data_request() is not ""
