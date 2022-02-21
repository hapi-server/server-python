""" guvi_config.py, specific config file for GUVI NetCDF files

 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'guvihapi.py' file for the NetCDF reader.


"""

from netcdf_hapireader import *
api_datatype = 'file'
floc={'dir':'home_netcdf/rawdata/'} # location of data, with a closing /
HAPI_HOME= 'home_netcdf/'
hapi_handler = do_data_netcdf
tags_allowed = ['testme=true'] # no subparams allowed
loaded_config = True # required, used to verify config variables exists on load
stream_flag=True  # True = stream per-file, False = process all then serve
