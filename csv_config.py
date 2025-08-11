""" csv_config.py, generic config file for local CSV files

 as per Jeremy's original code
 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'csvhapi.py' file for the reader program.

 Assumes data is flat files in a directory hierarchy of
 "data/[id]/YYYY/[id].YYYYMMDD.csv
"""


import csv_hapireader
HAPI_HOME = 'home_csv/'
title = 'HAPI CSV Server'
api_datatype = 'file'
floc={'dir':'home_csv'}
hapi_handler = csv_hapireader.do_data_csv
tags_allowed = [''] # no subparams allowed                                  
loaded_config = True # required, used to verify config variables exists on load
stream_flag=True # True = stream per file, False = process all then serve
