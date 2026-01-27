""" psp_config.py, specific config file for PSP web pass-thru

 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'hapi_psp.py' file for the pass-thru reader.

"""

from hapi_psp import *
api_datatype = 'web' # added here in case I later add files too
floc={}
HAPI_HOME= 'home_psp/'
title = 'PSP HAPI server'
hapi_handler = handle_hapi_data_request
tags_allowed = [] # allowed subparams
loaded_config =	True # required, used to verify config variables exists on load
# Currently PSP can't stream because it is a web pass-thru
stream_flag=False
# In theory, could stream by figuring out maximum interval PSP website
# allows, then loop over that and stream each chunk.  But this would require
# discussion/permission from PSP since their limits are for a reason.
