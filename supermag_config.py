""" supermag_config.py, specific config file for SuperMAG web pass-thru

 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'superhapi.py' file for the pass-thru reader.

"""

from supermag_api import *   # APL library as distributed by SuperMAG
from supermag_hapireader import * # APL includes 'do_data_supermag'
api_datatype = 'web' # added here in case I later add files too
floc={}
HAPI_HOME= 'home_supermag/'
hapi_handler = do_data_supermag
tags_allowed = ['delta=default','delta=start','baseline=yearly','baseline=none','baseline=default'] # allowed subparams
loaded_config =	True # required, used to verify config variables exists on load
# Currently SuperMAG can't stream because it is a web pass-thru
stream_flag=False  
# In theory, could stream by figuring out maximum interval SuperMAG website
# allows, then loop over that and stream each chunk.  But this would require
# discussion/permission from SuperMAG since their limits are for a reason.
