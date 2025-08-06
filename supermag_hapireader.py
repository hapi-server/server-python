""" superhapi.py, web pass-thru reader for SuperMAG

 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'supermag_config.py' file for the site-specific details.


"""

# hapi supermag test routines (and general python testing)

#import netCDF4 as nc
#import numpy as np
#import pandas as pd
import copy
import xarray as xr
import pandas as pd
import time
import os
#
import urllib.request
import certifi # needed at APL for SSL
from pandas import to_datetime # used for julian date, actually
import json
import re
#from datetime import datetime, timedelta
import datetime

from supermag_api import *

#https://supermag.jhuapl.edu/services/data-api.php?fmt=json&logon=superhapi&start=2019-10-15T10:40&extent=000000003600&station=HRN&mlt&aacgm&geo&decl&sza       #https://supermag.jhuapl.edu/services/data-api.php?fmt=json&logon=superhapi&start=2019-10-15T10:40&extent=000000003600&station=NCK&mlt&aacgm&geo&decl&sza      

def sm_filter_data(magdata, parameters, vectortype):
    # Handles wonky SuperMAG data_NNN API keys
    # because all queries return ext, iaga, and the NEZ set
    if 'Field_Vector' in parameters:
        if vectortype == 'NEZ':
            magdata['Field_Vector'] = magdata.apply(lambda row: [row['N'].get('nez'), row['E'].get('nez'), row['Z'].get('nez')], axis=1)
        else:
            magdata['Field_Vector'] = magdata.apply(lambda row: [row['N'].get('geo'), row['E'].get('geo'), row['Z'].get('geo')], axis=1)
    #if 'N_geo' in parameters:
    #    magdata['N_geo'] = magdata['N'].apply(lambda x: x.get('geo'))
    #if 'E_geo' in parameters:
    #    magdata['E_geo'] = magdata['E'].apply(lambda x: x.get('geo'))
    #if 'Z_geo' in parameters:
    #    magdata['Z_geo'] = magdata['Z'].apply(lambda x: x.get('geo'))

    if 'mlt' in parameters:
        magdata['mlt'] = magdata.apply(lambda row: [row['mlt'], row['mcolat']], axis=1)
        
    allparams = ['ext','iaga','N','E','Z','mcolat']
    dropme = []
    for para in allparams:
        if para not in parameters:
            dropme.append(para)
    #print("Debug, magdata pre-delete is ",magdata)
    #print("Debug, desired parameters are ",parameters)
    #print("Debug, deleting: ",dropme)
    if dropme != None:
        magdata=magdata.drop(columns=dropme,errors='ignore')
    #print("Debug, magdata after delete is ",magdata)

    # reorder to match original request (handles any bad munging)
    parameters_munged = copy.deepcopy(parameters)
    if 'Time' in parameters_munged:
        parameters_munged[parameters_munged.index('Time')] = 'tval'
    magdata = magdata[parameters_munged]

    return(magdata)
        

def sm_lookup(parameters):
    # converts HAPI 'parameters' into keywords expected by API call
    # for data that requires 2 API keywords, gives them here
    clean_out_later=[] # for storing 'excess' data items temp needed
    #parameters = [x.lower() for x in parameters]
    x_apikeys = {'SMLmlat':['sml','mlat'],
                 'SMLmlt':['sml','mlt'],
                 'SMLglon':['sml','glon'],
                 'SMLstid':['sml','stid'],
                 'SMLglat':['sml','glat'],
                 'SMLstid':['sml','stid'],
                 'SMUmlat':['smu','mlat'],
                 'SMUmlt':['smu','mlt'],
                 'SMUglon':['smu','glon'],
                 'SMUstid':['smu','stid'],
                 'SMUglat':['smu','glat'],
                 'SMUstid':['smu','stid'],

                 'SMLsmlat':['smls','mlats'],
                 'SMLsmlt':['smls','mlts'],
                 'SMLsglon':['smls','glons'],
                 'SMLsstid':['smls','stids'],
                 'SMLsglat':['smls','glats'],
                 'SMLsstid':['smls','stids'],
                 'SMUsmlat':['smus','mlats'],
                 'SMUsmlt':['smus','mlts'],
                 'SMUsglon':['smus','glons'],
                 'SMUsstid':['smus','stids'],
                 'SMUsglat':['smus','glats'],
                 'SMUsstid':['smus','stids'],

                 'SMLdmlat':['smld','mlatd'],
                 'SMLdmlt':['smld','mltd'],
                 'SMLdglon':['smld','glond'],
                 'SMLdstid':['smld','stidd'],
                 'SMLdglat':['smld','glatd'],
                 'SMLdstid':['smld','stidd'],
                 'SMUdmlat':['smud','mlatd'],
                 'SMUdmlt':['smud','mltd'],
                 'SMUdglon':['smud','glond'],
                 'SMUdstid':['smud','stidd'],
                 'SMUdglat':['smud','glatd'],
                 'SMUdstid':['smud','stidd'],

                 'SMLrmlat':['smlr','mlatd'],
                 'SMLrmlt':['smlr','mltd'],
                 'SMLrglon':['smlr','glonr'],
                 'SMLrstid':['smlr','stidr'],
                 'SMLrglat':['smlr','glatr'],
                 'SMLrstid':['smlr','stidr'],
                 'SMUrmlat':['smur','mlatr'],
                 'SMUrmlt':['smur','mltr'],
                 'SMUrglon':['smur','glonr'],
                 'SMUrstid':['smur','stidr'],
                 'SMUrglat':['smur','glatr'],
                 'SMUrstid':['smur','stidr']
                 }

    # note that we need to send msl, smu, sme in lowercase to supermag,
    # but the data they return is uppercase, so when we later filter
    # them out, we need the awkward .upper() casting below

    # Edge case, asking just for 'Time' or 'Time'+ other stuff, when
    # time is _always_ returned anyway
    if len(parameters) == 1 and parameters[0] == 'Time':
        parameters[0]='sme'  # replace Time with a junk data so data is fetched
        clean_out_later.append('SME') # and mark that junk data later

    # general clean-up
    try:
        parameters.remove('Time')
    except:
        pass
        
    for para in parameters:
        if para in x_apikeys.keys():
            # replace it
            #print("debug, replacing: ",para)
            parameters.remove(para)
            for element in x_apikeys[para]:
                # remember to check if spurious exists, before adding in
                #print("debug, checking on ",element)
                # See above note on why .upper() is here
                if element.upper() not in parameters:
                    clean_out_later.append(element.upper())
                parameters.append(element)

                #print("debug: para: ",parameters)

    # and again, have to cast to lower because api expects that
    parameters = [x.lower() for x in parameters]

    return(parameters, clean_out_later)
                

def sm_fill_empty(magdata,parameters,paramspec):
    # validate, fill if necessary
    # Although designed for SuperMAG, works well for any DataFrame data
    #
    # Note only can fill up to 1D lists (not built for 2D+ empty elements yet)
    # 'parameters' is the subset of data items we are using
    # 'paramspec' is the array potentially containing size and fill info
    # goes through each parameter, calls up paramspec['size'],
    # if data does not match, then puts in paramspec['fill']
    # Also, we search case-insensitive for param matches, to be more robust.
    
    nele = magdata.shape[0]
    for para in parameters:
        #print("debug, para: ",para," and columns: ",magdata.columns, "and paramspec:",paramspec)
        if para.lower() not in magdata.columns.str.lower():
            try:
                details=[item for item in paramspec if item['name'].lower() == para.lower()][0]
                #print("debug, param specs are: ",details)
            except:
                pass # no match, usually due to case-sensitivity
            try:
                mysize = details['size'][0]
            except:
                #print("debug, could not find size, using default")
                mysize=1
            try:
                myfill = details['fill']
            except:
                #print("debug, could not find fill, using default")
                myfill=0
            # cast to type since HAPI catalog uses strings for fills
            try:
                mytype = details['type']
                if mytype == 'double': myfill=float(myfill)
                if mytype == 'integer': myfill=int(myfill)
            except:
                pass
            #print("Debug-- missing data for ",para, "Got size ",mysize,", fill ",myfill)
            if mysize > 1:
                element = [myfill] * mysize
            else:
                element = myfill
            dummy = [element] * nele
            magdata[para] = dummy
    #n1=len(parameters)
    #n2 = magdata.shape[1] - 1 # remove Time and count
    #for i in range(n2,n1): # only fills if n1>n2
    #    print("debug: paramspec:",paramspec)
    #    dummy=[0] * nele
    #    magdata[parameters[i-n1]] = dummy
    #    print("Debug: adding col ",i,parameters[i-n1])
    #    #print("Debug: sizes compare:",n1,' vs ',n2)
    

def test_polar():
    (yyyy,mo,dd,hh,mm,ss)=(2020,11,2,13,24,00)
    (status,mydata)=serve_polar_df(yyyy,mo,dd,hh,mm)
    if status > 0: print("Success",mydata)
    else: print("Failed",mydata)
    return(status,mydata)

def samplerun_testtiming():

    base=time.time()

    filename='hapi_sample.ncdf'
    mydata = xr.open_dataset(filename) # get in as an xarray
    print(time.time()-base,'sec to open')
    base=time.time()

    minute, hour = 10, 20
    sub_d=mydata.sel(block=(hour*60)+minute)
    #my_df = data.to_dataframe().reset_index()  # if you need all the data
    ##minute, hour = 1, 1
    # data files are for 1 day, so this lets you grab any given minute
    ##sub_d=mydata.where( (mydata.time_mt == minute) & (mydata.time_hr == hour),drop=True)
    print(time.time()-base,'sec to subselect 1st frame')
    base=time.time()

    my_df=sub_d.to_dataframe().reset_index()
    print(time.time()-base,'sec to convert to dataframe')
    base=time.time()

    my_df.vector=maketimestamp_df(my_df)
    my_df.drop(columns=['id']) # not needed, might confuse users
    print(time.time()-base,'sec to make timestamps and drop 2 columns')
    base=time.time()

    stringize_df(my_df) # prep it for csv-ing
    print(time.time()-base,'sec to stringize for csv-ing')
    base=time.time()
    my_df.apply(csvme2screen,axis=1) # apply function to each row
    print(time.time()-base,'sec to convert to csv')
    base=time.time()

    #my_df = data.to_dataframe().reset_index()  # if you need all the data
    minute, hour = 2, 4
    sub_d2=mydata.where( (mydata.time_mt == minute) & (mydata.time_hr == hour),drop=True)
    print(time.time()-base,'sec to subselect 2nd frame')
    base=time.time()
    my_df=sub_d2.to_dataframe().reset_index()
    print(time.time()-base,'sec to convert to dataframe')
    base=time.time()



"""
from Robin 4/1 tagup:
SuperMAG polar plots aka level 3 derived data, take their netCDF files
and set up as a HAPI server, data is every 2 minutes and there are two
kinds of grids, the mlt map grid aka 1 hour of mlt is 5 degrees of
measured latitude so each point is a vector with north/vertical/mumble. 
note level 1 data is gappy so not good to use, stations drop in and out
etc, so it requires finesse to see if data is available.  use weatherwax
machine as it has the disks mounted on it, or go to SOF to get them to
mount.  Port 9000 fine for local testing but we will have to work with
network once we go for real.    current supermag is just curl request
using wget.  they also have an idl client.
"""

#  Fast Access code:
#filename='hapi_sample.ncdf'
#mydata = xr.open_dataset(filename)
#subme=mydata.sel(block=100)
# next line 24 sec if 1st operation; <1 sec otherwise
#sub_d=mydata.where( (mydata.time_mt == 1) & (mydata.time_hr == 1),drop=True) 
# next line 90 sec, if 1st operation; <1 sec otherwise
#full_df=mydata.to_dataframe() 

def sm_to_hapitimes(mytime):
    mytime=time.strftime('%Y-%m-%dT%H:%MZ',time.gmtime(mytime))
    return(mytime)

def unwind_csv_array(magdata):
    """ Takes json-like arrays of e.g.
        60.0,DOB,"[ -19.104668,-20.155156]"
    or
        60.0,DOB,[ -19.104668,-20.155156]
    and converts to unwound HAPI version of e.g.
       60.0,DOB,-19.104668,-20.155156
    """

    magdata = re.sub(r'\]\"','',magdata)
    magdata = re.sub(r'\"\[','',magdata)
    magdata = re.sub(r', ',',',magdata) # also remove extra spaces
    return(magdata)

def csv_removekeys(magdata):
    # use:    magdata = api_removekeys(magdata)
    # changes {k:v,k:v} to just [v,v]
    magdata = re.sub(r'\'\w+\':','',magdata)
    magdata = re.sub(r'\{','[',magdata)
    magdata = re.sub(r'\}',']',magdata)
    magdata = re.sub(r'  ',' ',magdata)
    magdata = re.sub(r', ',',',magdata)
    # also remove extraneous end commas from some parsings
    magdata = re.sub(r',\n','\n',magdata)

    return(magdata)

# TIMESTAMPING WORKS!
def maketimestamp_str(yr,mo,dy,hr,mt,sc):
    mytime = "%4.4d%2.2d%2.2d%2.2d%2.2d%2d" % (yr, mo, dy, hr, mt, sc)
    return(mytime)

def maketimestamp_df(data_df):
    timestamps = data_df.time_yr.apply(lambda x: "%4.4d" % x) + data_df.time_mo.apply(lambda x: "%2.2d" % x) + data_df.time_dy.apply(lambda x: "%2.2d" % x) + data_df.time_hr.apply(lambda x: "%2.2d" % x) + data_df.time_mt.apply(lambda x: "%2.2d" % x) + data_df.time_sc.apply(lambda x: "%2.2d" % x)
    return(timestamps)


def csv_me_polar(row,printme=1):
    me = '\"' + row['vector'] + '\",\"' + row['mlat'] + '\",\"' + row['mlon'] + '\",\"' +  row['mcolat'] + '\",\"' +  row['mlt'] + '\",\"' + row['dbn_nez'] + '\",\"' + row['dbe_nez'] + '\",\"' + row['dbz_nez'] + '\",\"' + row['dbn_geo'] + '\",\"' + row['dbe_geo'] + '\",\"' + row['dbz_geo'] + '\"'
    if printme == 0: print(me)
    return(me)

def stringize_df(data_df):
    # since data_df is a dataframe, this will permanently change it
    data_df.mlat = data_df.mlat.apply(str)
    data_df.mlon = data_df.mlon.apply(str)
    data_df.mcolat = data_df.mcolat.apply(str)
    data_df.mlt = data_df.mlt.apply(str)
    data_df.dbn_nez = data_df.dbn_nez.apply(str)
    data_df.dbe_nez = data_df.dbe_nez.apply(str)
    data_df.dbz_nez = data_df.dbz_nez.apply(str)
    data_df.dbn_geo = data_df.dbn_geo.apply(str)
    data_df.dbe_geo = data_df.dbe_geo.apply(str)
    data_df.dbz_geo = data_df.dbz_geo.apply(str)


def make_polarname(yyyy,mo,dd,orient='north'):
    # currently all files of the form 20201231.north.schavec-mlt-supermag.60s.rev-0005.ncdf.gz
    rev='rev-0005'
    mytime = "%4.4d%2.2d%2.2d" % (yyyy, mo, dd)
    fname = mytime + '.' + orient.lower() + '.schavec-mlt-supermag.60s.' + rev + '.ncdf' + '.gz'
    return(fname)


def serve_polar_df(yyyy,mo,dd,hh,mm,orient='north'):
    # for the given minute, returns the 600 vector field dataframe
    status = 0 # track whether this works in the end or not

    filename=make_polarname(yyyy,mo,dd,orient)

    filename = '../data/'+ filename
    if os.path.exists(filename):
        try:
            mydata = xr.open_dataset(filename) # get in as an xarray
            sub_d=mydata.sel(block=(hh*60)+mm)
            my_df=sub_d.to_dataframe().reset_index()
            my_df.vector=maketimestamp_df(my_df)
            my_df.drop(columns=['id']) # not needed, might confuse users
            stringize_df(my_df) # prep it for csv-ing
            mydata=my_df.apply(csv_me_polar,axis=1) # apply function to each row
            status=1 # it worked
        except:
            mydata=pd.DataFrame({0:["error","unable to process data"]})
    else:
        mydata=pd.DataFrame({0:["error",filename+" not found"]})
    
    return(status,mydata)

"""
# mini test

from superhapi import *
timemin= '2020-01-19T00:00Z'
timemax= '2020-01-20T00:00Z'
parameters='tbd'
sout='filehandle to be defined later'
myjson=do_data_supermag('inventory',timemin,timemax,parameters,sout,null)


"""

"""
#import pickle
#pickle.dump(dataframe,open("temp.sav","wb"))
#testing
import pickle
dataframe=pickle.load(open('temp.sav','rb'))
"""   

def tf_to_hapicode(status,datasize):
    # converts 0=bad, 1=good to HAPI code
    if status == 0:
        status = 1500 # 1500 is HAPI "Internal server error"
    elif datasize > 0:
        status=1200 # 1200 is HAPI "OK"
    else:
        status=1201 # 1200 is HAPI "OK - no data for time range"
    return(status)


# appends the given data to the file as csv
def sm_data_to_csv(filename,mydata):
        comma=""
        dsize=len(mydata)
        for i, rec in enumerate(mydata):
            if i == dsize-1: comma=""  # turn off comma for last entry
            s.wfile.write(bytes(comma+rec+"\n","utf-8"))
            comma=","   # ensures all entries past first get a comma
# e.g. key='N'


def do_data_supermag(id,timemin,timemax,parameters,catalog,floc,
                     stream_flag, stream):
    #print("debug, got parameters: ",parameters)
    # 'ignore' is because hapi-server uses that only for file-bsaed fetches
    userid='superhapi'  # debug, temporarily for now

    #timenow = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%M:%SZ')
    #timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%M:%SZ')
    start = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%MZ')
    timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%MZ')
    delta=timeend-start
    extent = delta.total_seconds()

    if len(floc['customOptions']) > 0:
        parameters += floc['customOptions']
    
    #if ( parameters!=None ):
    #    mp= do_parameters_map( id, parameters )
    #else:
    #    mp= None
    #print("debug: parameters found ",parameters)
    final_parameters = parameters  # save for later

    #print("debug: parameters updated ",parameters)

    #print("debug: id is ",id)
    if id.startswith("stations"):
        """ note this is NOT a proper HAPI function so we do not use it
            as it only returns a list of stations, not a time-ordered
            function
            To get a list of stations, construct a URL akin to:
        https://supermag.jhuapl.edu/services/inventory.php?python&nohead&start=2018-01-18T00:00&logon=superhapi&extent=000000086400
        """
        # no 'parameters' used by this
        #print("Debug:",userid,start,extent)
        (status,magdata) = supermag_getinventory(userid,start,extent) # FORMAT='list')
        ## add column 'window_end'
        for i, iaga in enumerate(magdata):
            #print(timemin, iaga, timemax)
            line = timemin + ',' + iaga + ',' + timemax
            magdata[i] = line
        magdata = '\n'.join(magdata)
        #magdata = "#Time,IAGA,window_end\n" + magdata
        #magdata = '\',\''.join(magdata) # cheap csv-ing
        #magdata = '\'' + magdata + '\'' # add leading and following quote
        status=tf_to_hapicode(status,len(magdata))
    elif id.startswith('indices'):
        # 'parameters' is which data items to fetch, HAPI default = 'all'
        (parameters, clean_out_later) = sm_lookup(parameters)
            
        (status,magdata)=supermag_getindices(userid,start,extent,parameters,FORMAT='json')
        # (note we remove 'row' because HAPI requires start as 1st var)

        # converts to csv string with \n
        #magdata = magdata.to_csv(header=1,index=False)
        try:
            magdata['tval'] = magdata['tval'].apply(sm_to_hapitimes)
        except:
            pass # pass when there is no valid data to parse
        #print("Debug: all magdata = ",magdata)

        if parameters != None:
            # verify and fill if no data exists
            #print("debug: catalog is ",catalog)
            #print("debug: catalog keys are ",catalog.keys())
            ### NOTE-- removed sm_fill_empty() BECAUSE SuperMAG data is weird
            ###sm_fill_empty(magdata,parameters,catalog['parameters'])
            #magdata.rename(indicesmap,inplace=True,errors='ignore')
            #print("Debug: renamed magdata = ",magdata)

            if len(clean_out_later) > 0:
                #print("Debug, removing ",clean_out_later,"\n from ",magdata.keys())
                magdata=magdata.drop(columns=clean_out_later,errors='ignore')
                #print("Debug, removed ",clean_out_later,"\n from ",magdata.keys())
        #print("Debug: empty filled magdata = ",magdata)
            
        magdata = magdata.to_csv(header=0,index=False,sep=',')
        magdata = csv_removekeys(magdata) # change {k:v,k:v} to just [v,v]
        magdata = unwind_csv_array(magdata) # change [v,v] to just v,v
        status=tf_to_hapicode(status,len(magdata))

    elif "/baseline_" in id: # was previously id.startswith('data'):
        """ spec data/iaga/baseline_[all/yearly/none]/PT1M/[XYX/NEZ].json """
        # New 'data' code, replaces prior mess
        if "NEZ" in id:
            vectortype = 'NEZ'
        else:
            vectortype = 'GEO'
        station = id.split('/')[0] # (dataword,station)=id.split('_')
        pattern = r'baseline_[^/]+'
        match = re.search(pattern, id)
        baseline = match.group()
        """ The SuperMAG Python API expects flags N, E, Z but our
            SuperHAPI spec renames to N_geo, E_geo, Z_geo and also
            defaults to providing a Field_Vector = [N_geo, E_geo, Z_geo]
            so the following code translates this, later sm_filter_data
            will handle the returned pandas array to match the HAPI request.
            The geomagnetic set is [N_nez, E_nez, Z_nez] in geomagnetic coords.
            We also removed fetching individual N, E, Z in favor of the vector.
        """
        if 'Time' not in parameters: parameters.insert(0, 'tval')
        parameters_munged = copy.deepcopy(parameters)
        if parameters_munged != None:
            if 'Field_Vector' in parameters:
                parameters_munged.extend(['N','E','Z'])
                parameters_munged.remove('Field_Vector')
            #if 'N_geo' in parameters and 'N' not in parameters_munged:
            #    parameters_munged[parameters_munged.index('N_geo')] = 'N'
            #if 'E_geo' in parameters and 'E' not in parameters_munged:
            #    parameters_munged[parameters_munged.index('E_geo')] = 'E'
            #if 'Z_geo' in parameters and 'Z' not in parameters_munged:
            #    parameters_munged[parameters_munged.index('Z_geo')] = 'Z'
            #if 'N_geo' in parameters_munged: parameters_munged.remove('N_geo')
            #if 'E_geo' in parameters_munged: parameters_munged.remove('E_geo')
            #if 'Z_geo' in parameters_munged: parameters_munged.remove('Z_geo')
            if 'mlt' in parameters_munged:
                i=parameters_munged.index('mlt')
                parameters_munged[i:i+1] = ['mlt','mcolat']
        else:
            #flagstring = "&mlt&mcolat&geo&decl&sza"
            parameters_munged = ['tval','Field_Vector','mlt','mcolat','sza','decl','N','E','Z']
        if 'Time' in parameters_munged:
            parameters_munged[parameters_munged.index('Time')] = 'tval'
        flagstring = '&'.join(parameters_munged) # more than needed, will filter later
        flagstring.replace("&Field_Vector","")

        flagstring += f"&baseline='{baseline}'"
        (status,magdata)=supermag_getdata(userid,start,extent,flagstring,station,FORMAT='json')
        try:
            magdata['tval'] = magdata['tval'].apply(sm_to_hapitimes)
        except:
            pass # pass when there is no valid data to parse

        # Massive filtering needed to match parameters requested
        magdata=sm_filter_data(magdata, parameters, vectortype)

        magdata = magdata.to_csv(header=0,index=False,sep=',')
        magdata = csv_removekeys(magdata) # change {k:v,k:v} to just [v,v]
        magdata = unwind_csv_array(magdata) # change [v,v] to just v,v
        #magdata = magdata.split('\n')# optional, converts csv string to list
        status=tf_to_hapicode(status,len(magdata))
            
    else:
        # did not match a SuperMAG-likely keyword, so produce error message
        status=1406 # 1406 is HAPI "unknown dataset id"
        magdata="Error, \"" + id + "\" is not a valid query"

    #print("Debug-- done for id,parameters: ",id,parameters)
    return(status,magdata)


def do_file_supermag( id, timemin, timemax, parameters):

    # need to figure out time limits, and enforce them
    #timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%d-%H-%M-%S')
    #(yyyy,mo,dd,hh,mm,ss)=(int(x) for x in timemin.split('-'))
    #timenow=datetime.datetime(yyyy,mo,dd,hh,mm,ss).timestamp()
    #timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%d-%H-%M-%S')
    #(yyyy,mo,dd,hh,mm,ss)=(int(x) for x in timemax.split('-'))

    timenow = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%M:%SZ')
    timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%M:%SZ')

    if ( parameters!=None ):
        mp= do_parameters_map( id, parameters )
    else:
        mp= None

    # go in X-minute increments?????
    increment = 10*60 # 10 minute increments

    # TO DO: do we actually serve the data?
    # TO DO: also pass parameters for subselecting data

    mydata = ""
    
    while timenow < timeend:
        status=0 # gets set to 1 if this timestep works
        if id == "polar":
            # returns 600 vectors for that given minute as a dataframe
            (status,mydata_df)= serve_polar_df(yyyy,dd,mo,hh,m)
            mydata += mydata_df
            status=tf_to_hapicode(status,len(mydata_list))
            
        elif id == "inventory":
            (status,mydata_list) = SuperMAGGetInventory(userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment)
            mydata += mydata_list
            status=tf_to_hapicode(status,len(mydata_list))
            
        elif id == 'indices':
            (status,magdata)=SuperMAGGetDataArray('indices',userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment,mystation)
            mydata += magdata
            status=tf_to_hapicode(status,len(magdata))

        elif id == "all" or strlength(id) == 3:
            # if a 3-digit string, is likely (?) a station request
            if id == "all":
                (status,stations_list) = SuperMAGGetInventory(userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment)
            else:
                stations_list=[id] # wants just 1 station
            for mystation in stations_list:
                (status,magdata)=SuperMAGGetDataStruct('data',userid,timenow.year,timenow.month,timenow.day,timenow.hour,timenow.minute,0,increment,mystation)
                mydata += magdata
            status=tf_to_hapicode(status,len(magdata))

        else:
            # did not match a HAPI-approved keyword, so produce error message
            status=1406 # 1406 is HAPI "unknown dataset id"
            mydata="Error, \"" + id + "\" is not a valid query"

        #s.wfile.write(bytes(',',"utf-8"))
        #s.wfile.write(bytes(ss[i],"utf-8"))
        timenow = timenow + datetime.timedelta(minutes=+increment)
        sm_data_to_csv(filename,mydata)

