"""HAPI Python Server, including sample reader programs.

original by jbfaden, Python3 update by sandyfreelance 04-06-2021 and onward

This program sets up a server to stream HAPI-specification data to any
existing HAPI client programs.  Setup requires making a configuration
file for your server file setup, a set of JSON configuration files to
comply with the HAPI specification, and use of a 'reader' program to
convert your files into HAPI-formatted data (sample readers provided).

The code and documentation resides at 
    https://github.com/hapi-server/server-python

Usage:
  python hapi-server3.py <MISSIONNAME> [localhost/http/https/custom]
(If no arguments provided, defaults to 'csv' and 'localhost')

where MISSIONNAME points to the appropriation MISSIONNAME.config file
and:
   localhost: server runs on localhost/port 8080
   http:      server runs on port 80
   https:     server runs on port 443
   custom:    server runs on custom port that you hardcode into this code

Configuration requirements
* capabilities and catalog responses must be formatted as JSON in SERVER_HOME
* info responses are in SERVER_HOME/info.
* responses can have templates like "lasthour" to mean the last hour boundary
  and "lastday-P1D" to mean the last midnight minus one day.

IDs must be defined, as per HAPI, in info/*.json.

The 'reader' routines (coded by the mission) then specify which data
to actually return for each id, in the handler code.  Currently this
HAPI server has sample readers that can handle:
1) csv flat files in a directory hierarchy of "data/[id]/YYYY/[id].YYYYMMDD.csv"
2) reading netCDF files and sending csv of a pre-defined sets of keys (GUVI)

Additional readers will be provided as they are developed, and you are
encouraged to create your own.  A reader has to read your data files
and return CSV-formatted data for the subset of variables selected.

Note server can implement per-file streaming or fetch-all then serve
via the _config.py "stream_flag".  Generally, per-file continues sending
data as it is processed and is generally recommended; fetch-all is useful
if you need to add anything to post-process data before sending, or
if data sets are small (so either way works).

Dev notes:
  server3: python2 version updated for python3
  server3b: configurable for different missions
  server3c: allows multiple missions via the command line
  server3d: url before /hapi can indicate different data archives
  server3e: fixes to bring more in line with HAPI spec
  server3f: imports '<MISSION>_config.py' with setup params (not hard-coded)
            and allows both HAPI 2.x and 3.x spec on keywords
  server3g: fixes to pass validation checks
  server3h: added customRequestionOptions
  server3i: refactored for readability
  server3j: choice to stream per-file data or wait for all data then serve

On Python2 versus Python3:
  difference between Python3 as provided to github and APL-site specific
  is items tagged as #APL  (2 imports, 1 line replaced)
                            imports = supermap-api, hapi-server
                            APL replaces 'do_data_csv' with
                           'do_data_supermag'/'do_data_guvi'
  also Python2 uses wfile.write("") but
       Python3 uses wfile.write(bytes("","utf-8"))
  Python3 removed 'has_key' from dictionaries, use 'in' or '__contains__(key)'

"""

### FLAGS YOU MAY WANT TO CHANGE (hard-coded)

noisy=False   #set True for unix command line feedback

###############################################################################
# Ideally, nothing in this code below this line needs to be changed
# Instead, a <MISSION>_config.py sets the parameters needed for the site.

### IMPORTS ###

import sys # only used for command-line arguments

import time
from time import gmtime, strftime
# Python2 uses urlparse, Python3 uses urllib.parse
#import urlparse
import urllib.parse as urlparse
import glob
import os
from os.path import exists
import dateutil.parser
from datetime import datetime, date, timedelta
import datetime
from dateutil.parser import parse, isoparse
import json
from email.utils import parsedate_tz,formatdate
import importlib
import sys

# note also conditional import of S3 & NetCDF items after USE_CASEs, below

#    USE_CASE = 'csv'
#    # APL choices are 'csv', 'guvi', 'guviaws', or 'supermag'

# Arg 2 can be 'localhost', 'http', 'https', or 'custom'
# Use 'custom' if need you need to mod this code to define a non-standard port


class defaultvars():
    #import csv_hapireader
    HAPI_HOME= 'home_csv/'
    api_datatype = 'file'
    floc={}
    reader_name = "csv_hapireader"
    csv_hapireader = importlib.import_module(reader_name, package=None)
    hapi_handler = csv_hapireader.do_data_csv
    tags_allowed = [''] # no subparams allowed
    stream_flag = True

def fetchdata(hapi_handler, id, timemin, timemax, parameters, mydata,
              floc, stream_flag, s):
    (status, data) = hapi_hander(id, timemin, timemax, parameters, mydata, floc, stream_flag, s)
    return (status, data)
    
def parse_config(myname):
    
    ### GET AND PARSE CONFIG FILE ###
    try:
        cname = myname + "_config"
        cfile = cname + ".py"
    except:
        cname = "config"
        cfile = "config.py"
    if exists(cfile):
        try:
            print("Loading config file for ",cname," (",cfile,")")
            CFG = importlib.import_module(cname, package=None)
            # created CFG.<varnames>
            # verify all required elements exist by making local copy
            # so the try/except will fail if a variable is missing
            HAPI_HOME = CFG.HAPI_HOME
            api_datatype=CFG.api_datatype
            floc = CFG.floc
            reader_name = CFG.reader_name
            hapi_handler = CFG.hapi_handler
            tags_allowed = CFG.tags_allowed
            stream_flag = CFG.stream_flag
            if CFG.loaded_config:
                print("Successfully loaded ",cfile)
        except:
            print("Error, config file ",cfile," missing required elements")
            quit()
    else:
        print("Note no config file ",cfile," exists, using generic CSV defaults")
        # csv_config.py, generic config file for local CSV files
        # as per Jeremy's original code
        import csv_hapireader
        CFG=defaultvars()        
    return CFG

##if CFG.api_datatype == 'aws':
##    import s3netcdf
    
### GET HAPI VERSION we need to support
# (mostly needed for id/dataset, time.min/start, time.max/stop keywords)
def get_hapiversion(hapi_home):
    fin=open( hapi_home + 'capabilities.json','r')
    jset=json.loads(fin.read())
    fin.close()
    hapi_version = float(jset['HAPI'])
    print("Using HAPI version",hapi_version)
    return hapi_version

# below now moved to info/*.json instead of capabilities.json
### potential "x_*" parameters in capabilities.json extracted here
##try:
##    xopts = jset['x_customRequestOptions']
##    #print("Debug, valid xopts are: ",xopts)
##except:
##    xopts=''
##print("debug, got x_capabilities of: ",xopts)

### CORE CLASS ###

class StdoutFeedback():
    def __init__(self):
        print('feedback is over stdout')
    def setup(self):    
        print('setup feedback.')
    def destroy(self):
        print('destroy feedback.')
    def start(self,requestHeaders):
        ##from time import gmtime, strftime
        print('-----', strftime("%Y-%m-%dT%H:%M:%SZ", gmtime()), '-----')
        print(requestHeaders)
    def finish(self,responseHeaders):
        print('---')
        for h in responseHeaders:
            print('%s: %s' % ( h, responseHeaders[h] ))
        print('-----')
    
class NoFeedback():
    # does not log anything
    def __init__(self):
        pass
    def setup(self):
        pass
    def destroy(self):
        pass
    def start(self,ignore):
        pass
    def finish(self,ignore):
        pass

    
if ( noisy ):
    feedback= StdoutFeedback()  # When testing at the unix command line.
else:
    feedback= NoFeedback()  # for quiet deployment


### CORE FUNCTIONS ###

def fetch_info_params(id, hapihome, isFile):
    # open the info/[id].json file and return the parameter array
    # set isFile=True if 'id' is a filename, False if 'id' is actually an id
    # e.g. mydata['startDate'], mydata['stopDate'], etc
    status=False
    try:
        if isFile:
            #print("Debug, json file is ",id)
            jo = open(id)
        else:
            #print("Debug: id file is ", HAPI_HOME + 'info/' + id + '.json' )
            jo = open( hapihome + 'info/' + id + '.json' )
        mydata=json.loads(jo.read())
        jo.close()
        status=True
    except:
        #print("debug: could not access file for ",id)
        mydata={'startDate':'30010101T00:00',
                'stopDate':'00010101T00:00',
                'parameters':{}}

    # handle HAPI's in-house 'lasthour' as a proper date
    if 'stopDate' in mydata.keys():
        mydata['stopDate'] = lasthour_mod(mydata['stopDate'])
    # added 'limitduration' as a new HAPI keyword that might exist
    # in info/[id].json, units=sec
    if 'limitduration' not in mydata.keys():
        mydata['limitduration']=0  # 0 = no limit enforced
    #print("Debug: status on checking limits is: ",status)
    return(status,mydata)


### Sample generic HAPI formatting utilities for user-made parsers
### (Put here so you don't have to rewrite your own)

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
    return(magdata)


### HAPI required error and support utilities

def generic_check_error(id, timemin, timemax, parameters, hapihome):
    # does check of generic HAPI parameters
    # note we already checked that time.min (1402),time.max (1403) are 
    # valid prior to this and also that id is valid (1406)

    timemax = lasthour_mod(timemax)
    errorcode = 0 # assume all is well
    (stat,mydata)=fetch_info_params(id,hapihome,False)
    if stat == False:
        errorcode = 1406
        # no valid json exists so 'Bad request - unknown dataset id'
        qtimemin= timemin # no change
        qtimemax= timemax # no change
    else:
        #print("debug: for id ",id," got ",mydata)
        limit_duration=mydata['limitduration']
        archive_startdate=parse(mydata['startDate'],ignoretz=True)
        # yet more datehandling, for now/lastday/lasthour/etc
        stopdate=mydata['stopDate']
        if 'now' in stopdate or 'last' in stopdate:
            archive_stopdate=datetime.datetime.now()
        else:
            archive_stopdate=parse(stopdate,ignoretz=True)
        # Handling below to even allow YYYY-DOYTHH:MMZ times via isoparse
        try:
            ptimemin=parse(timemin,ignoretz=True)
        except:
            #print("using isotime for timemin",timemin)
            ptimemin=isoparse(timemin).replace(tzinfo=None)
        try:
            ptimemax=parse(timemax,ignoretz=True)
        except:
            #print("using isotime for timemax",timemax)
            ptimemax=isoparse(timemax).replace(tzinfo=None)
        # Reformat so they are cleaner and return to main
        qtimemin= ptimemin.strftime('%Y-%m-%dT%H:%MZ')
        qtimemax= ptimemax.strftime('%Y-%m-%dT%H:%MZ')
        if ptimemin >= ptimemax:
            errorcode = 1404  # 'time.min equal to or after time.max'
        elif ptimemin < archive_startdate or ptimemax > archive_stopdate:
            errorcode=1405   # 'time outside valid range'
        elif limit_duration != 0 and ptimemax-ptimemin > limit_duration:
            errorcode=1408   # 'too much time or data requested'
        elif parameters != None and len(parameters) != len(set(parameters)):
            errorcode=1411   # 'out of order or duplicate parameters'
    # tbd: checking if parameters are legit  # 1407 'unknown dataset parameter'
    #print("debug: errorcode is ",errorcode)
    return(errorcode,qtimemin,qtimemax)

def do_write_info(id, parameters, hapi_home, prefix ):
    mystr = ""
    try:
        infoJson= open( hapi_home + 'info/' + id + '.json' ).read()
        ##import json
        infoJsonModel= json.loads(infoJson)
        if ( parameters!=None ):
            allParameters= infoJsonModel['parameters']
            newParameters= []
            includeParams= set(parameters)
            for i in range(len(allParameters)):
                if ( i==0 or allParameters[i]['name'] in includeParams ):
                    newParameters.append( allParameters[i] )
            infoJsonModel['parameters']= newParameters
        infoJson= json.dumps( infoJsonModel, indent=4, separators=(',', ': '))
        for l in infoJson.split('\n'):
            l= do_info_macros(l)
            if ( prefix!=None ): mystr += prefix
            mystr += l
            mystr += '\n'
    except:
        mystry = send_exception('Not Found')
    return mystr

def get_last_modified( id, hapi_home, timemin, timemax ):
    '''return the time stamp of the most recently modified file,
    from files in $Y/$(x,name=id).$Y$m$d.csv, seconds since epoch (1970) UTC'''
    ff= hapi_home + 'data/' + id + '/'
    #print("debug: checking last modified in ",ff,timemin)
    try:
        filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
        filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
        timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
        timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
        yrmin= int( timemin[0:4] )
        yrmax= int( timemax[0:4] )
    except:
        # time parsing problem, move on
        yrmin = 1
        yrmax = 0 # these two force loop to not happen
    lastModified= None
    ##from email.utils import formatdate
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  mtime= os.path.getmtime( ffyr + '/' + file )
                  if ( lastModified==None or mtime>lastModified ):
                      lastModified=mtime
    # if no files use current time
    if lastModified == None:
        lastModified = time.time()
    return int(lastModified)  # truncate since milliseconds are not transmitted
                              
def do_info_macros( line ):
    ss= line.split('"now"')
    if ( len(ss)==2 ):
        ###import time
        return ss[0] + '"' + time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                           time.gmtime())+ '"' + ss[1]
    ss= line.split('"lastday-P1D"')
    if ( len(ss)==2 ):
       midnight = datetime.datetime.combine(datetime.date.today(),
                                            datetime.time(0,0))
       yesterday_midnight = midnight - datetime.timedelta(days=1)
       return ss[0] + '"' + yesterday_midnight.strftime(
           '%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    ss= line.split('"lastday"')
    if ( len(ss)==2 ):
       midnight = datetime.datetime.combine(
           datetime.date.today(), datetime.time(0,0))
       # TODO: bug lastday is probably based on local time.
       return ss[0]+'"'+midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+'"'+ss[1]
    ss= line.split('"lasthour"')
    if ( len(ss)==2 ):
       midnight = datetime.datetime.combine(
           datetime.date.today(), datetime.time(0,0))
       # TODO: bug lasthour is implemented as lastday
       return ss[0]+'"'+midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+'"'+ ss[1]
    return line

# 'var' below does same as above but with no "" factoring in
def do_info_macros_var( line ):
    ss= line.split('now')
    if ( len(ss)==2 ):
        ###import time
        return ss[0]+time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+ss[1]
    ss= line.split('lastday-P1D')
    if ( len(ss)==2 ):
       midnight = datetime.datetime.combine(
           datetime.date.today(), datetime.time(0,0))
       yesterday_midnight = midnight - datetime.timedelta(days=1)
       return ss[0]+yesterday_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ss[1]
    ss= line.split('lastday')
    if ( len(ss)==2 ):
        midnight = datetime.datetime.combine(
            datetime.date.today(), datetime.time(0,0))
        # TODO: bug lastday is probably based on local time.
        return ss[0]+midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ss[1]
    ss= line.split('lasthour')
    if ( len(ss)==2 ):
       midnight = datetime.datetime.combine(
           datetime.date.today(), datetime.time(0,0))
       # TODO: bug lasthour is implemented as lastday
       return ss[0]+midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ss[1]
    return line

def send_exception(msg ):
    myjson = '{"HAPI": "2.0","status":{"code":1500,"message":"%s"} }' % msg
    return myjson
    
def do_get_parameters_orig( id ):
    if ( id=='10.CF3744000800' ):
        return [ 'Time','Temperature' ]
    elif ( id=='cputemp' ):
        return [ 'Time', 'GPUTemperature', 'CPUTemperature' ]
    else:
        raise Except("this is not implemented!")

def handle_key_parameters( query ):
    'return the parameters in an array, or None'
    if query.__contains__('parameters'):
        parameters= query['parameters'][0] 
        parameters= parameters.split(',')
    else:
        parameters= None
    return parameters

def handle_customRequestOptions( query, xopts ):
    """ return any optional arguments in an array, or None.
    xopts is the capabilities.json set of allowables
    """
    #print("Debug, comparing ",query," against ",xopts)
    cROset= []
    for param in xopts:
        param_name = param["name"]
        paramfull = 'x_customRequestOptions.' + param_name
        if query.__contains__(paramfull):
            cRO = query[paramfull][0]
            if 'constraint' in param.keys():
                constraint = param['constraint']
                if 'enum' in constraint.keys():
                    if cRO in constraint['enum']:
                        cROset.append(param_name+'='+cRO)
                elif 'number' in constraint.keys():
                    # need number-validator here
                    # for now, just convert to float as safety filter
                    cRO =float(cRO)
                    cROset.append(param_name+'='+str(cRO))

    return cROset

def do_parameters_map_orig( id, parameters ):
    pp= do_get_parameters_orig(id)
    result= list( map( pp.index, parameters ) )
    if ( result[0]!=0 ):
        result.insert(0,0)
    return result

def get_forwarded(headers):
    'This doesn''t work...'
    #for h in headers: print(h, '=', headers.get(h))
    if headers.__contains__('x-forwarded-server'):
        return headers.get('x-forwarded-server')
    else:
        return None 

def hapi_errors(code):
    # antunes, grabs HAPI-specific errors from json object
    # works for HAPI codes 1400-1411
    #print("debug, Got error ",code)
    msg="HAPI error: type unknown"
    errors = {
        1201: "HAPI OK: no data for time range",
        
        1400:"HAPI error 1400: user input error",
        1401:"HAPI error 1401: unknown API parameter name",
        1402:"HAPI error 1402: error in start time",
        1403:"HAPI error 1403: error in stop time",
        1404:"HAPI error 1404: start time equal to or after stop time",
        1405:"HAPI error 1405: time outside valid range",
        1406:"HAPI error 1406: unknown dataset id",
        1407:"HAPI error 1407: unknown dataset parameter",
        1408:"HAPI error 1408: too much time or data requested",
        1409:"HAPI error 1409: unsupported output format",
        1410:"HAPI error 1410: unsupported include value",
        1411:"HAPI error 1411: out of order or duplicate parameters",

        1413:"HAPI error 1413: Time range too long",
        
        1500:"HAPI error 1500: Internal server error",
        1501:"HAPI error 1501: Internal server error-- upstream request error"
    }
    try:
        msg=errors[code]
    except:
        print("Debug-- unknown error found, using 1400 instead of ",code)
        msg=errors[1400]  # default is assume user input error
    return(msg)

def lasthour_mod(timething):
    #print("Debug: lasthour_mod input:",timething)
    try:
        if timething == 'lasthour':
            timething = datetime.datetime.now().strftime('%Y-%m-%dT%H:%MZ')
    except:
        pass
    return(timething)
        

### Refactor of original 'do_GET()' method into subroutines

def get_hapi_tags(path,tags_allowed):
    tags=[]
    #print("debug: path=",path)
    if "hapi" in path and path.startswith('hapi') == False:
        (tagstr, ends) = path.split('hapi')
        # grab the tags
        tags = tagstr.split('/')
        tags = list(set(tags) & set(tags_allowed)) # filter for safety
        path = "hapi" + ends # restore the rest
        #print("Tags found!\nRestored path=$path\nLegit tags=",tags)
    return(tags, path)

def clean_hapi_path(path):
        while ( path.endswith('/') ):
            path= path[:-1]
        i= path.find('?')
        if ( i>-1 ): path= path[0:i] 
        while ( path.startswith('/') ):
            path= path[1:]            
        return(path)

def check_v2_v3(query):
    #print("Debug, calling: ",path,pp.query)
    # as part of the transition between 2.x to 3.0/3.1, allows both keys
    # allow synonyms 'id' and 'dataset', 'time.min/max' and 'start/stop'
    if 'dataset' in query.keys():
        query['id']=query['dataset']
    if 'start' in query.keys():
        query['time.min']=query['start']
    if 'stop' in query.keys():
        query['time.max']=query['stop']
    return(query)

def clean_query_time(query):
    errorcode = 0 # assume all is well
    timemin= query['time.min'][0]
    timemax= lasthour_mod(query['time.max'][0])
    # right now our parser can only handle format %Y-%m-%dT%H:%MZ
    # so truncatee entries with seconds.milliseconds
    timemin=timemin[0:16]+'Z'
    timemax=timemax[0:16]+'Z'
    # error-checking here
    try:
        parse(timemin)
    except:
        errorcode= 1402   # error in time.min
    try:
        parse(timemax)
    except:
        errorcode = 1403   # error in time.max
    #print("Debug: lasthour check:",timemax)    
    return(timemin, timemax, errorcode)

def get_lastModified(api_datatype, id, timemin, timemax):
    if api_datatype == 'file':
        #print(id,timemin,timemax)
        lastModified= get_last_modified( id, timemin, timemax ); # tag
    elif api_datatype == 'aws':
        #print(id,timemin,timemax)
        # temporary hack, should get time from json, right?
        lastModified= time.time()
    else:
        # web-based APIs use current date as 'last modified'
        ###import time
        lastModified=time.time()
    return(lastModified)

def prep_data(query, hapihome, tags):
    id= query['id'][0]
    (timemin, timemax, errorcode) = clean_query_time(query)
    parameters= handle_key_parameters(query)
    (check_error,timemin,timemax) = generic_check_error(
        id,timemin,timemax,parameters,hapihome)
    # Two passes here-- first, that no non-HAPI params exist
    (stat,mydata)=fetch_info_params(id,hapihome,False)
    allparams = [ item['name'] for item in mydata['parameters'] ]
    if parameters != None:
        if 'Time' not in allparams:
            allparams.append('Time') # always make sure this is there
        for para in parameters:
            #if para.lower() not in allparams:
            if para not in allparams:
                check_error = 1407
                #print("Debug, ",parameters," bad params found in ",allparams)
        # also check that parameters are in the same order
        j = 0 # start with first in user-given set
        jsize = len(parameters)
        for i,ele in enumerate(allparams):
            if parameters[j] == ele:
                j=j+1
            if j >= jsize: break
        if j != jsize:
            #print("Debug, params in wrong order ",j,jsize)
            check_error = 1411   # 'out of order or duplicate params'
    #print("debug, param crosscheck:",parameters,' VS ',allparams)
    # 2nd, that if they specified parameters, use that instead
    # if none specified, populate with defaults
    if parameters == None:
        parameters = allparams
    # 3d mod: allow additional tags as parameters
    #print("debug: tags also here: ",tags)
    if len(tags) > 0:
        parameters.extend(tags)
    # returns an array of validated additional query strings
    # (or empty array)
    if 'x_customRequestOptions' in mydata.keys():
        xopts = mydata['x_customRequestOptions']
    else:
        xopts=[]
    #print("Debug, xopts = ",xopts)
    ##    xopts = jset['x_customRequestOptions']
    return(parameters, xopts, mydata, check_error)

def print_hapi_intropage(myname, hapihome):
    mystr = ""
    hapi_version = get_hapiversion(hapihome)
    if hapi_version >= 3:
        datasetkey = 'dataset'
        startkey = 'start'
        stopkey = 'stop'
    else:
        # version 2.0
        datasetkey = 'id'
        startkey = 'time.min'
        stopkey = 'time.max'
    mystr += "<html><head><title>Python HAPI Server</title></head>\n"
    mystr += "<body>\n"
    # a simple info/demo page
    mystr += "<p>"+myname+" Catalogs:\n"
    u= "/hapi/catalog"
    mystr += "<a href='%s'>%s</a></p>\n" % ( u,u ) 
    mystr += "<p>HAPI requests:</p>\n"
    ff= glob.glob( hapihome + 'info/*.json' )
    n= len( hapihome + 'info/' )
    for f in sorted(ff):
        (stat,mydata)=fetch_info_params(f,hapihome,True)
        u= "/hapi/info?%s=%s" % ( datasetkey, f[n:-5] )
        mystr += "<a href='%s'>%s</a></br>\n" % ( u,u ) 
        # also extract dates etc from file
        #print("debug: checking info file ",f)
        #print("debug:",mydata)
        timemin=mydata['startDate']
        timemax=mydata['stopDate']
        try:
            timemin=mydata['sampleStartDate']
            timemax=mydata['sampleStopDate']
        except:
            pass
        u= "/hapi/data?%s=%s&%s=%s&%s=%s" % (
            datasetkey, f[n:-5], startkey, timemin, stopkey, timemax )
        u= do_info_macros_var(u)
        mystr += "<a href='%s'>%s</a></br>\n" % ( u,u ) 
        mystr += "Parameters:\n"
        mystr += "<table>"
        for para in mydata['parameters']:
            #mystr += "<li>"
            mystr += "<tr>"
            mystr += "<td>%s:</td>" % (para['name'])
            parakeys = sorted(para.keys())
            parakeys.remove("name")
            for parakey in parakeys:
                mystr += "<td>"
                mystr += "%s %s &nbsp; &nbsp; &nbsp; &nbsp;" % (parakey, para[parakey])
                mystr += "</td>"
            mystr += "</tr>"
        mystr += "</table>\n"
    # Also echo an optional 'splash.html' file that users can change
    try:
        fo=open('splash.html','r')
        for line in fo:
            mystr += line
        fo.close()
    except:
        pass
    mystr += "</body></html>\n"
    return mystr
            
        
def print_hapi_index(myname):
    # echo an index.html, if it exists, otherwise post a banner
    mystr = ""
    try:
        fo=open('index.html','r')
        for line in fo:
            mystr += line
        fo.close()
    except:
        mystr += "<html><head><title>Python HAPI Server</title></head>\n"
        mystr += "<body>\n"
        mystr += "<p>HAPI Server for " + mynameE + ", visit <a href='/hapi/'>/hapi/</a> for data.\n"
        mystr += "</body></html>\n"
    return mystr

def fetch_modifiedsince(lms):
    ###from email.utils import parsedate_tz,formatdate
    #import time
    timecomponents= parsedate_tz(lms) 
    os.environ['TZ']='gmt'
    theyHave= time.mktime( timecomponents[:-1] )
    theyHave = theyHave - timecomponents[-1]
    return(theyHave)

