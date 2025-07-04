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
  python hapi_server.py <MISSIONNAME> [localhost/http/https/custom]
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

isPi=False   #invokes "import RPi.GPIO as GPIO" later
noisy=False   #set True for unix command line feedback

###############################################################################
# Ideally, nothing in this code below this line needs to be changed
# Instead, a <MISSION>_config.py sets the parameters needed for the site.

### IMPORTS ###
import os.path
import sys # only used for command-line arguments
import time
from time import gmtime, strftime
from email.utils import parsedate_tz,formatdate
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib.parse as urlparse
import hapi_parser as hp
if (isPi):
    import RPi.GPIO as GPIO

# note also conditional import of S3 & NetCDF items after USE_CASEs, below

### GET COMMAND LINE ARGUMENTS FOR HOW TO RUN ###
# If none provided, defaults to 'csv' and 'localhost'
# python hapi_server.py <MISSIONNAME> [localhost/http/https/custom]
try:
    USE_CASE = str(sys.argv[1])
except:
    USE_CASE = 'csv'
    # APL choices are 'csv', 'guvi', 'guviaws', or 'supermag'

# Arg 2 can be 'localhost', 'http', 'https', or 'custom'
# Use 'custom' if need you need to mod this code to define a non-standard port
try:
    LOCALITY = str(sys.argv[2])
except:
    LOCALITY = 'localhost'

if LOCALITY == 'http':
    HOST_NAME = '0.0.0.0'
    PORT_NUMBER = 80
elif LOCALITY == 'https':
    HOST_NAME = '0.0.0.0'
    PORT_NUMBER = 443
elif LOCALITY == 'custom':
    # this is provided so you can hard-code your own site-specific needs
    HOST_NAME = '0.0.0.0'
    PORT_NUMBER = 1024
elif LOCALITY == 'localhost':
    # assume localhost
    HOST_NAME = 'localhost'
    PORT_NUMBER = 8000
else:
    print(f"Error, no known port {LOCALITY}, exiting")
    exit()
print("Running in",LOCALITY,"mode, initializing for",USE_CASE)

### GET AND PARSE CONFIG FILE ###
CFG = hp.parse_config(USE_CASE)

if CFG.api_datatype == 'aws':
    import s3netcdf

### GET HAPI VERSION we need to support
# (mostly needed for id/dataset, time.min/start, time.max/stop keywords)
hapi_version = hp.get_hapiversion(CFG.HAPI_HOME)

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

    
class GpioFeedback():
    def __init__(self,ledpin):
        print('feedback is over GPIO pin ',ledpin)
        self.ledpin=ledpin
    def setup(self):    
        GPIO.setwarnings(False)
        #set the gpio modes to BCM numbering
        GPIO.setmode(GPIO.BCM)
        #set LEDPIN's mode to output,and initial level to LOW(0V)
        GPIO.setup(self.ledpin,GPIO.OUT,initial=GPIO.LOW)
        GPIO.output(self.ledpin,GPIO.HIGH)
        time.sleep(0.2)
        GPIO.output(self.ledpin,GPIO.LOW)
    def destroy(self):
        #turn off LED
        GPIO.output(self.ledpin,GPIO.LOW)
        #release resource
        GPIO.cleanup()
    def start(self,requestHeaders):
        GPIO.output(self.ledpin,GPIO.HIGH)
    def finish(self,responseHeaders):
        GPIO.output(self.ledpin,GPIO.LOW)
     

### QUICK PI CHECK ###

if ( isPi ):
    feedback= GpioFeedback(27)  # When this is installed on the Raspberry PI
elif ( noisy ):
    feedback= StdoutFeedback()  # When testing at the unix command line.
else:
    feedback= NoFeedback()  # for quiet deployment


### CORE FUNCTIONS ###

### HAPI required error and support utilities

def send_exception( w, msg ):
    myjson = '{"HAPI": "3.1","status":{"code":1500,"message":"%s"} }' % msg
    w.write(bytes(myjson,"utf-8"))
    
def get_forwarded(headers):
    'This doesn''t work...'
    #for h in headers: print(h, '=', headers.get(h))
    if headers.__contains__('x-forwarded-server'):
        return headers.get('x-forwarded-server')
    else:
        return None 

def get_last_line(s: str) -> str:
    # Strip any trailing newlines (\n, \r\n, or combos)
    i = len(s) - 1
    while i >= 0 and s[i] in ('\n', '\r'):
        i -= 1
    if i < 0:
        return ''
    # Now search backwards for the previous newline
    j = i
    while j >= 0 and s[j] not in ('\n', '\r'):
        j -= 1
    return s[j + 1:i + 1]

### THE HAPI SERVER ###
    
class MyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_error(s,code,alt=400):
        msg=hp.hapi_errors(code)
        # try/except here to handle cases of broken pipe
        # (in which case error can not be sent either)
        try:
            send_exception(s.wfile,msg)
        except:
            pass
    
    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-Type", "application/json")
        s.end_headers()

    def do_GET(s):
        #print(f"Received GET request from {s.client_address}")
        ###import time
        feedback.start(s.headers)
        responseHeaders= {}
        path= s.path
        pp= urlparse.urlparse(s.path)
        path = hp.clean_hapi_path(path)
        #print('superhapi',path,pp)
        # allows for keywords to be placed before 'hapi/', hapi-server3d.py
        (tags,path) = hp.get_hapi_tags(path,CFG.tags_allowed)
        query= urlparse.parse_qs( pp.query )
        query = hp.check_v2_v3(query)
        #print('superhapi',query,path)
        #
        # HTML HEADERS
        #
        if ( path=='hapi/capabilities' ):                
           s.send_response(200)
           s.send_header("Content-Type", "application/json")

        elif ( path=='hapi/catalog' ):
           s.send_response(200)
           s.send_header("Content-Type", "application/json")

        elif ( path=='hapi/info' ):
           id= query['id'][0]
           if ( os.path.isfile(CFG.HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
           else:
               #s.send_response(404)
               s.do_error(1406,404)   # 'unknown dataset id'
           s.send_header("Content-Type", "application/json")

        elif ( path=='hapi/data' ):
           id= query['id'][0]
           (timemin, timemax, errorcode) = hp.clean_query_time(query)
           #print('superhapi',timemin,timemax,errorcode,id,query)
           if errorcode > 0:
               s.do_error(errorcode)
           lastModified = hp.get_lastModified(CFG.api_datatype, id, CFG.HAPI_HOME, timemin, timemax)
           if ( s.headers.__contains__('If-Modified-Since') ):
               theyHave = hp.fetch_modifiedsince(s.headers['If-Modified-Since'])
               if ( lastModified <= theyHave ):
                   s.send_response(304)
                   s.end_headers()
                   feedback.finish(responseHeaders)
                   return               
           # check request header for If-Modified-Since
           if ( os.path.isfile(CFG.HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
               s.send_header("Content-Type", "text/csv")
           else:
               s.send_response(404)

        elif ( path=='hapi' ):
           s.send_response(200)
           s.send_header("Content-Type", "text/html")

        elif ( path=='' ):
           # allow for a top-level index call
           s.send_response(200)
           s.send_header("Content-Type", "text/html")

        else:
           #print("debug: got here,",path);
           s.send_response(404)
           s.send_header("Content-Type", "application/json")

        s.send_header("Access-Control-Allow-Origin", "*")
        s.send_header("Access-Control-Allow-Methods", "GET")
        s.send_header("Access-Control-Allow-Headers", "Content-Type")

        if ( path=='hapi/data' ):
            ###from email.utils import formatdate
            responseHeaders['Last-Modified']=formatdate(
                timeval=lastModified, localtime=False, usegmt=True ) 
            
        for h in responseHeaders:
            s.send_header(h,responseHeaders[h])
            
        s.end_headers()

        #
        # HTML BODY
        #
        if ( path=='hapi/capabilities' ):
            for l in open( CFG.HAPI_HOME + 'capabilities.json' ):
                s.wfile.write(bytes(l,"utf-8"))
        elif ( path=='hapi/catalog' ):
            for l in open( CFG.HAPI_HOME + 'catalog.json' ):
                s.wfile.write(bytes(l,"utf-8"))
        elif ( path=='hapi/info' ):
            id= query['id'][0]
            #for l in open( CFG.HAPI_HOME + '/info/' + id + '.json' ):
            #    s.wfile.write(bytes(l,"utf-8"))
            parameters= hp.handle_key_parameters(query)
            para = hp.do_write_info(id, parameters, CFG.HAPI_HOME, None )
            s.wfile.write(bytes(para,"utf-8"))
        elif ( path=='hapi/data' ):
            (parameters, xopts, mydata, check_error) = hp.prep_data(query, CFG.HAPI_HOME, tags)
            CFG.floc['customOptions'] = hp.handle_customRequestOptions(query, xopts)
            #print('superhapi',xopts,mydata,check_error,parameters,query)
            if check_error > 0:
                s.do_error(check_error)
            else:
                # parameters are valid, so run the query
                if query.__contains__('include'):
                    if query['include'][0]=='header':
                        info = hp.do_write_info(id, parameters, CFG.HAPI_HOME, '#' )
                        s.wfile.write(bytes(info,"utf-8"))
                (stat,mydata)=hp.fetch_info_params(id,CFG.HAPI_HOME,False)
                #print('superhapi',stat,mydata)
                # FORMAT HERE IS: id (unique dataset endpoint)
                #    timemin and timemax (in HAPI format)
                #    parameters (as a list of parameter names)
                #    mydata (copy of the full json-parsed parameters spec)
                #    floc (site-specific required elements from *_config.py)
                (status,data)=CFG.hapi_handler(
                    id, timemin, timemax, parameters, mydata, CFG.floc,
                    CFG.stream_flag, s)
                #print('superhapi',status,data)
                if status >= 1400:
                    s.do_error(status)
                else:
                    if len(data) == 0 and CFG.stream_flag == False:
                        # likely redundant sanity check
                        status = 1201
                    if status == 1201:
                        # status 1201 is HAPI "OK- no data for time range"
                        s.do_error(status)
                    else:
                        status=1200 # status 1200 is HAPI "OK"
                    # presumed valid data, so serve it

                    try:
                        truestart = data[0:22].split(',')[0]
                        trueend = get_last_line(data).split(',')[0]
                        if hp.compare_times(truestart,timemin) == 'before' or hp.compare_times(trueend, timemax) == 'after':
                            data = hp.truncate_data(timemin, timemax, data)
                    except:
                        pass # in case of weird time formats, etc
                    
                    try:
                        # note for streaming, data is zero but legit
                        s.wfile.write(bytes(data,"utf-8"))
                    except:
                        # return general 'user input error' code 
                        s.do_error(1500) # HAPI internal server error

        elif ( path=='hapi' ):
            page = hp.print_hapi_intropage(USE_CASE, CFG.HAPI_HOME)
            s.wfile.write(bytes(page,"utf-8"))
            
        elif ( path=='' ):
            mystr = hp.print_hapi_index(USE_CASE)
            s.wfile.write(bytes(mystr,"utf-8"))

        else:
            # looks like error is 'not a known URL'
            s.do_error(1400)

        feedback.finish(responseHeaders)

class ThreadedHTTPServer( ThreadingMixIn, HTTPServer ):
   '''Handle requests in a separate thread.'''


### AND HERE WE GO!

if __name__ == '__main__':
    feedback.setup()

    httpd = ThreadedHTTPServer((HOST_NAME, PORT_NUMBER), MyHandler)
    print(time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    #httpd.serve_forever()
    feedback.destroy()

    httpd.server_close()
    print(time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))

