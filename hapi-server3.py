# original by jbfaden, Python3 update by sandyfreelance 04-06-2021
import time
# Python2 uses BaseHTTPServer, Python3 uses http.server
#from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from http.server import HTTPServer, BaseHTTPRequestHandler
# Python2 uses SocketServer, Python3 uses socketserver
#from SocketServer import ThreadingMixIn
from socketserver import ThreadingMixIn
# Python2 uses urlparse, Python3 uses urllib.parse
#import urlparse
import urllib.parse as urlparse
import glob
import os
import dateutil.parser

# also Python2 uses wfile.write("") but Python3 uses wfile.write(bytes("","utf-8"))
# Python3 removed 'has_key' from dictionaries, use 'in' or '__contains__(key)'

class StdoutFeedback():
    def __init__(self):
        print('feedback is over stdout')
    def setup(self):    
        print('setup feedback.')
    def destroy(self):
        print('destroy feedback.')
    def start(self,requestHeaders):
        from time import gmtime, strftime
        print('----------', strftime("%Y-%m-%dT%H:%M:%SZ", gmtime()), '----------')
        print(requestHeaders)
    def finish(self,responseHeaders):
        print('---')
        for h in responseHeaders:
            print('%s: %s' % ( h, responseHeaders[h] ))
        print('----------')
    
#import RPi.GPIO as GPIO

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
     
isTesting=True   #See "import RPi.GPIO as GPIO" above, which must be uncommented.
if ( not isTesting ):
    feedback= GpioFeedback(27)  # When this is installed on the Raspberry PI
    HOST_NAME = '192.168.0.205' # !!!REMEMBER TO CHANGE THIS!!!
    PORT_NUMBER = 9000 # Maybe set this to 9000.
    HAPI_HOME= '/home/jbf/hapi/'
else:
    feedback= StdoutFeedback()  # When testing at the unix command line.
    HOST_NAME = 'localhost' # !!!REMEMBER TO CHANGE THIS!!!
    PORT_NUMBER = 9000 # Maybe set this to 9000.
    HAPI_HOME= '/Users/antunak1/HAPI/server-python/hapi_home/'

# Configuration requirements
# * capabilities and catalog responses must be formatted as JSON in SERVER_HOME.
# * info responses are in SERVER_HOME/info.
# * responses can have templates like "lasthour" to mean the last hour boundary, and "lastday-P1D" to mean the last midnight minus one day.
# * data files must be in daily csv files, SERVER_HOME/data/<id>/$Y/<id>.$Y$m$d.csv
# * each data file must have time as $Y-$m-$dT$H:$M:$SZ
# * embedded info response, preceding the CSV response, is not yet supported.

def do_write_info( s, id, parameters, prefix ):
    try:
        infoJson= open( HAPI_HOME + 'info/' + id + '.json' ).read()
        import json
        infoJsonModel= json.loads(infoJson)
        if ( parameters!=None ):
            allParameters= infoJsonModel['parameters']
            newParameters= []
            includeParams= set(parameters)
            for i in xrange(len(allParameters)):
                if ( i==0 or allParameters[i]['name'] in includeParams ):
                    newParameters.append( allParameters[i] )
            infoJsonModel['parameters']= newParameters
        infoJson= json.dumps( infoJsonModel, indent=4, separators=(',', ': '))
        for l in infoJson.split('\n'):
            l= do_info_macros(l)
            if ( prefix!=None ): s.wfile.write(bytes(prefix,"utf-8"))
            s.wfile.write(bytes(l,"utf-8"))
            s.wfile.write(bytes('\n',"utf-8"))
    except:
        send_exception(s.wfile,'Not Found') 

def get_last_modified( id, timemin, timemax ):
    'return the time stamp of the most recently modified file, from files in $Y/$(x,name=id).$Y$m$d.csv, seconds since epoch (1970) UTC'
    ff= HAPI_HOME + 'data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    lastModified= None
    from email.utils import formatdate
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  mtime= os.path.getmtime( ffyr + '/' + file )
                  if ( lastModified==None or mtime>lastModified ): lastModified=mtime
                  #print('line87: ', file, lastModified, formatdate( timeval=mtime, localtime=False, usegmt=True ))
    #print('line89: ', formatdate( timeval=lastModified, localtime=False, usegmt=True ))
    return int(lastModified)  # truncate since milliseconds are not transmitted
	
def do_data_csv( id, timemin, timemax, parameters, s ):
    ff= HAPI_HOME + 'data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    if ( parameters!=None ):
        mm= do_parameters_map( id, parameters )
    else:
        mm= None
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  for rec in open( ffyr + '/' + file ):
                      ydmhms= rec[0:19]
                      if ( timemin<=ydmhms and ydmhms<timemax ):
                          if ( mm!=None ):
                              ss= rec.split(',')
                              comma= False
                              for i in mm:
                                 if comma: 
                                    s.wfile.write(bytes(',',"utf-8"))
                                 s.wfile.write(bytes(ss[i],"utf-8"))
                                 comma=True
                              if mm[-1]<(len(ss)-1):
                                 s.wfile.write(bytes('\n',"utf-8"))
                          else:
                              s.wfile.write(bytes(rec,"utf-8"))

def do_info_macros( line ):
    ss= line.split('"now"')
    if ( len(ss)==2 ):
       import time
       return ss[0] + '"' + time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())+ '"' + ss[1]
    ss= line.split('"lastday-P1D"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)
       from datetime import timedelta
       yesterday_midnight = midnight - timedelta(days=1)
       return ss[0] + '"' + yesterday_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    ss= line.split('"lastday"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min) # TODO: bug lastday is probably based on local time.
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    ss= line.split('"lasthour"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)  # TODO: bug lasthour is implemented as lastday
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    return line
    
def send_exception( w, msg ):
    w.write(bytes( '{ "HAPI": "2.0", "status": { "code": 1406, "message": "%s" } }\n' % msg ,"utf-8"))

def do_get_parameters( id ):
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

def do_parameters_map( id, parameters ):
    pp= do_get_parameters(id)
    result= map( pp.index, parameters )
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


class MyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "application/json")
        s.end_headers()

    def do_GET(s):
        
        feedback.start(s.headers)
        responseHeaders= {}
        
        path= s.path
        pp= urlparse.urlparse(s.path)
        
        while ( path.endswith('/') ):
            path= path[:-1]

        i= path.find('?')
        if ( i>-1 ): path= path[0:i] 

        while ( path.startswith('/') ):
            path= path[1:]

        query= urlparse.parse_qs( pp.query )

        if ( path=='hapi/capabilities' ):                
           s.send_response(200)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/catalog' ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/info' ):
           id= query['id'][0]
           if ( os.path.isfile(HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
           else:
               s.send_response(404)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/data' ):
           id= query['id'][0]
           timemin= query['time.min'][0]
           timemax= query['time.max'][0]
           lastModified= get_last_modified( id, timemin, timemax );
           # wget -O foo.csv --tries=1 --header="If-Modified-Since: Tue, 13 Mar 2018 21:47:02 GMT" 'http://192.168.0.205:9000/hapi/data?id=10.CF3744000800&time.min=2018-03-03T00:00Z&time.max=2018-03-12T00:00Z'
           if ( s.headers.__contains__('If-Modified-Since') ):
               lms= s.headers['If-Modified-Since']
               from email.utils import parsedate_tz,formatdate
               import time
               timecomponents= parsedate_tz(lms) 
               os.environ['TZ']='gmt'
               theyHave= time.mktime( timecomponents[:-1] )
               theyHave = theyHave - timecomponents[-1]
               #print('theyHave: ', theyHave, lms)
               #print('lm,delta: ', lastModified, ( lastModified-theyHave ), formatdate( timeval=lastModified, localtime=False, usegmt=True ))
               if ( lastModified <= theyHave ):
                   s.send_response(304)
                   s.end_headers()
                   feedback.finish(responseHeaders)
                   return
               
           # check request header for If-Modified-Since
           if ( os.path.isfile(HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
               s.send_header("Content-type", "text/csv")
           else:
               s.send_response(404)
           s.send_header("Content-type", "text/csv")

        elif ( path=='hapi' ):
           s.send_response(200)
           s.send_header("Content-type", "text/html")

        else:
           s.send_response(404)
           s.send_header("Content-type", "application/json")

        s.send_header("Access-Control-Allow-Origin", "*")
        s.send_header("Access-Control-Allow-Methods", "GET")
        s.send_header("Access-Control-Allow-Headers", "Content-Type")

        if ( path=='hapi/data' ):
            from email.utils import formatdate
            responseHeaders['Last-Modified']= formatdate( timeval=lastModified, localtime=False, usegmt=True ) 
        
        for h in responseHeaders:
            s.send_header(h,responseHeaders[h])
            
        s.end_headers()

        if ( path=='hapi/capabilities' ):
            for l in open( HAPI_HOME + 'capabilities.json' ):
                s.wfile.write(bytes(l,"utf-8"))
        elif ( path=='hapi/catalog' ):
            for l in open( HAPI_HOME + 'catalog.json' ):
                s.wfile.write(bytes(l,"utf-8"))
        elif ( path=='hapi/info' ):
            id= query['id'][0]
            parameters= handle_key_parameters(query)
            do_write_info( s, id, parameters, None )
        elif ( path=='hapi/data' ):
            id= query['id'][0]
            timemin= query['time.min'][0]
            timemax= query['time.max'][0]
            parameters= handle_key_parameters(query)
            if query.__contains__('include'):
                if query['include'][0]=='header':
                    do_write_info( s, id, parameters, '#' )
            do_data_csv( id, timemin, timemax, parameters, s )
        elif ( path=='hapi' ):
            s.wfile.write(bytes("<html><head><title>Python HAPI Server</title></head>\n","utf-8"))
            s.wfile.write(bytes("<body><p>This is a simple Python-based HAPI server, which can be run on a Raspberry PI.</p>\n","utf-8"))
            s.wfile.write(bytes("<p>Example requests:</p>\n","utf-8"))
            u= "hapi/catalog" 
            s.wfile.write(bytes("<a href='%s'>%s</a></br>\n" % ( u,u ) ,"utf-8"))
            ff= glob.glob( HAPI_HOME + 'info/*.json' )
            n= len( HAPI_HOME + 'info/' )
            timemin= '2018-01-19T00:00Z'
            timemax= '2018-01-20T00:00Z'
            for f in ff:
                u= "hapi/info?id=%s" % ( f[n:-5] )
                s.wfile.write(bytes("<a href='%s'>%s</a></br>\n" % ( u,u ) ,"utf-8"))
                u= "hapi/data?id=%s&time.min=%s&time.max=%s" % ( f[n:-5], timemin, timemax )
                s.wfile.write(bytes("<a href='%s'>%s</a></br>\n" % ( u,u ) ,"utf-8"))
            s.wfile.write(bytes("<br><a href='hapi/data?id=cputemp&time.min=2018-01-19T00:00Z&time.max=2018-01-20T00:00Z&parameters=Time,CPUTemperature'>subset of parameters</a>\n" ,"utf-8"))
            s.wfile.write(bytes("<br><a href='hapi/data?id=cputemp&time.min=2018-01-19T00:00Z&time.max=2018-01-20T00:00Z&include=header&parameters=Time,CPUTemperature'>withInclude</a>\n"  ,"utf-8"))
            s.wfile.write(bytes("<br><a href='hapi/info?id=cputemp&include=header&parameters=Time,CPUTemperature'>infoSubset</a>"  ,"utf-8"))
            s.wfile.write(bytes("<br><br><a href='http://192.168.0.46:2121/'>1-wire http</a>\n","utf-8"))
            s.wfile.write(bytes("</body></html>\n","utf-8"))
        else:
            for l in open( HAPI_HOME + 'error.json' ):
                s.wfile.write(bytes(l,"utf-8"))
        feedback.finish(responseHeaders)

class ThreadedHTTPServer( ThreadingMixIn, HTTPServer ):
   '''Handle requests in a separate thread.'''

if __name__ == '__main__':
    feedback.setup()

    httpd = ThreadedHTTPServer((HOST_NAME, PORT_NUMBER), MyHandler)
    print(time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    feedback.destroy()

    httpd.server_close()
    print(time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))

