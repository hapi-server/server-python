""" CSV ingest program from original hapi-server.py code

Good for CSV files in a file tree

 Part of the HAPI Python Server.  The code and documentation resides at:
    https://github.com/hapi-server/server-python

 See accompanying 'csv_config.py' file for site-specific details.
 Assumes data is flat files in a directory hierarchy of
 "data/[id]/YYYY/[id].YYYYMMDD.csv
"""

import os
import json
import math
import dateutil


def do_parameters_map( id, floc, parameters ):
    ff= floc['dir'] + '/info/' + id + '.json'
    fin=open(ff,'r')
    jset = json.loads(fin.read())
    fin.close()
    pp = [item['name'] for item in jset['parameters']]

    curr_col = 0
    param_dict = {}
    for idx, param_name in enumerate(pp):
        # Calculate the number of columns used for this parameter
        size = jset['parameters'][idx].get('size')
        if size:
            ncols = math.prod(size)
            col_indices = list(range(curr_col, curr_col + ncols))
            curr_col += ncols
        else:
            col_indices = [curr_col]
            curr_col += 1

        # Filter requested parameters
        if param_name in parameters:
            param_dict[idx] = col_indices
        
        # Ensure time (index 0) is always present and first
        if param_dict.get(0) != 0:
            param_dict = {0: [0]} | param_dict

    return param_dict


def do_data_csv( id, timemin, timemax, parameters, catalog, floc,
                 stream_flag, stream):

    ff= floc['dir'] + '/data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    if ( parameters!=None ):
        mm = do_parameters_map( id, floc, parameters )
    else:
        mm= None

    datastr = ""
    status = 0
    
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
                                for li in mm[i]:
                                    if comma:
                                        datastr += ','
                                    datastr += ss[li]
                                    comma=True
                            if list(mm.values())[-1][-1]<(len(ss)-1):
                                datastr += '\n'
                        else:
                            datastr += rec
                              
                        if len(datastr) > 0: status=1200
                        if stream_flag:
                            # write then flush
                            stream.wfile.write(bytes(datastr,"utf-8"))
                            datastr = ""
                              
    if status != 1200 and len(datastr) == 0:
        status=1201 # status 1200 is HAPI "OK- no data for time range"

    return(status,datastr)
