""" netcdfhapi.py, specific reader for (specially, GUVI) NetCDF files
    Feel free to adapt for your NetCDF dataset.

    Routines to read and parse GUVI's NetCDF datafiles and stream as CSV.
    Very HAPI-specific, but could be modded for a general API.

 Part of the HAPI Python Server.  The code and documentation resides at:
    https://github.com/hapi-server/server-python

 See accompanying 'netcdf_config.py' file for site-specific details.

    Note it inherits the location of the NetCDF datafiles from the 'floc'
    variable in hapi-server.py

    Known inefficiency: if time range is a partial day, currently
    returns list of all files for that day (typically 13-14)
    as the filter for subselecting in days at the file level is broken.
    Since the database-like reads of .nc files works, this is not
    a strong operational issue. But code stubs exist to improve, if desired.

    Uncomment "sample_netcdf_invocation()" to run a demo (assumes data exists)

IDs:
Legit IDs are as given in netcdf_get_keys(id)
Note that hapi/data/IDNAME directories have to exist (even though empty)
so that the hapi-server.py knows those ids are valid.
As a result, you can temporarily disable any id by simply removing
that directory from the server (no code changes needed).
Any new id must have a definition added into netcdf_get_keys(id).

NetCDF keywords:
To update or change which NetCDF vars to return, you can
edit the 'netcdf_get_keys(id)' function to change what data to serve.

Allowed parameters:
For any given id, we return a bunch of fields.  In theory with HAPI,
users could 
  use id=dayspec&parameters=DISK_INTENSITY_DAY,DISK_RECTIFIED_INTENSITY_DAY
and we will only return those keywords + Time, not the full default set.

"""

debug = False # True  # set to True when diagnosting/debugging setup

import xarray as xr
import os
import datetime
import re
#import s3netcdf

# As per request by Bon to exclude one channel of data,
# start of terrible hackery to delete, part 1 of 2
# Change the following flag to 'False' once GUVI fixes channel 6
nix_channel_six = True  # flag to turn off 'bad' data in channel 6
# The reason this is terrible hackery is we are deleting data that is
# in the data file (which users can get) but that ops suggest we not
# provide to users via HAPI until they fix the data.  So we use awkward
# code such as a global variable and a goto to handle this.
# As soon as the data is fixed, delete both parts 1 and 2.
# end of terrible hackery to delete, part 1 of 2

"""
A minimal set of GUVI parameters for HAPI that would meet most non-expert users research goals would be:
1.    Observed latitude
2.    Observed Longitude
3.    Observed Altitude
4.    Solar zenith angle of observation
5.    Time of observation
6.    Radiances (in 5 UV “colors”)
7.    Radiance uncertainties
8.    Data Quality Index.
"""

# Valid ids, even though the data appears from the same files
# (which are located in the 'floc' variable inherited from server-python.py)
# are as given in netcdf_get_keys(id)

def netcdf_get_keys(id):
    """ Defines data bundles, sets match NetCDF files.
    """
    
    # accepts id flags of 'dayspec', 'nightspec', 'auroralspec',
    #                     'daygrid', 'nightgrid', 'auroralgrid',
    # returns 'status=1' on success, 'status=1406' if unknown id used.
    status = 1
    if 'dayspec' in id:
        datakeys = ['LATITUDE_DAY','LONGITUDE_DAY','ALTITUDE_DAY',
                    'PIERCEPOINT_DAY_SZA',
                    'DISK_INTENSITY_DAY',
                    'DISK_CALIBRATION_UNCERTAINTY_DAY',
                    'DISK_RECTIFIED_INTENSITY_DAY',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_DAY']
        fetchkeys = ['YEAR_DAY','DOY_DAY','TIME_DAY']
    elif 'nightspec' in id:
        datakeys = ['LATITUDE_NIGHT','LONGITUDE_NIGHT','ALTITUDE_NIGHT',
                    'PIERCEPOINT_NIGHT_SZA',
                    'DISK_INTENSITY_NIGHT',
                    'DISK_CALIBRATION_UNCERTAINTY_NIGHT',
                    'DISK_RECTIFIED_INTENSITY_NIGHT',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_NIGHT']
        fetchkeys = ['YEAR_NIGHT','DOY_NIGHT','TIME_NIGHT']
    elif 'auroralspec' in id:
        datakeys = ['LATITUDE_AURORAL','LONGITUDE_AURORAL','ALTITUDE_AURORAL',
                    'PIERCEPOINT_AURORAL_SZA',
                    'DISK_INTENSITY_AURORAL',
                    'DISK_CALIBRATION_UNCERTAINTY_AURORAL',
                    'DISK_RECTIFIED_INTENSITY_AURORAL',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_AURORAL']
        fetchkeys = ['YEAR_AURORAL','DOY_AURORAL','TIME_AURORAL']
    elif id == 'dayspec_gaim':
        datakeys = ['LATITUDE_DAY','LONGITUDE_DAY','ALTITUDE_DAY',
                    'PIERCEPOINT_DAY_SZA',
                    'DISK_INTENSITY_DAY',
                    'DISK_CALIBRATION_UNCERTAINTY_DAY',
                    'DISK_RECTIFIED_INTENSITY_DAY',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_DAY']
        fetchkeys = ['YEAR_DAY','DOY_DAY','TIME_DAY']
    elif id == 'nightspec_gaim':
        datakeys = ['LATITUDE_NIGHT','LONGITUDE_NIGHT','ALTITUDE_NIGHT',
                    'PIERCEPOINT_NIGHT_SZA',
                    'DISK_INTENSITY_NIGHT',
                    'DISK_CALIBRATION_UNCERTAINTY_NIGHT',
                    'DISK_RECTIFIED_INTENSITY_NIGHT',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_NIGHT']
        fetchkeys = ['YEAR_NIGHT','DOY_NIGHT','TIME_NIGHT']
    elif id == 'auroralspec_gaim':
        datakeys = ['LATITUDE_AURORAL','LONGITUDE_AURORAL','ALTITUDE_AURORAL',
                    'PIERCEPOINT_AURORAL_SZA',
                    'DISK_INTENSITY_AURORAL',
                    'DISK_CALIBRATION_UNCERTAINTY_AURORAL',
                    'DISK_RECTIFIED_INTENSITY_AURORAL',
                    'DISK_RECTIFIED_RADIANCE_UNCERTAINTY_AURORAL']
        fetchkeys = ['YEAR_AURORAL','DOY_AURORAL','TIME_AURORAL']

    try:
        return(status,datakeys,fetchkeys)
    except:
        return(1406,"","") # 1406 is HAPI 'unknown dataset id' error
        
def unwind_csv_array(magdata):
    """ Takes json-like arrays of e.g.                                          
        60.0,DOB,"[ -19.104668,-20.155156]"                                     
    and converts to unwound HAPI version of e.g.                                
        60.0,DOB,-19.104668,-20.155156                                          
    """
    magdata = re.sub(r'\]\"','',magdata)
    magdata = re.sub(r'\"\[','',magdata)
    magdata = re.sub(r'\]','',magdata)
    magdata = re.sub(r'\[','',magdata)
    magdata = re.sub(r', ',',',magdata) # also remove extra spaces              
    return(magdata)


def dump_image_to_csv(dataset,div):
    # UNTESTED, images do not map well to csv but HAPI requires it
    # it is really not recommended to do 2D via CSV/HAPI
    xdim = dataset.shape[0]
    xdim = dataset.shape[1]
    dump=""
    for xs in range(xdim-1):
        #dump += "{" + str(w4[k][xs].data) + "}" + div
        dump += str(w4[k][xs].data) + div
    return(dump)
                             
                
def netcdf_parsefile(floc,dirname,fname,fetchkeys,datakeys,secstart,secend,s3handle):
    """ Given a valid file, ingests it as an xarray, converts to csv,
        returns the csv data.<
    """
    
    # fname is for a valid day
    # secstart and secend are units of 'seconds in day'
    if debug: print("debug: standalone",floc,fname,fetchkeys,'.',datakeys,'.',secstart,secend)

    if floc['dir'] == 'aws':
        # need to open xarray via S3 bucket
        www = s3netcdf.s3data(s3handle,dirname+fname)
    else:
        # local file access
        www = xr.open_dataset(dirname + fname)

    # what if they only need part of the day? Subselect!

    # 1) Filter down to choose data items first (avoids 'ghost columns' remaining later)
    ww=www[fetchkeys]
    #print("Debug, pre-filter fetchkeys are:", ww)
    # 2) Filter by time
    func = lambda year, day, sec: (datetime.datetime.strptime(str(int(year))+' '+str(int(day)),'%Y %j') + datetime.timedelta(seconds=int(sec))).isoformat() + 'Z'
    #
    # Filter down to time range
    ##w3 = ww.where( (ww.TIME_DAY > secstart) &
    ##               (ww.TIME_DAY < secend),drop=True)
    ww.load()   # annoying but required when using with many files
    
    w3 = ww.where( (ww[fetchkeys[2]] > secstart) &
                   (ww[fetchkeys[2]] < secend),drop=True)

    #print("Debug, time keys are:", ww[fetchkeys[2]])
    #print("Debug, versus window: ",secstart, secend)
    # 3) Now convert to timestamp and add
    ##datestr = list(map(func,w3['YEAR_DAY'].data,
    ##                   w3['DOY_DAY'].data, w3['TIME_DAY'].data))
    datestr = list(map(func,w3[fetchkeys[0]].data,
                       w3[fetchkeys[1]].data, w3[fetchkeys[2]].data))
    w4=w3.assign(Timestamp=datestr)

    #print("Debug, made timestamps for data, ",w4)

    # 4) Manually print as csv, since
    # to_dataframe().to_csv() keeps showing 'deleted' items
    extra_quotes = False
    if extra_quotes:
        # variant set:   '1.0','2.0','1.0'
        div="','"
        divo="'"
    else:
        # primary set:   1.0, 2.0, 1.0
        div=","
        divo=""
    
    #topstr = divo + div.join(csvkeys) + divo
        
    ##s.wfile.write(bytes(topstr,"utf-8"))
    retdata=""
    # sanity check for when no actual valid keys are asked for
    if debug: print("Debug, len(datakeys) is ",len(datakeys),datakeys)
    if len(datakeys) == 0:
        return(1201,retdata) # 1201 = no data

    # some variables are nchan, some are not
    # e.g. len(w4['LATITUDE_DAY'][i].shape) = 1
    #      len(w4['LIMB_INTENSITY_GAIM'][i].shape = ?
    
    # right now it does spectral, i.e. data is either
    # 'data' or 'data, nchan'
    # But image stuff is either
    # 'x, y, nchan' or the confusion 'x, y'
    # So maybe...

    status = 1200 # status 1200 is HAPI "OK"

    if 'nchan' not in w4.keys():
        nchan=1
    else:
        nchan = w4.dims['nchan']
    if debug: print("debug, keys are: ",w4.keys())
    
    if debug: print("Debug, nchan is ",nchan," and looking for keys ",datakeys)
    if debug: print("Debug, looking at range ",w4.dims['Timestamp'])
    for i in range( w4.dims['Timestamp']):

        mytime = w4['Timestamp'][i].data
        linestr=divo + str(mytime) + div
        for k in datakeys:
            # figure out if it is channel data or bulk
            if 'nchan' in w4[k].dims:
                nchan = w4.dims['nchan']
            else:
                nchan = 1
            ndims = len(w4[k].dims)

            #print('debug: time=',mytime,',key=',k,', ndims=',ndims,' nchan_flag=',nchan_flag,' nchan=',nchan)

            if ndims == 1:
                # primary case
                # Common tested case, presumed ndims==1 e.g. scalar
                meta = w4[k][i].data
                linestr += str(meta) + div
            elif ndims == 2 and nchan > 1:

                meta = []
                for j in range( nchan ):
                    # start of terrible hackery to delete, part 2 of 2
                    if nix_channel_six and nchan == 6 and j == 5:
                        # terrible hack to avoid channel 6 until data is fixed
                        continue
                    # Common tested case, is scalar channel data
                    meta.append(float(w4[k][i,j].data))
                    meta_clean = unwind_csv_array(str(meta))
                linestr += meta_clean + div
                    
                
            elif ndims == 2 and nchan == 1:
                # is image data, only 1 channel so only do once
                if j == 0:
                    linestr += dump_image_to_csv(w4[k][i],div)
                else:
                    linestr += '0' + div
            elif ndims == 3:
                # is nchan image data
                linestr += dump_image_to_csv(w4[k][i,:,:,j],div)
            else:
                print("Error, too many dimensions to data: ",ndims)


            #linestr += str(w4[k][i].data) + div

        # wrapping up this one timestep
        linestr=linestr[:-1] + '\n'

        ##s.wfile.write(bytes(linestr,"utf-8"))
        retdata += linestr

    # double check that actual data was found
    if len(retdata) == 0:
        status = 1201 # status 1201 is HAPI "OK - no data for time range"

    www.close()
    return(status,retdata)

def find_netcdf_files(floc,year_start,year_end,doy_start,doy_end,sec_start,sec_end):
    """ Hunts for valid GUVI NetCDF files in 'floc' for the given data range.
    """
    
    # 6/2021 modded to handle 2 name schemas-- web-served names & swains names
    # format of data is YYYY DOY SECONDS
    # Name_format_1 = 
    # TIMED_GUVI_L1C-2-disk-SPECT_2021077012521-2021077030209_REV104526_Av13-01r001.nc
    # pattern: "TIMED_GUVI_L1C-2-disk-SPECT_" + YYYYDOY + SSSSSS - YYYYDOY + SSSSSS + _REVnnnnnn_Avnn-nnrnnn + '.nc"
    # or
    # Name_format_2 =
    # GUVI_Av0115r001_2011203REV52100.sp_disk_sdr2
    # pattern: YYYY/DOY/ + "GUVI_Av" + nnnrnnn_ + YYYYDOY + "REV" + SSSSS + ".sp_disk_sdr2"
    #
    # will need time1-time2
    fstem1 = 'TIMED_GUVI_L1C-2-disk-SPECT_'
    ftail1 = '.nc'
    fstem2 = 'GUVI_Av0115r001_'
    ftail2 = '.sp_disk_sdr2'
    
    # note we add first, then int, to preserve interstitial zeroes
    sstart = int(year_start + doy_start)
    send = int(year_end + doy_end)
    yeartemp = int(year_start)
    
    # Does handle year and day rollovers!
    flist=[]
    seconds={}     
    #print("debug: scanning ",sstart,send+1)
    # go day-by-day
    for i in range(sstart,send+1):
        # First grab all files for that valid day
        # note files are in floc + '/' + YYYYDOY
        # adding year rollover here
        doy = i - (yeartemp*1000)
        if doy > 365:
            yeartemp += 1
            doy=1
        yyyydoy=  str(yeartemp) + ("%03d" %  doy)
        # also handle partial days here at start or end of date range
        temp_start=0
        if i == sstart: temp_start=int(sec_start)
        temp_end=86400+1
        if i == send: temp_end=int(sec_end)

        #print("debug: checking ",yyyydoy,floc+yyyydoy)
        try:
            if floc['dir'] == 'aws':
                # AWS S3 stuff
                potentials = s3netcdf.s3_search(yyyydoy)
            else:
                # local files
                potentials = sorted(os.listdir(floc['dir'] + yyyydoy))
            #print("debug: found potentials ",potentials)
        except:
            # usually this exception is when the data dir is not on the server
            #print("debug: probably should throw a server error here")
            potentials = []
            
        regpattern1 = fstem1 + str(i)
        regpattern2 = fstem2 + str(i)
        #print("debug: i:",i," looking regpatterns:",regpattern1,regpattern2)
        sublist = [name for name in potentials if
                   (name.startswith(regpattern1) and name.endswith(ftail1)) or
                   (name.startswith(regpattern2) and name.endswith(ftail2))]
        #print("debug: reduced to sublist ",sublist)


        # now populate its bracketing seconds, for later reads
        keepers=[]
        for fname in sublist:
            #print("debug: sublist checking",fname)
            #print("debug: seconds key is ",fname+'start')
            seconds[fname+'start']=temp_start
            seconds[fname+'end']=temp_end

            """ # This next bit did not work, hence commented out
            # regex to remove items < sec_start
            try:
                if fname.startswith(regpattern1):
                    # these files include sec range in name, so filter
                    f_start = int(fname[35:41])
                    f_end = int(fname[49:55])
                elif fname.startswith(regpattern2):
                    # these files have no seconds in name, so just allow
                    f_start = temp_start
                    f_end = temp_end
            except:
                # cannot parse so force fail
                f_end = temp_start -1
                
            if f_start < temp_end and f_end > temp_start:
                #keepers,append(fname)
                print("debug: keeping ",fname)
            """
                
        #print("Okay, added ",sublist," to ",flist)
        
        flist += sublist  # keepers # sublist

    return(flist,seconds)

                        
def do_data_netcdf(id, timemin, timemax, parameters, catalog, floc,
                   stream_flag, stream):
    """ Code needed by HAPI to go from 'here are times and a dataset id'
        to actually returning a 0/1 status flag plus the csv data.
        Note list and order of arguments CANNOT be changed because
        this is called by the HAPI server.
    """
    
    # ***** THE CODE *****
    # note-- if timestart/end does not occur in file, program will go badly

    # fetchkeys is to copy, datakeys is 2D data, csvkeys is for labels
    (status,datakeys,timekeys) = netcdf_get_keys(id)
    if status > 1:
        # bad id, so exit early
        return(status,"")


    # remove keys via parameters
    #print("initial parameters:",parameters)
    #print("initial datakeys:",datakeys)
    if len(parameters) > 0:
        newkeys=[]
        for mykey in datakeys:
            if mykey in parameters:
                newkeys.append(mykey)
        datakeys=newkeys
    #print("new datakeys:",datakeys)

    fetchkeys = timekeys + datakeys
    csvkeys = ['Timestamp','Channel'] + datakeys

        
    # get list of all valid files

    # convert strings to datetimes to lists
    # e.g. 2021-03-18T03:00Z and 2021-03-18T05:00Z
    # to [2021, 3, 18, 3, 0, 0] and [2021, 3, 18, 5, 0, 0]
    timestart = datetime.datetime.strptime(timemin,'%Y-%m-%dT%H:%MZ') 
    timeend = datetime.datetime.strptime(timemax,'%Y-%m-%dT%H:%MZ')
    timestartlist = list(timestart.timetuple())
    timeendlist = list(timeend.timetuple())

    year_start = timestart.strftime('%Y')
    year_end = timeend.strftime('%Y')
    doy_start = timestart.strftime('%j')
    doy_end = timeend.strftime('%j')
    sec_start = '%06d' % (timestartlist[3]*60*60 + timestartlist[4]*60)
    sec_end = '%06d' % (timeendlist[3]*60*60 + timeendlist[4]*60)

    #print("debug, hunting:",year_start,doy_start, sec_start)
    (flist,seconds) = find_netcdf_files(floc,year_start,year_end,doy_start,doy_end,sec_start,sec_end)
    #print("debug: Got: ",flist,seconds)
    #print('debug: ',id, 'P:',parameters,'Times:',timemin, timemax, timestart,timeend)

    # 2) Read in file(s)
    # HAPI has no headers, or alt start with the CSV header
    classic_csv_header = False
    if classic_csv_header:
        div="','"
        divo="'"
        data = '#' + divo + div.join(csvkeys) + divo + '\n'
    else:
        data = ''
    status=1201 # status 1201 is HAPI "OK - no data for time range"

    if floc['dir'] == 'aws':
        s3handle = s3netcdf.s3open(access)
    else:
        s3handle="" # not needed for local access

    for fname in sorted(flist):
        #print("debug: Parsing ",fname,seconds)
        mystartsec = seconds[fname+'start']
        myendsec = seconds[fname+'end']
        # awkward add of yyyydoy to filestem
        doy = fname[28:35]
        if floc['dir'] == 'aws':
            dirname = floc['bucket'] + '/' + doy + '/'
        else:
            dirname = floc['dir'] + doy + '/'
        (tstatus,tdata)=netcdf_parsefile(floc,dirname,fname,
                                       fetchkeys,datakeys,
                                       mystartsec,myendsec,s3handle)
        data += tdata
        # keep checking to verify total data is not zero
        # status 1201 is HAPI "OK - no data for time range"
        if status != 1200:
            if tstatus == 1200: status = 1200 # status 1200 is HAPI "OK"

        # HAPI has two modes, get data then stream all, or
        # stream on a per-file incremental basis.
        # Here is code for streaming (streams, then clears buffer)
        if stream_flag:
            stream.wfile.write(bytes(data,"utf-8"))
            data=''
            
    #print(data)
    #print('*******')

    return(status,data)


def sample_netcdf_invocation():
    # Sample invocation:
    #from netcdfhapi import *

    floc = {'dir':'home_netcdf/rawdata/'}   # location of data, with a closing /
    time_min='2021-07-06T03:00Z'
    time_max='2021-07-06T05:00Z'
    id = 'dayspec'                # data item 'bundle' to return
    (status, data) = do_data_netcdf(id, time_min, time_max, '', '', floc,
                                    False, '')

    # cute little printout of first two rows of csv string
    #print("Got ",len(data)," rows of data, sample row:")
    iloc1 = data.find('\n')
    iloc2 = 1 + iloc1 + data[iloc1+1:].find('\n')
    #print(data[0:iloc2])

    
#sample_netcdf_invocation()
