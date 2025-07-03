from supermag_api import *
start = '2020-01-01T00:00Z'
extent = 600
station = 'AAA'
flagstring = 'Time&ext&iaga&geo&mag&mlt&sza&N&E&Z'
userid = 'superhapi'
(status, magdata) = supermag_getdata(userid,start,extent,flagstring,station,FORMAT='json')
print(f"Got status {status}, JSON data:\n{magdata}")
