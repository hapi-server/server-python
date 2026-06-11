# HAPI Python Server, including sample reader programs

original by jbfaden, Python3 update by sandyfreelance 04-06-2021 and onward


# Introduction
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

# Requires the following Python packages
  hapi_server.py: dateutil

# Usage:
  python hapi_server.py <MISSIONNAME> [localhost/http/https/custom]

# Sample Data Sets

A sample of CSV flat-file data (home_csv.zip) and NetCDF files
(home_netcdf.zip) are provided, including all configuration, reader,
JSON, and data files.

# Fun Fact
Although intended as a big data server, this was originally a module needed
to run on jfaden's Raspberry Pi, and should still will work on a Raspberry Pi!
