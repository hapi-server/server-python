# This script will take user input queries for a particular set or subset of PSP ephemeris data,
# build the appropriate url, and fetch the data from the server where the PSP emphemeris
# data is hosted. This script will then return the data in json format.

import requests

def query_psp_ephemeris(
    url="http://yoyodyne.jhuapl.edu:8080/v1/psp/latest/ephemerides/json",
    collection="psp",
    version="latest",
    start=None,
    stop=None,
    stepsize=600.0,
    frame="ECLIPJ2000",
    center="SOLAR_SYSTEM_BARYCENTER",
    body=None,
    correction="NONE",
):
    """
    Takes user-defined query parameters for PSP ephemeris time series data, and returns the data in json format

    Parameters
    ----------
    url (str): Base URL for the PSP ephemeris API endpoint. Defaults to "http://yoyodyne.jhuapl.edu:8080/v1/psp/latest/ephemerides/json"
    collection (str): Geometry collection from which to compute. Default is 'psp.'
    version (str): Version of interest within the collection. Default is 'latest.'
    start (str): UTC start time in <YYYY>-<DOY>T<HH>:<MM>:<SS.SSSSSS> format. 
                 For example, '2024-001T12:15:00.123456.' Must be provided. Defaults to None.
    stop (str): UTC stop time in <YYYY>-<DOY>T<HH>:<MM>:<SS.SSSSSS> format.
                For example, '2024-001T12:15:00.123456.' Must be provided. Defaults to None.
    stepsize (float): Step size between queried records in TAI seconds. Defaults to 3600.0 seconds.
    frame (str): Name of the supported frame in which to compute ephemerides. Defaults to 'ECLIP2000.'
    center (str): Name of central body relative to which ephemeris state vectors are computed.
                  Defaults to 'SOLAR_SYSTEM_BARYCENTER.'
    body (str or None): Name of body for which to compute ephemerides. Must be provided. Defaults to None.
    correction (str): Aberration correction to apply. Defaults to the string value 'NONE.'

    Returns
    -------
    emphemeris_json (dict): JSON-serializable result
    """

    if start is None or stop is None:
        raise ValueError("start and stop must be provided")
    
    if body is None:
        raise ValueError("body must be provided")

    payload = {
        # "collection": collection,
        # "version": version,
        "start": start,
        "stop": stop,
        "stepsize": stepsize,
        "frame": frame,
        "center": center,
        "body[]": body,
        "correction": correction
    }
    
    r = requests.get(url, params=payload, timeout=30)
    r.raise_for_status()
    print(f"{r.url=}")
    print(f"{r.json()=}")
    # ephemeris_json = r.json()
    ephemeris_json_str = r.text

    return ephemeris_json_str
