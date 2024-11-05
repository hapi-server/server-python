"""
CSV ingest program from original hapi-server.py code.

Part of the HAPI Python Server.  The code and documentation resides at:
    https://github.com/hapi-server/server-python

See accompanying 'csv_config.py' file for site-specific details.

Assumes data is flat files in a directory hierarchy of:
    "data/[id]/YYYY/[id].YYYYMMDD.csv
"""

import json
import math
import os
from pathlib import Path

import dateutil


def do_parameters_map(id: str, floc: dict, parameters: list[str]) -> dict:
    """Maps requested parameter indices to their respective column indices in a CSV file.

    Reads metadata from a JSON info file to determine CSV column mappings for each parameter.
    Returns a dict where each requested parameter is mapped to its corresponding column indices.
    Multi-column parameters (e.g., arrays) are represented by consecutive indices. Ensures that
    time (index 0) is always included in the output as the first entry.

    Parameters
    ----------
    id : str
        Dataset identifier for locating the JSON info file.
    floc : dict
        Dictionary with a 'dir' key pointing to the base directory of the info file.
    parameters : list of str
        List of parameter names to include in the mapping.

    Returns
    -------
    dict
        A dictionary where keys are indices of requested parameters, and values are lists of
        their respective column indices in the CSV.
    """
    ff = Path(floc["dir"]) / "info" / f"{id}.json"
    with open(ff, "r", encoding="utf-8") as fin:
        jset = json.loads(fin.read())
    pp = [item["name"] for item in jset["parameters"]]

    curr_col = 0
    param_dict = {}
    for idx, param_name in enumerate(pp):
        # Calculate the number of columns used for this parameter
        size = jset["parameters"][idx].get("size")
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


def do_data_csv(
    id: str,
    timemin: str,
    timemax: str,
    parameters: list[str],
    catalog,
    floc: dict,
    stream_flag: bool,
    stream,
) -> tuple[int, str]:
    """
    Retrieves and filters CSV time-series data within a specified date range and parameter list.

    Parses CSV files located within a dataset directory to extract data for specified parameters
    and a time range. If streaming is enabled, writes the data incrementally to the stream object.
    Returns a tuple with a status code and the final data string.

    Parameters:
    ----------
    id : str
        Dataset identifier for locating data files.
    timemin : str
        Start time for filtering records.
    timemax : str
        End time for filtering records.
    parameters : list[str]
        List of parameter names to retrieve from the data files.
    catalog : _
        Not used
    floc : dict
        Dictionary with a 'dir' key pointing to the base directory of the info file.
    stream_flag : bool
        If True, enables data streaming to `stream`.
    stream : object
        Stream object for data output.

    Returns:
    -------
    tuple[int, str]
        Status code (1200 if data found, 1201 if no data for time range) and the collected data
        string.
    """
    ff = floc["dir"] + "/data/" + id + "/"
    filemin = dateutil.parser.parse(timemin).strftime("%Y%m%d")
    filemax = dateutil.parser.parse(timemax).strftime("%Y%m%d")
    timemin = dateutil.parser.parse(timemin).strftime("%Y-%m-%dT%H:%M:%S")
    timemax = dateutil.parser.parse(timemax).strftime("%Y-%m-%dT%H:%M:%S")
    yrmin = int(timemin[0:4])
    yrmax = int(timemax[0:4])

    if parameters is not None:
        mm = do_parameters_map(id, floc, parameters)
    else:
        mm = None

    datastr = ""
    status = 0
    for yr in range(yrmin, yrmax + 1):
        ffyr = ff + f"{yr:04d}"
        if not os.path.exists(ffyr):
            continue
        files = sorted(os.listdir(ffyr))
        for file in files:
            ymd = file[-12:-4]
            if filemin <= ymd <= filemax:
                with open(Path(ffyr) / file, "r", encoding="utf-8") as f:
                    for rec in f:
                        ydmhms = rec[0:19]
                        if timemin <= ydmhms < timemax:
                            if mm is not None:
                                ss = rec.split(",")
                                comma = False
                                for i in mm:
                                    for li in mm[i]:
                                        if comma:
                                            datastr += ","
                                        datastr += ss[li]
                                        comma = True
                                if list(mm.values())[-1][-1] < (len(ss) - 1):
                                    datastr += "\n"
                            else:
                                datastr += rec

                            if len(datastr) > 0:
                                status = 1200
                            if stream_flag:
                                # Write then flush
                                stream.wfile.write(bytes(datastr, "utf-8"))
                                datastr = ""

    if status != 1200 and len(datastr) == 0:
        status = 1201  # status 1200 is HAPI "OK- no data for time range"

    return status, datastr
