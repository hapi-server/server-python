"""
hapi_psp.py, web pass-thru reader for Parker Solar Probe

Authors:
Eric Winter
Lisa Knowles
"""


# ----------------------------------------------------------------------------


# Standard modules
import copy
import json
from datetime import datetime

# Third-party modules

# Project modules
import psp_query


# Module constants

# datetime-style format strings for HAPI and PSP dates.
HAPI_DATETIME_FORMAT = "%Y-%m-%dT%H:%MZ"
PSP_DATETIME_FORMAT  = "%Y-%jT%H:%M:%S.%f"

# dict to map HAPI parameter names to PSP REST server parameter names.
HAPI_TO_PSP_COLUMN_NAME_MAP = {
    "SPP.x":  {"body": "SPP", "column": "SPP.x",  "column_index": 0},
    "SPP.y":  {"body": "SPP", "column": "SPP.y",  "column_index": 1},
    "SPP.z":  {"body": "SPP", "column": "SPP.z",  "column_index": 2},
    "SPP.dx": {"body": "SPP", "column": "SPP.dx", "column_index": 3},
    "SPP.dy": {"body": "SPP", "column": "SPP.dy", "column_index": 4},
    "SPP.dz": {"body": "SPP", "column": "SPP.dz", "column_index": 5},
    "SUN.x":  {"body": "SUN", "column": "SUN.x",  "column_index": 0},
    "SUN.y":  {"body": "SUN", "column": "SUN.y",  "column_index": 1},
    "SUN.z":  {"body": "SUN", "column": "SUN.z",  "column_index": 2},
    "SUN.dx": {"body": "SUN", "column": "SUN.dx", "column_index": 3},
    "SUN.dy": {"body": "SUN", "column": "SUN.dy", "column_index": 4},
    "SUN.dz": {"body": "SUN", "column": "SUN.dz", "column_index": 5},
    "MERCURY.x":  {"body": "MERCURY", "column": "MERCURY.x",  "column_index": 0},
    "MERCURY.y":  {"body": "MERCURY", "column": "MERCURY.y",  "column_index": 1},
    "MERCURY.z":  {"body": "MERCURY", "column": "MERCURY.z",  "column_index": 2},
    "MERCURY.dx": {"body": "MERCURY", "column": "MERCURY.dx", "column_index": 3},
    "MERCURY.dy": {"body": "MERCURY", "column": "MERCURY.dy", "column_index": 4},
    "MERCURY.dz": {"body": "MERCURY", "column": "MERCURY.dz", "column_index": 5},
    "VENUS.x":  {"body": "VENUS", "column": "VENUS.x",  "column_index": 0},
    "VENUS.y":  {"body": "VENUS", "column": "VENUS.y",  "column_index": 1},
    "VENUS.z":  {"body": "VENUS", "column": "VENUS.z",  "column_index": 2},
    "VENUS.dx": {"body": "VENUS", "column": "VENUS.dx", "column_index": 3},
    "VENUS.dy": {"body": "VENUS", "column": "VENUS.dy", "column_index": 4},
    "VENUS.dz": {"body": "VENUS", "column": "VENUS.dz", "column_index": 5},
    "EARTH.x":  {"body": "EARTH", "column": "EARTH.x",  "column_index": 0},
    "EARTH.y":  {"body": "EARTH", "column": "EARTH.y",  "column_index": 1},
    "EARTH.z":  {"body": "EARTH", "column": "EARTH.z",  "column_index": 2},
    "EARTH.dx": {"body": "EARTH", "column": "EARTH.dx", "column_index": 3},
    "EARTH.dy": {"body": "EARTH", "column": "EARTH.dy", "column_index": 4},
    "EARTH.dz": {"body": "EARTH", "column": "EARTH.dz", "column_index": 5},
    "MARS_BARYCENTER.x":  {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.x",  "column_index": 0},
    "MARS_BARYCENTER.y":  {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.y",  "column_index": 1},
    "MARS_BARYCENTER.z":  {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.z",  "column_index": 2},
    "MARS_BARYCENTER.dx": {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.dx", "column_index": 3},
    "MARS_BARYCENTER.dy": {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.dy", "column_index": 4},
    "MARS_BARYCENTER.dz": {"body": "MARS_BARYCENTER", "column": "MARS_BARYCENTER.dz", "column_index": 5},
    "JUPITER_BARYCENTER.x":  {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.x",  "column_index": 0},
    "JUPITER_BARYCENTER.y":  {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.y",  "column_index": 1},
    "JUPITER_BARYCENTER.z":  {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.z",  "column_index": 2},
    "JUPITER_BARYCENTER.dx": {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.dx", "column_index": 3},
    "JUPITER_BARYCENTER.dy": {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.dy", "column_index": 4},
    "JUPITER_BARYCENTER.dz": {"body": "JUPITER_BARYCENTER", "column": "JUPITER_BARYCENTER.dz", "column_index": 5},
    "SATURN_BARYCENTER.x":  {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.x",  "column_index": 0},
    "SATURN_BARYCENTER.y":  {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.y",  "column_index": 1},
    "SATURN_BARYCENTER.z":  {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.z",  "column_index": 2},
    "SATURN_BARYCENTER.dx": {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.dx", "column_index": 3},
    "SATURN_BARYCENTER.dy": {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.dy", "column_index": 4},
    "SATURN_BARYCENTER.dz": {"body": "SATURN_BARYCENTER", "column": "SATURN_BARYCENTER.dz", "column_index": 5},
    "URANUS_BARYCENTER.x":  {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.x",  "column_index": 0},
    "URANUS_BARYCENTER.y":  {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.y",  "column_index": 1},
    "URANUS_BARYCENTER.z":  {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.z",  "column_index": 2},
    "URANUS_BARYCENTER.dx": {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.dx", "column_index": 3},
    "URANUS_BARYCENTER.dy": {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.dy", "column_index": 4},
    "URANUS_BARYCENTER.dz": {"body": "URANUS_BARYCENTER", "column": "URANUS_BARYCENTER.dz", "column_index": 5},
    "NEPTUNE_BARYCENTER.x":  {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.x",  "column_index": 0},
    "NEPTUNE_BARYCENTER.y":  {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.y",  "column_index": 1},
    "NEPTUNE_BARYCENTER.z":  {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.z",  "column_index": 2},
    "NEPTUNE_BARYCENTER.dx": {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.dx", "column_index": 3},
    "NEPTUNE_BARYCENTER.dy": {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.dy", "column_index": 4},
    "NEPTUNE_BARYCENTER.dz": {"body": "NEPTUNE_BARYCENTER", "column": "NEPTUNE_BARYCENTER.dz", "column_index": 5},
    "PLUTO_BARYCENTER.x":  {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.x",  "column_index": 0},
    "PLUTO_BARYCENTER.y":  {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.y",  "column_index": 1},
    "PLUTO_BARYCENTER.z":  {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.z",  "column_index": 2},
    "PLUTO_BARYCENTER.dx": {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.dx", "column_index": 3},
    "PLUTO_BARYCENTER.dy": {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.dy", "column_index": 4},
    "PLUTO_BARYCENTER.dz": {"body": "PLUTO_BARYCENTER", "column": "PLUTO_BARYCENTER.dz", "column_index": 5},
}

# ----------------------------------------------------------------------------


# def handle_hapi_request(
#         id, timemin, timemax, parameters, catalog, floc, stream_flag, stream
# ):
#     """Process a HAPI request.

#     Process a HAPI request.

#     Parameters
#     ----------
#     id : str
#         Identifier string for requested dataset.
#     timemin : str
#         Start datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
#     timemax : str
#         End datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
#     parameters : list of str
#         List of strings specifying requested parameters.
#     catalog : dict
#         Dataset information from catalog JSON file.
#     floc : dict
#         Site-specific information from *_config.py
#     stream_flag : bool
#         True if results should be streamed back to requestor.
#     stream : MyHandler object
#         Handler object for HAPI request.

#     Returns
#     -------
#     hapi_status, hapi_result_str : int, str
#         HAPI status code, and result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     # Initialize HAPI status code and return string.
#     hapi_status = -1
#     hapi_result_str = ""

#     # Process the query based on the HAPI endpoint requested.

#     # Return the HAPI status code and the query result string.
#     return hapi_status, hapi_result_str


# def handle_hapi_about_request(hapi_request_url: str):
#     """Process a HAPI "about" request.

#     Process a HAPI "about" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = "ABOUT"
#     return hapi_result_str


# def handle_hapi_capabilities_request(hapi_request_url: str):
#     """Process a HAPI "about" request.

#     Process a HAPI "about" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


# def handle_hapi_catalog_request(hapi_request_url: str):
#     """Process a HAPI "catalog" request.

#     Process a HAPI "catalog" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


# def handle_hapi_info_request(hapi_request_url: str):
#     """Process a HAPI "info" request.

#     Process a HAPI "info" request.

#     Parameters
#     ----------
#     hapi_request_url : str
#         HAPI request URL string.

#     Returns
#     -------
#     hapi_result_str : str
#         Result of HAPI request.

#     Raises
#     ------
#     None
#     """
#     hapi_result_str = ""
#     return hapi_result_str


def handle_hapi_data_request(
        id, timemin, timemax, parameters, catalog, floc, stream_flag, stream
):
    """Process a HAPI data request.

    Process a HAPI data request.

    Parameters
    ----------
    id : str
        Identifier string for requested dataset.
    timemin : str
        Start datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
    timemax : str
        End datetime string for request, format 'YYYY-MM-DDThh:mmZ'.
    parameters : list of str
        List of strings specifying requested parameters.
    catalog : dict
        Dataset information from catalog JSON file.
    floc : dict
        Site-specific information from *_config.py
    stream_flag : bool
        True if results should be streamed back to requestor.
    stream : MyHandler object
        Handler object for HAPI request.

    Returns
    -------
    hapi_status, hapi_result_str : int, str
        HAPI status code, and result of HAPI request.

    Raises
    ------
    None
    """
    # Initialize HAPI status code and return string.
    hapi_status = 0
    hapi_result_str = ""

    # Create datetime objects for the start and end times, then convert to
    # PSP format.
    hapi_start_datetime = datetime.strptime(timemin, HAPI_DATETIME_FORMAT)
    hapi_end_datetime = datetime.strptime(timemax, HAPI_DATETIME_FORMAT)
    psp_start_datetime_s = hapi_start_datetime.strftime(PSP_DATETIME_FORMAT)
    psp_end_datetime_s = hapi_end_datetime.strftime(PSP_DATETIME_FORMAT)

    # Map HAPI column names to PSP body and column name.
    bodies = []
    for p in parameters:
        if p == 'Time':
            continue
        bodies.append(HAPI_TO_PSP_COLUMN_NAME_MAP[p]["body"])
    bodies = sorted(set(bodies))

    # Determine the output format.
    output_format = "csv"
    if "format=csv" in stream.path:
        output_format = "csv"
    elif "format=json" in stream.path:
        output_format = "json"
    elif "format=binary" in stream.path:
        output_format = "binary"

    # Process the query.
    ephemeris_s = psp_query.query_psp_ephemeris(
        start=psp_start_datetime_s, stop=psp_end_datetime_s, body=bodies,
        outputformat=output_format
    )
    print(f"{ephemeris_s=}")

    # Filter the results string to include only the columns requested by the
    # client.
    new_ephemeris_s = filter_results_string(ephemeris_s, parameters,
                                            output_format)

    # Return the HAPI status code and the query result string.
    return hapi_status, new_ephemeris_s


def filter_results_string(ephemeris_s, parameters, output_format):
    """Extract requested columns from result string.

    Extract requested columns from result string.

    Parameters
    ----------
    ephemeris_s : str
        Original result string from PSP server.
    parameters : list of str
        List of column names requested by client.
    output_format : str
        String identifying output format ("csv" or "json")

    Returns
    -------
    new_result_s : str
        Filtered result string containing only requested columns.

    Raises
    ------
    TypeError
        If an invalid output format was specified.
    """
    # Filter the result based on the output format.
    new_ephemeris_s = None
    if output_format == "csv":
        new_ephemeris_s = filter_csv_results_string(ephemeris_s, parameters)
    elif output_format == "json":
        new_ephemeris_s = filter_json_results_string(ephemeris_s, parameters)
    else:
        raise TypeError(
            f"Invalid PSP trajectory query output format: {output_format}."
        )

    # Return the filtered result string.
    return new_ephemeris_s


def filter_csv_results_string(csv_s, parameters):
    """Extract requested columns from a CSV result string.

    Extract requested columns from a CSV result string.

    Parameters
    ----------
    csv_s : str
        Original CSV result string from PSP server.
    parameters : list of str
        List of column names requested by client.

    Returns
    -------
    new_csv_s : str
        Filtered CSV result string containing only requested columns.

    Raises
    ------
    None
    """
    # Strip trailing whitespace.
    csv_s = csv_s.rstrip()

    # Split the original strings into a list containing the header line
    # and individual record lines.
    lines = csv_s.split('\r\n')

    # Split the first row to get a list of column names.
    header_line = lines[0]
    column_names = header_line.split(',')

    # <HACK>
    # Make sure the first column is called "Time".
    column_names[0] = 'Time'
    # </HACK>

    # Map the column names to column number.
    column_numbers = {}
    for (i, s) in enumerate(column_names):
        column_numbers[s] = i

    # Make a list of the column numbers to extract.
    columns_to_keep = []
    for p in parameters:
        i_col = column_numbers[p]
        columns_to_keep.append(i_col)

    # Make a new list containing only the requested columns.
    records = lines[1:]  # Skip header line.
    new_records = []
    for r in records:
        cols = r.split(',')
        keep = []
        for ic in columns_to_keep:
            keep.append(cols[ic])
        s = ','.join(keep)
        new_records.append(s)

    # Assemble the new header line.
    new_header_line = ','.join(parameters)

    # Combine the filtered records without the header.
    lines = new_records

    # Reasemble the filtered data into a single string.
    new_csv_s = '\r\n'.join(lines)

    # Return the filtered result string.
    return new_csv_s


def filter_json_results_string(json_s, parameters):
    """Extract requested columns from a JSON result string.

    Extract requested columns from a JSON result string.

    Parameters
    ----------
    json_s : str
        Original JSON result string from PSP server.
    parameters : list of str
        List of column names requested by client.

    Returns
    -------
    new_json_s : str
        Filtered JSON result string containing only requested columns.

    Raises
    ------
    None
    """
    # Parse the result string to a JSON object.
    json_j = json.loads(json_s)

    # Make a copy of the list of bodies in the results.
    bodies = json_j["bodies"]

    # Invert the bodies list to a dictionary to map from the body name to the
    # sub-list of the result records.
    bodies_d = {}
    for (i, b) in enumerate(bodies):
        bodies_d[b] = i + 1  # Add 1 since Time is column 0.

    # Make a parameter map (pmap) for the requested columns to the
    # corresponding sub-array and index of the results. Time is always
    # parameter 0 and result column 0.
    pmap = {}
    for (ip, p) in enumerate(parameters[1:]):
        b = HAPI_TO_PSP_COLUMN_NAME_MAP[p]["body"]
        ib = bodies_d[b]
        ic = HAPI_TO_PSP_COLUMN_NAME_MAP[p]["column_index"]
        pmap[p] = {"ib": ib, "ic": ic}

    # Make a deep copy of the parsed JSON.
    new_json_j = copy.deepcopy(json_j)

    # Clear the data element of the copy.
    new_json_j["data"] = []

    # Create a new 2-D table using Time and the requested columns.
    for r in json_j["data"]:
        new_r = [r[0]]  # Time
        ib = pmap[p]["ib"]
        ic = pmap[p]["ic"]
        for p in pmap:
            new_r.append(r[ib][ic])
        new_json_j["data"].append(new_r)

    # Convert the JSON object to a string.
    new_json_s = json.dumps(new_json_j)

    # Return the filtered result string.
    return new_json_s


if __name__ == '__main__':
    pass
    # assert handle_hapi_request("http://yoyodyne.jhuapl.edu:8080/hapi/about") != ""
    # assert handle_hapi_capabilities_request() is not ""
    # assert handle_hapi_catalog_request() is not ""
    # assert handle_hapi_info_request() is not ""
    # assert handle_hapi_data_request() is not ""
