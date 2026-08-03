# Usage:
#   python test_demo.py

import logging

from util.prep_demo_repo import prep_demo_repo

format = '%(name)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, format=format)
logger = logging.getLogger(__name__)

wait = {
  "retries": 10,
  "delay": 0.5
}


def test_scripts():
  demo_dir = prep_demo_repo()
  config = demo_dir / "src" / "config-scripts.json"

  logger.info("Executing test_scripts()")
  _run_tests(config)
  logger.info("")
  logger.info("")


def test_functions():
  import os
  import sys

  demo_dir = prep_demo_repo()
  config = demo_dir / "src" / "config.json"

  # config.json references dotted paths such as
  # "src.catalog.catalog", which requires the demo repo's root directory
  # (the parent of src/) to be importable. Add it to sys.path for imports
  # performed in this process (e.g. hapiserver.config()'s function
  # validation) and to PYTHONPATH so the spawned uvicorn worker process
  # can resolve the same imports.
  demo_dir_str = str(demo_dir)
  if demo_dir_str not in sys.path:
    sys.path.insert(0, demo_dir_str)
  existing_pythonpath = os.environ.get("PYTHONPATH", "")
  if demo_dir_str not in existing_pythonpath.split(os.pathsep):
    os.environ["PYTHONPATH"] = os.pathsep.join(
      part for part in [demo_dir_str, existing_pythonpath] if part
    )

  logger.info("Executing test_functions()")
  _run_tests(config)
  logger.info("")
  logger.info("")


def _check_data_response(resp):
  assert resp.status_code == 200
  assert 'text/csv' in resp.headers['Content-Type']
  assert resp.text.strip() == "1970-01-01T00:00:00Z,0"


def _run_tests(config):
  import requests
  import socket

  import hapiserver

  # Get default configs and override with command line arguments.
  configs = hapiserver.cli(config=config)

  with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    configs['server']['--port'] = sock.getsockname()[1]

  port = configs['server']['--port']
  url_base = f"http://0.0.0.0:{port}/hapi"

  wait['url'] = url_base

  process = hapiserver.start(configs, wait)

  _log_test_title(url_base)

  url = url_base
  response = requests.get(url)
  assert response.status_code == 200
  assert 'text/html' in response.headers['Content-Type']

  url = f"{url_base}/catalog"
  response = requests.get(url)
  assert response.status_code == 200
  assert 'application/json' in response.headers['Content-Type']
  response_json = response.json()
  assert 'catalog' in response_json
  catalog = response_json['catalog']
  assert isinstance(catalog, list)
  assert len(catalog) > 0
  assert 'HAPI' in response_json
  assert 'status' in response_json

  url = f"{url_base}/catalog?depth=dataset"
  response = requests.get(url)
  assert response.status_code == 200
  assert 'application/json' in response.headers['Content-Type']
  response_json_dataset = response.json()
  assert response_json_dataset == response_json

  url = f"{url_base}/catalog?depth=all"
  response = requests.get(url)
  assert response.status_code == 200
  assert 'application/json' in response.headers['Content-Type']
  # Most tests on response_json_catalog_all later
  response_json_catalog_all = response.json()

  url = f"{url_base}/capabilities"
  response = requests.get(url)
  assert response.status_code == 200
  assert 'application/json' in response.headers['Content-Type']
  capabilities = response.json()
  assert isinstance(capabilities, dict)
  assert 'outputFormats' in capabilities
  assert 'HAPI' in response_json
  assert 'status' in response_json


  start = "1970-01-01T00:00:00Z"
  stop = "1970-01-01T00:00:01Z"

  for dataset in catalog:
    url = f"{url_base}/info?dataset={dataset['id']}"
    response = requests.get(url)
    assert response.status_code == 200
    assert 'application/json' in response.headers['Content-Type']
    info = response.json()
    assert isinstance(info, dict)
    assert 'HAPI' in info
    assert 'status' in info
    assert 'parameters' in info

    catalog_all_dataset = next(
      item for item in response_json_catalog_all['catalog']
      if item['id'] == dataset['id']
    )
    info_without_envelope = {
      key: value for key, value in info.items()
      if key not in ['HAPI', 'status']
    }
    assert catalog_all_dataset['info'] == info_without_envelope

    ds = dataset['id']

    url = f"{url_base}/info?dataset={ds}&parameters=scalar"
    response = requests.get(url)
    assert response.status_code == 200
    parameter_names = [parameter['name'] for parameter in response.json()['parameters']]
    assert parameter_names == ['Time', 'scalar']

    url = f"{url_base}/data?dataset={ds}&start={start}&stop={stop}"
    response = requests.get(url)
    _check_data_response(response)

    for q in ['dataset', 'id']:
      url = f"{url_base}/data?{q}={ds}&parameters=scalar&start={start}&stop={stop}"
      response = requests.get(url)
      _check_data_response(response)

    for q in ['time.min', 'start']:
      url = f"{url_base}/data?dataset={ds}&parameters=scalar&{q}={start}&stop={stop}"
      response = requests.get(url)
      _check_data_response(response)

    for q in ['time.max', 'stop']:
      url = f"{url_base}/data?dataset={ds}&parameters=scalar&start={start}&{q}={stop}"
      response = requests.get(url)
      _check_data_response(response)

    url = f"{url_base}/data?dataset={ds}&parameters=scalar&start={start}&stop={stop}&format=csv&include=header"
    response = requests.get(url)
    assert response.status_code == 200


  # Test error responses
  url = f"{url_base}x"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 404

  for endpoint in ['', 'catalog', 'info', 'data', 'about']:
    url = f"{url_base}/{endpoint}x"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 404


  url = f"{url_base}/about?xxxabc=123"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1401


  url = f"{url_base}/catalog?xxxabc=123"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1401

  url = f"{url_base}/info?dataset=demo1&xxxabc=123"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1401

  allowed_dataset_ids = ', '.join(dataset['id'] for dataset in catalog)

  url = f"{url_base}/info?dataset=INVALID"
  response = requests.get(url)
  assert response.status_code == 404
  json_response = response.json()
  assert json_response['status']['code'] == 1406
  assert 'Invalid dataset' in json_response['status']['message']
  assert f"Allowed datasets: {allowed_dataset_ids}" in json_response['status']['message']

  url = f"{url_base}/catalog?depth=invalid"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert json_response['status']['code'] == 1413

  url = f"{url_base}/info?dataset=demo1&resolve_references=true"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert json_response['status']['code'] == 1401

  url = f"{url_base}/info?dataset=demo1&resolve_references=invalid"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert json_response['status']['code'] == 1401


  start_stop = f"start={start}&stop={stop}"
  url = f"{url_base}/data?xxxdataset=demo1&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1401

  url = f"{url_base}/data?dataset={dataset['id']}&start=INVALIDZ&stop={stop}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1402

  url = f"{url_base}/data?dataset={dataset['id']}&start={start}&stop=INVALIDZ"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1403

  url = f"{url_base}/data?dataset={dataset['id']}&start={stop}&stop={start}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1404

  url = f"{url_base}/data?dataset={dataset['id']}&start=1969-12-31T23:59:59Z&stop={start}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1405

  url = f"{url_base}/data?dataset=INVALID&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 404
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1406
  assert 'Invalid dataset' in json_response['status']['message']
  assert f"Allowed datasets: {allowed_dataset_ids}" in json_response['status']['message']

  url = f"{url_base}/data?dataset={dataset['id']}&parameters=INVALID&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 404
  json_response = response.json()
  assert 'status' in json_response
  assert 'code' in json_response['status']
  assert json_response['status']['code'] == 1407

  url = f"{url_base}/info?dataset={dataset['id']}&parameters=INVALID1,INVALID2"
  response = requests.get(url)
  assert response.status_code == 404
  json_response = response.json()
  assert json_response['status']['code'] == 1407
  assert 'Invalid parameters: INVALID1, INVALID2' in json_response['status']['message']
  assert 'Allowed parameters: Time, scalar' in json_response['status']['message']

  url = f"{url_base}/data?dataset={dataset['id']}&parameters=INVALID1,INVALID2&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 404
  json_response = response.json()
  assert json_response['status']['code'] == 1407
  assert 'Invalid parameters: INVALID1, INVALID2' in json_response['status']['message']
  assert 'Allowed parameters: Time, scalar' in json_response['status']['message']

  url = f"{url_base}/data?dataset={dataset['id']}&format=invalid&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert json_response['status']['code'] == 1409

  url = f"{url_base}/data?dataset={dataset['id']}&include=invalid&{start_stop}"
  response = requests.get(url)
  assert response.status_code == 400
  json_response = response.json()
  assert json_response['status']['code'] == 1410


  hapiserver.stop(process)


def _log_test_title(url):
  msg = f"Testing {url}"
  line = len(msg)*"-"
  logger.info(line)
  logger.info(msg)
  logger.info(line)


if __name__ == "__main__":
  test_scripts()
  test_functions()
