# Usage:
#   python test_parameters_error.py

def test_parameters_error():
  from hapiserver.endpoints import _parameters_error

  info = {
    "parameters": [
      {"name": "Time"},
      {"name": "scalar"}
    ]
  }

  assert _parameters_error("", info) is None
  assert _parameters_error("Time,scalar", info) is None
  assert _parameters_error("scalar", info) is None

  error = _parameters_error("INVALID", info)
  assert error['code'] == 1407

  error = _parameters_error("scalar,scalar", info)
  assert error['code'] == 1411
  assert error['message'] == "Duplicate parameters: scalar"

  error = _parameters_error("scalar,Time", info)
  assert error['code'] == 1411
  assert 'Parameters out of order' in error['message']


if __name__ == "__main__":
  test_parameters_error()
