# Usage:
#   python test_metadata_json_error.py

def test_metadata_json_error():
  from hapiserver.endpoints import _get_catalog, _get_info

  def valid_catalog():
    return '[{"id": "demo1"}]'

  def malformed_catalog():
    return 'INVALID'

  def malformed_info(dataset):
    return 'INVALID'

  catalog, error = _get_catalog({}, {"functions": {"catalog": valid_catalog}})
  assert error is None
  assert catalog == [{"id": "demo1"}]

  catalog, error = _get_catalog({}, {"functions": {"catalog": malformed_catalog}})
  assert catalog is None
  assert error['code'] == 1500
  assert error['message'] == "Error parsing JSON returned by catalog function"
  assert 'exception' in error

  info, error = _get_info({"dataset": "demo1"}, {"functions": {"info": malformed_info}})
  assert info is None
  assert error['code'] == 1500
  assert error['message'] == "Error parsing JSON returned by info function"
  assert 'exception' in error


if __name__ == "__main__":
  test_metadata_json_error()
