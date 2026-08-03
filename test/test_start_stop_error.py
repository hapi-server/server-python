# Usage:
#   python test_start_stop_error.py

def test_start_stop_error():
  from hapiserver.endpoints import _start_stop_error

  def validate(
    start="1970-01-01T00:00:00Z",
    stop="1970-01-01T00:00:01Z",
    missing_info=None,
    **info_overrides):

    query = {
      "dataset": "demo1",
      "start": start,
      "stop": stop
    }

    info = {
      "startDate": "1970-01-01T00:00:00Z",
      "stopDate": "1970-01-01T00:00:02Z",
      **info_overrides
    }

    if missing_info:
      del info[missing_info]

    return _start_stop_error('data', query, {}, info), query

  error, query = validate()
  assert error is None
  assert 'start_datetime' in query
  assert 'stop_datetime' in query
  assert query['start_normalized'] == "1970-01-01T00:00:00.000000Z"
  assert query['stop_normalized'] == "1970-01-01T00:00:01.000000Z"

  error, _ = validate(start="INVALID")
  assert error['code'] == 1402
  assert error['message'] == "Invalid start time"

  error, _ = validate(stop="INVALID")
  assert error['code'] == 1403
  assert error['message'] == "Invalid stop time"

  error, _ = validate(stop="1970-01-01T00:00:00Z")
  assert error['code'] == 1404
  assert 'stop <= start' in error['message']

  error, _ = validate(startDate="INVALID")
  assert error['code'] == 1500
  assert 'Invalid value for startDate in info' in error['message']

  error, _ = validate(stopDate="INVALID")
  assert error['code'] == 1500
  assert 'Invalid value for stopDate in info' in error['message']

  error, _ = validate(missing_info="startDate")
  assert error['code'] == 1500
  assert error['message'] == "Missing startDate in info"

  error, _ = validate(missing_info="stopDate")
  assert error['code'] == 1500
  assert error['message'] == "Missing stopDate in info"

  error, _ = validate(start="1969-12-31T23:59:59Z")
  assert error['code'] == 1405
  assert 'start < startDate' in error['message']

  error, _ = validate(stop="1970-01-01T00:00:03Z")
  assert error['code'] == 1405
  assert 'stop > stopDate' in error['message']

  error, _ = validate(
    start="1969-12-31T23:59:59Z",
    stop="1970-01-01T00:00:03Z"
  )
  assert error['code'] == 1405
  assert 'start < startDate' in error['message']
  assert 'stop > stopDate' in error['message']

  error, _ = validate(maxRequestDuration="INVALID")
  assert error['code'] == 1500
  assert 'Invalid maxRequestDuration value in info' in error['message']

  error, _ = validate(stop="1970-01-01T00:00:02Z", maxRequestDuration="PT1S")
  assert error['code'] == 1408
  assert 'duration (2.0 seconds)' in error['message']
  assert 'maxRequestDuration (1.0 seconds)' in error['message']

  error, _ = validate(maxRequestDuration="PT1S")

  assert error is None


if __name__ == "__main__":
  test_start_stop_error()
