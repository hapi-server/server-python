import json
import logging

import hapiserver

logger = logging.getLogger(__name__)


def call(endpoint, query, config):

  args = {}
  if endpoint == 'catalog' and 'depth' in query:
    args = {"depth": query['depth']}

  if endpoint == 'info':
    args = {"dataset": query['dataset']}

  if endpoint == 'data':
    args = {
      'dataset': query['dataset'],
      'parameters': query.get('parameters', ''),
      'start': query['start_normalized'],
      'stop': query['stop_normalized']
    }
    if 'format' in query:
      args['format'] = query['format']

  if 'scripts' in config and endpoint in config['scripts']:
    return _call_script(endpoint, query, args, config)

  if 'functions' in config and endpoint in config['functions']:
    return _call_function(endpoint, args, config)

  return None, {
    "code": 1500,
    "message": f"No script or function configured for endpoint '{endpoint}'"
  }


def _call_script(endpoint, query, args, config):
  script_vals = {**query, **args}
  script, script_args = _script_command(config['scripts'][endpoint], script_vals)

  if len(script_args) > 0:
    data, error = hapiserver.exec(script, args=script_args)
  else:
    data, error = hapiserver.exec(script)
  if error:
    message = "Endpoint script returned error"
    error = {
      "code": 1500,
      "message": message,
      "message_console": message,
      "exception": error
    }
    return None, error

  if endpoint == 'data':
    # For /data, the script is expected to return binary or CSV, so no
    # JSON parsing is needed.
    return data, None

  try:
    data = json.loads(data)
  except Exception as e:
    message = f"Error parsing JSON returned by script: '{data}'"
    error = {
      "code": 1500,
      "message": message,
      "exception": e
    }
    return None, error

  return data, None


def _script_command(script, query):
  from hapiserver.config import _split_script

  script_path, configured_args = _split_script(script)
  script_args = []
  for arg in configured_args:
    try:
      script_args.append(arg.format(**query))
    except KeyError:
      script_args.append('')

  while script_args and script_args[-1] == '':
    script_args.pop()

  return script_path, script_args


def _call_function(endpoint, args, config):
  func = config['functions'][endpoint]
  logger.debug(f"Calling {func}({args})")
  try:
    import inspect

    func_params = inspect.signature(func).parameters
    args = [str(args[x]) for x in args.keys()]
    config_kwarg = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in func_params.values())
    if len(args) > 0:
      if 'config' in func_params or config_kwarg:
        data = func(*args, config=config)
      else:
        data = func(*args)
    else:
      if 'config' in func_params or config_kwarg:
        data = func(config=config)
      else:
        data = func()
  except Exception as e:
    message = f"Error executing {endpoint} function"
    error = {
      "code": 1500,
      "message": message,
      "message_console": message,
      "exception": e
    }
    return None, error

  return data, None
