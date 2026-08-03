import os
import json
import logging

logger = logging.getLogger(__name__)


def config(config_input=None, config_dir=None, resolve_functions=True):
  """Read, resolve, and return the hapiserver config as a dict.

  Accepts:
    - a file path string
    - a dict with a 'config' key (as returned by hapiserver.cli())
    - a fully-formed config dict (passed through after env expansion)

  ENV variables defined in the config are set as OS environment variables
  and all $VAR references throughout the config are expanded before returning.
  """

  logger.debug(f"Called with: {config_input!r}")

  if not isinstance(config_input, (str, dict)):
    _exit_error(f"Invalid config input type: {type(config_input).__name__}. Expected str or dict.")


  if isinstance(config_input, dict):
    if 'app' in config_input:
      logger.debug("Input is a dict with 'app' key. Calling config() on config_input['app'] and returning input with 'app' key set to result.")
      config_input['app'] = config(config_input['app'])
      return config_input
    else:
      logger.debug("Config input is a dict without 'app' key; treating as app config")


  if isinstance(config_input, str):
    logger.debug(f"Reading config from file path: {config_input}")
    config_path = os.path.abspath(config_input)
    if not os.path.exists(config_path):
      _exit_error(f"Config file not found: {config_path}")

    logger.debug(f"Reading: {config_path}")
    try:
      with open(config_path) as f:
        cfg = json.load(f)
    except Exception as e:
      _exit_error(f"Failed to read config file '{config_path}': {e}")

    return config(cfg, config_dir=os.path.dirname(config_path), resolve_functions=resolve_functions)


  _set_env(config_input)
  _resolve_env(config_input)
  _resolve_scripts(config_input, config_dir=config_dir)

  if resolve_functions:
    _resolve_functions(config_input)
  else:
    _check_functions(config_input)

  logger.debug(f"config resolved: {config_input!r}")

  _check_config(config_input)

  return config_input


def _exit_error(message):
  import sys

  logger.error(message)
  print(f"Error: {message}", file=sys.stderr)
  print("Exiting with code 1", file=sys.stderr)
  exit(1)


def _resolve_scripts(cfg, config_dir=None):
  import shlex

  for script_name, script in cfg.get("scripts", {}).items():
    script_path, script_args = _split_script(script)
    script_path = os.path.expanduser(script_path)
    if config_dir is not None:
      if not os.path.isabs(script_path):
        logger.debug(f"Resolving script path {script_path}")
        emsg = f". Relative path in config '{script_path}' is resolved relative to config file directory: '{config_dir}'"
        script_path = os.path.join(config_dir, script_path)
        logger.debug(f"Resolved script path {script_path}")
    else:
      if not os.path.isabs(script_path):
        logger.debug(f"Resolving script path {script_path}")
        emsg = f". Relative path in config '{script_path}' is resolved relative to current working directory: '{os.getcwd()}'"
        script_path = os.path.join(os.getcwd(), script_path)
        logger.debug(f"Resolved script path {script_path}")
    if not os.path.exists(script_path):
      msg = f"Script file for endpoint /{script_name} not found: '{script_path}'{emsg}"
      _exit_error(msg)
    cfg['scripts'][script_name] = ' '.join(
      shlex.quote(part) for part in [script_path, *script_args]
    )


def _split_script(script):
  import shlex
  parts = shlex.split(script)
  if not parts:
    return '', []
  return parts[0], parts[1:]


def _import_function(key, value):
  """Parse a dotted string and import it as a callable.

  Returns (callable, error_message). On success error_message is None;
  on failure callable is None and error_message describes the problem.
  """
  import importlib

  if not isinstance(value, str):
    return None, f"functions.{key}: expected str or callable, got {type(value).__name__}"
  last_dot = value.rfind('.')
  if last_dot == -1:
    return None, f"functions.{key}: '{value}' has no '.' separator"
  module_path = value[:last_dot]
  func_name = value[last_dot + 1:]
  try:
    module = importlib.import_module(module_path)
    attr = getattr(module, func_name)
    if not callable(attr):
      return None, f"functions.{key}: '{value}' resolved but is not callable (got {type(attr).__name__})"
    return attr, None
  except Exception as e:
    return None, f"functions.{key}: failed to resolve '{value}': {e}"


def _check_functions(cfg):
  """Validate that all string function references in cfg['functions'] can be resolved.

  Raises ValueError listing all unresolvable entries. Does not modify cfg.
  """
  errors = []
  for key, value in cfg.get('functions', {}).items():
    if callable(value) or not isinstance(value, str):
      continue
    _, error = _import_function(key, value)
    if error:
      errors.append(error)
    else:
      logger.debug(f"Validated that functions.{key} resolves.")

  if errors:
    raise ValueError("Unresolvable function references:\n" + "\n".join(errors))


def _resolve_functions(cfg):
  """Resolve string function references in cfg['functions'] to callables.

  String format: 'module.path.function_name' (standard Python dotted path).
  Skips values that are already callable.
  Raises ValueError listing all entries that cannot be resolved.
  """
  errors = []
  for key, value in cfg.get('functions', {}).items():
    if callable(value):
      continue
    attr, error = _import_function(key, value)
    if error:
      errors.append(error)
      continue
    cfg['functions'][key] = attr
    logger.debug(f"Resolved functions.{key} -> {value}")

  if errors:
    raise ValueError("Unresolvable function references:\n" + "\n".join(errors))


def _set_env(config):
  import os
  for name, value in config.get("ENV", {}).items():
    os.environ[name] = str(value)
    logger.debug(f"Environment variable set: {name}={value}")


def _resolve_env(config):
  import os
  import re

  regex =r'\$(?:{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)}|(?P<plain>[A-Za-z_][A-Za-z0-9_]*))'
  env_pattern = re.compile(regex)

  def _replace_env(obj):
    if isinstance(obj, dict):
      for k, v in obj.items():
        obj[k] = _replace_env(v)
      return obj
    if isinstance(obj, list):
      return [_replace_env(v) for v in obj]
    if isinstance(obj, tuple):
      return tuple(_replace_env(v) for v in obj)
    if isinstance(obj, str):
      def repl(m):
        name = m.group('braced') or m.group('plain')
        return os.environ.get(name, '')
      return env_pattern.sub(repl, obj)
    return obj

  logger.debug('Expanding ENV variables in configuration file')
  try:
    _replace_env(config)
  except Exception as e:
    logger.warning(f"Environment variable substitution failed: {e}")


def _check_config(config):
  import os

  logger.debug('Checking configuration file')

  endpoints = ['catalog', 'info', 'data']
  for endpoint in endpoints:
    has_script = endpoint in config.get('scripts', {})
    has_function = endpoint in config.get('functions', {})
    if not has_script and not has_function:
      logger.warning(f"No script or function configured for endpoint '/{endpoint}'.")

  if "about" not in config:
    _exit_error("Configuration missing 'about' section.")

  fname = config.get("index.html", None)
  if fname is not None:
    fname = os.path.expanduser(fname)
    config['index.html'] = os.path.expanduser(config['index.html'])
    if not os.path.exists(config['index.html']):
      _exit_error(f"index.html file not found: '{fname}'")

  for endpoint in config.get("scripts", {}):
    if 'functions' in config and endpoint in config['functions']:
      _exit_error(f"Both script and function defined for /{endpoint} in config.")

