
def cli(config=None):
  import logging
  import argparse

  # Define the text for the header
  description = """
  HAPI Server
  -----------
  Example usage:
    hapiserver --config CONFIG_FILE.json

  Pass additional Uvicorn arguments as needed
    hapiserver --config CONFIG_FILE.json [Uvicorn options]
  For Uvicorn options, see:
    python -m uvicorn --help
  """

  import utilrsw.uvicorn

  # Get default uvicorn command-line args
  clargs_uvicorn = utilrsw.uvicorn.cli()


  config_help = "Path to JSON configuration file. Relative paths in configuration "
  config_help += "file are interpreted as relative to the directory "
  config_help += "hapiserver.py is executed from."

  clargs = {
    "config": {
      "help": config_help,
      "type": str,
      "required": True
    },
    **clargs_uvicorn,
    "debug": {
      "help": "Verbose logging.",
      "action": "store_true",
      "default": False
    },
    "log-level": {
      "help": "Set the logging level. Overrides debug flag if set.",
      "type": str,
      "default": None
    }
  }

  if config is not None:
    # Test functions call cli() with a path to a config file, so set that
    # as the default and make --config not required in that case.
    clargs['config']['default'] = str(config)
    clargs['config']['required'] = False

  parser_kwargs = {
    "description": description,
    "formatter_class": argparse.RawDescriptionHelpFormatter
  }

  parser = argparse.ArgumentParser()
  parser = argparse.ArgumentParser(**parser_kwargs)

  for k, v in clargs.items():
    parser.add_argument(f'--{k}', **v)

  args, _ = parser.parse_known_args()
  if args.debug:
    logging.getLogger('hapiserver').setLevel(logging.DEBUG)
  if args.log_level:
    logging.getLogger('hapiserver').setLevel(args.log_level.upper())

  logger = logging.getLogger(__name__)
  logger.debug(f"Command line arguments: {args}")

  # Split args into dict wil keys 'server' and 'app', where 'server' contains
  # args for uvicorn and 'app' contains args for the app.
  configs = utilrsw.uvicorn.cli(parser=parser)

  configs['app'] = configs['app']['config']

  # Store the effective hapiserver log level inside the app config so it is
  # written to APP_CONFIG and available to the uvicorn worker process.
  if isinstance(configs['app'], dict):
    configs['app']['log_level'] = logging.getLogger('hapiserver').level

  return configs

def cl_call(func):

  import json
  import inspect
  import pathlib
  import argparse
  import textwrap

  def config(args):
    config = None
    config_path = args.config
    if not config_path:
      # No --config given. Look for a config.json alongside the script
      # that defines func.
      script_dir = pathlib.Path(inspect.getfile(func)).resolve().parent
      candidate = script_dir / "config.json"
      if candidate.exists():
        config_path = str(candidate)
    if config_path:
      with open(config_path) as file:
        config = json.load(file)
      config['config_path'] = config_path
    return config

  def add_common_args(parser):
    parser.add_argument(
      '--config',
      default=None,
      help="Path to JSON config file")

  # Get name of func
  func_name = func.__name__

  if func_name not in ['catalog', 'info', 'data']:
    raise ValueError(f"cl_call() only supports functions named 'catalog', 'info', or 'data'. Got: {func_name}")

  if func_name == 'catalog':
    epilog = """
      Examples:

        All datasets, no info:
          python catalog.py

        All datasets with depth (embeds info() for each dataset):
          python catalog.py --depth all
      """

    parser = argparse.ArgumentParser(
      epilog=textwrap.dedent(epilog),
      formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
      '--depth',
      default=None,
      help="Catalog depth. Use 'all' to embed info for each dataset.")
    add_common_args(parser)

    args = parser.parse_args()

    print(json.dumps(func(depth=args.depth, config=config(args)), indent=2))

  if func_name == 'info':
    epilog = """
      Examples:

        python info.py --dataset demo1
      """

    parser = argparse.ArgumentParser(
      epilog=textwrap.dedent(epilog),
      formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
      '--dataset',
      required=True,
      help="Dataset ID")
    add_common_args(parser)

    args = parser.parse_args()

    print(json.dumps(func(args.dataset, config=config(args)), indent=2))

  if func_name == 'data':
    import sys

    start_example = '1970-01-01T00:00:00.000000Z'
    stop_example = '1970-01-01T00:00:01.000000Z'
    start_stop = f"--start {start_example} --stop {stop_example}"

    examples = f"""
      Examples:

        All parameters (parameters = '' => all parameters):
          python data.py --dataset demo1 --parameters ''

        Only primary time parameter:
          python data.py --dataset demo1 --parameters Time {start_stop}

        One parameter (primary time parameter is always included):
          python data.py --dataset demo1 --parameters scalar {start_stop}

        Two parameters:
          python data.py --dataset demo1 --parameters Time,scalar {start_stop}
      """

    parser = argparse.ArgumentParser(
      epilog=textwrap.dedent(examples),
      formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
      '--dataset',
      required=True,
      help="Dataset ID")
    parser.add_argument(
      '--parameters',
      default='',
      help="Comma-separated list of parameters to return. If empty, return all parameters.")
    parser.add_argument(
      '--start',
      required=True,
      help="Start time in format '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ'")
    parser.add_argument(
      '--stop',
      required=True,
      help="Stop time in format '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ'")
    parser.add_argument(
      '--format',
      default='csv',
      help="Output format")
    add_common_args(parser)

    args = parser.parse_args()

    fargs = [args.dataset, args.parameters, args.start, args.stop]
    for chunk in func(*fargs, format=args.format, config=config(args)):
      sys.stdout.write(chunk)