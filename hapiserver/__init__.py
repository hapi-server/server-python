__version__ = "0.0.1"

__all__ = [
  "app",
  "call",
  "cli",
  "config",
  "endpoints",
  "error",
  "exec",
  "get",
  "openapi",
  "util"
]

from hapiserver import endpoints
from hapiserver import openapi
from hapiserver import util
from hapiserver.app import app
from hapiserver.call import call
from hapiserver.cli import cli
from hapiserver.config import config
from hapiserver.error import error
from hapiserver.exec import exec

import logging
# Attach a dedicated StreamHandler to the 'hapiserver' logger and disable
# propagation to root. This is necessary because uvicorn calls
# logging.config.dictConfig() on startup which resets the root logger —
# removing any root handlers. Without a dedicated handler here, all
# hapiserver.* log output would be silently dropped after uvicorn starts.
_fmt = '%(name)s.%(funcName)s() %(levelname)s: %(message)s'
_hapiserver_logger = logging.getLogger('hapiserver')
if not _hapiserver_logger.handlers:
  _handler = logging.StreamHandler()
  _handler.setFormatter(logging.Formatter(_fmt))
  _hapiserver_logger.addHandler(_handler)
  _hapiserver_logger.propagate = False
  _hapiserver_logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

__version__ = "0.0.1"
HAPI_VERSION = "3.3"


def _log_start(configs):
  at = f"http://{configs['server']['--host']}:{configs['server']['--port']}/hapi"
  logger.info(f"Starting HAPI server using Uvicorn at {at}")


def run(configs):
  start(configs)


def start(configs, wait=None):
  import hapiserver
  import utilrsw.uvicorn

  logger.info(f"configs: {configs}")

  # Check config but don't resolve functions so config can be serialized.
  logger.info("Checking config to ensure server will start.")
  hapiserver.config(configs['app'], resolve_functions=False)

  _log_start(configs)

  if wait is None:
    # Run in this thread and block until shutdown.
    utilrsw.uvicorn.run('hapiserver.app', configs)
  else:
    return utilrsw.uvicorn.start('hapiserver.app', configs, wait)


def stop(process):
  import utilrsw.uvicorn
  utilrsw.uvicorn.stop(process)


def run_cli():
  """
  Entry point for hapiserver script (set by [project.scripts] > hapiserver
  in pyproject.toml)
  """

  import hapiserver

  configs = hapiserver.cli()

  # configs is a dict with keys 'server' and 'app', where 'server' contains
  # args for uvicorn and 'app' contains args for the ASGI app.

  logger.info(f"configs: {configs}")

  run(configs)