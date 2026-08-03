# Usage:
#   python test_config_errors.py
#
# Covers config-loading error/edge paths in hapiserver.config.config()
# that are not reachable through the CLI-level tests in test_cli_errors.py
# (which only exercise the file-path input branch).

import contextlib
import io

import pytest

ABOUT = {"id": "Demo", "title": "Demo", "contact": ""}


def _stderr_of(func, *args, **kwargs):
  buf = io.StringIO()
  with contextlib.redirect_stderr(buf):
    with pytest.raises(SystemExit):
      func(*args, **kwargs)
  return buf.getvalue()


def test_invalid_config_input_type():
  from hapiserver.config import config

  output = _stderr_of(config, 123)
  assert "Invalid config input type" in output


def test_config_dict_with_app_key():
  from hapiserver.config import config

  wrapped = {"config": "unused", "app": {"about": ABOUT}}
  result = config(wrapped)
  assert result['app']['about'] == ABOUT


def test_script_and_function_both_defined():
  from hapiserver.config import config

  cfg = {
    "scripts": {"catalog": __file__},
    "functions": {"catalog": lambda: None},
    "about": ABOUT,
  }
  output = _stderr_of(config, cfg)
  assert "Both script and function defined for /catalog in config." in output


def test_index_html_not_found():
  from hapiserver.config import config

  cfg = {
    "functions": {"catalog": lambda: None, "info": lambda: None, "data": lambda: None},
    "about": ABOUT,
    "index.html": "/nonexistent/path/index.html",
  }
  output = _stderr_of(config, cfg)
  assert "index.html file not found" in output


def test_unresolvable_function_reference():
  from hapiserver.config import config

  cases = {
    "novalidname": "has no '.' separator",
    "os.path": "resolved but is not callable",
    "totally.nonexistent.module.func": "failed to resolve",
  }

  for value, expected in cases.items():
    cfg = {"functions": {"catalog": value}, "about": ABOUT}
    with pytest.raises(ValueError, match=expected):
      config(cfg)


if __name__ == "__main__":
  test_invalid_config_input_type()
  test_config_dict_with_app_key()
  test_script_and_function_both_defined()
  test_index_html_not_found()
  test_unresolvable_function_reference()
