# Usage:
#   pytest test_app.py
#   python test_app.py

import socket
import pathlib
import subprocess


def _demo_dir():
  from util.prep_demo_repo import prep_demo_repo
  sibling = pathlib.Path(__file__).resolve().parents[2] / "server-python-demo"
  if sibling.is_dir():
    return sibling
  return prep_demo_repo()


def _run_script(server):
  script = pathlib.Path(__file__).parent / "test_app.sh"
  demo_dir = _demo_dir()
  with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
  cmd = ["bash", str(script), server, str(demo_dir), str(port)]
  print(f"Testing: {' '.join(cmd)}")
  result = subprocess.run(cmd, cwd=script.parent, timeout=120)
  assert result.returncode == 0, f"test_app.sh {server} failed (exit code {result.returncode})"


def test_app_uvicorn():
  _run_script("uvicorn")


def test_app_gunicorn():
  _run_script("gunicorn")


if __name__ == "__main__":
  test_app_uvicorn()
  test_app_gunicorn()
