# Usage:
#   pytest test_app.py
#   python test_app.py

import pathlib
import subprocess

from util.prep_demo_repo import prep_demo_repo


def _run_script(server):
  script = pathlib.Path(__file__).parent / "test_app.sh"
  demo_dir = prep_demo_repo()
  cmd = ["bash", str(script), server, str(demo_dir)]
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
