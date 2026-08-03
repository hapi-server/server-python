# Shared helper for cloning/updating the server-python-demo repo used by
# test_app.py and test_demo.py.

DEMO_REPO = "https://github.com/hapi-server/server-python-demo.git"


def prep_demo_repo():
  import pathlib
  import subprocess

  tmp_dir = pathlib.Path(__file__).parent.parent / "tmp"
  tmp_dir.mkdir(exist_ok=True)
  demo_dir = tmp_dir / "server-python-demo"

  if (demo_dir / ".git").exists():
    cmd = ["git", "-C", str(demo_dir), "pull", "--quiet"]
  else:
    cmd = ["git", "clone", "--quiet", DEMO_REPO, str(demo_dir)]
  subprocess.run(cmd, check=True)

  return demo_dir
