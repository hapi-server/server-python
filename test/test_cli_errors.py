import pathlib
import subprocess


def test_cli_errors():
  configs = {
    "not_found.json": "Config file not found",
    "invalid_json.json": "Error: Failed to read config file",
    "no_about.json": "Error: Configuration missing 'about' section.",
    "invalid_script_filename.json": "Error: Script file for endpoint /catalog not found:",
    "invalid_env_path.json": " Script file for endpoint /catalog not found",
  }

  for config, expected in configs.items():
    config_path = pathlib.Path(__file__).parent / "configs" / "invalid" / config
    cmd = ["hapiserver", "--config", str(config_path), "--debug"]
    print(f"Testing: {' '.join(cmd)}")
    print(f"  Expecting message containing: '{expected}'")
    try:
      result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
      output = result.stdout + result.stderr
    except subprocess.TimeoutExpired as e:
      output = (e.stdout or '') + (e.stderr or '')
    emsg = f"String '{expected}' not found in output of command '{' '.join(cmd)}':\n{output}"
    assert expected in output, emsg


if __name__ == "__main__":
  test_cli_errors()