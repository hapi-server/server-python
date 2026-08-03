# Usage:
#   python test_resolve_script_with_arguments.py

def test_resolve_script_with_arguments():
  import pathlib
  import tempfile

  from hapiserver.config import _resolve_scripts, _split_script
  from hapiserver.call import _script_command

  with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = pathlib.Path(tmp_dir)
    bin_dir = tmp_path / "bin files"
    bin_dir.mkdir()
    script_path = bin_dir / "catalog.py"
    script_path.touch()
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    cfg = {"scripts": {"catalog": "'../bin files/catalog.py' {depth}"}}

    _resolve_scripts(cfg, config_dir=str(config_dir))

    resolved_path, script_args = _split_script(cfg['scripts']['catalog'])
    assert pathlib.Path(resolved_path).resolve() == script_path.resolve()
    assert script_args == ['{depth}']

    cmd_path, cmd = _script_command(cfg['scripts']['catalog'], {})
    assert cmd_path == resolved_path
    assert cmd == []

    cmd_path, cmd = _script_command(cfg['scripts']['catalog'], {'depth': 'all'}
    )
    assert cmd_path == resolved_path
    assert cmd == ['all']

    cmd_path, cmd = _script_command("catalog.py", {'depth': 'all'})
    assert cmd_path == "catalog.py"
    assert cmd == []

    data_script = "data.py --dataset={dataset} --parameters={parameters} "
    data_script += "--start={start} --stop={stop} --format={format} --config={config}"


    data_query = {
        'dataset': 'demo1',
        'parameters': '',
        'start': '1970-01-01T00:00:00.000000Z',
        'stop': '1970-01-01T00:00:01.000000Z'
    }
    _, cmd = _script_command(data_script, data_query)
    assert cmd == [
      '--dataset=demo1',
      '--parameters=',
      '--start=1970-01-01T00:00:00.000000Z',
      '--stop=1970-01-01T00:00:01.000000Z'
    ]


if __name__ == "__main__":
  test_resolve_script_with_arguments()
