"""Unit tests for hapiserver.exec module."""

import sys
import pathlib
import tempfile
import textwrap
import pytest

# Add parent directory to path to import hapiserver
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from hapiserver.exec import exec


# Test script paths
TEST_SCRIPTS_DIR = pathlib.Path(__file__).parent / "tmp" / "exec"

ENV = "#!/usr/bin/env python"
@pytest.fixture(scope="module", autouse=True)
def setup_test_scripts():
  """Create test scripts for execution tests."""
  TEST_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

  # Script that succeeds and prints to stdout
  success_script = TEST_SCRIPTS_DIR / "success.py"
  script = """
            import sys
            print("Success output")
            if len(sys.argv) > 1:
              print(f"Args: {' '.join(sys.argv[1:])}")
          """
  success_script.write_text(f"{ENV}\n" + textwrap.dedent(script))

  # Script that fails with non-zero exit
  fail_script = TEST_SCRIPTS_DIR / "fail.py"
  script = """
            import sys
            print("Output before failure")
            sys.exit(1)
          """
  fail_script.write_text(f"{ENV}\n" + textwrap.dedent(script))

  # Script with stderr output
  stderr_script = TEST_SCRIPTS_DIR / "stderr.py"
  script = """
            import sys
            print("stdout output")
            print("stderr output", file=sys.stderr)
          """
  stderr_script.write_text(f"{ENV}\n" + textwrap.dedent(script))

  # Script that outputs multiple chunks
  chunked_script = TEST_SCRIPTS_DIR / "chunked.py"
  script = """
            import time
            for i in range(5):
              print(f"Chunk {i}")
              time.sleep(0.1)
          """
  chunked_script.write_text(f"{ENV}\n" + textwrap.dedent(script))

  # Large output script
  large_script = TEST_SCRIPTS_DIR / "large.py"
  script = """
            for i in range(100):
              print(f"Line {i}: " + "x" * 1000)
          """
  large_script.write_text(f"{ENV}\n" + textwrap.dedent(script))

  yield

  # Cleanup (optional)
  # Can leave scripts for debugging or remove them
  for script_file in TEST_SCRIPTS_DIR.glob("*.py"):
    script_file.unlink()
  TEST_SCRIPTS_DIR.rmdir()


class TestExecNonStreaming:
  """Tests for non-streaming mode (_read)."""

  def test_script_not_found(self):
    """Test error when script does not exist."""
    result, error = exec("/nonexistent/script.py")

    assert result is None
    assert error is not None
    assert error["code"] == 1500
    assert "not found" in error["message"].lower()
    assert "/nonexistent/script.py" in error["message_console"]

  def test_successful_execution(self):
    """Test successful script execution."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    result, error = exec(script)

    assert error is None
    assert result is not None
    assert "Success output" in result

  def test_execution_with_string_args(self):
    """Test execution with space-separated string arguments."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    result, error = exec(script, args="arg1 arg2 arg3")

    assert error is None
    assert "Args: arg1 arg2 arg3" in result

  def test_execution_with_list_args(self):
    """Test execution with list of arguments."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    result, error = exec(script, args=["arg1", "arg2", "arg3"])

    assert error is None
    assert "Args: arg1 arg2 arg3" in result

  def test_script_failure(self):
    """Test handling of script that exits with non-zero code."""
    script = str(TEST_SCRIPTS_DIR / "fail.py")
    result, error = exec(script)

    assert result is None
    assert error is not None
    assert error["code"] == 1500
    assert "failed" in error["message"].lower()
    assert "exception" in error

  def test_script_with_stderr(self):
    """Test script that writes to stderr."""
    script = str(TEST_SCRIPTS_DIR / "stderr.py")
    result, error = exec(script)

    # stderr is not included in result (but may be visible if in logs).
    assert error is None
    assert "stdout output" in result


class TestExecStreaming:
  """Tests for streaming mode (_stream)."""

  def test_streaming_successful_execution(self):
    """Test successful script execution in streaming mode."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    stream_gen, error = exec(script, stream={})

    assert error is None
    assert callable(stream_gen)

    # Collect all output
    output = "".join(stream_gen())
    assert "Success output" in output

  def test_streaming_with_args(self):
    """Test streaming execution with arguments."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    stream_gen, error = exec(script, args="test arg", stream={})

    assert error is None
    output = "".join(stream_gen())
    assert "Args: test arg" in output

  def test_streaming_with_chunk_size(self):
    """Test streaming with custom chunk size."""
    script = str(TEST_SCRIPTS_DIR / "large.py")
    stream_gen, error = exec(script, stream={"chunk_size": 1000})

    assert error is None

    chunks = list(stream_gen())
    assert len(chunks) > 1  # Should be multiple chunks

    # Verify all output is present
    full_output = "".join(chunks)
    assert "Line 0:" in full_output
    assert "Line 99:" in full_output

  def test_streaming_line_mode(self):
    """Test streaming in line-by-line mode (chunk_size=0)."""
    script = str(TEST_SCRIPTS_DIR / "chunked.py")
    stream_gen, error = exec(script, stream={"chunk_size": 0})

    assert error is None

    lines = list(stream_gen())
    assert len(lines) >= 5
    assert any("Chunk 0" in line for line in lines)
    assert any("Chunk 4" in line for line in lines)


  def test_streaming_with_stderr(self):
    """Test streaming mode with stderr output."""
    script = str(TEST_SCRIPTS_DIR / "stderr.py")
    stream_gen, error = exec(script, stream={"stderr": False})

    assert error is None
    output = "".join(stream_gen())
    assert "stdout output" in output


  def test_streaming_with_stderr_enabled(self):
    """Test streaming mode with stderr streaming enabled."""
    script = str(TEST_SCRIPTS_DIR / "stderr.py")
    stream_gen, error = exec(script, stream={"stderr": True})

    assert error is None
    output = "".join(stream_gen())
    assert "stdout output" in output
    # stderr is logged but not included in output

  def test_streaming_script_failure(self):
    """Test streaming mode with failing script."""
    script = str(TEST_SCRIPTS_DIR / "fail.py")
    stream_gen, error = exec(script, stream={})

    assert error is None  # Error doesn't occur until streaming starts

    output = list(stream_gen())
    print(output)
    # Should get output before failure and error message
    full_output = "".join(output)
    assert "Output before failure" in full_output
    # Last chunk should contain error message
    assert any("exited with code" in chunk for chunk in output)


class TestExecEdgeCases:
  """Tests for edge cases and special scenarios."""

  def test_empty_script_path(self):
    """Test with empty script path."""
    result, error = exec("")

    assert result is None
    assert error is not None
    assert error["code"] == 1500

  def test_script_with_spaces_in_path(self):
    """Test script with spaces in filename."""
    with tempfile.TemporaryDirectory() as tmpdir:
      script_path = pathlib.Path(tmpdir) / "test script.py"
      script_path.write_text("print('Hello from spaced script')")

      result, error = exec(str(script_path))

      assert error is None
      assert "Hello from spaced script" in result

  def test_empty_args_string(self):
    """Test with empty args string."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    result, error = exec(script, args="")

    assert error is None
    assert "Success output" in result

  def test_empty_args_list(self):
    """Test with empty args list."""
    script = str(TEST_SCRIPTS_DIR / "success.py")
    result, error = exec(script, args=[])

    assert error is None
    assert "Success output" in result


if __name__ == "__main__":
  # Run with pytest
  pytest.main([__file__, "-v"])
