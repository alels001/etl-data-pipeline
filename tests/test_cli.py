import subprocess
import sys


def test_cli_imports_and_shows_help():
    """Runs in a fresh process: catches code that breaks at import time,
    e.g. Spark objects created before a session exists."""
    result = subprocess.run(
        [sys.executable, "-m", "off_pipeline", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--steps" in result.stdout
