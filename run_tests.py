import pytest
from pytest import ExitCode

if pytest.main(["-Wignore","--no-print-logs"]) != ExitCode.OK:
  raise Exception("Test failures")