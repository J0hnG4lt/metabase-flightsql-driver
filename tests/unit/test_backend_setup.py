"""Backend setup must not turn startup or query failures into success."""
import contextlib
import io
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import setup_influxdb3 as influx


def result(code=0, stdout="", stderr=""):
    return subprocess.CompletedProcess([], code, stdout, stderr)


class InfluxSetupTest(unittest.TestCase):
    def test_stopped_server_fails_without_attempting_token_creation(self):
        with patch.object(influx, "read_env", return_value={}), \
             patch.object(influx.time, "sleep"), \
             patch.object(influx, "podman_exec", return_value=result(1, stderr="container is not running")) as execute:
            with self.assertRaisesRegex(SystemExit, "did not become ready"):
                influx.main()
        self.assertTrue(all(call.args[1] == "show" for call in execute.call_args_list))

    def test_failed_verification_query_is_not_reported_ready(self):
        with patch.object(influx, "read_env", return_value={"INFLUXDB3_TOKEN": "test-token"}), \
             patch.object(influx, "podman_exec", side_effect=[result(), result(), result(1, stderr="query failed")]), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(SystemExit, "verification query failed"):
                influx.main()
        self.assertNotIn("influxdb3 ready", output.getvalue())


if __name__ == "__main__":
    unittest.main()
