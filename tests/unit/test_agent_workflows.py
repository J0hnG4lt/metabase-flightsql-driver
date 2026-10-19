"""Behavioral regressions for the local workflow helpers. No services needed."""
import contextlib
import io
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import check_agent_assets
import dev
import metabase_cli


class LocalWorkflowsTest(unittest.TestCase):
    def test_failed_build_does_not_restart_metabase(self):
        calls = []

        def run(command, **_kwargs):
            calls.append(command)
            if "run" in command:
                raise subprocess.CalledProcessError(1, command)

        with patch.object(dev.shutil, "which", return_value="podman-compose"), \
             patch.object(dev, "run", side_effect=run), patch.object(dev, "wait_ready") as health:
            with self.assertRaises(subprocess.CalledProcessError):
                dev.main(["rebuild"])
            health.assert_not_called()
        self.assertFalse(any("up" in call for call in calls))
        self.assertFalse(any("down" in call for call in calls))

    def test_normal_test_preserves_volumes_and_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = root / ".env"
            credentials.write_text("METABASE_API_KEY=preserve-me\n", encoding="utf-8")
            with patch.object(dev, "ROOT", root), patch.object(dev, "run") as run, \
                 patch.object(dev, "wait_ready"), patch.object(dev.shutil, "which", return_value="podman-compose"):
                dev.main(["test", "--pytest", "tests/e2e/test_core.py", "-v"])
            self.assertEqual(credentials.read_text(), "METABASE_API_KEY=preserve-me\n")
            self.assertTrue(any("pytest" in call.args[0] for call in run.call_args_list))
            self.assertFalse(any("down" in call.args[0] for call in run.call_args_list))

    def test_reset_needs_project_and_explicit_data_deletion(self):
        for args in (["reset"], ["reset", "--delete-data"], ["reset", "-p", "demo"]):
            with contextlib.redirect_stderr(io.StringIO()), patch.object(dev, "run") as run:
                with self.assertRaises(SystemExit):
                    dev.main(args)
                run.assert_not_called()

    def test_health_timeout_is_a_failure(self):
        with patch.object(dev.time, "monotonic", side_effect=[0, 1]), \
             patch.object(dev.urllib.request, "urlopen") as request:
            with self.assertRaisesRegex(RuntimeError, "did not become healthy"):
                dev.wait_ready("http://localhost:3001", timeout=0)
            request.assert_not_called()

    def test_env_reader_does_not_evaluate_shell(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text('# comment\nMETABASE_API_KEY="a=b"\nPAYLOAD=$(not-executed)\n', encoding="utf-8")
            self.assertEqual(dev.read_env(path), {"METABASE_API_KEY": "a=b", "PAYLOAD": "$(not-executed)"})

    def test_cli_uses_explicit_demo_pair_not_ambient_profile_credentials(self):
        env = metabase_cli.cli_environment(
            {"METABASE_URL": "http://localhost:3001", "METABASE_API_KEY": "test-key",
             "MB_URL": "https://other.example", "MB_API_KEY": "other-key"},
            {"METABASE_API_KEY": "old-key"})
        self.assertEqual((env["MB_URL"], env["MB_API_KEY"]), ("http://localhost:3001", "test-key"))


class AssetCheckTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for directory in (".claude", ".agents", ".codex", ".github", "docs", "tests"):
            shutil.copytree(ROOT / directory, self.root / directory,
                            ignore=shutil.ignore_patterns("*.png", "__pycache__"))
        for name in ("AGENTS.md", "CLAUDE.md", "docker-compose.yaml", "docker-compose.compat.yaml"):
            shutil.copyfile(ROOT / name, self.root / name)
        shutil.copyfile(ROOT / ".mcp.json", self.root / ".mcp.json")

    def test_new_claude_command_without_codex_mapping_is_rejected(self):
        (self.root / ".claude/commands/new-work.md").write_text("new workflow", encoding="utf-8")
        self.assertTrue(any("unmapped" in error for error in check_agent_assets.check(self.root)))

    def test_missing_codex_adapter_is_rejected_and_repaired(self):
        path = self.root / ".agents/skills/e2e-test/SKILL.md"
        path.unlink()
        self.assertTrue(any("stale/missing" in error for error in check_agent_assets.check(self.root)))
        check_agent_assets.check(self.root, write=True)
        self.assertTrue(path.is_file())

    def test_changed_release_pin_is_rejected(self):
        path = self.root / ".github/workflows/release-please.yml"
        path.write_text(path.read_text().replace("v0.64.2.8", "v0.63.1.6"), encoding="utf-8")
        self.assertTrue(any("pins differ" in error for error in check_agent_assets.check(self.root)))


if __name__ == "__main__":
    unittest.main()
