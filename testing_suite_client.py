"""Standalone client checks; run with python testing_suite_client.py."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import warnings

import pytest_recorder_client as client


class ClientTests(unittest.TestCase):
    def test_report_mapping(self):
        adapter = client.RecorderPlugin.__new__(client.RecorderPlugin)
        adapter.sid = "remote-run"
        adapter.items = {"test_sample.py::test_bad": {
            "repository": "github.com/acme/widget", "path": "test_sample.py",
            "namespace": "", "name": "test_bad", "variant": ""}}
        adapter.events = []

        def report(phase, outcome, output="", xfail=None):
            return SimpleNamespace(
                when=phase, outcome=outcome, failed=outcome == "failed",
                skipped=outcome == "skipped", passed=outcome == "passed",
                wasxfail=xfail, start=1000.0, stop=1001.0, duration=1.0,
                capstdout=output, capstderr="", longreprtext="assert false")

        adapter._record("test_sample.py::test_bad",
                        [report("setup", "passed"), report("call", "failed", "oops"),
                         report("teardown", "passed")])
        self.assertEqual(adapter.events[0]["status"], "failed")
        self.assertEqual(adapter.events[0]["stdout"], "oops")
        adapter._record("test_sample.py::test_bad", [report("call", "passed", xfail="expected")])
        self.assertEqual(adapter.events[1]["status"], "xpassed")

    def test_delivery_failure_spools_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            adapter = client.RecorderPlugin.__new__(client.RecorderPlugin)
            adapter.sid = "offline-run"
            adapter.events = [{"type": "session_start", "id": "offline-run",
                               "repository": "github.com/acme/widget"}]
            adapter.reports = {}
            adapter.items = {}
            adapter.spool = Path(directory)
            adapter.url = "http://127.0.0.1:1"
            adapter.token = None
            adapter.error = None
            with patch.object(client, "post_batch", side_effect=OSError("offline")):
                with warnings.catch_warnings(record=True):
                    adapter.pytest_sessionfinish(None, 0)
            payload = json.loads((Path(directory) / "offline-run.json").read_text())
            self.assertEqual(payload["events"][-1]["type"], "session_finish")
            self.assertEqual(adapter.error, "offline")

    def test_release_metadata(self):
        try:
            import tomllib
        except ImportError:
            self.skipTest("tomllib requires Python 3.11")
        root = Path(__file__).resolve().parent
        metadata = tomllib.loads((root / "pyproject.toml").read_text())
        self.assertEqual(metadata["project"]["name"], "docus")
        self.assertEqual(metadata["project"]["entry-points"]["pytest11"]["pydocusclient"],
                         "pytest_recorder_client")
        self.assertTrue((root / metadata["project"]["readme"]).is_file())
        workflow = (root / ".github" / "workflows" / "release.yml").read_text()
        self.assertIn("id-token: write", workflow)
        self.assertIn("pypa/gh-action-pypi-publish@release/v1", workflow)
    def test_existing_pytest_suite_without_edits(self):
        try:
            import pytest
        except ImportError:
            self.skipTest("pytest is not installed")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_sample.py").write_text(chr(10).join([
                "import pytest",
                "def test_ok(): pass",
                "def test_bad(): print(42); assert False",
                "@pytest.mark.skip(reason='exercise skip')",
                "def test_skip(): pass",
                ""]))
            spool = root / "spool"
            env = dict(os.environ, TEST_RECORDER_URL="http://127.0.0.1:1",
                       TEST_RECORDER_REPO="github.com/acme/widget",
                       TEST_RECORDER_SPOOL_DIR=str(spool),
                       TEST_RECORDER_SESSION_ID="00000000-0000-4000-8000-000000000001")
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", str(root / "test_sample.py")],
                cwd=root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            files = list(spool.glob("*.json"))
            self.assertEqual(len(files), 1)
            events = json.loads(files[0].read_text())["events"]
            self.assertEqual(events[0]["id"], "00000000-0000-4000-8000-000000000001")
            statuses = {event["status"] for event in events if event["type"] == "result"}
            self.assertTrue(all(event["sid"] == events[0]["id"] for event in events if event["type"] == "result"))
            self.assertEqual(statuses, {"passed", "failed", "skipped"})
            failed = next(event for event in events if event.get("status") == "failed")
            self.assertIn("42", failed["stdout"])
            self.assertEqual(events[-1]["type"], "session_finish")


if __name__ == "__main__":
    unittest.main(verbosity=2)
