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
        setup = report("setup", "passed")
        call = report("call", "passed")
        teardown = report("teardown", "passed")
        setup.sections = [("Captured stderr setup", "alpha\n")]
        call.sections = setup.sections + [("Captured stderr call", "beta\n")]
        teardown.sections = call.sections + [("Captured stderr teardown", "gamma\n")]
        setup.capstderr = "alpha\n"
        call.capstderr = "alpha\nbeta\n"
        teardown.capstderr = "alpha\nbeta\ngamma\n"
        adapter._record("test_sample.py::test_bad", [setup, call, teardown])
        self.assertEqual(adapter.events[2]["stderr"], "alpha\nbeta\ngamma\n")


    def test_xdist_crash_report_without_worker_metadata(self):
        adapter = client.RecorderPlugin.__new__(client.RecorderPlugin)
        adapter.root = Path.cwd()
        adapter.repository = "github.com/acme/widget"
        adapter.distributed = True
        adapter.items = {}
        adapter.sid = "crashed-run"
        adapter.events = [{"type": "session_start", "commit": None}]
        report = SimpleNamespace(when="???", outcome="failed", failed=True,
                                 skipped=False, passed=False, sections=[],
                                 longreprtext="worker crashed")
        with warnings.catch_warnings(record=True):
            adapter._record("tests/test_crash.py::test_case", [report])
        self.assertEqual(adapter.events[-1]["status"], "error")
        self.assertEqual(adapter.events[-1]["test"]["name"], "test_case")
        self.assertEqual(adapter.events[-1]["duration_s"], 0.0)


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
            files = list(Path(directory).glob("offline-run-*.json"))
            self.assertEqual(len(files), 1)
            payload = json.loads(files[0].read_text())
            self.assertEqual(payload["chunks"][-1]["events"][-1]["type"], "session_finish")
            self.assertTrue(payload["uncertain_first_chunk"])
            self.assertEqual(adapter.error, "offline")

    def test_xdist_controller_owns_one_complete_session(self):
        try:
            import pytest
            import xdist
        except ImportError as error:
            self.skipTest(str(error))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_parallel.py").write_text(chr(10).join([
                "import pytest",
                "@pytest.mark.parametrize(" + chr(39) + "n" + chr(39) + ", range(45))",
                "def test_many(n): assert n >= 0",
                "@pytest.mark.record_id(" + chr(39) + "stable_case" + chr(39) + ")",
                "def test_renamed(): pass",
                "@pytest.mark.skip(reason=" + chr(39) + "skip" + chr(39) + ")",
                "def test_skip(): pass",
                "@pytest.mark.xfail(reason=" + chr(39) + "expected" + chr(39) + ")",
                "def test_xfail(): assert False",
                "def test_fail(): assert False",
                "def test_plain(): pass",
                ""]))
            def run(mode):
                spool = root / ("spool-" + mode)
                sid = "00000000-0000-4000-8000-" + ("000000000001" if mode == "serial" else "000000000002")
                env = dict(os.environ, TEST_RECORDER_URL="http://127.0.0.1:1",
                           TEST_RECORDER_REPO="github.com/acme/widget",
                           TEST_RECORDER_SPOOL_DIR=str(spool), TEST_RECORDER_SESSION_ID=sid)
                command = [sys.executable, "-m", "pytest", "-q", "--tb=no", str(root / "test_parallel.py")]
                if mode == "xdist":
                    command.extend(["-n", "3"])
                result = subprocess.run(command, cwd=root, env=env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                files = list(spool.glob("*.json"))
                self.assertEqual(len(files), 1, result.stdout + result.stderr)
                pending = json.loads(files[0].read_text())
                return [event for chunk in pending["chunks"] for event in chunk["events"]]
            serial = run("serial")
            parallel = run("xdist")
            for events in (serial, parallel):
                self.assertEqual(sum(event["type"] == "session_start" for event in events), 1)
                self.assertEqual(sum(event["type"] == "session_finish" for event in events), 1)
                self.assertEqual(sum(event["type"] == "observe" for event in events), 50)
                results = [event for event in events if event["type"] == "result"]
                self.assertEqual(len(results), 50)
                self.assertEqual(len({event["nodeid"] for event in results}), 50)
                self.assertEqual(events[-1]["collected"], 50)
                self.assertEqual(events[-1]["exit_code"], 1)
                self.assertEqual({event["status"] for event in results},
                                 {"passed", "failed", "skipped", "xfailed"})
            serial_tests = {event["nodeid"]: event["test"] for event in serial if event["type"] == "result"}
            parallel_tests = {event["nodeid"]: event["test"] for event in parallel if event["type"] == "result"}
            self.assertEqual(serial_tests, parallel_tests)
            self.assertEqual(parallel_tests["test_parallel.py::test_renamed"]["name"], "stable_case")


    def test_large_delivery_chunks_and_resumable_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def adapter(sid, spool):
                recorder = client.RecorderPlugin.__new__(client.RecorderPlugin)
                recorder.sid = sid
                recorder.items = {str(i): {} for i in range(500)}
                recorder.reports = {}
                recorder.completed = []
                recorder.distributed = False
                recorder.events = [{"type": "session_start", "id": sid}] + [
                    {"type": "result", "id": str(i), "stdout": "x" * 60000}
                    for i in range(500)]
                recorder.spool = spool
                recorder.url = "http://127.0.0.1:1"
                recorder.token = None
                recorder.timeout = 0.1
                recorder.error = None
                return recorder
            delivered = set()
            calls = []
            def accept(url, token, payload, timeout):
                calls.append(payload)
                self.assertLessEqual(len(payload["events"]), client.MAX_BATCH_EVENTS)
                self.assertLessEqual(len(json.dumps(payload).encode()), client.MAX_BATCH_BYTES)
                delivered.update(event["id"] for event in payload["events"]
                                 if event["type"] == "result")
            success = adapter("success", root / "success-spool")
            with patch.object(client, "post_batch", side_effect=accept):
                success.pytest_sessionfinish(None, 0)
            self.assertEqual(len(delivered), 500)
            self.assertGreater(len(calls), 1)
            self.assertFalse(list(success.spool.glob("*.json")))
            delivered.clear()
            calls.clear()
            pending = adapter("timeout", root / "timeout-spool")
            def timeout_after_store(url, token, payload, timeout):
                accept(url, token, payload, timeout)
                if len(calls) == 2:
                    raise TimeoutError("response delayed after storage")
            with patch.object(client, "post_batch", side_effect=timeout_after_store):
                with warnings.catch_warnings(record=True):
                    pending.pytest_sessionfinish(None, 0)
            self.assertEqual(len(list(pending.spool.glob("*.json"))), 1)
            with patch.object(client, "post_batch", side_effect=accept):
                client.resend_main(["--url", pending.url, "--spool", str(pending.spool)])
            self.assertEqual(len(delivered), 500)
            self.assertFalse(list(pending.spool.glob("*.json")))


    def test_legacy_spool_is_replayed_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old-session.json"
            events = [{"type": "session_start", "id": "old-session"},
                      {"type": "result", "id": "old-result"},
                      {"type": "session_finish", "sid": "old-session"}]
            path.write_text(json.dumps({"events": events}))
            sent = []
            def accept(url, token, payload, timeout):
                sent.extend(payload["events"])
            with patch.object(client, "post_batch", side_effect=accept):
                client.resend_main(["--url", "http://127.0.0.1:1", "--spool", directory])
            self.assertEqual(sent, events)
            self.assertFalse(path.exists())


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
            events = [event for chunk in json.loads(files[0].read_text())["chunks"] for event in chunk["events"]]
            self.assertEqual(events[0]["id"], "00000000-0000-4000-8000-000000000001")
            statuses = {event["status"] for event in events if event["type"] == "result"}
            self.assertTrue(all(event["sid"] == events[0]["id"] for event in events if event["type"] == "result"))
            self.assertEqual(statuses, {"passed", "failed", "skipped"})
            failed = next(event for event in events if event.get("status") == "failed")
            self.assertIn("42", failed["stdout"])
            self.assertEqual(events[-1]["type"], "session_finish")


if __name__ == "__main__":
    unittest.main(verbosity=2)
