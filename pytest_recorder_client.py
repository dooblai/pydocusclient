"""Installable pytest adapter for the Test Recorder HTTP API."""
from datetime import datetime, timezone
import argparse
import getpass
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import urllib.request
from uuid import uuid4
import warnings


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def repository_id(root, override=None):
    value = override or os.getenv("TEST_RECORDER_REPO")
    if value:
        return value
    if os.getenv("GITHUB_REPOSITORY"):
        return "github.com/" + os.environ["GITHUB_REPOSITORY"]
    try:
        value = subprocess.check_output(["git", "-C", str(root), "remote", "get-url", "origin"],
                                        text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        raise ValueError("Set TEST_RECORDER_REPO when Git origin is unavailable")
    value = value.replace("git@", "", 1).replace(":", "/", 1) if value.startswith("git@") else value
    value = re.sub("^https?://", "", value)
    return value.removesuffix(".git").rstrip("/")


def git_info(root):
    info = {}
    for key, args in (("commit", ["rev-parse", "HEAD"]), ("branch", ["branch", "--show-current"])):
        try:
            info[key] = subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                                stderr=subprocess.DEVNULL).strip() or None
        except (OSError, subprocess.CalledProcessError):
            info[key] = None
    try:
        info["dirty"] = bool(subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"],
                                                     text=True, stderr=subprocess.DEVNULL).strip())
    except (OSError, subprocess.CalledProcessError):
        info["dirty"] = None
    return info


def post_batch(url, token, payload):
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url.rstrip("/") + "/v1/batches", data=body,
                                     headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def pytest_addoption(parser):
    group = parser.getgroup("test-recorder")
    group.addoption("--recorder-url", default=None, help="Test Recorder service URL")
    group.addoption("--recorder-repo", default=None, help="Canonical repository ID")
    group.addoption("--recorder-spool", default=None, help="Directory for unsent result batches")


def pytest_configure(config):
    config.addinivalue_line("markers", "record_id(value): stable case name for recorder identity")
    url = config.getoption("--recorder-url") or os.getenv("TEST_RECORDER_URL")
    if url:
        config.pluginmanager.register(RecorderPlugin(config, url), "test-recorder-client-active")


class RecorderPlugin:
    def __init__(self, config, url):
        self.root = Path(str(config.rootpath)).resolve()
        self.repository = repository_id(self.root, config.getoption("--recorder-repo"))
        self.url = url
        self.token = os.getenv("TEST_RECORDER_API_TOKEN")
        self.spool = Path(config.getoption("--recorder-spool") or
                          os.getenv("TEST_RECORDER_SPOOL_DIR") or
                          self.root / ".test-recorder" / "spool")
        self.sid = str(uuid4())
        self.items = {}
        self.reports = {}
        self.error = None
        git = git_info(self.root)
        self.events = [{
            "type": "session_start", "id": self.sid, "repository": self.repository,
            "actor": os.getenv("TEST_RECORDER_ACTOR") or getpass.getuser(),
            "commit": git["commit"], "started_at": timestamp(), "git": git,
            "host": socket.gethostname(),
            "framework": {"name": "pytest", "version": __import__("pytest").__version__},
            "ci": {"run_id": os.getenv("GITHUB_RUN_ID")} if os.getenv("GITHUB_RUN_ID") else None,
        }]

    def pytest_collection_finish(self, session):
        for item in session.items:
            try:
                path = Path(str(item.path)).resolve().relative_to(self.root).as_posix()
            except ValueError:
                warnings.warn("Recorder skipped test outside repository: " + item.nodeid)
                continue
            parts = item.nodeid.split("::")
            display = parts[-1]
            name, separator, variant = display.partition("[")
            if not separator:
                name, variant = display, ""
            elif variant.endswith("]"):
                variant = variant[:-1]
            marker = item.get_closest_marker("record_id")
            if marker and marker.args:
                name = str(marker.args[0])
            test = {"repository": self.repository, "path": path,
                    "namespace": "::".join(parts[1:-1]), "name": name, "variant": variant}
            self.items[item.nodeid] = test
            self.events.append({"type": "observe", "test": test,
                                "commit": self.events[0]["commit"], "seen_at": timestamp()})

    def pytest_runtest_logreport(self, report):
        self.reports.setdefault(report.nodeid, []).append(report)
        if report.when == "teardown":
            self._record(report.nodeid, self.reports.pop(report.nodeid))

    def _record(self, nodeid, reports):
        test = self.items.get(nodeid)
        if test is None or not reports:
            return
        failures = [report for report in reports if report.failed]
        skips = [report for report in reports if report.skipped]
        if failures:
            status = "xpassed" if any(getattr(report, "wasxfail", None) for report in failures) else (
                "failed" if any(report.when == "call" for report in failures) else "error")
        elif any(getattr(report, "wasxfail", None) for report in reports if report.passed):
            status = "xpassed"
        elif skips:
            status = "xfailed" if any(getattr(report, "wasxfail", None) for report in skips) else "skipped"
        else:
            status = "passed"
        at = lambda seconds: datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="microseconds")
        output = {}
        for key, field in (("stdout", "capstdout"), ("stderr", "capstderr")):
            value = chr(10).join(getattr(report, field, "") for report in reports
                                 if getattr(report, field, ""))
            output[key] = value[-65536:] if value else None
            output[key + "_truncated"] = len(value) > 65536
        trace = chr(10).join(report.longreprtext for report in failures
                             if getattr(report, "longreprtext", ""))
        self.events.append({
            "type": "result", "id": str(uuid4()), "test": test, "sid": self.sid,
            "status": status, "started_at": at(min(report.start for report in reports)),
            "finished_at": at(max(report.stop for report in reports)),
            "nodeid": nodeid, "phases": [
                {"phase": report.when, "outcome": report.outcome, "duration_s": report.duration}
                for report in reports],
            "duration_s": sum(report.duration for report in reports),
            "failure": trace[-65536:] if trace else None,
            "failure_truncated": len(trace) > 65536,
            **output,
        })

    def pytest_sessionfinish(self, session, exitstatus):
        for nodeid, reports in list(self.reports.items()):
            self._record(nodeid, reports)
        self.reports.clear()
        self.events.append({"type": "session_finish", "sid": self.sid,
                            "finished_at": timestamp(), "exit_code": int(exitstatus),
                            "collected": len(self.items)})
        payload = {"events": self.events}
        try:
            post_batch(self.url, self.token, payload)
        except Exception as error:
            self.error = str(error)
            try:
                self.spool.mkdir(parents=True, exist_ok=True)
                target = self.spool / (self.sid + ".json")
                target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                warnings.warn("Test Recorder delivery failed; saved " + str(target) +
                              ": " + self.error)
            except OSError as spool_error:
                warnings.warn("Test Recorder delivery and spooling failed: " +
                              str(error) + "; " + str(spool_error))

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_line("Test Recorder session: " + self.sid)
        if self.error:
            terminalreporter.write_line("Test Recorder delivery failed: " + self.error)


def resend_main(argv=None):
    parser = argparse.ArgumentParser(description="Resend saved Test Recorder batches")
    parser.add_argument("--url", default=os.getenv("TEST_RECORDER_URL"), required=False)
    parser.add_argument("--spool", default=os.getenv("TEST_RECORDER_SPOOL_DIR", ".test-recorder/spool"))
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("Set --url or TEST_RECORDER_URL")
    for path in sorted(Path(args.spool).glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        post_batch(args.url, os.getenv("TEST_RECORDER_API_TOKEN"), payload)
        path.unlink()
        print("Sent " + str(path))


if __name__ == "__main__":
    resend_main()
