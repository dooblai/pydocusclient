"""Installable pytest adapter for the Test Recorder HTTP API."""
from datetime import datetime, timezone
import argparse
import getpass
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import urllib.request
from uuid import UUID, uuid4
import warnings
import pytest


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


DEFAULT_TIMEOUT = 60.0
MAX_BATCH_EVENTS = 100
MAX_BATCH_BYTES = 1_000_000


def request_timeout(value):
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Recorder timeout must be a positive number of seconds")
    return timeout


def chunk_events(events, max_events=MAX_BATCH_EVENTS, max_bytes=MAX_BATCH_BYTES):
    """Keep ordered requests small enough for predictable delivery time."""
    chunks, current, size = [], [], 20
    for event in events:
        event_size = len(json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) + 1
        if current and (len(current) >= max_events or size + event_size > max_bytes):
            chunks.append({"events": current})
            current, size = [], 20
        current.append(event)
        size += event_size
    if current:
        chunks.append({"events": current})
    return chunks


def write_spool(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)


def post_batch(url, token, payload, timeout=DEFAULT_TIMEOUT):
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url.rstrip("/") + "/v1/batches", data=body,
                                     headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def pytest_addoption(parser):
    group = parser.getgroup("test-recorder")
    group.addoption("--recorder-url", default=None, help="Test Recorder service URL")
    group.addoption("--recorder-repo", default=None, help="Canonical repository ID")
    group.addoption("--recorder-session-id", default=None, help="UUID for this pytest session")
    group.addoption("--recorder-spool", default=None, help="Directory for unsent result batches")
    group.addoption("--recorder-timeout", type=float, default=None, help="HTTP timeout in seconds (default 60)")


def item_identity(item, root, repository):
    try:
        path = Path(str(item.path)).resolve().relative_to(root).as_posix()
    except ValueError:
        warnings.warn("Recorder skipped test outside repository: " + item.nodeid)
        return None
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
    return {"repository": repository, "path": path,
            "namespace": "::".join(parts[1:-1]), "name": name, "variant": variant}


def nodeid_identity(nodeid, root, repository):
    """Recover an executed test if every xdist worker loses its metadata."""
    parts = nodeid.split("::")
    try:
        path = Path(parts[0]).resolve().relative_to(root).as_posix()
    except ValueError:
        return None
    display = parts[-1]
    name, separator, variant = display.partition("[")
    if not separator:
        name, variant = display, ""
    elif variant.endswith("]"):
        variant = variant[:-1]
    return {"repository": repository, "path": path,
            "namespace": "::".join(parts[1:-1]), "name": name, "variant": variant}



def pytest_configure(config):
    config.addinivalue_line("markers", "record_id(value): stable case name for recorder identity")
    url = config.getoption("--recorder-url") or os.getenv("TEST_RECORDER_URL")
    if url:
        if hasattr(config, "workerinput"):
            config.pluginmanager.register(WorkerMetadataPlugin(config), "test-recorder-worker-metadata")
        else:
            config.pluginmanager.register(RecorderPlugin(config, url), "test-recorder-client-active")


class WorkerMetadataPlugin:
    """Workers share collection metadata but never send or spool results."""
    def __init__(self, config):
        self.config = config
        self.root = Path(str(config.rootpath)).resolve()
        self.repository = repository_id(self.root, config.getoption("--recorder-repo"))

    def pytest_collection_finish(self, session):
        self.config.workeroutput["docus_items"] = {
            item.nodeid: test for item in session.items
            if (test := item_identity(item, self.root, self.repository)) is not None
        }


class RecorderPlugin:
    def __init__(self, config, url):
        self.root = Path(str(config.rootpath)).resolve()
        self.repository = repository_id(self.root, config.getoption("--recorder-repo"))
        self.url = url
        self.token = os.getenv("TEST_RECORDER_API_TOKEN")
        configured_timeout = config.getoption("--recorder-timeout")
        self.timeout = request_timeout(configured_timeout if configured_timeout is not None
                                       else os.getenv("TEST_RECORDER_TIMEOUT", DEFAULT_TIMEOUT))
        self.distributed = bool(getattr(config.option, "numprocesses", 0))
        self.spool = Path(config.getoption("--recorder-spool") or
                          os.getenv("TEST_RECORDER_SPOOL_DIR") or
                          self.root / ".test-recorder" / "spool")
        session_id = config.getoption("--recorder-session-id") or os.getenv("TEST_RECORDER_SESSION_ID")
        self.sid = str(UUID(session_id)) if session_id else str(uuid4())
        self.items = {}
        self.reports = {}
        self.completed = []
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
        if self.distributed:
            return
        for item in session.items:
            test = item_identity(item, self.root, self.repository)
            if test is None:
                continue
            self.items[item.nodeid] = test
            self.events.append({"type": "observe", "test": test,
                                "commit": self.events[0]["commit"], "seen_at": timestamp()})

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node, error):
        for nodeid, test in getattr(node, "workeroutput", {}).get("docus_items", {}).items():
            previous = self.items.get(nodeid)
            if previous is not None and previous != test:
                warnings.warn("Recorder saw conflicting xdist identity for " + nodeid)
            else:
                self.items[nodeid] = test

    def pytest_runtest_logreport(self, report):
        key = (getattr(report, "worker_id", None), report.nodeid)
        self.reports.setdefault(key, []).append(report)
        if report.when == "teardown":
            reports = self.reports.pop(key)
            if self.distributed:
                self.completed.append((report.nodeid, reports))
            else:
                self._record(report.nodeid, reports)

    def _record(self, nodeid, reports):
        if not reports:
            return
        test = self.items.get(nodeid)
        if test is None and getattr(self, "distributed", False):
            test = nodeid_identity(nodeid, self.root, self.repository)
            if test is not None:
                warnings.warn("Recorder recovered xdist identity from node ID; record_id marker may be unavailable: " + nodeid)
                self.items[nodeid] = test
                self.events.append({"type": "observe", "test": test,
                                    "commit": self.events[0]["commit"], "seen_at": timestamp()})
        if test is None:
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
            fragments = []
            for report in reports:
                sections = getattr(report, "sections", None)
                if sections is None:
                    captured = getattr(report, field, "")
                    if captured:
                        fragments.append(captured)
                else:
                    fragments.extend(content for heading, content in sections
                                     if heading == "Captured " + key + " " + report.when)
            value = "".join(fragments)
            output[key] = value[-65536:] if value else None
            output[key + "_truncated"] = len(value) > 65536

        trace = chr(10).join(report.longreprtext for report in failures
                             if getattr(report, "longreprtext", ""))
        starts = [report.start for report in reports if getattr(report, "start", None) is not None]
        stops = [report.stop for report in reports if getattr(report, "stop", None) is not None]
        started_at = at(min(starts)) if starts else timestamp()
        finished_at = at(max(stops)) if stops else started_at
        self.events.append({
            "type": "result", "id": str(uuid4()), "test": test, "sid": self.sid,
            "status": status, "started_at": started_at,
            "finished_at": finished_at,
            "nodeid": nodeid, "phases": [
                {"phase": report.when, "outcome": report.outcome, "duration_s": getattr(report, "duration", 0.0)}
                for report in reports],
            "duration_s": sum(getattr(report, "duration", 0.0) for report in reports),
            "failure": trace[-65536:] if trace else None,
            "failure_truncated": len(trace) > 65536,
            **output,
        })

    def pytest_sessionfinish(self, session, exitstatus):
        distributed = getattr(self, "distributed", False)
        if distributed:
            for test in self.items.values():
                self.events.append({"type": "observe", "test": test,
                                    "commit": self.events[0]["commit"], "seen_at": timestamp()})
        for key, reports in list(self.reports.items()):
            nodeid = key[1] if isinstance(key, tuple) else key
            if distributed:
                self.completed.append((nodeid, reports))
            else:
                self._record(nodeid, reports)
        self.reports.clear()
        if distributed:
            for nodeid, reports in self.completed:
                self._record(nodeid, reports)
            self.completed.clear()
        collected = max(len(self.items), getattr(session, "testscollected", 0)) if distributed else len(self.items)
        self.events.append({"type": "session_finish", "sid": self.sid,
                            "finished_at": timestamp(), "exit_code": int(exitstatus),
                            "collected": collected})
        chunks = chunk_events(self.events)
        for index, payload in enumerate(chunks):
            try:
                post_batch(self.url, self.token, payload, timeout=getattr(self, "timeout", DEFAULT_TIMEOUT))
            except Exception as error:
                self.error = str(error)
                try:
                    target = self.spool / (self.sid + "-" + str(uuid4()) + ".json")
                    write_spool(target, {"format": "docus-spool-v2", "session_id": self.sid,
                                         "uncertain_first_chunk": True, "chunks": chunks[index:]})
                    warnings.warn("Test Recorder delivery unconfirmed; saved " + str(target) +
                                  ": " + self.error)
                except OSError as spool_error:
                    warnings.warn("Test Recorder delivery and spooling failed: " +
                                  str(error) + "; " + str(spool_error))
                break

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_line("Test Recorder session: " + self.sid)
        if self.error:
            terminalreporter.write_line("Test Recorder delivery failed: " + self.error)


def resend_main(argv=None):
    parser = argparse.ArgumentParser(description="Resend saved Test Recorder batches")
    parser.add_argument("--url", default=os.getenv("TEST_RECORDER_URL"))
    parser.add_argument("--spool", default=os.getenv("TEST_RECORDER_SPOOL_DIR", ".test-recorder/spool"))
    parser.add_argument("--timeout", type=float, default=os.getenv("TEST_RECORDER_TIMEOUT", DEFAULT_TIMEOUT))
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("Set --url or TEST_RECORDER_URL")
    timeout = request_timeout(args.timeout)
    for path in sorted(Path(args.spool).glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        chunks = payload["chunks"] if "chunks" in payload else chunk_events(payload["events"])
        for index, chunk in enumerate(chunks):
            post_batch(args.url, os.getenv("TEST_RECORDER_API_TOKEN"), chunk, timeout=timeout)
            if index + 1 < len(chunks):
                write_spool(path, {"format": "docus-spool-v2",
                                   "session_id": payload.get("session_id"),
                                   "uncertain_first_chunk": False, "chunks": chunks[index + 1:]})
        path.unlink()
        print("Sent " + str(path))


if __name__ == "__main__":
    resend_main()
