# docus

An installable pytest plugin that sends test sessions and results to a Docus
test recorder service. It uses the pytest11 entry point, so existing test
files do not need to change.

## Install and run

```sh
pip install docus
TEST_RECORDER_URL=https://recorder.example.com pytest
```

The plugin stays inactive until `TEST_RECORDER_URL` or `--recorder-url` is
provided. Optionally set `TEST_RECORDER_API_TOKEN` to a JWT containing a
nonempty `sub` claim. The service records that claim as the session and result
`user`; it does not verify signatures or expiration yet. Set
`TEST_RECORDER_REPO` to the canonical Git repository identifier;
otherwise the plugin uses `GITHUB_REPOSITORY` or Git origin. The optional
`TEST_RECORDER_ACTOR` overrides the current user.

Each pytest invocation has a UUID session ID. The client generates one unless
`TEST_RECORDER_SESSION_ID` or `--recorder-session-id` supplies a UUID. Use a
fresh UUID for each distinct run; spooled retries keep the original ID. The
service lists sessions by project and shows their individual results.

The plugin records collected tests, test results, pytest phase durations,
captured output, Git commit and branch, and a session ID. It sends a batch
at session end. If delivery fails, pytest results are unchanged and the batch
is saved under `.test-recorder/spool`. Retry with:

```sh
TEST_RECORDER_URL=https://recorder.example.com pydocus-resend
```

To keep a test identity across a pytest function rename, mark it with
`@pytest.mark.record_id("logical-name")`. Parameter IDs should remain stable.
Output and failure fields are capped at 64 KiB each; the batch indicates when
truncation occurred.

The Docus service is developed at
[dooblai/docus](https://github.com/dooblai/docus). This package only depends
on pytest and the Python standard library. Other languages can send the same
JSON event format directly to the service.

## Build and release to PyPI

1. Install the package and run `python testing_suite_client.py`.
2. Run `python -m pip install build twine`, `python -m build`, and
   `python -m twine check dist/*` from this repository.
3. Create a GitHub environment named `pypi` in `dooblai/pydocusclient`.
4. In PyPI, configure a **pending trusted publisher** for project name
   `docus`: owner `dooblai`, repository `pydocusclient`,
   workflow filename `release.yml`, environment `pypi`. This must match
   `.github/workflows/release.yml` exactly.
5. Push a version tag such as `v0.1.3`, then publish a GitHub Release for
   that tag. The release workflow builds a wheel and source distribution and
   publishes them using GitHub OIDC. No long lived PyPI API token is needed.
6. Verify with `pip install docus` in a fresh environment.

PyPI does not reserve a name when a pending publisher is configured. Check
that `docus` is available immediately before the first release.
For later releases, update the version in `pyproject.toml` and use a new
matching GitHub tag. Choose and add a license before public distribution if
you intend to grant reuse rights.

See the [PyPI trusted publishing guide](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [Python packaging guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
for the current publishing steps.
