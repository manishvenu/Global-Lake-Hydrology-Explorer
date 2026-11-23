"""
Run CLAY for one lake in a background subprocess and watch its progress.

Why a subprocess? CLAY keeps its settings in module-level globals and can
run for a long time; running it in its own process keeps the web app
responsive and makes every run start from a clean slate.

Typical use (inside the Panel app)::

    job = ClayJob(on_update=lambda job: print(job.status, job.log[-1:]))
    job.start(67)          # returns immediately
    ...
    job.status             # "idle" | "running" | "done" | "failed"
    job.output_dir         # set when CLAY prints GLHE_OUTPUT_DIR=... and exits OK
    job.cancel()

Threading note: ``on_update`` is called from the background reader thread.
In a Panel app, hand the work back to the session, e.g.
``pn.state.execute(lambda: ...)`` or by setting params on the AppState from
a periodic callback, rather than touching widgets directly.
"""

from __future__ import annotations

import collections
import logging
import subprocess
import sys
import threading
from typing import Callable

from GLHE.LIME.run_clay import OUTPUT_MARKER

logger = logging.getLogger(__name__)

STATUSES = ("idle", "running", "done", "failed")


def default_command(hylak_id: int) -> list[str]:
    """The command used to run CLAY: this Python, unbuffered, run_clay module."""
    return [sys.executable, "-u", "-m", "GLHE.LIME.run_clay", str(int(hylak_id))]


class ClayJob:
    """
    One CLAY run at a time, in a subprocess, with a bounded line log.

    Parameters
    ----------
    command : callable, optional
        ``command(hylak_id) -> list[str]`` building the argv to run. Defaults
        to :func:`default_command`; tests pass a fast fake command.
    cwd : str, optional
        Working folder for the subprocess. CLAY writes to a folder relative
        to it ("LakeOutputDirectory").
    max_log_lines : int
        Older lines are dropped once the log is this long.
    on_update : callable, optional
        ``on_update(job)`` is called after every new log line and on every
        status change (from a background thread).
    max_seconds : float, optional
        Stop a run that takes longer than this (status becomes "failed").
        None (the default) means no limit.
    """

    def __init__(
        self,
        command: Callable[[int], list[str]] | None = None,
        cwd: str | None = None,
        max_log_lines: int = 2000,
        on_update: Callable[["ClayJob"], None] | None = None,
        max_seconds: float | None = None,
    ):
        self.command = command or default_command
        self.cwd = cwd
        self.on_update = on_update
        self.hylak_id: int | None = None
        self.status: str = "idle"
        self.output_dir: str | None = None
        self.returncode: int | None = None
        self._log: collections.deque[str] = collections.deque(maxlen=max_log_lines)
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._cancelled = False
        self.max_seconds = max_seconds
        self._timer: threading.Timer | None = None

    # ------------------------------------------------------------------ API
    @property
    def log(self) -> list[str]:
        """A copy of the captured stdout/stderr lines (newest last)."""
        with self._lock:
            return list(self._log)

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    def start(self, hylak_id: int) -> bool:
        """
        Start CLAY for ``hylak_id`` in the background.

        Returns False (and does nothing) if a run is already in progress.
        """
        with self._lock:
            if self.status == "running":
                return False
            self.hylak_id = int(hylak_id)
            self.status = "running"
            self.output_dir = None
            self.returncode = None
            self._cancelled = False
            self._log.clear()
        argv = self.command(self.hylak_id)
        self._append(f"$ {' '.join(argv)}")
        try:
            self._proc = subprocess.Popen(
                argv,
                cwd=self.cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # merge so lines stay in order
                stdin=subprocess.DEVNULL,
                text=True,
                # CLAY or a library may print bytes that are not valid UTF-8
                # (e.g. a cp1252 lake name); never let that crash the reader.
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as exc:
            self._append(f"Could not start CLAY: {exc}")
            self._set_status("failed")
            return True
        self._thread = threading.Thread(
            target=self._watch, name=f"ClayJob-{self.hylak_id}", daemon=True
        )
        self._thread.start()
        if self.max_seconds:
            self._timer = threading.Timer(self.max_seconds, self._time_limit_reached)
            self._timer.daemon = True
            self._timer.start()
        self._notify()
        return True

    def cancel(self) -> None:
        """Stop the running subprocess (status becomes "failed")."""
        proc = self._proc
        if proc is None or self.status != "running":
            return
        self._cancelled = True
        self._append("Cancelling...")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    def _time_limit_reached(self) -> None:
        """Timer thread: stop a run that went past ``max_seconds``."""
        if self.status == "running":
            minutes = self.max_seconds / 60
            self._append(f"Time limit reached ({minutes:g} min); stopping CLAY.")
            self.cancel()

    def wait(self, timeout: float | None = None) -> str:
        """Block until the current run finishes (handy in scripts/tests)."""
        if self._thread is not None:
            self._thread.join(timeout)
        return self.status

    # ------------------------------------------------------------ internals
    def _watch(self) -> None:
        """
        Background thread: read output line by line, then record the result.

        Everything is wrapped in try/finally so the job can never be left
        stuck in "running" (which would block Run CLAY for every page).
        """
        proc = self._proc
        final = "failed"
        try:
            assert proc is not None and proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line.startswith(OUTPUT_MARKER):
                    self.output_dir = line[len(OUTPUT_MARKER) :].strip()
                self._append(line)
            self.returncode = proc.wait()
            if self._cancelled:
                self._append("Cancelled.")
            elif self.returncode == 0:
                self._append("CLAY finished.")
                final = "done"
            else:
                self._append(f"CLAY failed (exit code {self.returncode}).")
        except Exception as exc:  # e.g. a decoding error in the reader
            logger.exception("ClayJob reader thread failed")
            self._append(f"LIME stopped following CLAY: {exc!r}")
            if proc is not None and proc.poll() is None:
                proc.kill()
                try:
                    proc.wait(timeout=10)
                except Exception:
                    pass
            if proc is not None:
                self.returncode = proc.returncode
        finally:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._set_status(final)

    def _append(self, line: str) -> None:
        with self._lock:
            self._log.append(line)
        self._notify()

    def _set_status(self, status: str) -> None:
        self.status = status
        self._notify()

    def _notify(self) -> None:
        if self.on_update is None:
            return
        try:
            self.on_update(self)
        except Exception:  # a broken UI callback must not kill the reader thread
            logger.exception("ClayJob on_update callback failed")
