"""
Run CLAY for one lake in a background process and keep its output.

    job = ClayJob()
    job.start(67)      # returns at once; False if a run is already going
    job.status         # "idle", "running", "done" or "failed"
    job.log            # CLAY's output lines so far
    job.output_dir     # the lake's folder, once CLAY is done
    job.cancel()

CLAY runs in its own process because it keeps its settings in module globals
and can take a long time; this way the web app stays responsive.
"""

import subprocess
import sys
import threading

from GLHE.LIME.run_clay import OUTPUT_MARKER


def default_command(hylak_id: int) -> list[str]:
    """``python -u -m GLHE.LIME.run_clay <id>`` (-u: print lines as they come)."""
    return [sys.executable, "-u", "-m", "GLHE.LIME.run_clay", str(hylak_id)]


class ClayJob:
    def __init__(self, command=default_command, max_seconds=None):
        self.command = command  # hylak_id -> command line; tests pass a fake CLAY
        self.max_seconds = max_seconds  # stop longer runs (None = no limit)
        self.status = "idle"
        self.hylak_id = None
        self.output_dir = None
        self.log = []
        self.process = None

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    def start(self, hylak_id: int) -> bool:
        if self.is_running:
            return False
        self.hylak_id, self.output_dir, self.log = int(hylak_id), None, []
        try:
            self.process = subprocess.Popen(
                self.command(self.hylak_id),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # one stream, lines in order
                stdin=subprocess.DEVNULL,
                text=True,
                errors="replace",  # never crash on odd characters
            )
        except OSError as exc:
            self.log.append(f"Could not start CLAY: {exc}")
            self.status = "failed"
            return True
        self.status = "running"
        threading.Thread(target=self._follow, daemon=True).start()
        return True

    def cancel(self) -> None:
        if self.is_running:
            self.log.append("Cancelling...")
            self.process.kill()

    def _follow(self) -> None:
        """Background thread: collect CLAY's output, then record how it ended."""
        code = None
        timer = threading.Timer(self.max_seconds or 0, self._too_long)
        timer.daemon = True  # never keeps the app from exiting
        if self.max_seconds:
            timer.start()
        try:
            for line in self.process.stdout:
                line = line.rstrip("\n")
                if line.startswith(OUTPUT_MARKER):
                    self.output_dir = line[len(OUTPUT_MARKER) :].strip()
                self.log.append(line)
            code = self.process.wait()
            self.log.append(f"CLAY ended (exit code {code}).")
        finally:
            timer.cancel()
            # Always leave "running", or no one could start CLAY again.
            self.status = "done" if code == 0 else "failed"

    def _too_long(self) -> None:
        self.log.append(f"Time limit reached ({self.max_seconds / 60:g} min).")
        self.cancel()
